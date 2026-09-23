from django.core.management.base import BaseCommand
from etl.conexoes.postgres import get_postgres_connection
from etl.conexoes.mongo import get_mongo_database
from pymongo import UpdateOne
import logging
import time
import gc
import csv
from datetime import datetime, timedelta, date, time as dtime
from collections import Counter

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Migra dados do PostgreSQL para o MongoDB'

    def add_arguments(self, parser):
        parser.add_argument('--tabela', type=str, required=True, help='Tabela a ser migrada')
        parser.add_argument('--batch', type=int, default=20000, help='Tamanho do lote')
        parser.add_argument('--progress', type=int, default=100000, help='Progresso a cada N registros')
        parser.add_argument('--force', action='store_true', help='Força recriação da coleção')
        parser.add_argument('--test', type=int, default=0, help='Modo teste: processa apenas N registros')
        parser.add_argument('--verbose', action='store_true', help='Mostra detalhes por lote')
        parser.add_argument('--dump', action='store_true', help='Gera arquivo CSV com registros pulados')
        parser.add_argument('--gc-coletar', type=int, default=5, help='Liberar memória a cada N blocos (padrão: 5)')
        parser.add_argument('--filtro', type=str, default=None,
                            help='Filtro SQL WHERE (ex: "cd_sit_cadastro != 8")')

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _buscar_chaves_primarias(self, pg_cur, tabela):
        """Busca as chaves primárias da tabela no PostgreSQL"""
        try:
            pg_cur.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
                ORDER BY a.attnum
            """, (tabela,))
            return [row[0] for row in pg_cur.fetchall()] or None
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Erro ao buscar PK: {e}'))
            return None

    def _buscar_colunas_date(self, pg_cur, tabela):
        """Colunas com data_type='date' (sem hora) — precisam de cast p/ timestamp."""
        try:
            pg_cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                  AND data_type = 'date'
            """, (tabela,))
            return {row[0] for row in pg_cur.fetchall()}
        except Exception as e:
            self.stdout.write(self.style.WARNING(f'⚠️  Não foi possível detectar colunas date: {e}'))
            return set()

    def _gerar_id(self, doc, colunas_chave):
        """Gera um _id único baseado na PK"""
        campos_id = ['id_empresa', 'id_socio', 'id_estabelecimento', 'id_simples']
        for campo in campos_id:
            if campo in doc and doc[campo] is not None:
                return doc[campo]

        def normalizar(valor):
            if valor is None:
                return "NULL"
            return str(valor).strip().replace('\n', '').replace('\r', '').replace('\t', '')

        if len(colunas_chave) == 1:
            return normalizar(doc.get(colunas_chave[0]))
        return "|".join([normalizar(doc.get(col)) for col in colunas_chave])

    def _converter_valor(self, val):
        """Converte valores para tipos compatíveis com MongoDB."""
        import pandas as pd

        if val is None:
            return None

        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass

        # datetime.datetime primeiro (é subclasse de date!)
        if isinstance(val, datetime):
            return val
        if isinstance(val, pd.Timestamp):
            return val.to_pydatetime()
        # datetime.date "puro" -> vira datetime meia-noite
        if isinstance(val, date):
            return datetime.combine(val, dtime.min)

        if isinstance(val, (int, float)):
            return val

        return str(val)

    @staticmethod
    def _normalizar_doc(doc):
        """Rede de segurança: nenhum datetime.date sobrevive até o bulk_write."""
        for k, v in doc.items():
            if isinstance(v, date) and not isinstance(v, datetime):
                doc[k] = datetime.combine(v, dtime.min)
        return doc

    def _salvar_dump(self, registros, tabela, suffix=""):
        """Salva os registros pulados em um arquivo CSV"""
        if not registros:
            return None
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f'pulados_{tabela}_{timestamp}{suffix}.csv'
        with open(nome_arquivo, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['_id', 'motivo', 'data_registro']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(registros)
        return nome_arquivo

    # ------------------------------------------------------------------ #
    # Handle
    # ------------------------------------------------------------------ #
    def handle(self, *args, **options):
        import pandas as pd

        tabela = options['tabela']
        batch_size = options['batch']
        progress_interval = options['progress']
        force = options['force']
        test_limit = options['test']
        verbose = options['verbose']
        dump = options['dump']
        gc_coletar_a_cada = options['gc_coletar']
        filtro = options['filtro']

        inicio = time.time()
        self.stdout.write(f'🔄 Migrando {tabela} do PostgreSQL para MongoDB...')
        self.stdout.write('=' * 60)
        self.stdout.write(f'⏱️  Início: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')

        pg_conn = get_postgres_connection()
        pg_cur = pg_conn.cursor()

        self.stdout.write('🔍 Verificando chaves primárias...')
        pk_colunas = self._buscar_chaves_primarias(pg_cur, tabela)

        if not pk_colunas:
            self.stdout.write(self.style.ERROR('❌ Tabela não possui chave primária!'))
            pg_cur.close()
            pg_conn.close()
            return

        self.stdout.write(self.style.SUCCESS(f'   ✅ PK encontrada: {", ".join(pk_colunas)}'))

        mongo_db = get_mongo_database()
        colecao = mongo_db[tabela]

        if force:
            self.stdout.write(f'🗑️  Removendo coleção {tabela} (force=True)...')
            colecao.drop()
            self.stdout.write('   ✅ Coleção removida')

        # Colunas
        pg_cur.execute(f'SELECT * FROM {tabela} LIMIT 0')
        colunas = [desc[0] for desc in pg_cur.description]

        # Colunas DATE (precisam de cast para timestamp)
        colunas_date = self._buscar_colunas_date(pg_cur, tabela)
        if colunas_date:
            self.stdout.write(
                self.style.SUCCESS(
                    f'   ✅ {len(colunas_date)} coluna(s) DATE detectada(s): '
                    f'{", ".join(sorted(colunas_date))}'
                )
            )

        tem_id_artificial = any(
            c in colunas for c in ['id_empresa', 'id_socio', 'id_estabelecimento', 'id_simples']
        )
        if tem_id_artificial:
            self.stdout.write(self.style.SUCCESS(f'   ✅ ID artificial encontrado na tabela!'))

        # Contar registros (com filtro)
        if filtro:
            pg_cur.execute(f'SELECT COUNT(*) FROM {tabela} WHERE {filtro}')
        else:
            pg_cur.execute(f'SELECT COUNT(*) FROM {tabela}')
        total = pg_cur.fetchone()[0]

        if test_limit > 0 and test_limit < total:
            total = test_limit
            self.stdout.write(f'   🧪 Teste limitado a {total:,} registros')

        self.stdout.write(f'📊 Total de registros a migrar: {total:,}')
        if filtro:
            self.stdout.write(f'🔍 Filtro: {filtro}')

        if total == 0:
            self.stdout.write(self.style.WARNING('⚠️ Nenhum registro encontrado!'))
            pg_cur.close()
            pg_conn.close()
            return

        # ------------------------------------------------------------- #
        # Definir coluna de ordenação (também usada no keyset fallback)
        # ------------------------------------------------------------- #
        if 'id_estabelecimento' in colunas:
            ordem_coluna = 'id_estabelecimento'
        elif 'id_empresa' in colunas:
            ordem_coluna = 'id_empresa'
        elif 'id_socio' in colunas:
            ordem_coluna = 'id_socio'
        elif 'id_simples' in colunas:
            ordem_coluna = 'id_simples'
        else:
            ordem_coluna = pk_colunas[0] if len(pk_colunas) == 1 else pk_colunas[0]

        # ------------------------------------------------------------- #
        # Montar SELECT com cast para colunas DATE
        # ------------------------------------------------------------- #
        cols_sql = []
        for c in colunas:
            if c in colunas_date:
                cols_sql.append(f'{c}::timestamp AS {c}')
            else:
                cols_sql.append(c)

        select_base = f'SELECT {", ".join(cols_sql)} FROM {tabela}'
        if filtro:
            select_base += f' WHERE {filtro}'

        # ------------------------------------------------------------- #
        # Server-side cursor (named cursor) -> streaming real
        # ------------------------------------------------------------- #
        # Fechamos o cursor normal e abrimos um cursor nomeado dedicado.
        pg_cur.close()

        def abrir_cursor_nomeado(conn, order_by=None, where_extra=None):
            """Abre um cursor nomeado. Se where_extra for passado, usa keyset."""
            cur = conn.cursor(name=f'mig_{tabela}_{int(time.time() * 1000) % 1000000}')
            cur.itersize = batch_size

            sql = select_base
            if where_extra:
                if filtro:
                    sql += f' AND {where_extra}'
                else:
                    sql += f' WHERE {where_extra}'
            sql += f' ORDER BY {order_by or ordem_coluna}'
            cur.execute(sql)
            return cur

        pg_cur = abrir_cursor_nomeado(pg_conn)

        processados = 0
        inseridos = 0
        atualizados = 0
        existentes = 0
        erros = 0
        duplicados_encontrados = 0
        ultimo_progresso = 0
        chunk_count = 0

        registros_pulados = []
        ultimo_dump = 0

        total_blocos = (total + batch_size - 1) // batch_size
        ultimo_id_visto = None  # para keyset fallback

        self.stdout.write(f'📤 Transferindo em lotes de {batch_size:,} (server-side cursor)...')
        self.stdout.write(f'📈 Progresso e dump a cada: {progress_interval:,} registros')
        self.stdout.write(f'🧹 Liberando memória a cada {gc_coletar_a_cada} blocos')
        if dump:
            self.stdout.write(f'📝 Dump salvo automaticamente a cada {progress_interval:,} registros')
        self.stdout.write('')

        while True:
            if test_limit > 0 and processados >= test_limit:
                break

            # --------------------------------------------------------- #
            # Leitura do lote com fallback em caso de erro de conexão
            # --------------------------------------------------------- #
            try:
                registros = pg_cur.fetchmany(batch_size)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Erro na leitura do cursor: {e}'))
                self.stdout.write('   🔄 Reconectando via keyset pagination...')

                # Fecha cursor/conn problemáticos
                try:
                    pg_cur.close()
                except Exception:
                    pass
                try:
                    pg_conn.close()
                except Exception:
                    pass

                pg_conn = get_postgres_connection()

                # Keyset: WHERE ordem_coluna > ultimo_id_visto
                if ultimo_id_visto is None:
                    # Não temos posição — recomeça do zero (caso raro)
                    pg_cur = abrir_cursor_nomeado(pg_conn)
                else:
                    where_extra = f'{ordem_coluna} > %s'
                    cur = pg_conn.cursor(name=f'mig_{tabela}_{int(time.time() * 1000) % 1000000}')
                    cur.itersize = batch_size
                    sql = select_base
                    if filtro:
                        sql += f' AND {where_extra}'
                    else:
                        sql += f' WHERE {where_extra}'
                    sql += f' ORDER BY {ordem_coluna}'
                    cur.execute(sql, (ultimo_id_visto,))
                    pg_cur = cur

                try:
                    registros = pg_cur.fetchmany(batch_size)
                except Exception as e2:
                    self.stdout.write(self.style.ERROR(f'❌ Falha após reconexão: {e2}'))
                    break

            if not registros:
                break

            # --------------------------------------------------------- #
            # Converter para dicionários
            # --------------------------------------------------------- #
            docs = []
            for row in registros:
                doc = {}
                for i, col in enumerate(colunas):
                    doc[col] = self._converter_valor(row[i])

                # Guardar último id visto (para keyset fallback)
                ultimo_id_visto = doc.get(ordem_coluna, ultimo_id_visto)

                doc['_id'] = self._gerar_id(doc, pk_colunas)
                docs.append(doc)

            if docs:
                # Normalização final: nenhum datetime.date pode sobrar
                docs = [self._normalizar_doc(d) for d in docs]

                # Verificar duplicatas de _id no lote
                ids_validos = [doc['_id'] for doc in docs if doc['_id'] is not None]
                if len(ids_validos) != len(set(ids_validos)):
                    duplicados_encontrados += 1
                    if verbose:
                        self.stdout.write(self.style.WARNING('   ⚠️ DUPLICATAS DE _id NO LOTE!'))
                        contagem = Counter(ids_validos)
                        duplicados = [item for item, count in contagem.items() if count > 1]
                        self.stdout.write(f'   ⚠️ {len(duplicados)} _ids duplicados. Exemplo: {duplicados[:3]}')

                try:
                    operacoes = [
                        UpdateOne({'_id': doc['_id']}, {'$set': doc}, upsert=True)
                        for doc in docs
                    ]
                    resultado = colecao.bulk_write(operacoes, ordered=False)

                    total_lote = len(docs)
                    inseridos_lote = resultado.upserted_count or 0
                    atualizados_lote = resultado.modified_count or 0
                    existentes_lote = resultado.matched_count or 0
                    pulados_lote = existentes_lote - atualizados_lote

                    inseridos += inseridos_lote
                    atualizados += atualizados_lote
                    existentes += existentes_lote
                    processados += total_lote

                    if dump and pulados_lote > 0:
                        for doc in docs:
                            if colecao.find_one({'_id': doc['_id']}):
                                registros_pulados.append({
                                    '_id': doc['_id'],
                                    'motivo': 'JA_EXISTIA_IDENTICO',
                                    'data_registro': datetime.now().isoformat()
                                })

                    if verbose:
                        self.stdout.write(
                            f'   ℹ️  Lote: {total_lote} docs | '
                            f'✅ Inseridos: {inseridos_lote} | '
                            f'🔄 Atualizados: {atualizados_lote} | '
                            f'⏭️  Pulados: {pulados_lote}'
                        )

                except Exception as e:
                    erros += 1
                    self.stdout.write(self.style.ERROR(f'   ❌ Erro no bulk_write: {e}'))
                    # Fallback registro-a-registro
                    for doc in docs:
                        try:
                            colecao.update_one({'_id': doc['_id']}, {'$set': doc}, upsert=True)
                            processados += 1
                            inseridos += 1
                        except Exception as e2:
                            erros += 1
                            if verbose:
                                self.stdout.write(self.style.ERROR(f'      ❌ {doc["_id"]}: {e2}'))

            chunk_count += 1

            # Liberar memória
            if chunk_count % gc_coletar_a_cada == 0:
                gc.collect()
                if verbose:
                    self.stdout.write(f'   🧹 Memória liberada (bloco {chunk_count})')

            # --------------------------------------------------------- #
            # Progresso
            # --------------------------------------------------------- #
            if processados - ultimo_progresso >= progress_interval or processados >= total:
                tempo_decorrido = time.time() - inicio
                velocidade = processados / tempo_decorrido if tempo_decorrido > 0 else 0
                tempo_restante = (total - processados) / velocidade if velocidade > 0 else 0

                percentual = (processados / total) * 100 if total > 0 else 0
                tempo_str = str(timedelta(seconds=int(tempo_decorrido)))
                tempo_restante_str = str(timedelta(seconds=int(tempo_restante)))
                pulados_total = existentes - atualizados
                bloco_atual = (processados // batch_size) + 1

                self.stdout.write(
                    f'   📊 Bloco {bloco_atual}/{total_blocos} | {processados:,}/{total:,} ({percentual:.1f}%) | '
                    f'⏱️  {tempo_str} | '
                    f'🚀 {velocidade:,.0f} reg/s | '
                    f'✅ Inseridos: {inseridos:,} | '
                    f'🔄 Atualizados: {atualizados:,} | '
                    f'⏭️  Pulados: {pulados_total:,} | '
                    f'⏳ restante: {tempo_restante_str}'
                )

                if dump and registros_pulados and len(registros_pulados) > ultimo_dump:
                    nome_arquivo = self._salvar_dump(
                        registros_pulados, tabela, f"_progress_{processados}"
                    )
                    if nome_arquivo:
                        self.stdout.write(
                            self.style.SUCCESS(
                                f'      💾 Dump salvo: {nome_arquivo} ({len(registros_pulados):,} pulados)'
                            )
                        )
                    ultimo_dump = len(registros_pulados)

                ultimo_progresso = processados

        # ------------------------------------------------------------- #
        # Finalização
        # ------------------------------------------------------------- #
        try:
            pg_cur.close()
        except Exception:
            pass
        try:
            pg_conn.close()
        except Exception:
            pass

        total_mongo = colecao.count_documents({})
        tempo_total = time.time() - inicio
        pulados_total = existentes - atualizados

        if dump and registros_pulados:
            nome_arquivo = self._salvar_dump(registros_pulados, tabela, "_final")
            if nome_arquivo:
                self.stdout.write(self.style.SUCCESS(f'💾 Dump final salvo: {nome_arquivo}'))
                self.stdout.write(f'   Total de registros pulados: {len(registros_pulados):,}')

        self.stdout.write('=' * 60)
        self.stdout.write(self.style.SUCCESS('✅ Migração concluída!'))
        self.stdout.write(f'📊 Registros no PostgreSQL: {total:,}')
        self.stdout.write(f'📊 Registros no MongoDB: {total_mongo:,}')
        self.stdout.write(f'✅ Inseridos: {inseridos:,}')
        self.stdout.write(f'🔄 Atualizados: {atualizados:,}')
        self.stdout.write(f'⏭️  Pulados: {pulados_total:,}')
        self.stdout.write(f'❌ Erros: {erros:,}')
        if duplicados_encontrados > 0:
            self.stdout.write(self.style.WARNING(f'⚠️  Lotes com duplicatas de _id: {duplicados_encontrados}'))
        self.stdout.write(f'⏱️  Tempo total: {str(timedelta(seconds=int(tempo_total)))}')
        self.stdout.write(f'🚀 Velocidade média: {(total_mongo/tempo_total):,.0f} reg/s')
        self.stdout.write('=' * 60)