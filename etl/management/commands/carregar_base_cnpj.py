import glob
import hashlib
import os
import re
import time
from concurrent.futures import ProcessPoolExecutor

from django.core.management.base import BaseCommand, CommandError
from etl import estatisticas
from etl.conexoes.postgres import get_postgres_connection
from etl.models import Carga, EtapaCarga
from etl.processadores.cnpj_processor import CNPJProcessor, PADROES_ARQUIVO_TABELA


def carregar_arquivo(tarefa):
    """Worker: carrega um arquivo inteiro numa tabela (roda em processo separado)"""
    from etl.processadores.controle_arquivos import ControleArquivos

    arquivo, tabela, batch, rotulo = tarefa
    nome = os.path.basename(arquivo)
    inicio = time.time()
    processor = CNPJProcessor(arquivo, tabela, batch_size=batch,
                              conflict_strategy='none', formato='auto')
    total = processor.processar(destino='postgres')
    segundos = time.time() - inicio
    if total > 0 and rotulo:
        ControleArquivos.registrar_processamento(
            arquivo, 'postgres', tabela, total, 'SUCESSO', rotulo=f'{rotulo}/{nome}')
    return nome, total, segundos



class Command(BaseCommand):
    """Recarrega as tabelas da base CNPJ em 4 fases.

    ddl      -> salva a definicao dos indices e dropa indices/view (libera a carga)
    truncar  -> TRUNCATE ... RESTART IDENTITY
    carregar -> importar_cnpj --conflict none (COPY puro, o mais rapido)
    indices  -> recria constraints/indices salvos e roda ANALYZE

    Cobre as 10 tabelas da RFB que o processador conhece: os 6 dominios
    (c_cnaes, c_motivos, c_municipios, c_naturezas, c_paises, c_qualificacoes)
    e as 4 grandes (c_socios, c_simples, c_empresas, c_estabelecimentos).

    A bancocnpj local e recarregavel a partir dos CSVs da RFB, mas o TRUNCATE
    nao tem volta -- por isso o DDL dos indices e salvo em arquivo antes e a
    fase 'tudo' ignora tabela que nao tenha arquivo na pasta.
    """

    help = 'Recarrega tabelas da base CNPJ (ddl, truncar, carregar, indices)'

    carga = None

    # Tabelas de dominio (pequenas, 1 arquivo cada) e as grandes. A ordem e a de
    # dependencia logica: dominios primeiro, depois as grandes que os referenciam.
    TABELAS_DOMINIO = ['c_cnaes', 'c_motivos', 'c_municipios', 'c_naturezas',
                       'c_paises', 'c_qualificacoes']
    TABELAS_GRANDES = ['c_socios', 'c_simples', 'c_empresas', 'c_estabelecimentos']
    TABELAS_ORDEM = TABELAS_DOMINIO + TABELAS_GRANDES
    DDL_PADRAO = '/mnt/NOTE_EXT4_SDD/DocumentosSSD/cnpj_lote/ddl_carga.sql'
    VIEW_REMOVER = 'vw_empresa_completa'

    def add_arguments(self, parser):
        parser.add_argument('--pasta', required=True,
                            help='Pasta com os .zip (ou .parquet) da RFB')
        parser.add_argument('--tabelas', type=str, default=','.join(self.TABELAS_ORDEM),
                            help='Tabelas a processar (default: todas, na ordem de dependencia)')
        parser.add_argument('--fase', type=str,
                            choices=['ddl', 'truncar', 'carregar', 'indices', 'tudo'], default='tudo',
                            help='tudo = as 4 fases; em tudo/truncar a tabela e truncada '
                                 '(em tudo, so se a pasta tiver arquivo dela)')
        parser.add_argument('--arquivo-ddl', type=str, default=self.DDL_PADRAO,
                            help='Onde salvar/ler a definicao dos indices')
        parser.add_argument('--batch', type=int, default=50000)
        parser.add_argument('--rotulo', type=str, default='2026-11',
                            help='Prefixo do lote no controle de arquivos')
        parser.add_argument('--limit', type=int, default=0,
                            help='Limita o numero de arquivos por tabela (teste)')
        parser.add_argument('--maintenance-work-mem', type=str, default='2GB',
                            help='maintenance_work_mem da sessao que cria os indices')
        parser.add_argument('--workers', type=int, default=4,
                            help='Processos paralelos na carga (1 = serial)')

    # ------------------------------------------------------------------ helpers

    def _conectar(self):
        conn = get_postgres_connection()
        cur = conn.cursor()
        # commits sem fsync: a base e recarregavel e isso corta o custo por lote
        cur.execute("SET synchronous_commit = off")
        return conn, cur

    def _write(self, msg):
        self.stdout.write(msg)

    def _registrar(self, etapa, tabela='', detalhe='', quantidade=0, duracao_s=0.0):
        """Grava a etapa no banco do admin (best-effort: nunca derruba a carga)"""
        estatisticas.registrar(self.carga, etapa, tabela, detalhe,
                               quantidade, duracao_s)

    def _iniciar(self, etapa, tabela='', detalhe=''):
        """Abre a etapa como em andamento; o _registrar fecha depois"""
        estatisticas.iniciar_etapa(self.carga, etapa, tabela, detalhe)

    @staticmethod
    def _nome_do_sql(sql):
        m = re.match(r'ALTER TABLE\s+\S+\s+ADD CONSTRAINT\s+(\S+)', sql, re.IGNORECASE)
        if m:
            return m.group(1)
        m = re.match(r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(\S+)',
                     sql, re.IGNORECASE)
        return m.group(1) if m else ''

    @staticmethod
    def _tabela_do_sql(sql):
        m = re.match(r'ALTER TABLE\s+(\S+)', sql, re.IGNORECASE)
        if m:
            return m.group(1).split('.')[-1].strip('"')
        m = re.search(r'\sON\s+(?:ONLY\s+)?(\S+)', sql, re.IGNORECASE)
        return m.group(1).split('.')[-1].strip('"') if m else ''


    def _tabelas(self, options):
        tabelas = [t.strip() for t in options['tabelas'].split(',') if t.strip()]
        desconhecidas = [t for t in tabelas if t not in self.TABELAS_ORDEM]
        if desconhecidas:
            raise CommandError(f'Tabela(s) nao suportada(s): {desconhecidas}')
        # respeita a ordem de dependencia logica, nao a ordem digitada
        return [t for t in self.TABELAS_ORDEM if t in tabelas]

    def _tabelas_com_arquivo(self, pasta, tabelas):
        """Separa as tabelas que tem arquivo na pasta das que nao tem"""
        com_arquivo, sem_arquivo = [], []
        for tabela in tabelas:
            if self._arquivos(pasta, tabela):
                com_arquivo.append(tabela)
            else:
                sem_arquivo.append(tabela)
        return com_arquivo, sem_arquivo

    def _existe_tabela(self, cur, tabela):
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (tabela,))
        return cur.fetchone()[0]

    # ------------------------------------------------------------------- fase 1

    def _coletar_ddl(self, cur, tabelas):
        """Devolve (constraints, indices) na ordem em que devem ser recriados"""
        cur.execute(
            """
            SELECT c.conrelid::regclass::text, c.conname, c.contype,
                   pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            WHERE c.conrelid::regclass::text = ANY(%s) AND c.contype IN ('p', 'u')
            ORDER BY c.conrelid::regclass::text, c.conname
            """,
            (tabelas,),
        )
        constraints = [
            (r[0], r[1], r[2], f'ALTER TABLE {r[0]} ADD CONSTRAINT {r[1]} {r[3]};')
            for r in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT c.relname, i.relname, pg_get_indexdef(ix.indexrelid)
            FROM pg_index ix
            JOIN pg_class c ON c.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            WHERE c.relname = ANY(%s)
              AND NOT EXISTS (SELECT 1 FROM pg_constraint pc WHERE pc.conindid = ix.indexrelid)
            ORDER BY c.relname, i.relname
            """,
            (tabelas,),
        )
        indices = [(r[0], r[1], f'{r[2]};') for r in cur.fetchall()]
        return constraints, indices

    def _salvar_ddl(self, cur, tabelas, caminho):
        constraints, indices = self._coletar_ddl(cur, tabelas)
        pasta = os.path.dirname(caminho)
        if pasta:
            os.makedirs(pasta, exist_ok=True)
        with open(caminho, 'w', encoding='utf-8') as fh:
            fh.write('-- DDL salvo antes da recarga da base CNPJ\n')
            fh.write(f'-- tabelas: {", ".join(tabelas)}\n')
            fh.write(f'-- gerado em: {time.strftime("%Y-%m-%d %H:%M:%S")}\n')
            fh.write('-- 1) constraints (primary key / unique)\n')
            for _, _, _, sql in constraints:
                fh.write(sql + '\n')
            fh.write('-- 2) indices\n')
            for _, _, sql in indices:
                fh.write(sql + '\n')
        self._write(f'   💾 DDL salvo em {caminho} '
                    f'({len(constraints)} constraint(s), {len(indices)} indice(s))')
        return caminho

    def _caminho_ddl(self, options, tabelas):
        """Arquivo do DDL: o padrao e por conjunto de tabelas (nao se sobrescrevem)"""
        caminho = options['arquivo_ddl']
        if caminho != self.DDL_PADRAO or tabelas == self.TABELAS_ORDEM:
            return caminho
        base, ext = os.path.splitext(caminho)
        sufixo = '_'.join(t[2:] if t.startswith('c_') else t for t in tabelas)
        if len(sufixo) > 40:
            resumo = hashlib.md5(sufixo.encode()).hexdigest()[:8]
            sufixo = f'{len(tabelas)}tabelas_{resumo}'
        return f'{base}_{sufixo}{ext}'

    def _dropar_objetos(self, cur, constraints, indices):
        cur.execute("SELECT to_regclass(%s)", (self.VIEW_REMOVER,))
        if cur.fetchone()[0]:
            cur.execute(f'DROP VIEW IF EXISTS {self.VIEW_REMOVER}')
            self._write(f'   🗑️  view {self.VIEW_REMOVER} removida '
                        f'(codigo morto: filtra cd_matriz_filial = 1)')

        for tabela, nome, _, _ in constraints:
            cur.execute(f'ALTER TABLE {tabela} DROP CONSTRAINT IF EXISTS {nome}')
        for _, nome, _ in indices:
            cur.execute(f'DROP INDEX IF EXISTS {nome}')
        self._write(f'   🗑️  {len(constraints)} constraint(s) e {len(indices)} indice(s) dropados')

    def fase_ddl(self, cur, conn, tabelas, caminho):
        self._write('🔧 FASE ddl: salvando e removendo indices')
        constraints, indices = self._coletar_ddl(cur, tabelas)
        if not constraints and not indices:
            raise CommandError('Nenhum indice encontrado -- as tabelas existem?')
        self._salvar_ddl(cur, tabelas, caminho)
        inicio = time.time()
        self._iniciar(EtapaCarga.DDL, detalhe='drop de indices/view')
        self._dropar_objetos(cur, constraints, indices)
        conn.commit()
        self._registrar(EtapaCarga.DDL, detalhe='drop de indices/view',
                        quantidade=len(constraints) + len(indices),
                        duracao_s=time.time() - inicio)


    # ------------------------------------------------------------------- fase 2

    def fase_truncar(self, cur, conn, tabelas):
        self._write('🔧 FASE truncar: TRUNCATE RESTART IDENTITY')
        for tabela in tabelas:
            if not self._existe_tabela(cur, tabela):
                raise CommandError(f'Tabela {tabela} nao existe')
            cur.execute(f'SELECT count(*) FROM {tabela}')
            antes = cur.fetchone()[0]
            inicio = time.time()
            self._iniciar(EtapaCarga.TRUNCAR, tabela)
            cur.execute(f'TRUNCATE TABLE {tabela} RESTART IDENTITY')
            conn.commit()
            decorrido = time.time() - inicio
            self._write(f'   🧹 {tabela}: {antes:,} linhas removidas '
                        f'({decorrido:.1f}s)')
            self._registrar(EtapaCarga.TRUNCAR, tabela, quantidade=antes,
                            duracao_s=decorrido)

    # ------------------------------------------------------------------- fase 3

    def _arquivos(self, pasta, tabela):
        """Arquivos da tabela na pasta, ordenados pelo numero do sufixo"""
        arquivos = []
        caminhos = glob.glob(os.path.join(pasta, '*.zip')) + glob.glob(os.path.join(pasta, '*.parquet'))
        for caminho in caminhos:
            nome = os.path.basename(caminho)
            for padrao, alvo in PADROES_ARQUIVO_TABELA.items():
                if re.match(padrao, nome, re.IGNORECASE):
                    if alvo == tabela:
                        arquivos.append(caminho)
                    break
        arquivos.sort(key=self._numero_arquivo)
        return arquivos

    @staticmethod
    def _numero_arquivo(caminho):
        m = re.search(r'(\d+)\.(parquet|zip)$', os.path.basename(caminho), re.IGNORECASE)
        return int(m.group(1)) if m else 0

    def fase_carregar(self, options, tabelas):
        self._write('🔧 FASE carregar: COPY puro (conflict none)')
        workers = max(1, options['workers'])
        for tabela in tabelas:
            arquivos = self._arquivos(options['pasta'], tabela)
            if options['limit'] > 0:
                arquivos = arquivos[:options['limit']]
            if not arquivos:
                self._write(f'   ⚠️  {tabela}: nenhum arquivo encontrado')
                continue

            self._write(f'   ▶️  {tabela}: {len(arquivos)} arquivo(s), {workers} processo(s)')
            tarefas = [(a, tabela, options['batch'], options['rotulo']) for a in arquivos]
            inicio = time.time()
            total = 0
            self._iniciar(EtapaCarga.CARREGAR, tabela)
            if workers == 1 or len(tarefas) == 1:
                for nome, n, seg in map(carregar_arquivo, tarefas):
                    total += n
                    self._relatar_arquivo(tabela, nome, n, seg)
            else:
                with ProcessPoolExecutor(max_workers=min(workers, len(tarefas))) as pool:
                    for nome, n, seg in pool.map(carregar_arquivo, tarefas):
                        total += n
                        self._relatar_arquivo(tabela, nome, n, seg)

            decorrido = time.time() - inicio
            self._write(f'   ⏱️  {tabela}: {total:,} linhas em {decorrido:.1f}s '
                        f'({total / max(decorrido, 1):,.0f} lin/s)')
            self._registrar(EtapaCarga.CARREGAR, tabela, quantidade=total,
                            duracao_s=decorrido)

    def _relatar_arquivo(self, tabela, nome, quantidade, duracao_s):
        self._write(f'      ✅ {nome}: {quantidade:,} linhas ({duracao_s:.1f}s)')
        self._registrar(EtapaCarga.ARQUIVO, tabela, detalhe=nome,
                        quantidade=quantidade, duracao_s=duracao_s)

    # ------------------------------------------------------------------- fase 4

    def _objeto_existe(self, cur, sql):
        """True se o indice/constraint do statement ja existe (fase idempotente)"""
        m = re.match(r'ALTER TABLE\s+\S+\s+ADD CONSTRAINT\s+(\S+)', sql, re.IGNORECASE)
        if m:
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = %s", (m.group(1),))
            return cur.fetchone() is not None
        m = re.match(r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(\S+)', sql, re.IGNORECASE)
        if m:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL", (m.group(1),))
            return cur.fetchone()[0]
        return False

    def _executar_ddl(self, cur, caminho):
        with open(caminho, encoding='utf-8') as fh:
            statements = [linha.strip() for linha in fh
                          if linha.strip() and not linha.strip().startswith('--')]
        conn = cur.connection
        for sql in statements:
            if self._objeto_existe(cur, sql):
                self._write(f'   ⏭️  ja existe | {sql[:70]}...')
                continue
            inicio = time.time()
            self._iniciar(EtapaCarga.INDICE, self._tabela_do_sql(sql),
                          detalhe=self._nome_do_sql(sql))
            cur.execute(sql)
            conn.commit()
            decorrido = time.time() - inicio
            self._write(f'   🔨 {decorrido:7.1f}s | {sql[:70]}...')
            self._registrar(EtapaCarga.INDICE, self._tabela_do_sql(sql),
                            detalhe=self._nome_do_sql(sql), duracao_s=decorrido)

    def fase_indices(self, cur, conn, tabelas, caminho, maintenance_work_mem='2GB'):
        self._write('🔧 FASE indices: recriando constraints/indices + ANALYZE')
        if not os.path.exists(caminho):
            raise CommandError(f'DDL nao encontrado: {caminho}')
        cur.execute("SET maintenance_work_mem = %s", (maintenance_work_mem,))
        cur.execute("SET max_parallel_maintenance_workers = 2")
        cur.execute("SET maintenance_io_concurrency = 8")
        self._executar_ddl(cur, caminho)
        for tabela in tabelas:
            inicio = time.time()
            self._iniciar(EtapaCarga.ANALYZE, tabela)
            cur.execute(f'ANALYZE {tabela}')
            conn.commit()
            decorrido = time.time() - inicio
            self._write(f'   📊 ANALYZE {tabela} ({decorrido:.1f}s)')
            self._registrar(EtapaCarga.ANALYZE, tabela, duracao_s=decorrido)

    # -------------------------------------------------------------------- handle

    def handle(self, *args, **options):
        tabelas = self._tabelas(options)
        fase = options['fase']
        pasta = options['pasta']

        if fase in ('carregar', 'tudo') and not os.path.isdir(pasta):
            raise CommandError(f'Pasta nao encontrada: {pasta}')

        sem_arquivo = []
        if fase == 'tudo':
            # Recarga completa nunca trunca tabela sem arquivo na pasta: ela
            # ficaria vazia e o site pararia de responder.
            tabelas, sem_arquivo = self._tabelas_com_arquivo(pasta, tabelas)
            if not tabelas:
                raise CommandError('Nenhuma das tabelas selecionadas tem '
                                   f'arquivo na pasta: {pasta}')

        caminho = self._caminho_ddl(options, tabelas)

        self._write('=' * 60)
        self._write(f'🚚 Recarga base CNPJ | fase: {fase}')
        self._write(f'   tabelas: {", ".join(tabelas)}')
        if sem_arquivo:
            self._write('   ⚠️  sem arquivo na pasta, nao serao tocadas: '
                        f'{", ".join(sem_arquivo)}')
        self._write(f'   pasta: {pasta}')
        if fase in ('ddl', 'indices', 'tudo'):
            self._write(f'   ddl: {caminho}')
        self._write('=' * 60)

        self.carga = estatisticas.iniciar_carga(
            options['rotulo'], pasta,
            observacao=f'carregar_base_cnpj --fase {fase}')

        conn = cur = None
        try:
            if fase == 'carregar':
                self.fase_carregar(options, tabelas)
            else:
                conn, cur = self._conectar()
                if fase in ('ddl', 'tudo'):
                    self.fase_ddl(cur, conn, tabelas, caminho)
                if fase in ('truncar', 'tudo'):
                    self.fase_truncar(cur, conn, tabelas)
                if fase == 'tudo':
                    # a carga abre as proprias conexoes (uma por chunk)
                    conn.commit()
                    self.fase_carregar(options, tabelas)
                if fase in ('indices', 'tudo'):
                    self.fase_indices(cur, conn, tabelas, caminho,
                                      options['maintenance_work_mem'])
        except (Exception, KeyboardInterrupt):
            # KeyboardInterrupt tambem fecha a carga: sem isso, um Ctrl+C
            # deixaria a carga "em andamento" para sempre na pagina.
            estatisticas.concluir_carga(self.carga, Carga.STATUS_FALHOU)
            raise
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

        estatisticas.concluir_carga(self.carga)
        self._write('')
        self._write('✅ fase(s) concluida(s)')

