"""Carrega coordenadas e nomes de CEP na tabela c_ceps do bancocnpj.

Fontes suportadas (--fonte):

    tbl_cep  base eDNE/Correios 2018 (cp1252, separador virgula)
             colunas usadas: cep, logradouro, bairro, cidade, estado,
             cidade_ibge, latitude, longitude
    geocep   base de CEPs geocodificados (utf-8, separador ';')
             coordenada POR NUMERO: o carregador agrega por CEP (media)
             colunas usadas: cep, latitude, longitude, ibge (15 digitos)
    bq       export do BigQuery 2026 (utf-8, separador virgula)
             coluna centroide em WKT 'POINT(lon lat)'
             colunas usadas: cep, logradouro, localidade (bairro),
             id_municipio, nome_municipio, sigla_uf, centroide

Cada fonte grava o mesmo registro em c_ceps com ON CONFLICT (cep) DO UPDATE:
logradouro/bairro/municipio sao preenchidos com COALESCE (nunca apagam o que
ja existe) e a coordenada so e sobrescrita por fonte de prioridade maior ou
igual (ver PRIORIDADE). A carga e idempotente e a ordem de execucao nao
estraga o resultado.

A coluna c_ceps.generico marca o CEP que cobre o municipio inteiro (o Correios
nao associa logradouro a ele, ex.: 89669-000 Ipira/SC). Nesses CEPs a
coordenada e o centroide da area do municipio, ou seja, costuma cair no interior
- quem monta o link do Maps deve preferir o endereco em texto.

Uso:
    python manage.py carregar_ceps --criar
    python manage.py carregar_ceps --fonte tbl_cep --arquivo /caminho/tbl_cep_201805_csv.csv
    python manage.py carregar_ceps --fonte geocep  --arquivo /caminho/geocep.csv
    python manage.py carregar_ceps --fonte bq      --arquivo /caminho/bq-results-....csv
"""
import csv
import io
import os
import time

from django.core.management.base import BaseCommand

from etl.conexoes.postgres import get_postgres_connection

# A coluna 'estabelecimentos' do export do BigQuery passa do limite padrao do csv
csv.field_size_limit(1 << 30)

COLUNAS = ['cep', 'logradouro', 'bairro', 'id_municipio', 'nome_municipio',
           'uf', 'latitude', 'longitude', 'fonte', 'generico']

# Prioridade da coordenada por fonte: quanto maior, mais confiavel.
# Medido em 1M de CEPs comuns (mediana da distancia entre fontes):
#   bq vs tbl_cep = 64 m | bq vs geocep = 125 m | geocep vs tbl_cep = 169 m
# p99: bq vs geocep = 21 km (melhor) | bq vs tbl_cep = 637 km | geocep vs tbl_cep = 1449 km
# Ou seja: bq e o mais consensual e geocep tem a menor cauda de erro;
# o tbl_cep (2018) tem erro grosseiro em ~1% dos CEPs.
PRIORIDADE = {'tbl_cep': 1, 'geocep': 2, 'bq': 3}

# ORDEM RECOMENDADA DE CARGA: tbl_cep -> geocep -> bq.
# Os nomes (logradouro/bairro/municipio) sao preenchidos por COALESCE, entao
# quem carrega primeiro prevalece: o tbl_cep tem o nome oficial do logradouro.
# A coordenada nao depende da ordem (usa PRIORIDADE).


# Limites geograficos do Brasil, para descartar coordenada invalida/trocada
LAT_MIN, LAT_MAX = -34.0, 6.5
LON_MIN, LON_MAX = -74.5, -32.0

SQL_CRIAR = """
    CREATE TABLE IF NOT EXISTS c_ceps (
        cep            varchar(8) PRIMARY KEY,
        logradouro     varchar(150),
        bairro         varchar(100),
        id_municipio   integer,
        nome_municipio varchar(100),
        uf             varchar(2),
        latitude       numeric(9,6),
        longitude      numeric(9,6),
        fonte          varchar(10),
        generico       boolean NOT NULL DEFAULT false,
        atualizado_em  timestamp NOT NULL DEFAULT now()
    )
"""

# Upgrade de tabela criada antes da coluna generico existir (idempotente)
SQL_ALTER = ('ALTER TABLE c_ceps '
             'ADD COLUMN IF NOT EXISTS generico boolean NOT NULL DEFAULT false')

SQL_TEMP = """
    CREATE TEMP TABLE tmp_c_ceps (
        cep            varchar(8),
        logradouro     varchar(150),
        bairro         varchar(100),
        id_municipio   integer,
        nome_municipio varchar(100),
        uf             varchar(2),
        latitude       numeric(9,6),
        longitude      numeric(9,6),
        fonte          varchar(10),
        generico       boolean
    )
"""

class Command(BaseCommand):
    help = 'Carrega coordenadas/nomes de CEP na tabela c_ceps (fonte: tbl_cep, geocep ou bq)'

    def add_arguments(self, parser):
        parser.add_argument('--fonte', type=str, choices=['tbl_cep', 'geocep', 'bq'],
                            help='Fonte dos dados (obrigatorio, exceto com --criar)')
        parser.add_argument('--arquivo', type=str,
                            help='Caminho do CSV da fonte (obrigatorio, exceto com --criar)')
        parser.add_argument('--criar', action='store_true',
                            help='Cria a tabela c_ceps se nao existir (DDL)')
        parser.add_argument('--batch', type=int, default=20000, help='Tamanho do lote (padrao: 20000)')
        parser.add_argument('--progress', type=int, default=200000,
                            help='Progresso a cada N linhas (padrao: 200000)')
        parser.add_argument('--test', type=int, default=0, help='Modo teste: le apenas N linhas')

    def handle(self, *args, **options):
        fonte = options['fonte']
        arquivo = options['arquivo']
        criar = options['criar']
        batch_size = options['batch']
        progress_interval = options['progress']
        test_limit = options['test']

        if not fonte and not criar:
            self.stdout.write(self.style.ERROR(
                '❌ Informe --fonte e --arquivo (ou use --criar para criar a tabela)'))
            return

        if fonte and not arquivo:
            self.stdout.write(self.style.ERROR('❌ --arquivo é obrigatório junto com --fonte'))
            return

        if arquivo and not os.path.exists(arquivo):
            self.stdout.write(self.style.ERROR(f'❌ Arquivo não encontrado: {arquivo}'))
            return

        conn = get_postgres_connection()
        cur = conn.cursor()

        try:
            if criar:
                cur.execute(SQL_CRIAR)
                cur.execute(SQL_ALTER)
                conn.commit()
                self.stdout.write(self.style.SUCCESS('✅ Tabela c_ceps verificada/criada'))
                if not fonte:
                    self._resumo(cur)
                    return

            cur.execute(SQL_TEMP)
            conn.commit()

            self.stdout.write(f'📄 Fonte: {fonte}')
            self.stdout.write(f'📁 Arquivo: {arquivo}')
            self.stdout.write(f'⚙️  Prioridade da coordenada: {PRIORIDADE[fonte]}')
            self.stdout.write('-' * 60)

            total_lido = 0
            total_gravado = 0
            inicio = time.time()
            lote = []
            # geocep traz uma linha por numero do endereco: a media por CEP so
            # fecha depois de ler o arquivo inteiro, entao agrega no final
            agregar_no_fim = fonte == 'geocep'

            for linha in self._iter_linhas(fonte, arquivo):
                lote.append(linha)
                total_lido += 1

                if len(lote) >= batch_size:
                    if agregar_no_fim:
                        self._copiar_lote(conn, cur, lote)
                    else:
                        total_gravado += self._enviar_lote(conn, cur, lote, fonte)
                    lote = []
                    self._progresso(total_lido, total_gravado, inicio, progress_interval)

                if test_limit and total_lido >= test_limit:
                    break

            if lote:
                if agregar_no_fim:
                    self._copiar_lote(conn, cur, lote)
                else:
                    total_gravado += self._enviar_lote(conn, cur, lote, fonte)

            if agregar_no_fim:
                cur.execute(self._sql_insert(), {'prioridade': PRIORIDADE[fonte]})
                total_gravado += cur.rowcount
                conn.commit()

            cur.execute('DROP TABLE IF EXISTS tmp_c_ceps')
            conn.commit()

            duracao = time.time() - inicio
            self.stdout.write('-' * 60)
            self.stdout.write(self.style.SUCCESS('✅ CARGA CONCLUÍDA'))
            self.stdout.write(f'📊 Linhas lidas: {total_lido:,}')
            self.stdout.write(f'📊 Linhas gravadas (insert + update): {total_gravado:,}')
            self.stdout.write(f'⏱️  Tempo: {duracao:.1f}s')
            self._resumo(cur)
        finally:
            cur.close()
            conn.close()

    def _progresso(self, lido, gravado, inicio, intervalo):
        if not intervalo or lido % intervalo:
            return
        decorrido = time.time() - inicio
        velocidade = lido / decorrido if decorrido else 0
        self.stdout.write(f'   ⏳ {lido:,} lidas | {gravado:,} gravadas | {velocidade:,.0f} linhas/s')

    def _copiar_lote(self, conn, cur, lote):
        """COPY do lote para a temp table (sem gravar em c_ceps)"""
        buffer = io.StringIO()
        escritor = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
        for linha in lote:
            escritor.writerow(['' if valor is None else valor for valor in linha])
        buffer.seek(0)

        try:
            cur.copy_expert(
                f"COPY tmp_c_ceps ({', '.join(COLUNAS)}) FROM STDIN WITH (FORMAT csv, NULL '')",
                buffer
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f'Falha no COPY ({len(lote)} linhas): {e}')

    def _enviar_lote(self, conn, cur, lote, fonte):
        """COPY do lote para a temp table e merge em c_ceps"""
        self._copiar_lote(conn, cur, lote)
        try:
            cur.execute(self._sql_insert(), {'prioridade': PRIORIDADE[fonte]})
            gravado = cur.rowcount
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f'Falha no merge ({len(lote)} linhas): {e}')

        cur.execute('TRUNCATE tmp_c_ceps')
        conn.commit()
        return gravado

    def _sql_insert(self):
        """INSERT agregando por CEP (geocep tem uma linha por numero) + ON CONFLICT"""
        prioridade_atual = 'CASE c_ceps.fonte ' + ' '.join(
            f"WHEN '{origem}' THEN {peso}" for origem, peso in PRIORIDADE.items()
        ) + ' ELSE 0 END'
        return f"""
            INSERT INTO c_ceps (cep, logradouro, bairro, id_municipio, nome_municipio,
                                uf, latitude, longitude, fonte, generico, atualizado_em)
            SELECT cep,
                   NULLIF(min(logradouro), ''),
                   NULLIF(min(bairro), ''),
                   min(id_municipio),
                   NULLIF(min(nome_municipio), ''),
                   NULLIF(min(uf), ''),
                   avg(latitude)::numeric(9,6),
                   avg(longitude)::numeric(9,6),
                   min(fonte),
                   bool_or(coalesce(generico, false)),
                   now()
              FROM tmp_c_ceps
             WHERE cep IS NOT NULL
             GROUP BY cep
            ON CONFLICT (cep) DO UPDATE SET
                logradouro     = COALESCE(EXCLUDED.logradouro, c_ceps.logradouro),
                bairro         = COALESCE(EXCLUDED.bairro, c_ceps.bairro),
                id_municipio   = COALESCE(EXCLUDED.id_municipio, c_ceps.id_municipio),
                nome_municipio = COALESCE(EXCLUDED.nome_municipio, c_ceps.nome_municipio),
                uf             = COALESCE(EXCLUDED.uf, c_ceps.uf),
                generico       = c_ceps.generico OR EXCLUDED.generico,
                latitude       = CASE WHEN %(prioridade)s >= ({prioridade_atual})
                                      THEN COALESCE(EXCLUDED.latitude, c_ceps.latitude)
                                      ELSE c_ceps.latitude END,
                longitude      = CASE WHEN %(prioridade)s >= ({prioridade_atual})
                                      THEN COALESCE(EXCLUDED.longitude, c_ceps.longitude)
                                      ELSE c_ceps.longitude END,
                fonte          = CASE WHEN %(prioridade)s >= ({prioridade_atual})
                                      THEN EXCLUDED.fonte ELSE c_ceps.fonte END,
                atualizado_em  = now()
        """

    def _resumo(self, cur):
        cur.execute('SELECT COUNT(*), COUNT(latitude), COUNT(*) FILTER (WHERE generico) '
                    'FROM c_ceps')
        total, com_coordenada, genericos = cur.fetchone()
        self.stdout.write(f'📈 c_ceps: {total:,} CEPs | {com_coordenada:,} com coordenada'
                          f' | {genericos:,} genericos (CEP do municipio inteiro)')
        cur.execute('SELECT fonte, COUNT(*) FROM c_ceps GROUP BY fonte ORDER BY 2 DESC')
        for origem, quantidade in cur.fetchall():
            self.stdout.write(f'   {origem or "(vazio)"}: {quantidade:,}')

    # ------------------------------------------------------------------ leitura

    def _iter_linhas(self, fonte, caminho):
        if fonte == 'tbl_cep':
            return self._iter_tbl_cep(caminho)
        if fonte == 'geocep':
            return self._iter_geocep(caminho)
        return self._iter_bq(caminho)

    def _iter_tbl_cep(self, caminho):
        with open(caminho, encoding='cp1252', newline='') as arquivo:
            for linha in csv.DictReader(arquivo):
                cep = self._cep(linha.get('cep'))
                if not cep:
                    continue
                latitude, longitude = self._coord(
                    self._num(linha.get('latitude')),
                    self._num(linha.get('longitude'))
                )
                logradouro = self._txt(linha.get('logradouro'), 150)
                yield (cep,
                       logradouro,
                       self._txt(linha.get('bairro'), 100),
                       self._municipio(linha.get('cidade_ibge')),
                       self._txt(linha.get('cidade'), 100),
                       self._uf(linha.get('estado')),
                       latitude, longitude, 'tbl_cep',
                       # Correios sem logradouro = CEP do municipio inteiro
                       # (ex.: 89669-000 Ipira/SC cobre 208 logradouros rurais)
                       logradouro is None)

    def _iter_geocep(self, caminho):
        with open(caminho, encoding='utf-8', newline='') as arquivo:
            for linha in csv.DictReader(arquivo, delimiter=';'):
                cep = self._cep(linha.get('cep'))
                if not cep:
                    continue
                latitude, longitude = self._coord(
                    self._num(linha.get('latitude')),
                    self._num(linha.get('longitude'))
                )
                yield (cep, None, None,
                       self._municipio(linha.get('ibge')),
                       None, None, latitude, longitude, 'geocep', False)

    def _iter_bq(self, caminho):
        with open(caminho, encoding='utf-8', newline='') as arquivo:
            for linha in csv.DictReader(arquivo):
                cep = self._cep(linha.get('cep'))
                if not cep:
                    continue
                latitude, longitude = self._coord(*self._wkt(linha.get('centroide')))
                yield (cep,
                       self._txt(linha.get('logradouro'), 150),
                       self._txt(linha.get('localidade'), 100),
                       self._municipio(linha.get('id_municipio')),
                       self._txt(linha.get('nome_municipio'), 100),
                       self._uf(linha.get('sigla_uf')),
                       latitude, longitude, 'bq', False)

    # ---------------------------------------------------------------- conversao

    @staticmethod
    def _cep(valor):
        cep = ''.join(caractere for caractere in str(valor or '') if caractere.isdigit())
        if len(cep) != 8 or cep == '00000000':
            return None
        return cep

    @staticmethod
    def _txt(valor, limite=None):
        if valor is None:
            return None
        texto = ' '.join(str(valor).split())
        if not texto:
            return None
        return texto[:limite] if limite else texto

    @staticmethod
    def _uf(valor):
        """Sigla da UF com 2 letras. CEP compartilhado traz 'RJ,SP' no bq."""
        texto = str(valor or '').upper()
        for parte in texto.replace(';', ',').replace('/', ',').replace('|', ',').split(','):
            parte = parte.strip()
            if len(parte) == 2 and parte.isalpha():
                return parte
        return None

    @staticmethod
    def _municipio(valor):
        """Codigo IBGE do municipio: 7 primeiros digitos, faixa valida do IBGE.

        O export do BigQuery as vezes traz id_municipio com 14 digitos
        (ex.: 41004004105805) e o geocep traz 15 digitos; em ambos o
        codigo do municipio esta no inicio.
        """
        digitos = ''.join(caractere for caractere in str(valor or '') if caractere.isdigit())
        if len(digitos) < 7:
            return None
        codigo = int(digitos[:7])
        return codigo if 1100000 <= codigo <= 5399999 else None

    @staticmethod
    def _num(valor):
        if valor is None or str(valor).strip() == '':
            return None
        try:
            return float(str(valor).replace(',', '.').strip())
        except ValueError:
            return None

    @staticmethod
    def _coord(latitude, longitude):
        """Descarta coordenada fora do Brasil (evita latitude/longitude trocadas)"""
        if latitude is None or longitude is None:
            return None, None
        if not (LAT_MIN <= latitude <= LAT_MAX):
            return None, None
        if not (LON_MIN <= longitude <= LON_MAX):
            return None, None
        return latitude, longitude

    @staticmethod
    def _wkt(valor):
        """'POINT(lon lat)' -> (lat, lon). O primeiro numero do WKT e a longitude"""
        texto = str(valor or '').strip()
        if not texto.upper().startswith('POINT(') or not texto.endswith(')'):
            return None, None
        partes = texto[6:-1].split()
        if len(partes) != 2:
            return None, None
        try:
            longitude, latitude = float(partes[0]), float(partes[1])
        except ValueError:
            return None, None
        return latitude, longitude
