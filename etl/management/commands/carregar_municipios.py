# /home/neto/ProjetoCargaDados/etl/management/commands/carregar_municipios.py
"""Cria e carrega a tabela c_municipios_geo (municipio por TOM + IBGE + centroide).

A tabela c_municipios (vinda da Receita Federal) NAO e alterada: ela continua
sendo a lista de municipios do CNPJ. Esta tabela nova acrescenta, por municipio:

    cd_municipio  codigo TOM  - casa com c_estabelecimentos.municipio
    cd_ibge       codigo IBGE - casa com c_ceps.id_municipio
    nome          nome IBGE (com acento), para exibir na tela
    nome_tom      nome da RFB (maiusculo), para casar texto
    uf            sigla da UF
    latitude/longitude  centroide dos CEPs do municipio
    n_ceps        quantos CEPs entraram na media

Fontes:

    municipios.csv  CODigO TOM;CODIGO IBGE;MUNICIPIO TOM;MUNICIPIO IBGE;UF
                    (latin-1, separador ';') - da Malha Municipal do IBGE
    c_municipios    espinha dorsal: todo TOM do CNPJ tem uma linha aqui
    c_ceps          centroide: media das coordenadas dos CEPs do municipio
    c_estabelecimentos  fallback de UF e de centroide para quem nao tem CEP

Ordem de confianca: c_municipios define QUAIS linhas existem; o CSV do IBGE
acrescenta cd_ibge/nome/uf; c_ceps acrescenta coordenada. Municipio novo que
ainda nao esta no CSV (ex.: TOM 1182 Boa Esperanca do Norte/MT) entra com
cd_ibge NULL e uf/centroide tirados dos proprios estabelecimentos.

Uso:
    python manage.py carregar_municipios --criar
    python manage.py carregar_municipios --arquivo /home/neto/DataSetsTeste/municipios.csv
    python manage.py carregar_municipios --sem-fallback       # nao usa c_estabelecimentos
    python manage.py carregar_municipios --exportar /tmp/municipios_geo.csv
"""
import csv
import io
import os
import time

from django.core.management.base import BaseCommand

from etl.conexoes.postgres import get_postgres_connection

ARQUIVO_PADRAO = '/home/neto/DataSetsTeste/municipios.csv'

# Posicao das colunas no CSV do IBGE (latin-1)
COL_TOM, COL_IBGE, COL_NOME_TOM, COL_NOME_IBGE, COL_UF = range(5)

SQL_CRIAR = """
    CREATE TABLE IF NOT EXISTS c_municipios_geo (
        cd_municipio  integer PRIMARY KEY,
        cd_ibge       integer UNIQUE,
        nome          varchar(100) NOT NULL,
        nome_tom      varchar(100) NOT NULL,
        uf            varchar(2),
        latitude      numeric(9,6),
        longitude     numeric(9,6),
        n_ceps        integer NOT NULL DEFAULT 0,
        atualizado_em timestamp NOT NULL DEFAULT now()
    )
"""

SQL_INDICES = [
    'CREATE INDEX IF NOT EXISTS idx_municipios_geo_uf ON c_municipios_geo (uf)',
    'CREATE INDEX IF NOT EXISTS idx_municipios_geo_nome ON c_municipios_geo (upper(nome))',
]

SQL_TEMP = """
    CREATE TEMP TABLE tmp_mun_ibge (
        cd_municipio integer,
        cd_ibge      integer,
        nome         varchar(100),
        uf           varchar(2)
    )
"""

# Centroide por municipio: uma varredura de c_ceps (nao ha indice em
# id_municipio, entao um LATERAL por municipio seria 5.571 seq scans).
SQL_AGG_CEPS = """
    CREATE TEMP TABLE tmp_mun_geo AS
    SELECT id_municipio,
           round(avg(latitude), 6)  AS latitude,
           round(avg(longitude), 6) AS longitude,
           count(*)                 AS n_ceps
      FROM c_ceps
     WHERE id_municipio IS NOT NULL
       AND latitude IS NOT NULL
       AND longitude IS NOT NULL
     GROUP BY id_municipio
"""

SQL_UPSERT = """
    INSERT INTO c_municipios_geo AS g
        (cd_municipio, cd_ibge, nome, nome_tom, uf, latitude, longitude, n_ceps, atualizado_em)
    SELECT m.cd_municipio,
           i.cd_ibge,
           COALESCE(i.nome, m.municipio),
           m.municipio,
           i.uf,
           c.latitude,
           c.longitude,
           COALESCE(c.n_ceps, 0),
           now()
      FROM c_municipios m
      LEFT JOIN tmp_mun_ibge i ON i.cd_municipio = m.cd_municipio
      LEFT JOIN tmp_mun_geo  c ON c.id_municipio = i.cd_ibge
    ON CONFLICT (cd_municipio) DO UPDATE SET
        cd_ibge       = COALESCE(EXCLUDED.cd_ibge, g.cd_ibge),
        nome          = EXCLUDED.nome,
        nome_tom      = EXCLUDED.nome_tom,
        uf            = COALESCE(EXCLUDED.uf, g.uf),
        latitude      = COALESCE(EXCLUDED.latitude, g.latitude),
        longitude     = COALESCE(EXCLUDED.longitude, g.longitude),
        n_ceps        = GREATEST(EXCLUDED.n_ceps, g.n_ceps),
        atualizado_em = now()
"""

# Fallback 1: UF de quem o CSV do IBGE nao tem (municipio novo), lendo os
# proprios estabelecimentos. Usa idx_estabelecimentos_municipio.
SQL_FALLBACK_UF = """
    UPDATE c_municipios_geo g
       SET uf = x.uf
      FROM (SELECT min(uf) AS uf
              FROM c_estabelecimentos
             WHERE municipio = %(municipio)s
               AND uf IS NOT NULL) x
     WHERE g.cd_municipio = %(municipio)s
       AND g.uf IS NULL
       AND x.uf IS NOT NULL
"""

# Fallback 2: centroide dos municipios sem coordenada, usando o CEP de cada
# estabelecimento (JOIN pelo indice c_ceps_pkey).
SQL_FALLBACK_CENTROIDE = """
    UPDATE c_municipios_geo g
       SET latitude  = x.latitude,
           longitude = x.longitude,
           n_ceps    = x.n_ceps
      FROM (SELECT e.municipio,
                   round(avg(c.latitude), 6)  AS latitude,
                   round(avg(c.longitude), 6) AS longitude,
                   count(*)                   AS n_ceps
              FROM c_estabelecimentos e
              JOIN c_ceps c ON c.cep = e.cep
             WHERE e.municipio IN (SELECT cd_municipio FROM c_municipios_geo
                                    WHERE latitude IS NULL)
               AND c.latitude IS NOT NULL
               AND c.longitude IS NOT NULL
             GROUP BY e.municipio) x
     WHERE g.cd_municipio = x.municipio
"""

SQL_RESUMO = """
    SELECT count(*)                                AS total,
           count(cd_ibge)                          AS com_ibge,
           count(latitude)                         AS com_centroide,
           count(*) FILTER (WHERE uf IS NULL)      AS sem_uf,
           count(*) FILTER (WHERE cd_ibge IS NULL) AS sem_ibge
      FROM c_municipios_geo
"""


class Command(BaseCommand):
    help = 'Cria e carrega c_municipios_geo (TOM + IBGE + UF + centroide dos CEPs)'

    def add_arguments(self, parser):
        parser.add_argument('--arquivo', default=ARQUIVO_PADRAO,
                            help=f'CSV TOM/IBGE do IBGE (padrao: {ARQUIVO_PADRAO})')
        parser.add_argument('--criar', action='store_true',
                            help='Cria a tabela c_municipios_geo se nao existir (DDL)')
        parser.add_argument('--sem-fallback', action='store_true',
                            help='Nao usa c_estabelecimentos para uf/centroide que faltar')
        parser.add_argument('--exportar', type=str,
                            help='Grava um CSV (utf-8) da tabela para copiar na VPS')
        parser.add_argument('--test', type=int, default=0,
                            help='Modo teste: le apenas N linhas do CSV')

    def handle(self, *args, **options):
        arquivo = options['arquivo']
        inicio = time.time()

        if not os.path.exists(arquivo):
            self.stdout.write(self.style.ERROR(f'❌ Arquivo não encontrado: {arquivo}'))
            return

        conn = get_postgres_connection()
        cur = conn.cursor()

        try:
            if options['criar']:
                cur.execute(SQL_CRIAR)
                for sql in SQL_INDICES:
                    cur.execute(sql)
                conn.commit()
                self.stdout.write(self.style.SUCCESS(
                    '✅ Tabela c_municipios_geo verificada/criada'))

            self.stdout.write(f'📁 CSV do IBGE: {arquivo}')
            linhas = self._ler_csv(arquivo, options['test'])
            self.stdout.write(f'📄 Linhas lidas do CSV: {len(linhas):,}')

            cur.execute(SQL_TEMP)
            self._copiar(cur, linhas)
            conn.commit()

            cur.execute('DROP TABLE IF EXISTS tmp_mun_geo')
            cur.execute(SQL_AGG_CEPS)
            conn.commit()
            cur.execute('SELECT count(*) FROM tmp_mun_geo')
            self.stdout.write(f'📍 Municipios com centroide em c_ceps: {cur.fetchone()[0]:,}')

            cur.execute(SQL_UPSERT)
            gravados = cur.rowcount
            conn.commit()
            self.stdout.write(self.style.SUCCESS(
                f'✅ Upsert em c_municipios_geo: {gravados:,} linhas'))

            if not options['sem_fallback']:
                self._fallback(cur, conn)

            cur.execute('ANALYZE c_municipios_geo')
            conn.commit()

            self._resumo(cur)
            self.stdout.write(f'⏱️  Tempo: {time.time() - inicio:.1f}s')

            if options['exportar']:
                self._exportar(cur, options['exportar'])

            cur.execute('DROP TABLE IF EXISTS tmp_mun_ibge')
            cur.execute('DROP TABLE IF EXISTS tmp_mun_geo')
            conn.commit()
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------ CSV
    def _ler_csv(self, caminho, limite=0):
        """Le o CSV do IBGE (latin-1, ';') e devolve lista de tuplas."""
        linhas = []
        with io.open(caminho, encoding='latin-1', newline='') as f:
            leitor = csv.reader(f, delimiter=';')
            next(leitor, None)  # cabecalho
            for row in leitor:
                if len(row) < 5:
                    continue
                try:
                    tom = int(row[COL_TOM])
                    ibge = int(row[COL_IBGE])
                except ValueError:
                    continue
                linhas.append((tom, ibge,
                               row[COL_NOME_IBGE].strip()[:100],
                               row[COL_UF].strip().upper()[:2]))
                if limite and len(linhas) >= limite:
                    break
        return linhas

    def _copiar(self, cur, linhas):
        """COPY dos municipios para a tabela temporaria."""
        buf = io.StringIO()
        for tom, ibge, nome, uf in linhas:
            nome = nome.replace('\t', ' ').replace('\n', ' ')
            buf.write(f'{tom}\t{ibge}\t{nome}\t{uf}\n')
        buf.seek(0)
        cur.copy_from(buf, 'tmp_mun_ibge',
                      columns=('cd_municipio', 'cd_ibge', 'nome', 'uf'))

    # ------------------------------------------------------------- fallback
    def _fallback(self, cur, conn):
        """Completa uf e centroide de quem nao veio do CSV/c_ceps."""
        cur.execute('SELECT cd_municipio FROM c_municipios_geo WHERE uf IS NULL')
        sem_uf = [r[0] for r in cur.fetchall()]
        for municipio in sem_uf:
            cur.execute(SQL_FALLBACK_UF, {'municipio': municipio})
        conn.commit()
        if sem_uf:
            self.stdout.write(f'🔧 UF recuperada de c_estabelecimentos para: {sem_uf}')

        cur.execute(SQL_FALLBACK_CENTROIDE)
        recuperados = cur.rowcount
        conn.commit()
        if recuperados:
            self.stdout.write(f'🔧 Centroide recuperado pelo CEP das empresas: {recuperados}')

    # --------------------------------------------------------------- resumo
    def _resumo(self, cur):
        cur.execute(SQL_RESUMO)
        total, com_ibge, com_centroide, sem_uf, sem_ibge = cur.fetchone()
        self.stdout.write(f'📈 c_municipios_geo: {total:,} municipios | '
                          f'{com_ibge:,} com codigo IBGE | '
                          f'{com_centroide:,} com centroide | '
                          f'{sem_uf} sem UF | {sem_ibge} sem IBGE')
        cur.execute('SELECT cd_municipio, nome_tom, uf FROM c_municipios_geo '
                    'WHERE cd_ibge IS NULL ORDER BY cd_municipio')
        for linha in cur.fetchall():
            self.stdout.write(f'   sem IBGE: TOM {linha[0]} - {linha[1]}/{linha[2]}')

    def _exportar(self, cur, caminho):
        """CSV (utf-8) da tabela, para COPY na VPS."""
        cur.execute('SELECT cd_municipio, cd_ibge, nome, nome_tom, uf, '
                    'latitude, longitude, n_ceps '
                    'FROM c_municipios_geo ORDER BY cd_municipio')
        with io.open(caminho, 'w', encoding='utf-8', newline='') as f:
            escritor = csv.writer(f, delimiter=';', lineterminator='\n')
            escritor.writerow(['cd_municipio', 'cd_ibge', 'nome', 'nome_tom', 'uf',
                               'latitude', 'longitude', 'n_ceps'])
            for row in cur.fetchall():
                escritor.writerow(['' if v is None else v for v in row])
        self.stdout.write(self.style.SUCCESS(f'💾 CSV exportado: {caminho}'))
