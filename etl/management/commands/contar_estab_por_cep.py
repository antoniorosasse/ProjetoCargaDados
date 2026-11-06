# /home/neto/ProjetoCargaDados/etl/management/commands/contar_estab_por_cep.py
"""Cria/atualiza c_ceps_estab: quantos estabelecimentos existem em cada CEP.

O agregado resolve dois problemas da tela /proximidade/ do cnpj_interface:

    total de empresas no raio  - o COUNT direto no raio custa 5-15 s (bitmap
                                 scan em 38,5 M de estabelecimentos); somando
                                 c_ceps_estab o total sai junto com a pagina
                                 (~30-60 ms a mais)
    paginacao "Pagina X de Y"  - sem o total a tela so podia dizer "ha mais"

Tabela (1,18 M linhas, ~105 MB):

    cep         varchar(8) PRIMARY KEY  (8 digitos, sem hifen)
    n_matrizes  integer                 cd_matriz_filial = 1
    n_total     integer                 matrizes + filiais

Roda depois de importar_cnpj: e um GROUP BY em c_estabelecimentos. A troca e
feita numa transacao (monta em tabela temporaria e so entao substitui), entao a
tela nunca ve a tabela pela metade.

Uso:
    python manage.py contar_estab_por_cep --criar
    python manage.py contar_estab_por_cep                 # recalcula
    python manage.py contar_estab_por_cep --exportar /tmp/ceps_estab.csv
"""
import csv
import io
import time

from django.core.management.base import BaseCommand

from etl.conexoes.postgres import get_postgres_connection

SQL_CRIAR = """
    CREATE TABLE IF NOT EXISTS c_ceps_estab (
        cep        varchar(8) PRIMARY KEY,
        n_matrizes integer NOT NULL DEFAULT 0,
        n_total    integer NOT NULL DEFAULT 0
    )
"""

SQL_TEMP = """
    CREATE TEMP TABLE tmp_ceps_estab (
        cep        varchar(8) PRIMARY KEY,
        n_matrizes integer NOT NULL,
        n_total    integer NOT NULL
    ) ON COMMIT DROP
"""

# Estabelecimento sem CEP (149.940 em 2026-11) nao entra: nao ha chave para
# somar. A tela so consulta por CEP de c_ceps, entao o efeito e nulo.
SQL_AGG = """
    INSERT INTO tmp_ceps_estab (cep, n_matrizes, n_total)
    SELECT cep,
           count(*) FILTER (WHERE cd_matriz_filial = 1)::int AS n_matrizes,
           count(*)::int                                      AS n_total
      FROM c_estabelecimentos
     WHERE cep IS NOT NULL
       AND cep <> ''
     GROUP BY cep
"""

SQL_TROCAR = """
    DELETE FROM c_ceps_estab;
    INSERT INTO c_ceps_estab (cep, n_matrizes, n_total)
    SELECT cep, n_matrizes, n_total FROM tmp_ceps_estab
"""

SQL_RESUMO = """
    SELECT count(*)          AS ceps,
           sum(n_matrizes)   AS matrizes,
           sum(n_total)      AS total,
           pg_size_pretty(pg_total_relation_size('c_ceps_estab')) AS tamanho
      FROM c_ceps_estab
"""


class Command(BaseCommand):
    help = 'Cria/atualiza c_ceps_estab (contagem de estabelecimentos por CEP)'

    def add_arguments(self, parser):
        parser.add_argument('--criar', action='store_true',
                            help='Cria a tabela c_ceps_estab se nao existir (DDL)')
        parser.add_argument('--exportar', type=str,
                            help='Grava um CSV (utf-8) da tabela para copiar na VPS')
        parser.add_argument('--timeout', type=int, default=1800,
                            help='statement_timeout em segundos (padrao: 1800)')

    def handle(self, *args, **options):
        inicio = time.time()
        conn = get_postgres_connection()
        cur = conn.cursor()

        try:
            if options['criar']:
                cur.execute(SQL_CRIAR)
                conn.commit()
                self.stdout.write(self.style.SUCCESS(
                    '✅ Tabela c_ceps_estab verificada/criada'))

            cur.execute("SELECT to_regclass('c_ceps_estab')")
            if cur.fetchone()[0] is None:
                self.stdout.write(self.style.ERROR(
                    '❌ c_ceps_estab não existe: rode com --criar'))
                return

            cur.execute(f"SET statement_timeout = '{options['timeout']}s'")
            cur.execute("SET max_parallel_workers_per_gather = 4")

            self.stdout.write('🔢 Agregando c_estabelecimentos por CEP...')
            t = time.time()
            cur.execute(SQL_TEMP)
            cur.execute(SQL_AGG)
            self.stdout.write(f'   temporária: {cur.rowcount:,} CEPs em {time.time() - t:.1f}s')

            t = time.time()
            cur.execute(SQL_TROCAR)
            conn.commit()
            self.stdout.write(f'   troca aplicada em {time.time() - t:.1f}s')

            cur.execute('ANALYZE c_ceps_estab')
            conn.commit()

            # DELETE + INSERT deixa bloat na tabela (105 MB -> 191 MB na primeira
            # recarga); o VACUUM devolve o espaco para reuso e exige autocommit.
            try:
                conn.autocommit = True
                cur.execute('VACUUM ANALYZE c_ceps_estab')
            except Exception as e:  # nao e erro fatal: so espaco em disco
                self.stdout.write(self.style.WARNING(
                    f'⚠️  VACUUM não executado: {e}'))

            cur.execute(SQL_RESUMO)
            ceps, matrizes, total, tamanho = cur.fetchone()
            self.stdout.write(self.style.SUCCESS(
                f'📈 c_ceps_estab: {ceps:,} CEPs | {matrizes:,} matrizes | '
                f'{total:,} estabelecimentos | {tamanho}'))
            self.stdout.write(f'⏱️  Tempo total: {time.time() - inicio:.1f}s')

            if options['exportar']:
                self._exportar(cur, options['exportar'])
        finally:
            cur.close()
            conn.close()

    def _exportar(self, cur, caminho):
        """CSV (utf-8, ';') para COPY na VPS, igual ao carregar_municipios."""
        cur.execute('SELECT cep, n_matrizes, n_total FROM c_ceps_estab ORDER BY cep')
        with io.open(caminho, 'w', encoding='utf-8', newline='') as f:
            escritor = csv.writer(f, delimiter=';', lineterminator='\n')
            escritor.writerow(['cep', 'n_matrizes', 'n_total'])
            escritor.writerows(cur.fetchall())
        self.stdout.write(self.style.SUCCESS(f'💾 CSV exportado: {caminho}'))
