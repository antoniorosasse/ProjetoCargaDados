# /home/neto/ProjetoCargaDados/etl/management/commands/contar_estab_por_cep_situacao.py
"""Cria/atualiza c_ceps_estab_sit: estabelecimentos por CEP e situacao cadastral.

Complementa `contar_estab_por_cep` (c_ceps_estab). La o agregado e por CEP e a
tela /proximidade/ do cnpj_interface nao consegue somar o total quando o usuario
filtra por situacao; aqui o agregado e por (CEP, situacao), entao o total sai
restrito ao filtro pelo mesmo custo (~30-60 ms: 1 busca de CEP no indice).

Nao substitui c_ceps_estab: a tela usa c_ceps_estab quando nao ha filtro
(1 linha por CEP, 1,18 M) e c_ceps_estab_sit quando ha.

Tabela (uma linha por CEP que tem aquela situacao):

    cep             varchar(8) NOT NULL  (8 digitos, sem hifen)
    cd_sit_cadastro smallint   NOT NULL  (1 nula, 2 ativa, 3 suspensa, 4 inapta,
                                          8 baixada)
    n_matrizes      integer              cd_matriz_filial = 1
    n_total         integer              matrizes + filiais

    PRIMARY KEY (cep, cd_sit_cadastro)

Roda depois de importar_cnpj e de contar_estab_por_cep. A troca e feita numa
transacao (monta em tabela temporaria e so entao substitui), entao a tela nunca
ve a tabela pela metade.

Uso:
    python manage.py contar_estab_por_cep_situacao --criar
    python manage.py contar_estab_por_cep_situacao            # recalcula
    python manage.py contar_estab_por_cep_situacao --exportar /tmp/ceps_estab_sit.csv
"""
import csv
import io
import time

from django.core.management.base import BaseCommand

from etl.conexoes.postgres import get_postgres_connection

SQL_CRIAR = """
    CREATE TABLE IF NOT EXISTS c_ceps_estab_sit (
        cep             varchar(8) NOT NULL,
        cd_sit_cadastro smallint   NOT NULL,
        n_matrizes      integer    NOT NULL DEFAULT 0,
        n_total         integer    NOT NULL DEFAULT 0,
        PRIMARY KEY (cep, cd_sit_cadastro)
    )
"""

SQL_TEMP = """
    CREATE TEMP TABLE tmp_ceps_estab_sit (
        cep             varchar(8) NOT NULL,
        cd_sit_cadastro smallint   NOT NULL,
        n_matrizes      integer    NOT NULL,
        n_total         integer    NOT NULL,
        PRIMARY KEY (cep, cd_sit_cadastro)
    ) ON COMMIT DROP
"""

# Mesmo recorte de contar_estab_por_cep: estabelecimento sem CEP nao entra (nao
# ha chave para somar). right(cep, 8) mantem a chave igual a de c_ceps, que e
# quem a tela consulta.
SQL_AGG = """
    INSERT INTO tmp_ceps_estab_sit (cep, cd_sit_cadastro, n_matrizes, n_total)
    SELECT right(cep, 8) AS cep,
           coalesce(cd_sit_cadastro, 0)::smallint AS cd_sit_cadastro,
           count(*) FILTER (WHERE cd_matriz_filial = 1)::int AS n_matrizes,
           count(*)::int                                      AS n_total
      FROM c_estabelecimentos
     WHERE cep IS NOT NULL
       AND cep <> ''
     GROUP BY 1, 2
"""

SQL_TROCAR = """
    DELETE FROM c_ceps_estab_sit;
    INSERT INTO c_ceps_estab_sit (cep, cd_sit_cadastro, n_matrizes, n_total)
    SELECT cep, cd_sit_cadastro, n_matrizes, n_total FROM tmp_ceps_estab_sit
"""

SQL_RESUMO = """
    SELECT count(*)        AS pares,
           sum(n_matrizes) AS matrizes,
           sum(n_total)    AS total,
           pg_size_pretty(pg_total_relation_size('c_ceps_estab_sit')) AS tamanho
      FROM c_ceps_estab_sit
"""

SQL_POR_SITUACAO = """
    SELECT cd_sit_cadastro, count(*) AS ceps, sum(n_total) AS total
      FROM c_ceps_estab_sit
     GROUP BY cd_sit_cadastro
     ORDER BY cd_sit_cadastro
"""


class Command(BaseCommand):
    help = ('Cria/atualiza c_ceps_estab_sit (estabelecimentos por CEP e '
            'situacao cadastral)')

    def add_arguments(self, parser):
        parser.add_argument('--criar', action='store_true',
                            help='Cria a tabela c_ceps_estab_sit se nao existir (DDL)')
        parser.add_argument('--exportar', type=str,
                            help='Grava um CSV (utf-8) da tabela para copiar na VPS')
        parser.add_argument('--timeout', type=int, default=3600,
                            help='statement_timeout em segundos (padrao: 3600)')

    def handle(self, *args, **options):
        inicio = time.time()
        conn = get_postgres_connection()
        cur = conn.cursor()

        try:
            if options['criar']:
                cur.execute(SQL_CRIAR)
                conn.commit()
                self.stdout.write(self.style.SUCCESS(
                    '✅ Tabela c_ceps_estab_sit verificada/criada'))

            cur.execute("SELECT to_regclass('c_ceps_estab_sit')")
            if cur.fetchone()[0] is None:
                self.stdout.write(self.style.ERROR(
                    '❌ c_ceps_estab_sit não existe: rode com --criar'))
                return

            cur.execute(f"SET statement_timeout = '{options['timeout']}s'")
            cur.execute("SET max_parallel_workers_per_gather = 4")

            self.stdout.write('🔢 Agregando c_estabelecimentos por CEP e situação...')
            t = time.time()
            cur.execute(SQL_TEMP)
            cur.execute(SQL_AGG)
            self.stdout.write(
                f'   temporária: {cur.rowcount:,} pares CEP/situação em '
                f'{time.time() - t:.1f}s')

            t = time.time()
            cur.execute(SQL_TROCAR)
            conn.commit()
            self.stdout.write(f'   troca aplicada em {time.time() - t:.1f}s')

            cur.execute('ANALYZE c_ceps_estab_sit')
            conn.commit()

            # DELETE + INSERT deixa bloat (mesmo motivo do contar_estab_por_cep).
            try:
                conn.autocommit = True
                cur.execute('VACUUM ANALYZE c_ceps_estab_sit')
            except Exception as e:  # nao e erro fatal: so espaco em disco
                self.stdout.write(self.style.WARNING(
                    f'⚠️  VACUUM não executado: {e}'))

            cur.execute(SQL_RESUMO)
            pares, matrizes, total, tamanho = cur.fetchone()
            self.stdout.write(self.style.SUCCESS(
                f'📈 c_ceps_estab_sit: {pares:,} pares | {matrizes:,} matrizes | '
                f'{total:,} estabelecimentos | {tamanho}'))

            cur.execute(SQL_POR_SITUACAO)
            for cd_sit, ceps, total_sit in cur.fetchall():
                self.stdout.write(
                    f'   cd_sit_cadastro={cd_sit}: {ceps:,} CEPs | '
                    f'{total_sit:,} estabelecimentos')

            self.stdout.write(f'⏱️  Tempo total: {time.time() - inicio:.1f}s')

            if options['exportar']:
                self._exportar(cur, options['exportar'])
        finally:
            cur.close()
            conn.close()

    def _exportar(self, cur, caminho):
        """CSV (utf-8, ';') para COPY na VPS, igual ao carregar_municipios."""
        cur.execute('SELECT cep, cd_sit_cadastro, n_matrizes, n_total '
                    'FROM c_ceps_estab_sit ORDER BY cep, cd_sit_cadastro')
        with io.open(caminho, 'w', encoding='utf-8', newline='') as f:
            escritor = csv.writer(f, delimiter=';', lineterminator='\n')
            escritor.writerow(['cep', 'cd_sit_cadastro', 'n_matrizes', 'n_total'])
            escritor.writerows(cur.fetchall())
        self.stdout.write(self.style.SUCCESS(f'💾 CSV exportado: {caminho}'))
