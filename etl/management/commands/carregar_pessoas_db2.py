# etl/management/commands/carregar_pessoas_db2.py
"""
Carrega a tabela AROSASSE1.PESSOAS (DB2/IBM i) com dados aleatórios,
usando sócios PF do Postgres (c_socios) e o endereço da matriz da empresa
(c_estabelecimentos + c_municipios).

Uso:
    # Dry-run (gera e imprime, sem INSERT):
    python manage.py carregar_pessoas_db2 --quantidade 10 --test

    # Carga real:
    python manage.py carregar_pessoas_db2 --quantidade 1000000 --batch 500
"""
import logging
import random
import re
import time
import unicodedata
from datetime import date, timedelta

import jpype
from psycopg2 import extras

from django.core.management.base import BaseCommand

from etl.conexoes.postgres import get_postgres_connection
from etl.conexoes.as400 import get_as400_connection

logger = logging.getLogger(__name__)

UF_BRASIL = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
    'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
    'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO',
]

DOMINIOS_EMAIL = ['exemplo.com', 'email.com', 'teste.com.br', 'mail.com']

COLUNAS_DB2 = (
    'CPF', 'NOMECOMPL', 'EMAIL', 'TELEFONE', 'DATA_NASC', 'STATUS', 'CEP',
    'ESTADO', 'CIDADE', 'BAIRRO', 'ENDERECO', 'NUMERO', 'COMPLEMENTO',
)

SQL_AMOSTRA = """
WITH alvo AS (
    SELECT s.nome_ou_razao_social, s.cnpj_basico
    FROM c_socios s
    WHERE s.id_tipo_socio = 2
      AND btrim(s.nome_ou_razao_social) <> ''
    ORDER BY random()
    LIMIT %s
)
SELECT a.nome_ou_razao_social,
       e.tip_logradouro, e.logradouro, e.numero, e.complemento,
       e.bairro, e.cep, e.uf, e.email, e.ddd_1, e.tel_1,
       m.municipio
FROM alvo a
LEFT JOIN c_estabelecimentos e
       ON e.cnpj_basico = a.cnpj_basico AND e.cd_matriz_filial = 1
LEFT JOIN c_municipios m ON m.cd_municipio = e.municipio
"""


def _so_digitos(valor):
    return re.sub(r'\D', '', str(valor or ''))


def _truncar(valor, tamanho):
    return re.sub(r'\s+', ' ', str(valor or '')).strip()[:tamanho]


def _sem_acentos(texto):
    return unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')


class Command(BaseCommand):
    help = 'Carrega AROSASSE1.PESSOAS (DB2) com dados aleatorios de socios + empresas'

    def add_arguments(self, parser):
        parser.add_argument('--quantidade', type=int, required=True,
                            help='Quantidade de registros a inserir')
        parser.add_argument('--batch', type=int, default=2000,
                            help='Tamanho do lote no JDBC (padrao: 2000)')
        parser.add_argument('--seed', type=int, default=None,
                            help='Seed do gerador aleatorio (reprodutivel)')
        parser.add_argument('--progress', type=int, default=10000,
                            help='Progresso a cada N registros')
        parser.add_argument('--test', action='store_true',
                            help='Dry-run: apenas gera e imprime, sem INSERT no DB2')

    # ------------------------------------------------------------------ #
    # Geradores de dados
    # ------------------------------------------------------------------ #
    def _gerar_email(self, nome, rng):
        partes = [p for p in re.split(r'\s+', _sem_acentos(nome).lower()) if p]
        if len(partes) >= 2:
            login = partes[0] + '.' + partes[-1]
        elif len(partes) == 1:
            login = partes[0]
        else:
            login = 'pessoa'
        login = re.sub(r'[^a-z0-9._]', '', login) or 'pessoa'
        return f"{login}{rng.randint(1, 999):03d}@{rng.choice(DOMINIOS_EMAIL)}"

    def _gerar_telefone(self, rng):
        ddd = rng.randint(11, 99)
        if rng.random() < 0.7:
            return f"{ddd}9{rng.randint(10000000, 99999999)}"  # celular (11 digitos)
        return f"{ddd}{rng.randint(20000000, 99999999)}"        # fixo (10 digitos)

    def _gerar_cep(self, rng):
        return f"{rng.randint(1, 9)}{rng.randint(0, 9999999):07d}"

    def _gerar_numero(self, rng):
        return str(rng.randint(1, 9999))

    def _gerar_endereco(self, rng):
        tipo = rng.choice(['RUA', 'AVENIDA', 'TRAVESSA', 'ALAMEDA'])
        nome = rng.choice(['BRASIL', 'DAS FLORES', 'SAO PAULO', 'DOS IPES',
                           'CENTRAL', 'PRINCIPAL'])
        return f"{tipo} {nome}"

    def _gerar_data_nasc(self, rng):
        dias = rng.randint(0, (date(2005, 12, 31) - date(1940, 1, 1)).days)
        return date(1940, 1, 1) + timedelta(days=dias)

    def _digito_cpf(self, digitos, peso_inicial):
        soma = 0
        peso = peso_inicial
        for d in digitos:
            soma += d * peso
            peso -= 1
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    def _gerar_cpf(self, rng):
        """Gera CPF valido (9 digitos + 2 DV) e unico (fonte cnpj_cpf esta mascarada)."""
        while True:
            base = [rng.randint(0, 9) for _ in range(9)]
            d1 = self._digito_cpf(base, 10)
            d2 = self._digito_cpf(base + [d1], 11)
            cpf = ''.join(map(str, base + [d1, d2]))
            if len(set(cpf)) == 1:
                continue  # evita CPFs como 00000000000 / 11111111111
            if cpf not in self._cpfs_usados:
                self._cpfs_usados.add(cpf)
                return cpf

    def _gerar_pessoa(self, row, rng):
        """Recebe uma linha do Postgres (RealDictRow) e devolve o dict do PESSOAS."""
        cpf = self._gerar_cpf(rng)
        nome = _truncar(row['nome_ou_razao_social'], 100) or 'SEM NOME'

        # EMAIL
        email = _truncar(row['email'], 100)
        if not email or '@' not in email:
            email = self._gerar_email(nome, rng)

        # TELEFONE
        ddd = _so_digitos(row['ddd_1'])
        tel = _so_digitos(row['tel_1'])
        telefone = ''
        if len(ddd) >= 2 and len(tel) >= 8:
            telefone = (ddd[:2] + tel)[:15]
        if not telefone:
            telefone = self._gerar_telefone(rng)

        # DATA_NASC
        data_nasc = self._gerar_data_nasc(rng)

        # STATUS
        status = 'A' if rng.random() < 0.9 else 'I'

        # CEP (CHAR(8))
        cep = _so_digitos(row['cep'])
        if len(cep) != 8:
            cep = self._gerar_cep(rng)

        # ESTADO (CHAR(2))
        estado = _truncar(row['uf'], 2).upper()
        if estado not in UF_BRASIL:
            estado = rng.choice(UF_BRASIL)

        # CIDADE / BAIRRO
        cidade = _truncar(row['municipio'], 50)
        bairro = _truncar(row['bairro'], 50)

        # ENDERECO
        tip_log = _truncar(row['tip_logradouro'], 40)
        logr = _truncar(row['logradouro'], 100)
        endereco = _truncar(f"{tip_log} {logr}", 100)
        if not endereco:
            endereco = self._gerar_endereco(rng)

        # NUMERO
        numero = _so_digitos(row['numero'])[:10]
        if not numero:
            numero = self._gerar_numero(rng)

        # COMPLEMENTO
        complemento = _truncar(row['complemento'], 50)

        return {
            'cpf': cpf,
            'nome': nome,
            'email': email,
            'telefone': telefone,
            'data_nasc': data_nasc,
            'status': status,
            'cep': cep,
            'estado': estado,
            'cidade': cidade,
            'bairro': bairro,
            'endereco': endereco,
            'numero': numero,
            'complemento': complemento,
        }

    # ------------------------------------------------------------------ #
    # INSERT no DB2
    # ------------------------------------------------------------------ #
    def _inserir_lote(self, conn, pessoas):
        sql = ("INSERT INTO AROSASSE1.PESSOAS "
               "(CPF, NOMECOMPL, EMAIL, TELEFONE, DATA_NASC, STATUS, CEP, ESTADO, "
               "CIDADE, BAIRRO, ENDERECO, NUMERO, COMPLEMENTO) "
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        pstmt = conn.prepareStatement(sql)
        java_sql_date = jpype.JClass('java.sql.Date')
        try:
            for p in pessoas:
                pstmt.setString(1, p['cpf'])
                pstmt.setString(2, p['nome'])
                pstmt.setString(3, p['email'])
                pstmt.setString(4, p['telefone'])
                pstmt.setDate(5, java_sql_date.valueOf(p['data_nasc'].isoformat()))
                pstmt.setString(6, p['status'])
                pstmt.setString(7, p['cep'])
                pstmt.setString(8, p['estado'])
                pstmt.setString(9, p['cidade'])
                pstmt.setString(10, p['bairro'])
                pstmt.setString(11, p['endereco'])
                pstmt.setString(12, p['numero'])
                pstmt.setString(13, p['complemento'])
                pstmt.addBatch()
            pstmt.executeBatch()
        finally:
            pstmt.close()

    # ------------------------------------------------------------------ #
    # Handle principal
    # ------------------------------------------------------------------ #
    def handle(self, *args, **options):
        quantidade = options['quantidade']
        batch = options['batch']
        seed = options['seed']
        progress = options['progress']
        test = options['test']

        if quantidade <= 0:
            self.stdout.write(self.style.ERROR('❌ --quantidade deve ser > 0'))
            return

        rng = random.Random(seed)
        self._cpfs_usados = set()
        inicio = time.time()

        pg_conn = get_postgres_connection()

        # ---- MODO TESTE ----
        if test:
            self.stdout.write(self.style.WARNING('🧪 MODO TESTE: nada sera inserido no DB2.'))
            self._executar_teste(pg_conn, quantidade, rng)
            pg_conn.close()
            return

        # ---- CARGA REAL ----
        self.stdout.write('🔌 Conectando ao DB2 (IBM i)...')
        # Obs: PESSOAS nao e journaled -> nao usar commitment control
        # (setAutoCommit(False) gera SQL7008). Mantemos autoCommit=True.
        as400_conn = get_as400_connection()

        inseridos = 0
        erros = 0
        lote = []
        ultimo_progresso = 0

        try:
            with pg_conn.cursor(name='amostra_pessoas',
                                cursor_factory=extras.RealDictCursor) as cur:
                cur.itersize = batch
                cur.execute(SQL_AMOSTRA, (quantidade,))

                for i, row in enumerate(cur, 1):
                    lote.append(self._gerar_pessoa(row, rng))

                    if len(lote) >= batch:
                        if self._despachar_lote(as400_conn, lote):
                            inseridos += len(lote)
                        else:
                            erros += len(lote)
                        lote = []

                    if i - ultimo_progresso >= progress or i == quantidade:
                        self._imprimir_progresso(i, quantidade, inseridos, erros, inicio)
                        ultimo_progresso = i

                # restante
                if lote:
                    if self._despachar_lote(as400_conn, lote):
                        inseridos += len(lote)
                    else:
                        erros += len(lote)
        finally:
            try:
                as400_conn.close()
            except Exception:
                pass
            pg_conn.close()

        tempo_total = time.time() - inicio
        self.stdout.write('=' * 60)
        self.stdout.write(self.style.SUCCESS('✅ Carga concluída!'))
        self.stdout.write(f'✅ Inseridos: {inseridos:,}')
        self.stdout.write(f'❌ Erros: {erros:,}')
        self.stdout.write(f'⏱️  Tempo total: {tempo_total:.1f}s')
        if tempo_total > 0:
            self.stdout.write(f'🚀 Velocidade média: {inseridos / tempo_total:,.0f} reg/s')
        self.stdout.write('=' * 60)

    # ------------------------------------------------------------------ #
    # Helpers do handle
    # ------------------------------------------------------------------ #
    def _despachar_lote(self, conn, lote):
        try:
            self._inserir_lote(conn, lote)
            return True
        except Exception as exc:
            msg = str(exc)
            self.stdout.write(self.style.ERROR(f'❌ Erro no lote ({len(lote)} regs): {msg}'))
            if 'SQL0904' in msg:  # limite de recurso (cota de armazenamento) -> aborta
                self.stdout.write(self.style.ERROR(
                    '⛔ Limite de recursos do DB2 atingido. Abortando a carga.'
                ))
                raise
            return False

    def _imprimir_progresso(self, processados, total, inseridos, erros, inicio):
        tempo = time.time() - inicio
        velocidade = processados / tempo if tempo > 0 else 0
        self.stdout.write(
            f'   📊 {processados:,}/{total:,} | '
            f'✅ inseridos: {inseridos:,} | ❌ erros: {erros:,} | '
            f'🚀 {velocidade:,.0f} reg/s'
        )

    def _executar_teste(self, pg_conn, quantidade, rng):
        n = 0
        with pg_conn.cursor(name='amostra_teste',
                            cursor_factory=extras.RealDictCursor) as cur:
            cur.itersize = 100
            cur.execute(SQL_AMOSTRA, (quantidade,))
            for row in cur:
                p = self._gerar_pessoa(row, rng)
                n += 1
                self.stdout.write(
                    f"[{n:03d}] CPF={p['cpf']} | {p['nome'][:60]} | "
                    f"{p['email'][:38]} | tel={p['telefone']} | nasc={p['data_nasc']} | "
                    f"st={p['status']} | CEP={p['cep']} | {p['estado']} | "
                    f"{p['cidade'][:18]} | {p['endereco'][:36]}, {p['numero']}"
                )
        self.stdout.write(self.style.SUCCESS(f'✅ Gerados {n} registros (nenhum INSERT realizado).'))



