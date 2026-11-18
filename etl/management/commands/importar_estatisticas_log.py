"""Semeia as estatisticas a partir dos logs de uma carga ja executada.

Le os logs do carregar_base_cnpj (truncar, carregar, indices), o log dos
contadores por CEP e o DDL salvo antes da recarga, e monta as linhas de
Carga/EtapaCarga no banco do admin (django_internal) -- assim a pagina
/admin/etl/carga/estatisticas/ nasce com o historico da carga que ja rodou.

    python manage.py importar_estatisticas_log --rotulo 2026-11
"""

import os
import re
from datetime import datetime

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from etl import estatisticas
from etl.models import Carga, EtapaCarga
from etl.processadores.cnpj_processor import PADROES_ARQUIVO_TABELA


RE_TRUNCAR = re.compile(r'🧹\s+(c_\w+):\s+([\d,]+)\s+linhas removidas\s+\(([\d.]+)s\)')
RE_ARQUIVO = re.compile(r'✅\s+(\S+\.(?:zip|parquet)):\s+([\d,]+)\s+linhas\s+\(([\d.]+)s\)')
RE_TABELA_CARGA = re.compile(r'▶️\s+(c_\w+)')
RE_TOTAL_TABELA = re.compile(r'⏱️\s+(c_\w+):\s+([\d,]+)\s+linhas em\s+([\d.]+)s')
RE_OBJETO = re.compile(r'🔨\s+([\d.]+)s\s+\|\s+(.+)$')
RE_ANALYZE = re.compile(r'📊\s+ANALYZE\s+(c_\w+)\s+\(([\d.]+)s\)')
RE_CONTADOR = re.compile(r'📈\s+(c_\w+):\s+([\d,]+)\s+(?:CEPs|pares)')
RE_TEMPO_TOTAL = re.compile(r'⏱️\s+Tempo total:\s+([\d.]+)s')
RE_CONSTRAINT = re.compile(
    r'ALTER TABLE\s+(\S+)\s+ADD CONSTRAINT\s+(\S+)', re.IGNORECASE)
RE_INDICE = re.compile(
    r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(\S+)', re.IGNORECASE)

# fallback quando o DDL nao esta acessivel (nome do indice -> tabela)
PREFIXOS_TABELA = [
    ('c_empresas', 'c_empresas'),
    ('idx_empresa', 'c_empresas'),
    ('idx_estab', 'c_estabelecimentos'),
    ('c_estabelecimentos', 'c_estabelecimentos'),
    ('idx_simples', 'c_simples'),
    ('c_simples', 'c_simples'),
    ('idx_socios', 'c_socios'),
    ('c_socios', 'c_socios'),
]


def _numero(texto):
    return int(str(texto).replace(',', '').replace('.', '') or 0)


class Command(BaseCommand):
    help = 'Importa as estatisticas de uma carga ja executada a partir dos logs'

    def add_arguments(self, parser):
        parser.add_argument('--rotulo', default='2026-11',
                            help='Rotulo do lote da RFB (ex.: 2026-11)')
        parser.add_argument('--pasta-logs', default='/tmp',
                            help='Pasta onde estao os logs da carga')
        parser.add_argument('--log-truncar', default='fase_socios.log')
        parser.add_argument('--log-ddl', default='fase_ddl.log')
        parser.add_argument('--log-carga', nargs='+',
                            default=['carga_socios.log', 'carga_resto.log'])
        parser.add_argument('--log-indices', default='idx2.log')
        parser.add_argument('--log-contadores', default='contadores.log')
        parser.add_argument('--ddl', default=(
            '/mnt/NOTE_EXT4_SDD/DocumentosSSD/cnpj_lote/ddl_carga.sql'))
        parser.add_argument('--pasta-dados', default=(
            '/mnt/NOTE_EXT4_SDD/DocumentosSSD/cnpj_lote/2026-11'))
        parser.add_argument('--recriar', action='store_true',
                            help='Apaga as cargas com este rotulo antes de importar')
        parser.add_argument('--ajuste', nargs='*', default=[], metavar='TABELA=LINHAS',
                            help='Corrige a quantidade de uma etapa "carregar" '
                                 '(ex.: c_empresas=71068585 apos remover duplicata)')

    # ------------------------------------------------------------------ leitura

    def _ler(self, pasta, nome, obrigatorio=False):
        caminho = os.path.join(pasta, nome)
        if not os.path.exists(caminho):
            if obrigatorio:
                raise CommandError(f'Log nao encontrado: {caminho}')
            return ''
        with open(caminho, encoding='utf-8', errors='replace') as fh:
            return fh.read()

    @staticmethod
    def _mtimes(caminhos):
        """(inicio, fim) pela data de modificacao dos logs usados"""
        fuso = timezone.get_current_timezone()
        tempos = [os.path.getmtime(c) for c in caminhos if os.path.exists(c)]
        if not tempos:
            agora = timezone.now()
            return agora, agora
        return (datetime.fromtimestamp(min(tempos), fuso),
                datetime.fromtimestamp(max(tempos), fuso))

    @staticmethod
    def _tabela_do_arquivo(nome):
        for padrao, tabela in PADROES_ARQUIVO_TABELA.items():
            if re.match(padrao, nome, re.IGNORECASE):
                return tabela
        return ''

    @staticmethod
    def _tabela_do_objeto(nome, mapa_ddl):
        if nome in mapa_ddl:
            return mapa_ddl[nome]
        for prefixo, tabela in PREFIXOS_TABELA:
            if nome.startswith(prefixo):
                return tabela
        return ''

    @staticmethod
    def _nome_do_objeto(sql):
        m = RE_CONSTRAINT.match(sql)
        if m:
            return m.group(2)
        m = RE_INDICE.match(sql)
        return m.group(1) if m else ''

    def _ler_ddl(self, caminho):
        """nome do objeto -> tabela (DDL salvo antes da recarga)"""
        mapa = {}
        if not os.path.exists(caminho):
            return mapa
        with open(caminho, encoding='utf-8') as fh:
            for linha in fh:
                linha = linha.strip()
                if not linha or linha.startswith('--'):
                    continue
                m = RE_CONSTRAINT.match(linha)
                if m:
                    mapa[m.group(2)] = m.group(1).split('.')[-1]
                    continue
                m = re.match(r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(\S+).*?\sON\s+(?:ONLY\s+)?(\S+)',
                             linha, re.IGNORECASE)
                if m:
                    mapa[m.group(1)] = m.group(2).split('.')[-1]
        return mapa

    # ------------------------------------------------------------------ parsers

    def _parse_truncar(self, texto, etapas):
        for m in RE_TRUNCAR.finditer(texto):
            etapas.append((EtapaCarga.TRUNCAR, m.group(1), '',
                           _numero(m.group(2)), float(m.group(3))))

    def _parse_carga(self, texto, etapas):
        tabela_atual = ''
        for linha in texto.splitlines():
            m = RE_TABELA_CARGA.search(linha)
            if m:
                tabela_atual = m.group(1)
                continue
            m = RE_ARQUIVO.search(linha)
            if m:
                nome = m.group(1)
                etapas.append((EtapaCarga.ARQUIVO,
                               self._tabela_do_arquivo(nome) or tabela_atual,
                               nome, _numero(m.group(2)), float(m.group(3))))
                continue
            m = RE_TOTAL_TABELA.search(linha)
            if m:
                etapas.append((EtapaCarga.CARREGAR, m.group(1), '',
                               _numero(m.group(2)), float(m.group(3))))

    def _parse_indices(self, texto, etapas, mapa_ddl):
        for linha in texto.splitlines():
            m = RE_ANALYZE.search(linha)
            if m:
                etapas.append((EtapaCarga.ANALYZE, m.group(1), '', 0,
                               float(m.group(2))))
                continue
            m = RE_OBJETO.search(linha)
            if not m:
                continue
            nome = self._nome_do_objeto(m.group(2))
            if not nome:
                continue
            etapas.append((EtapaCarga.INDICE,
                           self._tabela_do_objeto(nome, mapa_ddl),
                           nome, 0, float(m.group(1))))

    def _parse_contadores(self, texto, etapas):
        pendente = None
        for linha in texto.splitlines():
            m = RE_CONTADOR.search(linha)
            if m:
                pendente = (m.group(1), _numero(m.group(2)))
                continue
            m = RE_TEMPO_TOTAL.search(linha)
            if m and pendente:
                etapas.append((EtapaCarga.CONTADOR, pendente[0], '',
                               pendente[1], float(m.group(1))))
                pendente = None

    @staticmethod
    def _fmt(segundos):
        segundos = float(segundos or 0)
        minutos, resto = divmod(int(round(segundos)), 60)
        horas, minutos = divmod(minutos, 60)
        if horas:
            return f'{horas}h{minutos:02d}'
        return f'{minutos}min {resto:02d}s' if minutos else f'{segundos:.1f}s'

    # ------------------------------------------------------------------- handle

    def handle(self, *args, **options):
        pasta = options['pasta_logs']

        if options['recriar']:
            removidas = Carga.objects.filter(rotulo=options['rotulo'])
            total = removidas.count()
            removidas.delete()
            self.stdout.write(f'🗑️  recriar: {total} carga(s) do rotulo '
                              f'{options["rotulo"]} removida(s)')

        caminhos = [os.path.join(pasta, nome) for nome in
                    [options['log_ddl'], options['log_truncar'],
                     options['log_indices'], options['log_contadores']]
                    + list(options['log_carga'])]
        iniciado, finalizado = self._mtimes(caminhos)

        mapa_ddl = self._ler_ddl(options['ddl'])
        etapas = []

        texto_ddl = self._ler(pasta, options['log_ddl'])
        m = re.search(r'(\d+)\s+constraint\(s\) e\s+(\d+)\s+indice\(s\) dropados',
                      texto_ddl)
        if m:
            etapas.append((EtapaCarga.DDL, '', 'drop de indices/view',
                           int(m.group(1)) + int(m.group(2)), 0.0))

        self._parse_truncar(self._ler(pasta, options['log_truncar']), etapas)
        for nome_log in options['log_carga']:
            self._parse_carga(self._ler(pasta, nome_log), etapas)
        self._parse_indices(self._ler(pasta, options['log_indices']), etapas, mapa_ddl)
        self._parse_contadores(self._ler(pasta, options['log_contadores']), etapas)

        if not etapas:
            raise CommandError('Nenhuma etapa encontrada nos logs -- '
                               'confira --pasta-logs e os nomes dos logs')

        observacao = 'Importado dos logs por importar_estatisticas_log'
        ajustes = {}
        for item in options['ajuste']:
            tabela, _, valor = item.partition('=')
            ajustes[tabela.strip()] = _numero(valor)

        if ajustes:
            corrigidas = []
            for etapa in etapas:
                if etapa[0] == EtapaCarga.CARREGAR and etapa[1] in ajustes:
                    etapa = (etapa[0], etapa[1], etapa[2], ajustes[etapa[1]], etapa[4])
                corrigidas.append(etapa)
            etapas = corrigidas
            detalhes = ', '.join(f'{tabela}={numero:,}'.replace(',', '.')
                                 for tabela, numero in ajustes.items())
            observacao += f' | quantidade corrigida pos-carga ({detalhes})'

        carga = Carga.objects.create(
            rotulo=options['rotulo'][:60],
            pasta=options['pasta_dados'][:255],
            host=os.uname().nodename[:80],
            iniciado_em=iniciado,
            finalizado_em=finalizado,
            status=Carga.STATUS_CONCLUIDA,
            observacao=observacao,
        )
        for etapa, tabela, detalhe, quantidade, duracao in etapas:
            estatisticas.registrar(carga, etapa, tabela, detalhe, quantidade, duracao)

        numeros = f'{carga.linhas_carregadas:,}'.replace(',', '.')
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'✅ Carga {carga.rotulo} registrada'))
        self.stdout.write(
            f'   inicio {timezone.localtime(carga.iniciado_em):%d/%m/%Y %H:%M}'
            f' | fim {timezone.localtime(carga.finalizado_em):%d/%m/%Y %H:%M}')
        self.stdout.write(f'   etapas: {carga.etapas.count()}')
        self.stdout.write(f'   linhas: {numeros}')
        self.stdout.write(f'   tempo de carga: {self._fmt(carga.tempo_carga_s)}'
                          f' | indices: {self._fmt(carga.tempo_indices_s)}'
                          f' | total: {self._fmt(carga.duracao_s)}')


