from django.core.management.base import BaseCommand
from etl.processadores.cnpj_processor import CNPJProcessor
from etl.processadores.controle_arquivos import ControleArquivos
import os
import glob
import re
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Importa arquivos Parquet de CNPJ para PostgreSQL e/ou MongoDB'

    def add_arguments(self, parser):
        parser.add_argument('--pasta', type=str, required=True, help='Pasta contendo os arquivos .parquet')
        parser.add_argument('--destino', type=str, choices=['postgres', 'mongo', 'ambos'], default='ambos')
        parser.add_argument('--tabela', type=str, choices=['c_cnaes', 'c_empresas', 'c_estabelecimentos', 
                          'c_motivos', 'c_municipios', 'c_naturezas', 'c_paises', 'c_qualificacoes',
                          'c_simples', 'c_socios'], default=None)
        parser.add_argument('--force', action='store_true', help='Força reprocessamento')
        parser.add_argument('--limit', type=int, default=0, help='Limita número de arquivos')
        parser.add_argument('--batch', type=int, default=10000, help='Tamanho do lote')
        parser.add_argument('--progress', type=int, default=50000, help='Progresso a cada N registros')
        parser.add_argument('--conflict', type=str, choices=['update', 'nothing', 'none'], default='none')
        parser.add_argument('--test', type=int, default=0, help='Modo teste: processa apenas N registros')
        parser.add_argument('--dump', action='store_true', help='Gera arquivo CSV com registros pulados (apenas para MongoDB)')
        # 🔥 NOVO: filtro para aplicar nos dados
        parser.add_argument('--filtro', type=str, default=None, 
                          help='Filtro a aplicar nos dados (ex: "cd_sit_cadastro != 8")')

    def handle(self, *args, **options):
        pasta = options['pasta']
        destino = options['destino']
        force = options['force']
        tabela_especifica = options['tabela']
        limit = options['limit']
        batch_size = options['batch']
        progress_interval = options['progress']
        conflict_strategy = options['conflict']
        test_limit = options['test']
        dump = options['dump']
        filtro = options['filtro']  # 🔥 NOVO

        if not os.path.exists(pasta):
            self.stdout.write(self.style.ERROR(f'❌ Pasta não encontrada: {pasta}'))
            return

        todos_arquivos = glob.glob(os.path.join(pasta, '*.parquet'))
        if not todos_arquivos:
            self.stdout.write(self.style.WARNING(f'⚠️ Nenhum arquivo .parquet encontrado em: {pasta}'))
            return

        self.stdout.write(f'📁 Encontrados {len(todos_arquivos)} arquivos .parquet')

        PADROES_TABELAS = {
            r'^C_Cnaes': 'c_cnaes', r'^C_Empresas': 'c_empresas',
            r'^C_Estabelecimentos': 'c_estabelecimentos', r'^C_Motivos': 'c_motivos',
            r'^C_Municipios': 'c_municipios', r'^C_Naturezas': 'c_naturezas',
            r'^C_Paises': 'c_paises', r'^C_Qualificacoes': 'c_qualificacoes',
            r'^C_Simples': 'c_simples', r'^C_Socios': 'c_socios',
            r'^Cnaes': 'c_cnaes', r'^Empresas': 'c_empresas',
            r'^Estabelecimentos': 'c_estabelecimentos', r'^Motivos': 'c_motivos',
            r'^Municipios': 'c_municipios', r'^Naturezas': 'c_naturezas',
            r'^Paises': 'c_paises', r'^Qualificacoes': 'c_qualificacoes',
            r'^Simples': 'c_simples', r'^Socios': 'c_socios',
        }

        arquivos_por_tabela = {}
        for arquivo in todos_arquivos:
            nome_base = os.path.basename(arquivo)
            tabela_encontrada = None
            for padrao, tabela in PADROES_TABELAS.items():
                if re.match(padrao, nome_base, re.IGNORECASE):
                    tabela_encontrada = tabela
                    break
            if tabela_encontrada:
                arquivos_por_tabela.setdefault(tabela_encontrada, []).append(arquivo)
            else:
                self.stdout.write(self.style.WARNING(f'⚠️ Arquivo não mapeado: {nome_base}'))

        for tabela in arquivos_por_tabela:
            arquivos_por_tabela[tabela].sort(key=lambda x: self._extrair_numero(x))

        if tabela_especifica:
            if tabela_especifica in arquivos_por_tabela:
                tabelas = {tabela_especifica: arquivos_por_tabela[tabela_especifica]}
            else:
                self.stdout.write(self.style.ERROR(f'❌ Tabela {tabela_especifica} não encontrada'))
                return
        else:
            tabelas = arquivos_por_tabela

        bancos = []
        if destino in ['postgres', 'ambos']:
            bancos.append('postgres')
        if destino in ['mongo', 'ambos']:
            bancos.append('mongo')

        self.stdout.write(f'📊 Encontradas {len(tabelas)} tabela(s) para processar')
        self.stdout.write(f'🎯 Destino: {destino}')
        self.stdout.write(f'⚙️  Estratégia de conflito: {conflict_strategy}')
        if filtro:
            self.stdout.write(f'🔍 Filtro: {filtro}')
        self.stdout.write('-' * 60)

        total_geral = 0
        tabelas_processadas = 0

        for tabela_nome, arquivos in tabelas.items():
            self.stdout.write(f'\n📄 Processando tabela: {tabela_nome}')
            self.stdout.write(f'   📁 {len(arquivos)} arquivo(s) encontrado(s)')
            
            if limit > 0 and len(arquivos) > limit:
                arquivos = arquivos[:limit]
                self.stdout.write(f'   ⚠️ Limitado a {limit} arquivo(s)')
            
            total_registros_tabela = 0
            arquivos_processados_tabela = 0
            
            for i, arquivo in enumerate(arquivos, 1):
                nome_arquivo = os.path.basename(arquivo)
                self.stdout.write(f'\n   📄 [{i}/{len(arquivos)}] {nome_arquivo}')
                
                for banco in bancos:
                    if not force and ControleArquivos.ja_processado(arquivo, banco, tabela_nome):
                        self.stdout.write(f'      ⏭️  Já processado para {banco} (pular)')
                        continue

                    try:
                        # 🔥 PASSAR O PARÂMETRO FILTRO
                        processor = CNPJProcessor(
                            arquivo, 
                            tabela_nome, 
                            batch_size=batch_size,
                            progress_interval=progress_interval,
                            conflict_strategy=conflict_strategy,
                            test_limit=test_limit,
                            dump=dump,
                            filtro=filtro  # 🔥 NOVO
                        )
                        total = processor.processar(destino=banco)
                        
                        if total > 0:
                            ControleArquivos.registrar_processamento(arquivo, banco, tabela_nome, total, 'SUCESSO')
                            total_registros_tabela += total
                            arquivos_processados_tabela += 1
                            total_geral += total
                            self.stdout.write(self.style.SUCCESS(f'      ✅ {banco}: {total:,} registros importados'))
                        else:
                            self.stdout.write(self.style.WARNING(f'      ⚠️ {banco}: Nenhum registro'))
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f'      ❌ {banco}: Erro - {str(e)}'))
                        ControleArquivos.registrar_processamento(arquivo, banco, tabela_nome, 0, 'ERRO')

            self.stdout.write(f'\n   📊 Resumo {tabela_nome}:')
            self.stdout.write(f'      Arquivos: {arquivos_processados_tabela}/{len(arquivos)}')
            self.stdout.write(f'      Registros: {total_registros_tabela:,}')
            tabelas_processadas += 1

        self.stdout.write('\n' + '=' * 60)
        self.stdout.write(self.style.SUCCESS(f'✅ PROCESSAMENTO CONCLUÍDO!'))
        self.stdout.write(f'📁 Tabelas processadas: {tabelas_processadas}')
        self.stdout.write(f'📊 Total de registros: {total_geral:,}')
        self.stdout.write('=' * 60)

    def _extrair_numero(self, arquivo):
        nome = os.path.basename(arquivo)
        match = re.search(r'(\d+)\.parquet$', nome, re.IGNORECASE)
        return int(match.group(1)) if match else 0