from django.core.management.base import BaseCommand
from etl.conexoes.postgres import get_postgres_connection
from etl.conexoes.mongo import get_mongo_database
from django.core.management import call_command
import os
import glob
import logging
import re

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Migra dados do PostgreSQL para o MongoDB (detecta tabelas automaticamente)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--pasta',
            type=str,
            required=True,
            help='Pasta contendo os arquivos .parquet (para detectar tabelas)'
        )
        parser.add_argument(
            '--tabela',
            type=str,
            default=None,
            help='Migra apenas uma tabela específica (se não informado, migra todas)'
        )
        parser.add_argument(
            '--batch',
            type=int,
            default=5000,
            help='Tamanho do lote (padrão: 5000)'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Força recriação da coleção no MongoDB'
        )
        parser.add_argument(
            '--destino',
            type=str,
            choices=['postgres', 'mongo', 'ambos'],
            default='mongo',
            help='Banco de dados destino (padrão: mongo)'
        )
        parser.add_argument(
            '--listar',
            action='store_true',
            help='Apenas lista as tabelas encontradas sem migrar'
        )
        parser.add_argument(
            '--padrao',
            type=str,
            default='*.parquet',
            help='Padrão de arquivos (ex: *.parquet, c_*.parquet)'
        )

    def handle(self, *args, **options):
        pasta = options['pasta']
        tabela_especifica = options['tabela']
        batch_size = options['batch']
        force = options['force']
        destino = options['destino']
        listar = options['listar']
        padrao = options['padrao']

        # Verificar se a pasta existe
        if not os.path.exists(pasta):
            self.stdout.write(self.style.ERROR(f'❌ Pasta não encontrada: {pasta}'))
            return

        # ============================================================
        # DETECTAR TABELAS DISPONÍVEIS
        # ============================================================
        
        # Mapeamento de padrões de nome para tabelas
        PADROES_TABELAS = {
            r'(?i)^.*cnaes.*': 'c_cnaes',
            r'(?i)^.*empresas.*': 'c_empresas',
            r'(?i)^.*estabelecimentos.*': 'c_estabelecimentos',
            r'(?i)^.*motivos.*': 'c_motivos',
            r'(?i)^.*municipios.*': 'c_municipios',
            r'(?i)^.*naturezas.*': 'c_naturezas',
            r'(?i)^.*paises.*': 'c_paises',
            r'(?i)^.*qualificacoes.*': 'c_qualificacoes',
            r'(?i)^.*simples.*': 'c_simples',
            r'(?i)^.*socios.*': 'c_socios',
        }

        # Listar arquivos
        padrao_completo = os.path.join(pasta, padrao)
        arquivos = glob.glob(padrao_completo)
        
        if not arquivos:
            self.stdout.write(self.style.WARNING(f'⚠️ Nenhum arquivo encontrado com padrão: {padrao_completo}'))
            return

        # Agrupar arquivos por tabela
        tabelas_encontradas = {}
        for arquivo in arquivos:
            nome_base = os.path.basename(arquivo)
            tabela_encontrada = None
            
            for padrao_regex, tabela in PADROES_TABELAS.items():
                if re.match(padrao_regex, nome_base, re.IGNORECASE):
                    tabela_encontrada = tabela
                    break
            
            if tabela_encontrada:
                if tabela_encontrada not in tabelas_encontradas:
                    tabelas_encontradas[tabela_encontrada] = []
                tabelas_encontradas[tabela_encontrada].append(arquivo)

        if not tabelas_encontradas:
            self.stdout.write(self.style.WARNING('⚠️ Nenhuma tabela reconhecida encontrada'))
            return

        # Ordenar tabelas
        tabelas_ordenadas = sorted(tabelas_encontradas.keys())

        # ============================================================
        # VERIFICAR QUAIS TABELAS EXISTEM NO POSTGRESQL
        # ============================================================
        
        # Conectar ao PostgreSQL para verificar existência
        try:
            pg_conn = get_postgres_connection()
            pg_cur = pg_conn.cursor()
            
            tabelas_existentes = []
            tabelas_inexistentes = []
            
            for tabela in tabelas_ordenadas:
                pg_cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = %s
                    )
                """, (tabela,))
                existe = pg_cur.fetchone()[0]
                
                if existe:
                    tabelas_existentes.append(tabela)
                else:
                    tabelas_inexistentes.append(tabela)
            
            pg_cur.close()
            pg_conn.close()
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Erro ao verificar tabelas no PostgreSQL: {e}'))
            return

        # ============================================================
        # LISTAR TABELAS (se --listar)
        # ============================================================
        
        if listar:
            self.stdout.write('=' * 60)
            self.stdout.write('📋 TABELAS ENCONTRADAS')
            self.stdout.write('=' * 60)
            self.stdout.write(f'\n📁 Pasta: {pasta}')
            self.stdout.write(f'📄 Arquivos: {len(arquivos)}')
            self.stdout.write(f'🗂️  Tabelas detectadas: {len(tabelas_ordenadas)}\n')
            
            for tabela in tabelas_ordenadas:
                arquivos_tabela = tabelas_encontradas[tabela]
                status = '✅' if tabela in tabelas_existentes else '❌'
                self.stdout.write(f'{status} {tabela}')
                self.stdout.write(f'   📄 {len(arquivos_tabela)} arquivo(s)')
                
                # Mostrar primeiros 3 arquivos como exemplo
                for arquivo in arquivos_tabela[:3]:
                    self.stdout.write(f'      - {os.path.basename(arquivo)}')
                if len(arquivos_tabela) > 3:
                    self.stdout.write(f'      ... e mais {len(arquivos_tabela) - 3} arquivo(s)')
                self.stdout.write('')
            
            if tabelas_inexistentes:
                self.stdout.write(self.style.WARNING('\n⚠️ Tabelas não encontradas no PostgreSQL:'))
                for tabela in tabelas_inexistentes:
                    self.stdout.write(f'   - {tabela}')
            
            self.stdout.write(f'\n✅ Tabelas no PostgreSQL: {len(tabelas_existentes)}')
            self.stdout.write(f'❌ Tabelas não encontradas: {len(tabelas_inexistentes)}')
            self.stdout.write('=' * 60)
            return

        # ============================================================
        # FILTRAR TABELA ESPECÍFICA
        # ============================================================
        
        if tabela_especifica:
            if tabela_especifica not in tabelas_ordenadas:
                self.stdout.write(self.style.ERROR(f'❌ Tabela {tabela_especifica} não encontrada'))
                self.stdout.write(f'📋 Tabelas disponíveis: {", ".join(tabelas_ordenadas)}')
                return
            
            if tabela_especifica not in tabelas_existentes:
                self.stdout.write(self.style.ERROR(f'❌ Tabela {tabela_especifica} não existe no PostgreSQL'))
                return
            
            tabelas_para_migrar = [tabela_especifica]
        else:
            tabelas_para_migrar = tabelas_existentes

        if not tabelas_para_migrar:
            self.stdout.write(self.style.WARNING('⚠️ Nenhuma tabela encontrada no PostgreSQL para migrar'))
            return

        # ============================================================
        # MIGRAÇÃO
        # ============================================================
        
        # Determinar bancos destino
        bancos = []
        if destino in ['postgres', 'ambos']:
            bancos.append('postgres')
        if destino in ['mongo', 'ambos']:
            bancos.append('mongo')

        self.stdout.write('=' * 60)
        self.stdout.write(f'🚀 INICIANDO MIGRAÇÃO')
        self.stdout.write('=' * 60)
        self.stdout.write(f'📁 Pasta: {pasta}')
        self.stdout.write(f'🗂️  Tabelas: {len(tabelas_para_migrar)}')
        self.stdout.write(f'🎯 Destino: {destino}')
        self.stdout.write(f'📦 Lote: {batch_size}')
        self.stdout.write(f'🔄 Force: {force}')
        self.stdout.write('=' * 60)

        total_geral = 0
        tabelas_migradas = 0

        for tabela in tabelas_para_migrar:
            self.stdout.write(f'\n📄 Migrando {tabela}...')
            
            for banco in bancos:
                try:
                    # Chamar o comando de migração específico
                    call_command(
                        'migrar_postgres_para_mongo',
                        tabela=tabela,
                        batch=batch_size,
                        force=force
                    )
                    tabelas_migradas += 1
                    
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'   ❌ Erro ao migrar {tabela}: {e}'))

        # ============================================================
        # RESUMO FINAL
        # ============================================================
        
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write(self.style.SUCCESS('✅ MIGRAÇÃO CONCLUÍDA!'))
        self.stdout.write(f'📁 Tabelas migradas: {tabelas_migradas}/{len(tabelas_para_migrar)}')
        self.stdout.write('=' * 60)

        # Verificar total no MongoDB
        try:
            mongo_db = get_mongo_database()
            self.stdout.write('\n📊 Resumo no MongoDB:')
            for tabela in tabelas_para_migrar:
                colecao = mongo_db[tabela]
                total = colecao.count_documents({})
                self.stdout.write(f'   {tabela}: {total:,} registros')
        except Exception as e:
            pass