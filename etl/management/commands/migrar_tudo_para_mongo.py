from django.core.management.base import BaseCommand
from django.core.management import call_command

class Command(BaseCommand):
    help = 'Migra todas as tabelas do PostgreSQL para o MongoDB'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Força recriação das coleções no MongoDB'
        )

    def handle(self, *args, **options):
        tabelas = [
            'c_cnaes',
            'c_empresas',
            'c_estabelecimentos',
            'c_motivos',
            'c_municipios',
            'c_naturezas',
            'c_paises',
            'c_qualificacoes',
            'c_simples',
            'c_socios'
        ]

        self.stdout.write('🚀 Iniciando migração de TODAS as tabelas...')
        self.stdout.write('=' * 60)

        for tabela in tabelas:
            self.stdout.write(f'\n📄 Migrando {tabela}...')
            try:
                call_command(
                    'migrar_postgres_para_mongo',
                    tabela=tabela,
                    force=options['force'],
                    batch=10000
                )
                self.stdout.write(self.style.SUCCESS(f'✅ {tabela} migrada com sucesso!'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Erro ao migrar {tabela}: {e}'))

        self.stdout.write('\n' + '=' * 60)
        self.stdout.write(self.style.SUCCESS('🎉 Todas as tabelas foram migradas!'))