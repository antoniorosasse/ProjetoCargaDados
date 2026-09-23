from django.core.management.base import BaseCommand
from etl.conexoes.mongo import get_mongo_database

class Command(BaseCommand):
    help = 'Testa conexão com MongoDB'

    def handle(self, *args, **options):
        try:
            self.stdout.write("Tentando conectar ao MongoDB...")
            db = get_mongo_database()
            self.stdout.write("Conexão obtida. Listando coleções...")
            colecoes = db.list_collection_names()
            self.stdout.write(self.style.SUCCESS(f'Conexão OK! Coleções encontradas: {len(colecoes)}'))
            if colecoes:
                self.stdout.write(f'Coleções: {", ".join(colecoes)}')
            else:
                self.stdout.write(self.style.WARNING('Nenhuma coleção encontrada (banco vazio)'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Erro: {e}'))