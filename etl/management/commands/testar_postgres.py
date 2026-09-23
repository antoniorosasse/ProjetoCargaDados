from django.core.management.base import BaseCommand
from etl.conexoes.postgres import get_postgres_connection

class Command(BaseCommand):
    help = 'Testa conexão com PostgreSQL'

    def handle(self, *args, **options):
        try:
            self.stdout.write("Tentando conectar ao PostgreSQL...")
            conn = get_postgres_connection()
            self.stdout.write("Conexão obtida. Executando consulta de teste...")
            cur = conn.cursor()
            cur.execute("SELECT version()")
            resultado = cur.fetchone()
            self.stdout.write(self.style.SUCCESS(f'Conexão OK! Versão: {resultado[0]}'))
            cur.close()
            conn.close()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Erro: {e}'))
