# etl/management/commands/testar_as400.py
from django.core.management.base import BaseCommand
from etl.conexoes.as400 import get_as400_connection

class Command(BaseCommand):
    help = 'Testa conexão com AS/400'

    def handle(self, *args, **options):
        try:
            self.stdout.write("Tentando conectar ao AS/400...")
            conn = get_as400_connection()
            self.stdout.write("Conexão obtida. Executando consulta de teste...")
            stmt = conn.createStatement()
            rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1")
            if rs.next():
                resultado = rs.getInt(1)
                self.stdout.write(self.style.SUCCESS(f'Conexão OK! Resultado: {resultado}'))
            else:
                self.stdout.write(self.style.WARNING('Nenhum resultado retornado.'))
            rs.close()
            stmt.close()
            conn.close()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Erro: {e}'))