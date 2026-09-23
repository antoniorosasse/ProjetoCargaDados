import psycopg2
from django.conf import settings

def get_postgres_connection():
    """
    Retorna uma conexão psycopg2 para o PostgreSQL.
    Usa as configurações do .env (PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD).
    """
    try:
        conn = psycopg2.connect(
            dbname=settings.PG_DB,
            user=settings.PG_USER,
            password=settings.PG_PASSWORD,
            host=settings.PG_HOST,
            port=settings.PG_PORT
        )
        return conn
    except Exception as e:
        raise Exception(f"Erro ao conectar ao PostgreSQL: {e}")