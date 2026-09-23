from pymongo import MongoClient
from django.conf import settings
from urllib.parse import quote_plus

def get_mongo_client():
    """
    Retorna um cliente MongoDB conectado ao host/porta configurados.
    """
    try:
        # Se tiver autenticação (usuário/senha), usa a URI com credenciais escapadas
        if hasattr(settings, 'MONGO_USER') and settings.MONGO_USER and settings.MONGO_PASSWORD:
            user = quote_plus(settings.MONGO_USER)
            password = quote_plus(settings.MONGO_PASSWORD)
            # Usa a URI simples (sem authSource - o MongoDB descobre automaticamente)
            uri = f"mongodb://{user}:{password}@{settings.MONGO_HOST}:{settings.MONGO_PORT}/{settings.MONGO_DB}"
            client = MongoClient(uri)
        else:
            client = MongoClient(settings.MONGO_HOST, int(settings.MONGO_PORT))
        
        # Testa a conexão
        client.admin.command('ping')
        return client
    except Exception as e:
        raise Exception(f"Erro ao conectar ao MongoDB: {e}")

def get_mongo_database():
    """
    Retorna o banco de dados MongoDB configurado em MONGO_DB.
    """
    client = get_mongo_client()
    return client[settings.MONGO_DB]