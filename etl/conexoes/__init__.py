# Exporta as funções principais para facilitar o uso
from .as400 import get_as400_connection
from .postgres import get_postgres_connection
from .mongo import get_mongo_client, get_mongo_database
