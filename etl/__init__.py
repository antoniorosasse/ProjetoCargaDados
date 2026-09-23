# Exporta as funções principais para facilitar o uso
from .conexoes.as400 import get_as400_connection
from .conexoes.postgres import get_postgres_connection
from .conexoes.mongo import get_mongo_client, get_mongo_database 