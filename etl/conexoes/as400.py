# etl/conexoes/as400.py
import os
import jpype
import jpype.imports
from django.conf import settings

def get_as400_connection():
    """
    Inicia a JVM e retorna uma conexão JDBC com o AS/400 (DB2).
    """
    # Caminho absoluto para o jt400.jar
    jar_path = settings.AS400_JDBC_JAR  # já está definido no .env

    if not os.path.exists(jar_path):
        raise FileNotFoundError(f"Arquivo jt400.jar não encontrado em: {jar_path}")

    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[jar_path])
        print("JVM iniciada com sucesso.")

    host = settings.AS400_HOST
    user = settings.AS400_USER
    password = settings.AS400_PASSWORD
    library = settings.AS400_LIBRARY

    # Use 'naming=sql' ou 'naming=system' conforme seu projeto original
    jdbc_url = f"jdbc:as400://{host};libraries={library};naming=sql"

    DriverManager = jpype.JClass("java.sql.DriverManager")
    return DriverManager.getConnection(jdbc_url, user, password)