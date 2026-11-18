"""
Django settings for carga_dados project.
"""

from pathlib import Path
import os
from dotenv import load_dotenv

# ==================== CARREGAR .env ====================
BASE_DIR = Path(__file__).resolve().parent.parent

# Carrega o .env usando caminho absoluto
env_path = BASE_DIR / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    # Fallback: tenta carregar do diretório atual
    load_dotenv()
    if not os.getenv('AS400_HOST'):
        print("⚠️  ATENÇÃO: Arquivo .env não encontrado ou vazio!")

# ==================== SEGURANÇA ====================
SECRET_KEY = os.getenv('SECRET_KEY')
if not SECRET_KEY:
    raise ValueError("A variável SECRET_KEY não está definida no .env")

DEBUG = os.getenv('DEBUG', 'False') == 'True'
ALLOWED_HOSTS = os.getenv('ALLOWED_HOSTS', '').split(',')

# ==================== APPS ====================
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'etl',  # nosso app de ETL
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'carga_dados.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'carga_dados.wsgi.application'

# ==================== BANCO DE DADOS (Postgres django_internal) ====================
# O admin deste projeto usa o MESMO banco de auth do cnpj_interface
# (django_internal): um usuario criado em um admin entra no outro, sem
# duplicar conta. As tabelas c_* continuam no bancocnpj, acessadas por
# psycopg2 direto (etl/conexoes/postgres.py) e nao pelo ORM.
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv('DJANGO_DB', 'django_internal'),
        'USER': os.getenv('DJANGO_DB_USER', 'django_app'),
        'PASSWORD': os.getenv('DJANGO_DB_PASSWORD'),
        'HOST': os.getenv('DJANGO_DB_HOST', 'localhost'),
        'PORT': os.getenv('DJANGO_DB_PORT', '5432'),
        'CONN_MAX_AGE': 60,
    }
}

# ==================== VALIDAÇÃO DE SENHA ====================
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

# ==================== INTERNACIONALIZAÇÃO ====================
LANGUAGE_CODE = 'pt-br'
TIME_ZONE = 'America/Sao_Paulo'
USE_I18N = True
USE_TZ = True

# ==================== ARQUIVOS ESTÁTICOS ====================
STATIC_URL = 'static/'
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# ==================== CONFIGURAÇÕES DE CONEXÃO COM OS BANCOS ====================
# AS/400 (DB2)
AS400_HOST = os.getenv('AS400_HOST')
AS400_USER = os.getenv('AS400_USER')
AS400_PASSWORD = os.getenv('AS400_PASSWORD')
AS400_LIBRARY = os.getenv('AS400_LIBRARY')
AS400_JDBC_JAR = os.getenv('AS400_JDBC_JAR')

# PostgreSQL (opcional)
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DB = os.getenv('PG_DB')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# MongoDB (opcional)
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
MONGO_DB = os.getenv('MONGO_DB')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')

# ==================== CARGA PELO ADMIN ====================
# O admin dispara `manage.py carregar_base_cnpj` em subprocesso (etl/execucao.py).
# O log de cada execucao fica em logs/execucoes/ (git ignora a pasta logs/).
CARGA_LOG_DIR = BASE_DIR / 'logs' / 'execucoes'

# Raizes onde o formulario aceita procurar pasta de .zip/.parquet. Sem isso o
# campo viraria execucao de comando com caminho arbitrario. Ajuste no .env:
#   CARGA_RAIZES=/mnt/NOTE_EXT4_SDD/DocumentosSSD/cnpj_lote:/home/neto/DataSetsTeste
CARGA_RAIZES = [p.strip() for p in os.getenv(
    'CARGA_RAIZES',
    '/mnt/NOTE_EXT4_SDD/DocumentosSSD/cnpj_lote:/home/neto/DataSetsTeste',
).split(':') if p.strip()]

# ==================== PUBLICACAO NA VPS ====================
# Leva a base daqui para a `bancocnpj` de producao (etl/publicacao.py +
# `manage.py publicar_base_cnpj`). O acesso e sempre por SSH: a porta 5432 da
# VPS e fechada para fora e o psql/pg_restore de la usam o ~/.pgpass do usuario
# remoto -- nenhuma senha de producao passa pelo .env local.
#   PUBLICACAO_SSH=neto@<host-da-vps>
# Vazio = a secao de producao aparece na pagina desabilitada (nada e executado).
PUBLICACAO_SSH = os.getenv('PUBLICACAO_SSH', '').strip()

# Alvo do restore, no formato do PGPASSFILE: host:porta:banco:usuario
PUBLICACAO_PG = os.getenv('PUBLICACAO_PG',
                          'localhost:5432:bancocnpj:arosasse')

# Servico systemd que publica a base (o script para/religa na janela)
PUBLICACAO_SERVICO = os.getenv('PUBLICACAO_SERVICO', 'cnpj')

# Dumps por tabela ficam aqui, um subdiretorio por rotulo. Precisa de ~8 GB
# livres no lote completo (nao cabe em /, que tem pouco espaco).
PUBLICACAO_DUMP_DIR = Path(os.getenv(
    'PUBLICACAO_DUMP_DIR',
    '/mnt/NOTE_EXT4_SDD/DocumentosSSD/cnpj_lote/pubs'))

# Pasta na VPS que recebe dumps, script e log
PUBLICACAO_REMOTO = os.getenv('PUBLICACAO_REMOTO', 'carga')

# Restore/indices em paralelo (1 conexao por job)
PUBLICACAO_JOBS = int(os.getenv('PUBLICACAO_JOBS', '4') or 4)

# ==================== VALIDACAO (APENAS AS ESSENCIAIS) ====================
# Apenas AS/400 é realmente necessário para os primeiros comandos.
# As outras são opcionais; se não existirem, serão None.
if not all([AS400_HOST, AS400_USER, AS400_PASSWORD, AS400_LIBRARY, AS400_JDBC_JAR]):
    raise ValueError("Variáveis do AS/400 não estão completamente definidas no .env!")

# O admin nao sobe sem as credenciais do django_internal (banco de auth
# compartilhado com o cnpj_interface).
if not os.getenv('DJANGO_DB_PASSWORD'):
    raise ValueError("A variável DJANGO_DB_PASSWORD não está definida no .env!")