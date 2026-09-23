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

# ==================== BANCO DE DADOS (SQLite para simplicidade) ====================
# Usaremos SQLite como banco padrão (para admin, sessões, etc.)
# As conexões com PostgreSQL, MongoDB e AS/400 serão feitas separadamente
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
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

# ==================== VALIDAÇÃO (APENAS AS ESSENCIAIS) ====================
# Apenas AS/400 é realmente necessário para os primeiros comandos.
# As outras são opcionais; se não existirem, serão None.
if not all([AS400_HOST, AS400_USER, AS400_PASSWORD, AS400_LIBRARY, AS400_JDBC_JAR]):
    raise ValueError("Variáveis do AS/400 não estão completamente definidas no .env!")