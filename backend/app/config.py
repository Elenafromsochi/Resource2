import os
from pathlib import Path


POSTGRES_SCHEMA_PATH = Path(__file__).resolve().parent / 'storage' / 'schema.sql'

APP_TITLE = 'Resource 2'
API_ROOT_PATH = '/api'
CORS_ORIGINS = ['*']

POSTGRES_URL = os.environ['POSTGRES_URL']
DB_POOL_MIN = 1
DB_POOL_MAX = 10

MONGO_URL = os.environ['MONGO_URL']
MONGO_DB_NAME = 'db'

TELEGRAM_API_ID = int(os.environ['TELEGRAM_API_ID'])
TELEGRAM_API_HASH = os.environ['TELEGRAM_API_HASH']
TELETHON_STRING_SESSION = os.environ['TELETHON_STRING_SESSION']

DEEPSEEK_API_KEY = os.environ['DEEPSEEK_API_KEY']
DEEPSEEK_BASE_URL = 'https://api.deepseek.com'
