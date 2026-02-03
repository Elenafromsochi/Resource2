import os

APP_NAME = 'Telegram Activity Monitor'
APP_HOST = '0.0.0.0'
APP_PORT = 8000
LOG_LEVEL = 'INFO'
APP = {'host': APP_HOST, 'port': APP_PORT}

POSTGRES_URL = os.environ['POSTGRES_URL']
DB_POOL_MIN = 1
DB_POOL_MAX = 10

TELEGRAM_API_ID = int(os.environ['TELEGRAM_API_ID'])
TELEGRAM_API_HASH = os.environ['TELEGRAM_API_HASH']
TELETHON_SESSION = os.environ['TELETHON_SESSION']

DEEPSEEK_API_KEY = os.environ['DEEPSEEK_API_KEY']
DEEPSEEK_BASE_URL = 'https://api.deepseek.com'

CORS_ORIGINS = ['*']
API_ROOT_PATH = '/api'
