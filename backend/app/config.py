import os
from pathlib import Path


POSTGRES_SCHEMA_PATH = Path(__file__).resolve().parent / 'storage' / 'schema.sql'

APP_TITLE = 'Resource 2'
API_ROOT_PATH = '/api'
CORS_ORIGINS = ['*']

POSTGRES_URL = os.environ['POSTGRES_URL']
DB_POOL_MIN = 1
DB_POOL_MAX = 10

TELEGRAM_API_ID = int(os.environ['TELEGRAM_API_ID'])
TELEGRAM_API_HASH = os.environ['TELEGRAM_API_HASH']
TELETHON_STRING_SESSION = os.environ['TELETHON_STRING_SESSION']

DEEPSEEK_API_KEY = os.environ['DEEPSEEK_API_KEY']
DEEPSEEK_BASE_URL = 'https://api.deepseek.com'
DEEPSEEK_MODEL = os.environ.get('DEEPSEEK_MODEL', 'deepseek-chat')


def _parse_csv_env(value: str) -> list[str]:
    items = [item.strip() for item in value.split(',')]
    return [item for item in items if item]


def _parse_int_env(value: str | None, default: int, min_value: int, max_value: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return min(max(parsed, min_value), max_value)


MONITOR_DEFAULT_CHANNELS = _parse_csv_env(
    os.environ.get('MONITOR_DEFAULT_CHANNELS', 'https://t.me/barterboard'),
)
MONITOR_USERS_CONTEXT_LIMIT = _parse_int_env(
    os.environ.get('MONITOR_USERS_CONTEXT_LIMIT'),
    default=200,
    min_value=1,
    max_value=2000,
)
