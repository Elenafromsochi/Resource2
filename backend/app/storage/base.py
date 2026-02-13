import json
from datetime import date
from datetime import datetime
from datetime import timezone

import asyncpg

from app.config import DB_POOL_MAX
from app.config import DB_POOL_MIN
from app.config import POSTGRES_SCHEMA_PATH
from app.config import POSTGRES_URL


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    # Keep message export resilient if unknown types appear in payload.
    return str(value)


def _json_encode(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(',', ':'),
        default=_json_default,
    )


def _encode_date(value: date | datetime) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f'Unsupported date value type: {type(value).__name__}')


def _encode_timestamptz(value: datetime | date) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return datetime.combine(
            value,
            datetime.min.time(),
            tzinfo=timezone.utc,
        ).isoformat()
    raise TypeError(f'Unsupported datetime value type: {type(value).__name__}')


def _decode_timestamptz(value: str) -> datetime:
    return datetime.fromisoformat(value)


async def _init_connection_codecs(connection: asyncpg.Connection) -> None:
    await connection.set_type_codec(
        'json',
        schema='pg_catalog',
        encoder=_json_encode,
        decoder=json.loads,
        format='text',
    )
    await connection.set_type_codec(
        'jsonb',
        schema='pg_catalog',
        encoder=_json_encode,
        decoder=json.loads,
        format='text',
    )
    await connection.set_type_codec(
        'date',
        schema='pg_catalog',
        encoder=_encode_date,
        decoder=date.fromisoformat,
        format='text',
    )
    await connection.set_type_codec(
        'timestamptz',
        schema='pg_catalog',
        encoder=_encode_timestamptz,
        decoder=_decode_timestamptz,
        format='text',
    )


class PostgresEngine:
    def __init__(self, url: str):
        self.url: str = url
        self.pool: asyncpg.Pool | None = None

    async def init(self):
        self.pool = await asyncpg.create_pool(
            dsn=self.url,
            min_size=DB_POOL_MIN,
            max_size=DB_POOL_MAX,
            init=_init_connection_codecs,
        )
        schema_sql = POSTGRES_SCHEMA_PATH.read_text(encoding='utf-8')
        async with self.pool.acquire() as conn:
            await conn.execute(schema_sql)

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None


class BaseRepository:
    db: PostgresEngine = PostgresEngine(POSTGRES_URL)

    @property
    def pool(self) -> asyncpg.Pool:
        return self.db.pool


class BaseStorage:
    async def init(self):
        await BaseRepository.db.init()

    async def close(self):
        await BaseRepository.db.close()
