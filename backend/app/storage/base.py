from logging import getLogger

import asyncpg
from pymongo import AsyncMongoClient

from app.config import DB_POOL_MAX
from app.config import DB_POOL_MIN
from app.config import MONGO_DB_NAME
from app.config import MONGO_URL
from app.config import POSTGRES_URL
from app.config import SCHEMA_SQL_PATH


logger = getLogger(__name__)


async def initialize_postgres_schema(pool: asyncpg.Pool) -> None:
    if not SCHEMA_SQL_PATH.exists():
        raise FileNotFoundError(f'Postgres schema file not found: {SCHEMA_SQL_PATH}')

    schema_sql = SCHEMA_SQL_PATH.read_text(encoding='utf-8').strip()
    if not schema_sql:
        logger.warning('Schema file is empty, skipping init: %s', SCHEMA_SQL_PATH)
        return

    async with pool.acquire() as conn:
        await conn.execute(schema_sql)


class PostgresEngine:
    def __init__(self, url: str):
        self.url: str = url
        self.pool: asyncpg.Pool | None = None

    async def init(self):
        self.pool = await asyncpg.create_pool(
            dsn=self.url,
            min_size=DB_POOL_MIN,
            max_size=DB_POOL_MAX,
        )
        await initialize_postgres_schema(self.pool)

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None


class MongoEngine:
    def __init__(self, url: str, db_name: str):
        self.url = url
        self.db_name = db_name
        self.client: AsyncMongoClient | None = None
        self.db = None

    async def init(self):
        if self.client:
            return
        self.client = AsyncMongoClient(self.url)
        self.db = self.client[self.db_name]

    async def close(self):
        if self.client:
            self.client.close()
            self.client = None
            self.db = None


class BaseRepository:
    db: PostgresEngine = PostgresEngine(POSTGRES_URL)

    @property
    def pool(self):
        return self.db.pool


class BaseMongoRepository:
    mongo: MongoEngine = MongoEngine(MONGO_URL, MONGO_DB_NAME)

    @property
    def db(self):
        return self.mongo.db


class BaseStorage:
    async def init(self):
        await BaseRepository.db.init()
        await BaseMongoRepository.mongo.init()

    async def close(self):
        await BaseMongoRepository.mongo.close()
        await BaseRepository.db.close()
