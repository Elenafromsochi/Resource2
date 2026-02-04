from logging import getLogger

import asyncpg
from motor.motor_asyncio import AsyncIOMotorClient

from app.config import DB_POOL_MAX
from app.config import DB_POOL_MIN
from app.config import MONGO_DB_NAME
from app.config import MONGO_URL
from app.config import POSTGRES_URL


logger = getLogger(__name__)


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

    async def close(self):
        await self.pool.close()


class MongoEngine:
    def __init__(self, url: str, db_name: str):
        self.url = url
        self.db_name = db_name
        self.client: AsyncIOMotorClient | None = None
        self.db = None

    async def init(self):
        if self.client:
            return
        self.client = AsyncIOMotorClient(self.url)
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
