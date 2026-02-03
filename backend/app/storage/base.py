from logging import getLogger

import asyncpg

from app.config import DB_POOL_MAX
from app.config import DB_POOL_MIN
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


class BaseRepository:
    db: PostgresEngine = PostgresEngine(POSTGRES_URL)

    @property
    def pool(self):
        return self.db.pool


class BaseStorage:
    async def init(self):
        return await BaseRepository.db.init()

    async def close(self):
        return await BaseRepository.db.close()
