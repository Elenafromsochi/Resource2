import asyncpg

from app.config import DB_POOL_MAX
from app.config import DB_POOL_MIN
from app.config import POSTGRES_SCHEMA_PATH
from app.config import POSTGRES_URL


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
