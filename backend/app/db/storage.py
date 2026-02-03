import asyncpg

from app import config
from app.db.repositories.channels import ChannelsRepository
from app.db.repositories.users import UsersRepository


class Storage:
    def __init__(self) -> None:
        self.pool: asyncpg.Pool | None = None
        self.channels: ChannelsRepository | None = None
        self.users: UsersRepository | None = None

    async def init(self) -> None:
        self.pool = await asyncpg.create_pool(
            dsn=config.POSTGRES_URL,
            min_size=config.DB_POOL_MIN,
            max_size=config.DB_POOL_MAX,
        )
        self.channels = ChannelsRepository(self.pool)
        self.users = UsersRepository(self.pool)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
