import asyncpg


class BaseRepository:
    pool: asyncpg.Pool | None = None

    def __init__(self, pool: asyncpg.Pool) -> None:
        if BaseRepository.pool is None:
            BaseRepository.pool = pool
        self.pool = BaseRepository.pool
