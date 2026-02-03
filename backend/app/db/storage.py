from pathlib import Path

import asyncpg

from app import config
from app.db.repositories.channels import ChannelsRepository
from app.db.repositories.users import UsersRepository


class Storage:
    def __init__(self):
        self.pool = None
        self.channels = None
        self.users = None

    async def init(self):
        self.pool = await asyncpg.create_pool(
            dsn=config.POSTGRES_URL,
            min_size=config.DB_POOL_MIN,
            max_size=config.DB_POOL_MAX,
        )
        await self._apply_migrations()
        self.channels = ChannelsRepository(self.pool)
        self.users = UsersRepository(self.pool)

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def _apply_migrations(self):
        migrations_path = Path(__file__).resolve().parent / "migrations"
        migrations = sorted(migrations_path.glob("*.sql"))
        async with self.pool.acquire() as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
            )
            rows = await conn.fetch("SELECT name FROM schema_migrations")
            applied = {row["name"] for row in rows}
            for migration in migrations:
                if migration.name in applied:
                    continue
                content = migration.read_text(encoding="utf-8")
                if content.strip():
                    await conn.execute(content)
                await conn.execute(
                    "INSERT INTO schema_migrations (name) VALUES ($1)",
                    migration.name,
                )
