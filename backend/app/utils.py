import logging

import asyncpg

from .config import MIGRATIONS_DIR


logger = logging.getLogger('migrations')


async def migrate(dsn: str) -> None:
    migrations = sorted(MIGRATIONS_DIR.glob('*.sql'))
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migrations (name TEXT PRIMARY KEY)
            """,
        )
        rows = await conn.fetch('SELECT name FROM migrations')
        applied = {row['name'] for row in rows}
        for migration in migrations:
            if migration.name in applied:
                logger.info('= %s', migration.name)
                continue
            try:
                content = migration.read_text(encoding='utf-8')
                await conn.execute(content)
                await conn.execute(
                    'INSERT INTO migrations (name) VALUES ($1)',
                    migration.name,
                )
                logger.info('+ %s', migration.name)
            except Exception:
                logger.error('x %s', migration.name)
                raise
    finally:
        await conn.close()
