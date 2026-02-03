from __future__ import annotations

from pathlib import Path

import asyncpg


async def apply_migrations(dsn: str) -> None:
    migrations_path = Path(__file__).resolve().parent / 'migrations'
    migrations = sorted(migrations_path.glob('*.sql'))
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migrations (
                name TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
        )
        rows = await conn.fetch('SELECT name FROM migrations')
        applied = {row['name'] for row in rows}
        for migration in migrations:
            if migration.name in applied:
                print(f'= {migration.name}')
                continue
            try:
                content = migration.read_text(encoding='utf-8')
                await conn.execute(content)
                await conn.execute(
                    'INSERT INTO migrations (name) VALUES ($1)',
                    migration.name,
                )
                print(f'+ {migration.name}')
            except Exception:
                print(f'x {migration.name}')
                raise
    finally:
        await conn.close()
