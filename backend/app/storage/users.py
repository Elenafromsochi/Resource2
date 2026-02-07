from typing import Any

from .base import BaseRepository


class UsersRepository(BaseRepository):
    async def upsert(self, user):
        row = await self.pool.fetchrow(
            """
            INSERT INTO users (
                id,
                username,
                first_name,
                last_name,
                bio,
                photo,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (id)
            DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                bio = EXCLUDED.bio,
                photo = EXCLUDED.photo,
                updated_at = NOW()
            RETURNING *
            """,
            user['id'],
            user.get('username'),
            user.get('first_name'),
            user.get('last_name'),
            user.get('bio'),
            user.get('photo'),
        )
        return dict(row) if row else None

    async def upsert_profile(self, user):
        row = await self.pool.fetchrow(
            """
            INSERT INTO users (
                id,
                username,
                first_name,
                last_name,
                bio,
                photo,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (id)
            DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                bio = EXCLUDED.bio,
                photo = EXCLUDED.photo,
                updated_at = NOW()
            RETURNING *
            """,
            user['id'],
            user.get('username'),
            user.get('first_name'),
            user.get('last_name'),
            user.get('bio'),
            user.get('photo'),
        )
        return dict(row) if row else None

    async def ensure_users_exist(self, user_ids):
        if not user_ids:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO users (id, updated_at)
                VALUES ($1, NOW())
                ON CONFLICT (id)
                DO UPDATE SET updated_at = NOW()
                """,
                [(user_id,) for user_id in user_ids],
            )

    async def list_by_ids(self, user_ids: list[int]) -> list[dict[str, Any]]:
        if not user_ids:
            return []
        normalized: list[int] = []
        for user_id in user_ids:
            if user_id is None:
                continue
            try:
                normalized.append(int(user_id))
            except (TypeError, ValueError):
                continue
        if not normalized:
            return []
        rows = await self.pool.fetch(
            'SELECT * FROM users WHERE id = ANY($1)',
            normalized,
        )
        return [dict(row) for row in rows]

    async def list(self, offset, limit, search: str | None = None):
        if search:
            search_value = search.strip()
            if search_value:
                normalized = search_value.lstrip('@')
                if not normalized:
                    normalized = search_value
                id_pattern = f"%{search_value}%"
                text_pattern = f"%{normalized}%"
                rows = await self.pool.fetch(
                    """
                    SELECT *
                    FROM users
                    WHERE CAST(id AS TEXT) ILIKE $1
                       OR COALESCE(username, '') ILIKE $2
                       OR COALESCE(first_name, '') ILIKE $2
                       OR COALESCE(last_name, '') ILIKE $2
                       OR CONCAT_WS(' ', first_name, last_name) ILIKE $2
                       OR CONCAT_WS(' ', last_name, first_name) ILIKE $2
                    ORDER BY updated_at DESC, id DESC
                    OFFSET $3 LIMIT $4
                    """,
                    id_pattern,
                    text_pattern,
                    offset,
                    limit,
                )
                return [dict(row) for row in rows]
        rows = await self.pool.fetch(
            """
            SELECT *
            FROM users
            ORDER BY updated_at DESC, id DESC
            OFFSET $1 LIMIT $2
            """,
            offset,
            limit,
        )
        return [dict(row) for row in rows]

    async def get(self, user_id: int) -> dict[str, Any] | None:
        row = await self.pool.fetchrow(
            'SELECT * FROM users WHERE id = $1',
            user_id,
        )
        return dict(row) if row else None
