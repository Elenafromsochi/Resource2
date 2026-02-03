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
                messages_count,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (id)
            DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                bio = EXCLUDED.bio,
                photo = EXCLUDED.photo,
                messages_count = EXCLUDED.messages_count,
                updated_at = NOW()
            RETURNING *
            """,
            user['id'],
            user.get('username'),
            user.get('first_name'),
            user.get('last_name'),
            user.get('bio'),
            user.get('photo'),
            user.get('messages_count', 0),
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
                messages_count,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
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
            user.get('messages_count', 0),
        )
        return dict(row) if row else None

    async def replace_user_channels(self, user_id, channel_counts):
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM channel_users WHERE user_id = $1', user_id)
            if not channel_counts:
                return
            values = [
                (channel_id, user_id, messages_count)
                for channel_id, messages_count in channel_counts.items()
            ]
            await conn.executemany(
                """
                INSERT INTO channel_users (channel_id, user_id, messages_count)
                VALUES ($1, $2, $3)
                """,
                values,
            )

    async def list(self, offset, limit):
        rows = await self.pool.fetch(
            """
            SELECT *
            FROM users
            ORDER BY messages_count DESC, updated_at DESC, id DESC
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

    async def get_by_ids(self, user_ids):
        if not user_ids:
            return []
        rows = await self.pool.fetch(
            'SELECT * FROM users WHERE id = ANY($1::bigint[])',
            user_ids,
        )
        return [dict(row) for row in rows]

    async def list_user_groups(self, user_id):
        rows = await self.pool.fetch(
            """
            SELECT
                c.id,
                c.username,
                c.title,
                c.channel_type,
                c.link
            FROM channel_users cu
            JOIN channels c ON c.id = cu.channel_id
            WHERE cu.user_id = $1
              AND c.channel_type = 'group'
            ORDER BY c.title ASC, c.id ASC
            """,
            user_id,
        )
        return [dict(row) for row in rows]
