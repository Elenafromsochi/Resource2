from typing import Any

from .base import BaseRepository


class UsersRepository(BaseRepository):
    async def attach_channel_messages(self, rows):
        items = [dict(row) for row in rows]
        if not items:
            return items
        user_ids = [item['id'] for item in items]
        channel_rows = await self.pool.fetch(
            """
            SELECT user_id, channel_id, messages_count
            FROM channel_users
            WHERE user_id = ANY($1::bigint[])
            ORDER BY channel_id ASC
            """,
            user_ids,
        )
        messages_by_user: dict[int, list[dict[str, int]]] = {}
        for row in channel_rows:
            messages_by_user.setdefault(row['user_id'], []).append(
                {
                    'channel_id': row['channel_id'],
                    'messages_count': row['messages_count'],
                }
            )
        for item in items:
            item['channel_messages'] = messages_by_user.get(item['id'], [])
        return items

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
        if not channel_counts:
            return
        values = [
            (channel_id, user_id, messages_count)
            for channel_id, messages_count in channel_counts.items()
        ]
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO channel_users (channel_id, user_id, messages_count)
                VALUES ($1, $2, $3)
                ON CONFLICT (channel_id, user_id)
                DO UPDATE SET messages_count = EXCLUDED.messages_count
                """,
                values,
            )

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
                    ORDER BY messages_count DESC, updated_at DESC, id DESC
                    OFFSET $3 LIMIT $4
                    """,
                    id_pattern,
                    text_pattern,
                    offset,
                    limit,
                )
                return await self.attach_channel_messages(rows)
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
        return await self.attach_channel_messages(rows)

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
            ORDER BY c.id ASC
            """,
            user_id,
        )
        return [dict(row) for row in rows]
