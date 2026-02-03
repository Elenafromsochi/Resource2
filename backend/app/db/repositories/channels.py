class ChannelsRepository:
    def __init__(self, pool):
        self.pool = pool

    async def upsert(self, channel):
        row = await self.pool.fetchrow(
            """
            INSERT INTO channels (id, username, title, channel_type, link, updated_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (id)
            DO UPDATE SET
                username = EXCLUDED.username,
                title = EXCLUDED.title,
                channel_type = EXCLUDED.channel_type,
                link = EXCLUDED.link,
                updated_at = NOW()
            RETURNING *
            """,
            channel['id'],
            channel.get('username'),
            channel['title'],
            channel['channel_type'],
            channel.get('link'),
        )
        return dict(row) if row else None

    async def delete(self, channel_id):
        await self.pool.execute('DELETE FROM channels WHERE id = $1', channel_id)

    async def list(self, offset, limit):
        rows = await self.pool.fetch(
            """
            SELECT *
            FROM channels
            ORDER BY updated_at DESC, id DESC
            OFFSET $1 LIMIT $2
            """,
            offset,
            limit,
        )
        return [dict(row) for row in rows]

    async def list_all(self):
        rows = await self.pool.fetch(
            'SELECT * FROM channels ORDER BY updated_at DESC, id DESC',
        )
        return [dict(row) for row in rows]

    async def get_by_ids(self, channel_ids):
        if not channel_ids:
            return []
        rows = await self.pool.fetch(
            'SELECT * FROM channels WHERE id = ANY($1::bigint[])',
            channel_ids,
        )
        return [dict(row) for row in rows]
