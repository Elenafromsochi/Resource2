from typing import Any, List

from app.common import normalize_int_list
from app.common import safe_int

from .base import BaseRepository


class ChannelsRepository(BaseRepository):
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
                    FROM channels
                    WHERE CAST(id AS TEXT) ILIKE $1
                       OR COALESCE(username, '') ILIKE $2
                       OR COALESCE(title, '') ILIKE $2
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

    async def get(self, channel_id: int) -> dict[str, Any] | None:
        row = await self.pool.fetchrow(
            'SELECT * FROM channels WHERE id = $1',
            channel_id,
        )
        return dict(row) if row else None

    async def get_by_ids(self, channel_ids):
        if not channel_ids:
            return []
        rows = await self.pool.fetch(
            'SELECT * FROM channels WHERE id = ANY($1::bigint[])',
            channel_ids,
        )
        return [dict(row) for row in rows]

    async def bulk_update_monitoring(
        self,
        channel_ids: List[int],
        enabled: bool,
        prompt_id: int | None,
    ) -> List[dict[str, Any]]:
        normalized = normalize_int_list(channel_ids)
        if not normalized:
            return []
        normalized_prompt_id = safe_int(prompt_id)
        if not enabled:
            normalized_prompt_id = None
        rows = await self.pool.fetch(
            """
            UPDATE channels
            SET monitoring_enabled = $2,
                monitoring_prompt_id = $3,
                monitoring_last_error = NULL,
                monitoring_updated_at = NOW(),
                updated_at = NOW()
            WHERE id = ANY($1::BIGINT[])
            RETURNING *
            """,
            normalized,
            bool(enabled),
            normalized_prompt_id,
        )
        return [dict(row) for row in rows]

    async def set_monitoring_success(
        self,
        channel_id: int,
        message_id: int,
        message_date: Any,
    ) -> None:
        normalized_channel_id = safe_int(channel_id)
        normalized_message_id = safe_int(message_id)
        if normalized_channel_id is None or normalized_message_id is None:
            return
        await self._update_monitoring_state(
            normalized_channel_id,
            last_message_id=normalized_message_id,
            last_message_at=message_date,
            last_error=None,
        )

    async def set_monitoring_error(self, channel_id: int, error: str) -> None:
        normalized_channel_id = safe_int(channel_id)
        if normalized_channel_id is None:
            return
        await self._update_monitoring_state(
            normalized_channel_id,
            last_error=str(error or 'Monitoring error')[:4000],
        )

    async def _update_monitoring_state(
        self,
        channel_id: int,
        last_message_id: int | None = None,
        last_message_at: Any = None,
        last_error: str | None = None,
    ) -> None:
        await self.pool.execute(
            """
            UPDATE channels
            SET monitoring_last_message_id = COALESCE($2, monitoring_last_message_id),
                monitoring_last_message_at = COALESCE($3, monitoring_last_message_at),
                monitoring_last_error = $4,
                monitoring_updated_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
            """,
            channel_id,
            last_message_id,
            last_message_at,
            last_error,
        )
