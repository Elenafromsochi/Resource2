from typing import Any

from .base import BaseRepository


class ChannelsRepository(BaseRepository):
    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _normalize_channel_ids(cls, channel_ids: list[int] | None) -> list[int]:
        if not channel_ids:
            return []
        normalized: list[int] = []
        seen: set[int] = set()
        for channel_id in channel_ids:
            normalized_channel_id = cls._safe_int(channel_id)
            if normalized_channel_id is None:
                continue
            if normalized_channel_id in seen:
                continue
            seen.add(normalized_channel_id)
            normalized.append(normalized_channel_id)
        return normalized

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
        channel_ids: list[int],
        enabled: bool,
        prompt_id: int | None,
    ) -> list[dict[str, Any]]:
        normalized = self._normalize_channel_ids(channel_ids)
        if not normalized:
            return []
        normalized_prompt_id = self._safe_int(prompt_id)
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
        normalized_channel_id = self._safe_int(channel_id)
        normalized_message_id = self._safe_int(message_id)
        if normalized_channel_id is None or normalized_message_id is None:
            return
        await self.pool.execute(
            """
            UPDATE channels
            SET monitoring_last_message_id = $2,
                monitoring_last_message_at = $3,
                monitoring_last_error = NULL,
                monitoring_updated_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
            """,
            normalized_channel_id,
            normalized_message_id,
            message_date,
        )

    async def set_monitoring_error(self, channel_id: int, error: str) -> None:
        normalized_channel_id = self._safe_int(channel_id)
        if normalized_channel_id is None:
            return
        await self.pool.execute(
            """
            UPDATE channels
            SET monitoring_last_error = $2,
                monitoring_updated_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
            """,
            normalized_channel_id,
            str(error or 'Monitoring error')[:4000],
        )
