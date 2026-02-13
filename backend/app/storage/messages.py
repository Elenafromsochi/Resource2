import json
from datetime import date
from datetime import datetime
from datetime import timezone
from typing import Any

from .base import BaseRepository


class MessagesRepository(BaseRepository):
    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_utc_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @classmethod
    def _to_json_compatible(cls, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, datetime):
            return cls._to_utc_datetime(value).isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(key): cls._to_json_compatible(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [cls._to_json_compatible(item) for item in value]
        return str(value)

    @classmethod
    def _normalize_message_detail(
        cls,
        message: dict[str, Any],
        message_id: int,
    ) -> dict[str, Any]:
        normalized = cls._to_json_compatible(message)
        if not isinstance(normalized, dict):
            normalized = {'value': normalized}
        normalized['id'] = message_id
        return normalized

    @staticmethod
    def _decode_detail(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}
            if isinstance(parsed, dict):
                return parsed
        return {}

    @staticmethod
    def _normalize_message_ids(message_ids: list[int] | None) -> list[int]:
        if not message_ids:
            return []
        normalized: list[int] = []
        for message_id in message_ids:
            if message_id is None:
                continue
            try:
                normalized.append(int(message_id))
            except (TypeError, ValueError):
                continue
        return normalized

    async def list_by_channel_and_date(
        self,
        channel_id: int,
        date_from: datetime,
        date_to: datetime,
    ) -> list[dict[str, Any]]:
        normalized_channel_id = self._safe_int(channel_id)
        if normalized_channel_id is None:
            return []
        normalized_date_from = self._to_utc_datetime(date_from)
        normalized_date_to = self._to_utc_datetime(date_to)
        rows = await self.pool.fetch(
            """
            SELECT channel_id, message_id, detail
            FROM messages
            WHERE channel_id = $1
              AND date BETWEEN $2 AND $3
            ORDER BY date ASC, message_id ASC
            """,
            normalized_channel_id,
            normalized_date_from,
            normalized_date_to,
        )
        return self._rows_to_details(rows)

    async def list_by_channel_and_ids(
        self,
        channel_id: int,
        message_ids: list[int],
    ) -> list[dict[str, Any]]:
        normalized_channel_id = self._safe_int(channel_id)
        if normalized_channel_id is None:
            return []
        normalized = self._normalize_message_ids(message_ids)
        if not normalized:
            return []
        rows = await self.pool.fetch(
            """
            SELECT channel_id, message_id, detail
            FROM messages
            WHERE channel_id = $1
              AND message_id = ANY($2::BIGINT[])
            ORDER BY date ASC NULLS FIRST, message_id ASC
            """,
            normalized_channel_id,
            normalized,
        )
        return self._rows_to_details(rows)

    @staticmethod
    def _normalize_user_ids(user_ids: list[int] | None) -> list[int]:
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
        return normalized

    def _rows_to_details(self, rows: list[Any]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for row in rows:
            detail = self._decode_detail(row['detail'])
            if self._safe_int(detail.get('id')) is None:
                detail['id'] = row['message_id']
            items.append(detail)
        return items

    async def upsert_many(
        self,
        channel_id: int,
        messages: list[dict[str, Any]],
    ) -> dict[str, int]:
        if not messages:
            return {'processed': 0, 'upserted': 0, 'modified': 0, 'skipped': 0}

        normalized_channel_id = self._safe_int(channel_id)
        if normalized_channel_id is None:
            return {
                'processed': 0,
                'upserted': 0,
                'modified': 0,
                'skipped': len(messages),
            }

        normalized_by_id: dict[int, dict[str, Any]] = {}
        skipped = 0
        for message in messages:
            if not isinstance(message, dict):
                skipped += 1
                continue
            message_id = self._safe_int(message.get('id'))
            if message_id is None:
                skipped += 1
                continue
            normalized_by_id[message_id] = self._normalize_message_detail(
                message,
                message_id,
            )

        if not normalized_by_id:
            return {'processed': 0, 'upserted': 0, 'modified': 0, 'skipped': skipped}

        processed = len(normalized_by_id)
        skipped += max(len(messages) - skipped - processed, 0)
        message_ids = list(normalized_by_id.keys())
        channel_ids = [normalized_channel_id] * processed
        details_json = [
            json.dumps(
                normalized_by_id[message_id],
                ensure_ascii=False,
                separators=(',', ':'),
            )
            for message_id in message_ids
        ]

        rows = await self.pool.fetch(
            """
            WITH payload AS (
                SELECT
                    channel_id,
                    message_id,
                    detail_text::JSONB AS detail,
                    (detail_text::JSONB->>'date')::TIMESTAMPTZ AS date
                FROM unnest(
                    $1::BIGINT[],
                    $2::BIGINT[],
                    $3::TEXT[]
                ) AS value(channel_id, message_id, detail_text)
            )
            INSERT INTO messages (channel_id, message_id, detail, date)
            SELECT channel_id, message_id, detail, date
            FROM payload
            ON CONFLICT (channel_id, message_id)
            DO UPDATE SET
                detail = EXCLUDED.detail,
                date = EXCLUDED.date
            WHERE messages.detail IS DISTINCT FROM EXCLUDED.detail
            RETURNING (xmax = 0) AS inserted
            """,
            channel_ids,
            message_ids,
            details_json,
        )
        upserted = sum(1 for row in rows if row['inserted'])
        modified = len(rows) - upserted
        return {
            'processed': processed,
            'upserted': upserted,
            'modified': modified,
            'skipped': skipped,
        }

    async def _aggregate_user_message_stats(
        self,
        user_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """
            WITH extracted AS (
                SELECT
                    channel_id,
                    CASE
                        WHEN COALESCE(detail->'from_id'->>'user_id', '') ~ '^-?\\d+$' THEN
                            (detail->'from_id'->>'user_id')::BIGINT
                        WHEN COALESCE(detail->>'sender_id', '') ~ '^-?\\d+$' THEN
                            (detail->>'sender_id')::BIGINT
                        ELSE NULL
                    END AS sender_id
                FROM messages
            ),
            filtered AS (
                SELECT channel_id, sender_id
                FROM extracted
                WHERE sender_id IS NOT NULL
                  AND sender_id <> channel_id
                  AND ($1::BIGINT[] IS NULL OR sender_id = ANY($1::BIGINT[]))
            ),
            by_channel AS (
                SELECT
                    sender_id AS user_id,
                    channel_id,
                    COUNT(*)::BIGINT AS messages_count
                FROM filtered
                GROUP BY sender_id, channel_id
            )
            SELECT
                user_id,
                SUM(messages_count)::BIGINT AS total,
                COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'channel_id',
                            channel_id,
                            'messages_count',
                            messages_count
                        )
                        ORDER BY channel_id
                    ),
                    '[]'::JSONB
                ) AS channels
            FROM by_channel
            GROUP BY user_id
            ORDER BY user_id
            """,
            user_ids,
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            user_id = self._safe_int(row['user_id'])
            total = self._safe_int(row['total']) or 0
            if user_id is None:
                continue
            channels_raw = row['channels']
            if isinstance(channels_raw, str):
                try:
                    channels_raw = json.loads(channels_raw)
                except json.JSONDecodeError:
                    channels_raw = []
            channels: list[dict[str, int]] = []
            if isinstance(channels_raw, list):
                for entry in channels_raw:
                    if not isinstance(entry, dict):
                        continue
                    channel_id = self._safe_int(entry.get('channel_id'))
                    messages_count = self._safe_int(entry.get('messages_count'))
                    if channel_id is None or messages_count is None:
                        continue
                    channels.append(
                        {
                            'channel_id': channel_id,
                            'messages_count': messages_count,
                        }
                    )
            result.append({'user_id': user_id, 'total': total, 'channels': channels})
        return result

    async def aggregate_user_message_stats(self) -> list[dict[str, Any]]:
        return await self._aggregate_user_message_stats()

    async def aggregate_user_message_stats_for_users(
        self,
        user_ids: list[int],
    ) -> list[dict[str, Any]]:
        normalized = self._normalize_user_ids(user_ids)
        if not normalized:
            return []
        return await self._aggregate_user_message_stats(normalized)
