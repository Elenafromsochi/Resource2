from datetime import datetime
from typing import Any

from app.common import normalize_int_list
from app.common import safe_int

from .base import BaseRepository


class MessagesRepository(BaseRepository):
    async def list_by_channel_and_date(
        self,
        channel_id: int,
        date_from: datetime,
        date_to: datetime,
    ) -> list[dict[str, Any]]:
        normalized_channel_id = safe_int(channel_id)
        if normalized_channel_id is None:
            return []
        rows = await self.pool.fetch(
            """
            SELECT channel_id, message_id, detail, date
            FROM messages
            WHERE channel_id = $1
              AND date BETWEEN $2 AND $3
            ORDER BY date ASC, message_id ASC
            """,
            normalized_channel_id,
            date_from,
            date_to,
        )
        return self._rows_to_details(rows)

    async def list_by_channel_and_ids(
        self,
        channel_id: int,
        message_ids: list[int],
    ) -> list[dict[str, Any]]:
        normalized_channel_id = safe_int(channel_id)
        if normalized_channel_id is None:
            return []
        normalized = normalize_int_list(message_ids)
        if not normalized:
            return []
        rows = await self.pool.fetch(
            """
            SELECT channel_id, message_id, detail, date
            FROM messages
            WHERE channel_id = $1
              AND message_id = ANY($2::BIGINT[])
            ORDER BY date ASC NULLS FIRST, message_id ASC
            """,
            normalized_channel_id,
            normalized,
        )
        return self._rows_to_details(rows)

    def _rows_to_details(self, rows: list[Any]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for row in rows:
            detail = dict(row['detail'])
            if detail.get('id') is None:
                detail['id'] = row['message_id']
            if row['date'] is not None:
                detail['date'] = row['date']
            items.append(detail)
        return items

    async def upsert_many(
        self,
        channel_id: int,
        messages: list[dict[str, Any]],
    ) -> dict[str, int]:
        if not messages:
            return {'processed': 0, 'upserted': 0, 'modified': 0, 'skipped': 0}

        normalized_channel_id = safe_int(channel_id)
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
            message_id = safe_int(message.get('id'))
            message_date = message.get('date')
            if message_id is None or message_date is None:
                skipped += 1
                continue
            payload = dict(message)
            payload['id'] = message_id
            normalized_by_id[message_id] = payload

        if not normalized_by_id:
            return {'processed': 0, 'upserted': 0, 'modified': 0, 'skipped': skipped}

        processed = len(normalized_by_id)
        skipped += max(len(messages) - skipped - processed, 0)
        message_ids = list(normalized_by_id.keys())
        channel_ids = [normalized_channel_id] * processed
        details = [normalized_by_id[message_id] for message_id in message_ids]
        dates = [normalized_by_id[message_id]['date'] for message_id in message_ids]

        rows = await self.pool.fetch(
            """
            WITH payload AS (
                SELECT *
                FROM unnest(
                    $1::BIGINT[],
                    $2::BIGINT[],
                    $3::JSONB[],
                    $4::TIMESTAMPTZ[]
                ) AS value(channel_id, message_id, detail, date)
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
            details,
            dates,
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
            user_id = safe_int(row['user_id'])
            total = safe_int(row['total']) or 0
            if user_id is None:
                continue
            channels: list[dict[str, int]] = []
            for entry in row['channels']:
                channels.append(
                    {
                        'channel_id': int(entry['channel_id']),
                        'messages_count': int(entry['messages_count']),
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
        normalized = normalize_int_list(user_ids)
        if not normalized:
            return []
        return await self._aggregate_user_message_stats(normalized)
