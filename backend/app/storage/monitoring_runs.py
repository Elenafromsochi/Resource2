from typing import Any

from .base import BaseRepository


class MonitoringRunsRepository(BaseRepository):
    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    async def has_successful_result(
        self,
        channel_id: int,
        message_id: int,
        prompt_id: int,
    ) -> bool:
        normalized_channel_id = self._safe_int(channel_id)
        normalized_message_id = self._safe_int(message_id)
        normalized_prompt_id = self._safe_int(prompt_id)
        if (
            normalized_channel_id is None
            or normalized_message_id is None
            or normalized_prompt_id is None
        ):
            return False
        exists = await self.pool.fetchval(
            """
            SELECT 1
            FROM message_monitoring_runs
            WHERE channel_id = $1
              AND message_id = $2
              AND prompt_id = $3
              AND status = 'success'
            LIMIT 1
            """,
            normalized_channel_id,
            normalized_message_id,
            normalized_prompt_id,
        )
        return exists is not None

    async def save_success(
        self,
        channel_id: int,
        message_id: int,
        prompt_id: int,
        request_payload: dict[str, Any],
        response_text: str,
    ) -> dict[str, Any] | None:
        return await self._save(
            channel_id=channel_id,
            message_id=message_id,
            prompt_id=prompt_id,
            request_payload=request_payload,
            response_text=response_text,
            error=None,
            status='success',
        )

    async def save_error(
        self,
        channel_id: int,
        message_id: int,
        prompt_id: int,
        request_payload: dict[str, Any],
        error: str,
    ) -> dict[str, Any] | None:
        return await self._save(
            channel_id=channel_id,
            message_id=message_id,
            prompt_id=prompt_id,
            request_payload=request_payload,
            response_text=None,
            error=error,
            status='error',
        )

    async def _save(
        self,
        channel_id: int,
        message_id: int,
        prompt_id: int,
        request_payload: dict[str, Any],
        response_text: str | None,
        error: str | None,
        status: str,
    ) -> dict[str, Any] | None:
        normalized_channel_id = self._safe_int(channel_id)
        normalized_message_id = self._safe_int(message_id)
        normalized_prompt_id = self._safe_int(prompt_id)
        if (
            normalized_channel_id is None
            or normalized_message_id is None
            or normalized_prompt_id is None
        ):
            return None
        row = await self.pool.fetchrow(
            """
            INSERT INTO message_monitoring_runs (
                channel_id,
                message_id,
                prompt_id,
                request_payload,
                response_text,
                error,
                status,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (channel_id, message_id, prompt_id)
            DO UPDATE SET
                request_payload = EXCLUDED.request_payload,
                response_text = EXCLUDED.response_text,
                error = EXCLUDED.error,
                status = EXCLUDED.status,
                updated_at = NOW()
            RETURNING *
            """,
            normalized_channel_id,
            normalized_message_id,
            normalized_prompt_id,
            request_payload,
            response_text,
            error,
            status,
        )
        return dict(row) if row else None
