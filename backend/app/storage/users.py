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

    async def upsert_conclusions(
        self,
        conclusions: list[dict[str, Any]],
    ) -> int:
        if not conclusions:
            return 0
        incoming_by_id: dict[int, dict[str, Any]] = {}
        for item in conclusions:
            if not isinstance(item, dict):
                continue
            user_id = item.get('id')
            try:
                user_id = int(user_id)
            except (TypeError, ValueError):
                continue
            conclusion = item.get('conclusion')
            if not isinstance(conclusion, dict):
                continue
            existing_incoming = incoming_by_id.get(user_id)
            if existing_incoming is None:
                incoming_by_id[user_id] = conclusion
            else:
                incoming_by_id[user_id] = self._merge_conclusion_dicts(
                    existing_incoming,
                    conclusion,
                )
        if not incoming_by_id:
            return 0
        existing_by_id: dict[int, dict[str, Any]] = {}
        existing_rows = await self.pool.fetch(
            'SELECT id, conclusion FROM users WHERE id = ANY($1)',
            list(incoming_by_id.keys()),
        )
        for row in existing_rows:
            existing_conclusion = row['conclusion']
            if not isinstance(existing_conclusion, dict):
                continue
            existing_by_id[int(row['id'])] = existing_conclusion
        rows: list[tuple[int, dict[str, Any]]] = []
        for user_id, incoming_conclusion in incoming_by_id.items():
            existing_conclusion = existing_by_id.get(user_id)
            if existing_conclusion is None:
                merged = incoming_conclusion
            else:
                merged = self._merge_conclusion_dicts(
                    existing_conclusion,
                    incoming_conclusion,
                )
            rows.append((user_id, merged))
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO users (id, conclusion, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (id)
                DO UPDATE SET
                    conclusion = EXCLUDED.conclusion,
                    updated_at = NOW()
                """,
                rows,
            )
        return len(rows)

    @classmethod
    def _merge_conclusion_dicts(
        cls,
        current: dict[str, Any],
        incoming: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(current)
        for key, incoming_value in incoming.items():
            if key not in merged:
                merged[key] = incoming_value
                continue
            merged[key] = cls._merge_conclusion_values(
                merged[key],
                incoming_value,
            )
        return merged

    @classmethod
    def _merge_conclusion_values(cls, current: Any, incoming: Any) -> Any:
        if current is None:
            return incoming
        if incoming is None:
            return current
        if isinstance(current, dict) and isinstance(incoming, dict):
            return cls._merge_conclusion_dicts(current, incoming)
        if isinstance(current, list) and isinstance(incoming, list):
            return cls._merge_conclusion_lists(current, incoming)
        if isinstance(current, list):
            return cls._merge_conclusion_lists(current, [incoming])
        if isinstance(incoming, list):
            return cls._merge_conclusion_lists([current], incoming)
        if current == incoming:
            return current
        return cls._merge_conclusion_lists([current], [incoming])

    @staticmethod
    def _merge_conclusion_lists(current: list[Any], incoming: list[Any]) -> list[Any]:
        merged: list[Any] = []
        for value in [*current, *incoming]:
            if value not in merged:
                merged.append(value)
        return merged

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

    async def list_with_conclusions(
        self,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        normalized_limit = 200
        try:
            normalized_limit = int(limit)
        except (TypeError, ValueError):
            normalized_limit = 200
        normalized_limit = max(1, min(normalized_limit, 2000))
        rows = await self.pool.fetch(
            """
            SELECT id, conclusion
            FROM users
            WHERE conclusion IS NOT NULL
            ORDER BY updated_at DESC, id DESC
            LIMIT $1
            """,
            normalized_limit,
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
