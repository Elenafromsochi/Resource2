from .base import BaseRepository


class PromptsRepository(BaseRepository):
    async def list(self):
        rows = await self.pool.fetch(
            """
            SELECT id, title, text, created_at, updated_at
            FROM prompts
            ORDER BY id
            """,
        )
        return [dict(row) for row in rows]

    async def get(self, prompt_id: int):
        row = await self.pool.fetchrow(
            """
            SELECT id, title, text, created_at, updated_at
            FROM prompts
            WHERE id = $1
            """,
            prompt_id,
        )
        return dict(row) if row else None

    async def create(self, title: str, text: str):
        row = await self.pool.fetchrow(
            """
            INSERT INTO prompts (title, text, created_at, updated_at)
            VALUES ($1, $2, NOW(), NOW())
            RETURNING id, title, text, created_at, updated_at
            """,
            title,
            text,
        )
        return dict(row) if row else None

    async def update(self, prompt_id: int, title: str, text: str):
        row = await self.pool.fetchrow(
            """
            UPDATE prompts
            SET title = $1, text = $2, updated_at = NOW()
            WHERE id = $3
            RETURNING id, title, text, created_at, updated_at
            """,
            title,
            text,
            prompt_id,
        )
        return dict(row) if row else None

    async def delete(self, prompt_id: int):
        await self.pool.execute(
            'DELETE FROM prompts WHERE id = $1',
            prompt_id,
        )
