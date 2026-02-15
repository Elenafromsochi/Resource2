from openai import AsyncOpenAI

from .config import DEEPSEEK_API_KEY
from .config import DEEPSEEK_BASE_URL
from .config import DEEPSEEK_MODEL


class DeepSeek:
    def __init__(self) -> None:
        self.client: AsyncOpenAI = AsyncOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
        )

    @staticmethod
    def _normalize_chat_content(value) -> str:
        if value is None:
            return ''
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            parts: list[str] = []
            for item in value:
                if isinstance(item, str):
                    normalized = item.strip()
                    if normalized:
                        parts.append(normalized)
                    continue
                if isinstance(item, dict):
                    text = item.get('text')
                    if isinstance(text, str):
                        normalized = text.strip()
                        if normalized:
                            parts.append(normalized)
            return '\n'.join(parts).strip()
        return str(value).strip()

    async def analyze_messages(self, base_prompt: str, messages: list[str]) -> str:
        rendered_messages = '\n'.join(
            str(message).strip() for message in messages if str(message).strip()
        ).strip()
        if not rendered_messages:
            return ''
        completion = await self.client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {'role': 'system', 'content': base_prompt.strip()},
                {'role': 'user', 'content': rendered_messages},
            ],
            stream=False,
        )
        if not completion.choices:
            return ''
        return self._normalize_chat_content(completion.choices[0].message.content)
