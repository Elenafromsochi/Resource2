from openai import AsyncOpenAI

from .config import DEEPSEEK_API_KEY
from .config import DEEPSEEK_BASE_URL


class DeepSeek:
    def __init__(self) -> None:
        self.client: AsyncOpenAI = AsyncOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
        )
