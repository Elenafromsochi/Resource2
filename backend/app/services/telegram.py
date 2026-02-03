from telethon import TelegramClient
from telethon.sessions import StringSession

from app.config import TELEGRAM_API_HASH
from app.config import TELEGRAM_API_ID
from app.config import TELETHON_SESSION


class Telegram:
    def __init__(self) -> None:
        self.client: TelegramClient = TelegramClient(
            StringSession(TELETHON_SESSION),
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH,
        )

    async def init(self) -> None:
        await self.client.start()

    async def close(self) -> None:
        await self.client.disconnect()
