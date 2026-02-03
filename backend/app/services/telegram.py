from telethon import TelegramClient

from app import config


class Telegram:
    def __init__(self) -> None:
        self.client: TelegramClient = TelegramClient(
            config.TELETHON_SESSION,
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )

    async def init(self) -> None:
        await self.client.start()

    async def close(self) -> None:
        await self.client.disconnect()
