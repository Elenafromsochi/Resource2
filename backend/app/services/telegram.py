from telethon import TelegramClient

from app import config


class Telegram:
    def __init__(self):
        self.client = TelegramClient(
            config.TELETHON_SESSION,
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )

    async def init(self):
        await self.client.start()

    async def close(self):
        await self.client.disconnect()
