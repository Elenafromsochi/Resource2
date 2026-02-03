from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import Channel
from telethon.tl.types import Chat
from telethon.tl.types import User

from .deepseek import DeepSeek
from .storage import Storage
from .telegram import Telegram


class Mediator:
    def __init__(self, telegram: Telegram, deepseek: DeepSeek, storage: Storage) -> None:
        self.telegram = telegram
        self.deepseek = deepseek
        self.storage = storage

    async def get_channel_entity(self, value: str) -> Channel | Chat:
        normalized = self.normalize_identifier(value)
        if normalized is None:
            raise ValueError('Empty channel identifier')
        entity = await self.telegram.client.get_entity(normalized)
        if not isinstance(entity, (Channel, Chat)):
            raise ValueError('Entity is not a channel or group')
        return entity

    async def get_user_entity(self, value: str) -> User:
        normalized = self.normalize_identifier(value)
        if normalized is None:
            raise ValueError('Empty user identifier')
        entity = await self.telegram.client.get_entity(normalized)
        if not isinstance(entity, User):
            raise ValueError('Entity is not a user')
        return entity

    async def import_dialogs(self) -> list[dict[str, Any]]:
        dialogs = await self.telegram.client.get_dialogs()
        saved = []
        for dialog in dialogs:
            entity = dialog.entity
            if isinstance(entity, (Channel, Chat)):
                channel = self.format_channel(entity)
                saved.append(await self.storage.channels.upsert(channel))
        return saved

    async def analyze_activity(
        self,
        date_from: datetime,
        date_to: datetime,
        channel_ids: list[int] | None = None,
    ) -> dict[str, int]:
        if channel_ids:
            channels = await self.storage.channels.get_by_ids(channel_ids)
        else:
            channels = await self.storage.channels.list_all()
        user_stats = {}
        for channel in channels:
            entity = await self.resolve_channel_entity(channel)
            async for message in self.telegram.client.iter_messages(
                entity,
                offset_date=date_to,
            ):
                if message.date < date_from:
                    break
                sender_id = message.sender_id
                if not sender_id:
                    continue
                stats = user_stats.setdefault(
                    sender_id,
                    {'total': 0, 'channels': {}},
                )
                stats['total'] += 1
                stats['channels'][channel['id']] = (
                    stats['channels'].get(channel['id'], 0) + 1
                )

        for user_id, stats in user_stats.items():
            user_entity, about = await self.get_user_details(user_id)
            user = self.format_user(user_entity, about)
            user['messages_count'] = stats['total']
            await self.storage.users.upsert(user)
            await self.storage.users.replace_user_channels(
                user_id,
                stats['channels'],
            )

        return {'users_analyzed': len(user_stats)}

    def normalize_identifier(self, value: str) -> str | int | None:
        raw = value.strip()
        if not raw:
            return None
        match = re.search(r'(?:https?://)?t\.me/([^/?#]+)', raw)
        if match:
            return match.group(1)
        if raw.startswith('@'):
            return raw[1:]
        if raw.isdigit():
            return int(raw)
        return raw

    async def resolve_channel_entity(self, channel: dict[str, Any]) -> Channel | Chat:
        if channel.get('username'):
            return await self.telegram.client.get_entity(channel['username'])
        return await self.telegram.client.get_entity(channel['id'])

    def format_channel(self, entity: Channel | Chat) -> dict[str, Any]:
        channel_type = 'channel'
        if isinstance(entity, Chat):
            channel_type = 'group'
        elif isinstance(entity, Channel) and entity.megagroup:
            channel_type = 'group'
        username = getattr(entity, 'username', None)
        link = f'https: //t.me/{username}' if username else None
        title = getattr(entity, 'title', None) or 'Untitled'
        return {
            'id': entity.id,
            'username': username,
            'title': title,
            'channel_type': channel_type,
            'link': link,
        }

    async def get_user_details(self, user_id: int) -> tuple[User, str | None]:
        user = await self.telegram.client.get_entity(user_id)
        about = None
        try:
            full = await self.telegram.client(GetFullUserRequest(user_id))
            about = full.full_user.about
        except Exception:
            about = None
        return user, about

    def format_user(self, entity: User, about: str | None) -> dict[str, Any]:
        return {
            'id': entity.id,
            'username': entity.username,
            'first_name': entity.first_name,
            'last_name': entity.last_name,
            'bio': about,
            'photo': str(entity.photo) if entity.photo else None,
            'messages_count': 0,
        }

    def format_user_details(self, entity: User, about: str | None) -> dict[str, Any]:
        return {
            'id': entity.id,
            'username': entity.username,
            'first_name': entity.first_name,
            'last_name': entity.last_name,
            'bio': about,
            'photo': str(entity.photo) if entity.photo else None,
            'phone': getattr(entity, 'phone', None),
        }
