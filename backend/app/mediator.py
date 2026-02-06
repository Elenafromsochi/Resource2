from __future__ import annotations

import asyncio
import functools
import re
from datetime import datetime
from typing import Any

from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import Channel
from telethon.tl.types import Chat
from telethon.tl.types import User

from .deepseek import DeepSeek
from .exceptions import ChannelEntityTypeError
from .exceptions import ChannelHasNoUsernameError
from .exceptions import ChannelNotFoundError
from .exceptions import EmptyChannelIdentifierError
from .exceptions import UserEntityTypeError
from .storage import Storage
from .telegram import Telegram


def async_cache(func):
    cache_name = f'_{func.__name__}_cache'

    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        cache = getattr(self, cache_name, None)
        if cache is None:
            cache = {}
            setattr(self, cache_name, cache)
        key = (args, tuple(sorted(kwargs.items())))
        if key in cache:
            cached = cache[key]
            if asyncio.isfuture(cached):
                return await cached
            return cached
        task = asyncio.create_task(func(self, *args, **kwargs))
        cache[key] = task
        try:
            result = await task
        except Exception:
            cache.pop(key, None)
            raise
        cache[key] = result
        return result

    return wrapper


class Mediator:
    def __init__(self, telegram: Telegram, deepseek: DeepSeek, storage: Storage) -> None:
        self.telegram = telegram
        self.deepseek = deepseek
        self.storage = storage

    @async_cache
    async def get_channel_entity(self, channel_id: int) -> Channel | Chat:
        channel = await self.storage.channels.get(channel_id)
        if not channel:
            raise ChannelNotFoundError()
        entity = None
        if channel_id > 0:
            peer_id = self.make_channel_peer_id(channel_id)
            entity = await self.safe_get_entity(peer_id)
        if not isinstance(entity, (Channel, Chat)):
            entity = await self.safe_get_entity(channel_id)
        if isinstance(entity, (Channel, Chat)):
            return entity
        username = channel.get('username')
        if not username:
            raise ChannelHasNoUsernameError()
        identifier = self.normalize_identifier(username)
        if identifier is None:
            raise ChannelHasNoUsernameError()
        entity = await self.telegram.client.get_entity(identifier)
        if not isinstance(entity, (Channel, Chat)):
            raise ChannelEntityTypeError()
        return entity

    async def get_channel_entity_by_identifier(self, value: str) -> Channel | Chat:
        normalized = self.normalize_identifier(value)
        if normalized is None:
            raise EmptyChannelIdentifierError()
        entity = None
        if isinstance(normalized, int):
            if normalized > 0:
                peer_id = self.make_channel_peer_id(normalized)
                entity = await self.safe_get_entity(peer_id)
            if not isinstance(entity, (Channel, Chat)):
                entity = await self.safe_get_entity(normalized)
        else:
            entity = await self.safe_get_entity(normalized)
        if not isinstance(entity, (Channel, Chat)):
            raise ChannelEntityTypeError()
        return entity

    @async_cache
    async def get_user_entity(self, user_id: int) -> User:
        try:
            entity = await self.telegram.client.get_entity(user_id)
        except Exception:
            entity = None
        if isinstance(entity, User):
            return entity
        user_data = await self.storage.users.get(user_id)
        username = user_data.get('username') if user_data else None
        if not username:
            raise UserEntityTypeError()
        identifier = self.normalize_identifier(username)
        if identifier is None:
            raise UserEntityTypeError()
        entity = await self.telegram.client.get_entity(identifier)
        if not isinstance(entity, User):
            raise UserEntityTypeError()
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

    async def refresh_messages_cache(
        self,
        date_from: datetime,
        date_to: datetime,
        channel_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        if channel_ids:
            channels = await self.storage.channels.get_by_ids(channel_ids)
        else:
            channels = await self.storage.channels.list_all()
        errors: list[str] = []
        message_batch_size = 200
        channels_processed = 0
        messages_upserted = 0
        messages_updated = 0
        channel_stats: list[dict[str, Any]] = []

        def init_channel_stats(channel: dict[str, Any]) -> dict[str, Any]:
            return {
                'channel_id': channel.get('id'),
                'channel_title': channel.get('title') or 'Untitled',
                'channel_username': channel.get('username'),
                'messages_upserted': 0,
                'messages_updated': 0,
            }

        def apply_upsert_stats(
            stats: dict[str, int],
            channel_stat: dict[str, Any],
        ) -> None:
            upserted = int(stats.get('upserted', 0) or 0)
            modified = int(stats.get('modified', 0) or 0)
            nonlocal messages_upserted
            nonlocal messages_updated
            messages_upserted += upserted
            messages_updated += modified
            channel_stat['messages_upserted'] += upserted
            channel_stat['messages_updated'] += modified

        async def flush_batch(
            batch: list[dict[str, Any]],
            channel_stat: dict[str, Any],
        ) -> None:
            if not batch:
                return
            stats = await self.storage.messages.upsert_many(batch)
            apply_upsert_stats(stats, channel_stat)
            batch.clear()
        for channel in channels:
            channel_stat = init_channel_stats(channel)
            channel_id = channel_stat['channel_id']
            try:
                entity = await self.resolve_channel_entity(channel)
            except Exception as exc:
                errors.append(f'channel {channel_id}: {exc}')
                channel_stats.append(channel_stat)
                continue
            channels_processed += 1
            try:
                message_batch: list[dict[str, Any]] = []
                async for message in self.telegram.client.iter_messages(
                    entity,
                    offset_date=date_to,
                ):
                    if message.date < date_from:
                        break
                    message_data = message.to_dict()
                    if not message_data:
                        continue
                    message_batch.append(message_data)
                    if len(message_batch) >= message_batch_size:
                        await flush_batch(message_batch, channel_stat)
                await flush_batch(message_batch, channel_stat)
            except Exception as exc:
                errors.append(f'channel {channel_id}: {exc}')
            channel_stats.append(channel_stat)
        return {
            'channels_processed': channels_processed,
            'messages_upserted': messages_upserted,
            'messages_updated': messages_updated,
            'channels': channel_stats,
            'errors': errors,
        }

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
        user_stats: dict[int, dict[str, Any]] = {}
        errors: list[str] = []
        message_batch_size = 200
        for channel in channels:
            channel_id = channel.get('id')
            try:
                entity = await self.resolve_channel_entity(channel)
            except Exception as exc:
                errors.append(f'channel {channel_id}: {exc}')
                continue
            try:
                message_batch: list[dict[str, Any]] = []
                async for message in self.telegram.client.iter_messages(
                    entity,
                    offset_date=date_to,
                ):
                    if message.date < date_from:
                        break
                    message_batch.append(message.to_dict())
                    if len(message_batch) >= message_batch_size:
                        await self.storage.messages.upsert_many(message_batch)
                        message_batch.clear()
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
                if message_batch:
                    await self.storage.messages.upsert_many(message_batch)
            except Exception as exc:
                errors.append(f'channel {channel_id}: {exc}')
                continue

        users_analyzed = 0
        for user_id, stats in user_stats.items():
            try:
                user_entity, about = await self.get_user_details(user_id)
                user = self.format_user(user_entity, about)
                await self.storage.users.upsert(user)
                await self.storage.users.replace_user_channels(
                    user_id,
                    stats['channels'],
                )
                users_analyzed += 1
            except Exception as exc:
                errors.append(f'user {user_id}: {exc}')

        return {'users_analyzed': users_analyzed, 'errors': errors}

    async def refresh_user_message_stats(self) -> dict[str, int | list[str]]:
        stats = await self.storage.messages.aggregate_user_message_stats()
        channels = await self.storage.channels.list_all()
        channel_ids = {channel['id'] for channel in channels}
        user_ids: list[int] = []
        channel_rows: list[tuple[int, int, int]] = []
        messages_total = 0
        unknown_channel_ids: set[int] = set()

        for entry in stats:
            user_id = entry.get('user_id')
            if user_id is None:
                continue
            try:
                user_id = int(user_id)
            except (TypeError, ValueError):
                continue
            total = entry.get('total', 0)
            try:
                total = int(total)
            except (TypeError, ValueError):
                total = 0
            user_ids.append(user_id)
            messages_total += total
            for channel in entry.get('channels', []) or []:
                channel_id = channel.get('channel_id')
                if channel_id is None:
                    continue
                try:
                    channel_id = int(channel_id)
                except (TypeError, ValueError):
                    continue
                if channel_id not in channel_ids:
                    unknown_channel_ids.add(channel_id)
                    continue
                count = channel.get('messages_count', 0)
                try:
                    count = int(count)
                except (TypeError, ValueError):
                    count = 0
                channel_rows.append((channel_id, user_id, count))

        await self.storage.users.ensure_users_exist(user_ids)
        await self.storage.users.replace_all_channel_users(channel_rows)

        errors: list[str] = []
        if unknown_channel_ids:
            errors.append(
                'Пропущены сообщения по '
                f'{len(unknown_channel_ids)} каналам, которых нет в базе.',
            )

        channels_with_messages = len({row[0] for row in channel_rows})

        return {
            'users_updated': len(user_ids),
            'channels_with_messages': channels_with_messages,
            'messages_total': messages_total,
            'errors': errors,
        }

    def normalize_identifier(self, value: str) -> str | int | None:
        raw = value.strip()
        if not raw:
            return None
        match = re.search(r'(?:https?://)?t\.me/([^/?#]+)', raw)
        if match:
            return match.group(1)
        if raw.startswith('@'):
            return raw[1:]
        if raw.lstrip('-').isdigit():
            return int(raw)
        return raw

    @staticmethod
    def make_channel_peer_id(channel_id: int) -> int:
        if channel_id <= 0:
            return channel_id
        return int(f'-100{channel_id}')

    async def safe_get_entity(self, identifier: str | int) -> Channel | Chat | User | None:
        try:
            return await self.telegram.client.get_entity(identifier)
        except Exception:
            return None

    async def resolve_channel_entity(self, channel: dict[str, Any]) -> Channel | Chat:
        return await self.get_channel_entity(channel['id'])

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
        user = await self.get_user_entity(user_id)
        about = None
        try:
            full = await self.telegram.client(GetFullUserRequest(user))
            about = full.full_user.about
        except Exception:
            about = None
        return user, about

    async def get_channel_details(
        self,
        channel_id: int,
    ) -> tuple[Channel | Chat, str | None, int | None]:
        entity = await self.get_channel_entity(channel_id)
        about = None
        members_count = None
        try:
            if isinstance(entity, Channel):
                full = await self.telegram.client(GetFullChannelRequest(entity))
            else:
                full = await self.telegram.client(GetFullChatRequest(entity.id))
            full_chat = full.full_chat
            about = getattr(full_chat, 'about', None)
            members_count = getattr(full_chat, 'participants_count', None)
        except Exception:
            about = None
            members_count = None
        return entity, about, members_count

    def format_user(self, entity: User, about: str | None) -> dict[str, Any]:
        return {
            'id': entity.id,
            'username': entity.username,
            'first_name': entity.first_name,
            'last_name': entity.last_name,
            'bio': about,
            'photo': str(entity.photo) if entity.photo else None,
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

    def format_channel_details(
        self,
        entity: Channel | Chat,
        about: str | None,
        members_count: int | None,
    ) -> dict[str, Any]:
        data = self.format_channel(entity)
        data.update(
            {
                'about': about,
                'members_count': members_count,
            }
        )
        return data
