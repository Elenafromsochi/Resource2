from __future__ import annotations

import asyncio
import functools
import re
from datetime import datetime, timezone
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

    async def render_messages(
        self,
        channel_id: int,
        date_from: datetime,
        date_to: datetime,
    ) -> list[str]:
        messages = await self.storage.messages.list_by_channel_and_date(
            channel_id,
            date_from,
            date_to,
        )
        messages = await self._extend_messages_with_missing_replies(
            channel_id,
            messages,
        )
        user_ids = self._collect_message_user_ids(messages)
        usernames = await self._get_usernames_by_ids(user_ids)
        rendered: list[str] = []
        for message in messages:
            line = self._format_message_line(message, usernames)
            if line:
                rendered.append(line)
        return rendered

    async def _extend_messages_with_missing_replies(
        self,
        channel_id: int,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not messages:
            return []
        message_by_id: dict[int, dict[str, Any]] = {}
        ordered: list[dict[str, Any]] = []
        for message in messages:
            message_id = self._safe_int(message.get('id'))
            if message_id is None:
                continue
            if message_id in message_by_id:
                continue
            message_by_id[message_id] = message
            ordered.append(message)
        missing_reply_ids: set[int] = set()
        for message in ordered:
            reply_id = self._get_reply_message_id(message)
            if reply_id is not None and reply_id not in message_by_id:
                missing_reply_ids.add(reply_id)
        if missing_reply_ids:
            reply_messages = await self.storage.messages.list_by_channel_and_ids(
                channel_id,
                list(missing_reply_ids),
            )
            for reply_message in reply_messages:
                reply_id = self._safe_int(reply_message.get('id'))
                if reply_id is None or reply_id in message_by_id:
                    continue
                message_by_id[reply_id] = reply_message
                ordered.append(reply_message)
        return sorted(ordered, key=self._message_sort_key)

    def _collect_message_user_ids(
        self,
        messages: list[dict[str, Any]],
    ) -> set[int]:
        user_ids: set[int] = set()
        for message in messages:
            user_id = self._get_message_user_id(message)
            if user_id is not None:
                user_ids.add(user_id)
            user_ids.update(self._get_forward_user_ids(message))
        return user_ids

    async def _get_usernames_by_ids(
        self,
        user_ids: set[int],
    ) -> dict[int, str]:
        if not user_ids:
            return {}
        users = await self.storage.users.list_by_ids(list(user_ids))
        usernames: dict[int, str] = {}
        for user in users:
            user_id = self._safe_int(user.get('id'))
            if user_id is None:
                continue
            username = user.get('username')
            if username:
                usernames[user_id] = username
        return usernames

    async def refresh_user_message_stats(self) -> dict[str, int | list[str]]:
        stats = await self.storage.messages.aggregate_user_message_stats()
        user_ids: list[int] = []
        messages_total = 0
        channel_ids: set[int] = set()

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
                channel_ids.add(channel_id)
        return {
            'users_updated': len(user_ids),
            'channels_with_messages': len(channel_ids),
            'messages_total': messages_total,
            'errors': [],
        }

    def _format_message_line(
        self,
        message: dict[str, Any],
        usernames: dict[int, str],
    ) -> str | None:
        message_id = self._safe_int(message.get('id'))
        if message_id is None:
            return None
        message_time = self._format_message_time(message.get('date'))
        user_id = self._get_message_user_id(message)
        if user_id is None:
            user_id = 0
        user_tag = self._format_user_tag(user_id, usernames)
        parts = []
        if message_time:
            parts.append(message_time)
        parts.append(f'm{message_id}')
        if user_tag:
            parts.append(user_tag)
        reply_id = self._get_reply_message_id(message)
        if reply_id is not None:
            parts.append(f'-> m{reply_id}')
        forward_tag = self._format_forward_tag(message, usernames)
        if forward_tag:
            parts.append(forward_tag)
        if self._message_has_media(message):
            parts.append('[media]')
        if self._message_has_poll(message):
            parts.append('[pool]')
        text = self._normalize_message_text(
            message.get('message') or message.get('text')
        )
        if text:
            parts.append(text)
        return ' '.join(parts).strip()

    @staticmethod
    def _format_message_time(value: Any) -> str | None:
        if isinstance(value, datetime):
            return value.strftime('%H:%M:%S')
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                return None
            return parsed.strftime('%H:%M:%S')
        return None

    @staticmethod
    def _message_sort_key(message: dict[str, Any]) -> tuple[float, int]:
        date_value = message.get('date')
        timestamp = 0.0
        if isinstance(date_value, datetime):
            if date_value.tzinfo:
                timestamp = date_value.timestamp()
            else:
                timestamp = date_value.replace(tzinfo=timezone.utc).timestamp()
        elif isinstance(date_value, str):
            try:
                parsed = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            except ValueError:
                parsed = None
            if isinstance(parsed, datetime):
                if parsed.tzinfo:
                    timestamp = parsed.timestamp()
                else:
                    timestamp = parsed.replace(tzinfo=timezone.utc).timestamp()
        message_id = Mediator._safe_int(message.get('id')) or 0
        return (timestamp, message_id)

    @staticmethod
    def _format_user_tag(user_id: int, usernames: dict[int, str]) -> str | None:
        if user_id is None:
            return None
        username = usernames.get(user_id)
        if username:
            return f'u{username}'
        return f'u{user_id}'

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_message_text(value: Any) -> str:
        if value is None:
            return ''
        text = str(value)
        text = ' '.join(text.splitlines())
        return text.strip()

    def _get_message_user_id(self, message: dict[str, Any]) -> int | None:
        from_id = message.get('from_id')
        if isinstance(from_id, dict):
            user_id = self._safe_int(from_id.get('user_id'))
            if user_id is not None:
                return user_id
        return self._safe_int(message.get('sender_id'))

    def _get_reply_message_id(self, message: dict[str, Any]) -> int | None:
        reply_to = message.get('reply_to')
        if not isinstance(reply_to, dict):
            return None
        reply_id = reply_to.get('reply_to_msg_id')
        if reply_id is None:
            reply_id = reply_to.get('reply_to_message_id')
        return self._safe_int(reply_id)

    def _format_forward_tag(
        self,
        message: dict[str, Any],
        usernames: dict[int, str],
    ) -> str | None:
        fwd = message.get('fwd_from')
        if not isinstance(fwd, dict):
            return None
        parts: list[str] = []

        def add_part(value: str | None) -> None:
            if not value:
                return
            if value in parts:
                return
            parts.append(value)

        from_id = fwd.get('from_id')
        if isinstance(from_id, dict):
            user_id = self._safe_int(from_id.get('user_id'))
            if user_id is not None:
                add_part(self._format_user_tag(user_id, usernames))
            channel_id = self._safe_int(from_id.get('channel_id'))
            if channel_id is not None:
                channel_post = self._safe_int(fwd.get('channel_post'))
                if channel_post is not None:
                    add_part(f'ch{channel_id} m{channel_post}')
                else:
                    add_part(f'ch{channel_id}')
        user_id = self._safe_int(fwd.get('user_id'))
        if user_id is not None:
            add_part(self._format_user_tag(user_id, usernames))
        channel_id = self._safe_int(fwd.get('channel_id'))
        if channel_id is not None:
            channel_post = self._safe_int(fwd.get('channel_post'))
            if channel_post is not None:
                add_part(f'ch{channel_id} m{channel_post}')
            else:
                add_part(f'ch{channel_id}')
        if not parts:
            return None
        return f"forwarded from {' | '.join(parts)}"

    def _get_forward_user_ids(self, message: dict[str, Any]) -> set[int]:
        fwd = message.get('fwd_from')
        if not isinstance(fwd, dict):
            return set()
        user_ids: set[int] = set()
        from_id = fwd.get('from_id')
        if isinstance(from_id, dict):
            user_id = self._safe_int(from_id.get('user_id'))
            if user_id is not None:
                user_ids.add(user_id)
        user_id = self._safe_int(fwd.get('user_id'))
        if user_id is not None:
            user_ids.add(user_id)
        return user_ids

    @staticmethod
    def _message_has_media(message: dict[str, Any]) -> bool:
        media = message.get('media')
        if not media:
            return False
        if isinstance(media, dict) and media.get('_') == 'MessageMediaEmpty':
            return False
        return True

    @staticmethod
    def _message_has_poll(message: dict[str, Any]) -> bool:
        if message.get('poll'):
            return True
        media = message.get('media')
        if isinstance(media, dict):
            if media.get('_') == 'MessageMediaPoll':
                return True
            if media.get('poll') is not None:
                return True
        return False

    async def refresh_user_profiles(
        self,
        user_ids: list[int],
        concurrency: int = 5,
    ) -> tuple[list[int], list[str]]:
        if not user_ids:
            return [], []
        semaphore = asyncio.Semaphore(concurrency)

        async def refresh_one(user_id: int) -> tuple[bool, str | None]:
            async with semaphore:
                try:
                    entity, about = await self.get_user_details(user_id)
                    user_data = self.format_user(entity, about)
                    await self.storage.users.upsert(user_data)
                    return True, None
                except Exception as exc:
                    return False, f'user {user_id}: {exc}'

        results = await asyncio.gather(
            *(refresh_one(user_id) for user_id in user_ids)
        )
        updated_ids = [
            user_id
            for user_id, (success, _) in zip(user_ids, results)
            if success
        ]
        errors = [error for _, error in results if error]
        return updated_ids, errors

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
