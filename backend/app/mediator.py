from __future__ import annotations

import asyncio
import functools
import logging
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
from .exceptions import AppException
from .exceptions import ChannelEntityTypeError
from .exceptions import ChannelHasNoUsernameError
from .exceptions import ChannelNotFoundError
from .exceptions import EmptyChannelIdentifierError
from .exceptions import PromptNotFoundError
from .exceptions import UserEntityTypeError
from .storage import Storage
from .telegram import Telegram

logger = logging.getLogger(__name__)


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
    _DROP_PAYLOAD_VALUE = object()

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
        message_batch_size = 200
        messages_total = 0
        messages_created = 0
        messages_updated = 0
        channel_stats: list[dict[str, Any]] = []

        def to_count(value: Any) -> int:
            normalized = self._safe_int(value)
            return normalized if normalized is not None else 0

        def apply_upsert_stats(stats: dict[str, int]) -> tuple[int, int, int]:
            processed = to_count(stats.get('processed'))
            upserted = to_count(stats.get('upserted'))
            modified = to_count(stats.get('modified'))
            nonlocal messages_total
            nonlocal messages_created
            nonlocal messages_updated
            messages_total += processed
            messages_created += upserted
            messages_updated += modified
            return processed, upserted, modified

        for channel in channels:
            channel_id = self._safe_int(channel.get('id'))
            if channel_id is None:
                continue
            channel_title = channel.get('title')
            if channel_title is not None:
                channel_title = str(channel_title)
            channel_total = 0
            channel_created = 0
            channel_updated = 0

            def apply_channel_upsert_stats(stats: dict[str, int]) -> None:
                nonlocal channel_total
                nonlocal channel_created
                nonlocal channel_updated
                processed, upserted, modified = apply_upsert_stats(stats)
                channel_total += processed
                channel_created += upserted
                channel_updated += modified

            async def flush_batch(batch: list[dict[str, Any]]) -> None:
                if not batch:
                    return
                stats = await self.storage.messages.upsert_many(channel_id, batch)
                apply_channel_upsert_stats(stats)
                batch.clear()

            try:
                entity = await self.resolve_channel_entity(channel)
            except Exception:
                logger.exception(
                    'Failed to resolve channel entity for refresh (channel_id=%s)',
                    channel_id,
                )
                channel_stats.append(
                    {
                        'channel_id': channel_id,
                        'channel_title': channel_title,
                        'total': 0,
                        'created': 0,
                        'updated': 0,
                    }
                )
                continue
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
                    message_data = self._sanitize_message_payload(message_data)
                    if not message_data:
                        continue
                    message_data['date'] = message.date
                    message_batch.append(message_data)
                    if len(message_batch) >= message_batch_size:
                        await flush_batch(message_batch)
                await flush_batch(message_batch)
            except Exception:
                logger.exception(
                    'Failed to refresh messages for channel (channel_id=%s)',
                    channel_id,
                )
            channel_stats.append(
                {
                    'channel_id': channel_id,
                    'channel_title': channel_title,
                    'total': channel_total,
                    'created': channel_created,
                    'updated': channel_updated,
                }
            )
        channel_stats.sort(key=lambda item: item['channel_id'])
        return {
            'total': messages_total,
            'created': messages_created,
            'updated': messages_updated,
            'channels': channel_stats,
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
        if not rendered:
            return []
        format_hint = (
            'FORMAT: time message_id user_id [@username] '
            '[-> reply_message_id] [->> user_id|channel_id-message_id]: text'
        )
        return [format_hint, *rendered]

    async def analyze_rendered_messages(
        self,
        prompt_id: int,
        messages: list[str],
    ) -> dict[str, Any]:
        prompt = await self.storage.prompts.get(prompt_id)
        if not prompt:
            raise PromptNotFoundError()
        prompt_text = str(prompt.get('text') or '').strip()
        if not prompt_text:
            raise AppException('Prompt text is empty')
        normalized_messages = [
            str(message).strip()
            for message in (messages or [])
            if message is not None and str(message).strip()
        ]
        if not normalized_messages:
            raise AppException('No rendered messages to analyze')
        try:
            analysis = await self.deepseek.analyze_messages(
                prompt_text,
                normalized_messages,
            )
        except Exception:
            logger.exception(
                'Failed to analyze rendered messages (prompt_id=%s)',
                prompt_id,
            )
            raise AppException('DeepSeek analysis request failed')
        if not analysis:
            raise AppException('DeepSeek returned an empty analysis')
        return {
            'prompt_id': prompt_id,
            'prompt_title': str(prompt.get('title') or prompt_id),
            'analysis': analysis,
        }

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
        if not message_time:
            message_time = '00:00:00'
        user_id = self._get_message_user_id(message)
        if user_id is None:
            user_id = 0
        user_tag = self._format_user_tag(user_id, usernames)
        text = self._normalize_message_text(message.get('message'))
        if not text:
            text = self._normalize_message_text(message.get('text'))
        if not text:
            return None
        parts = [message_time, str(message_id), user_tag]
        reply_id = self._get_reply_message_id(message)
        if reply_id is not None:
            parts.append(f'-> {reply_id}')
        forward_reference = self._format_forward_reference(message)
        if forward_reference:
            parts.append(f'->> {forward_reference}')
        return f"{' '.join(parts)}: {text}"

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
    def _format_user_tag(user_id: int, usernames: dict[int, str]) -> str:
        username = usernames.get(user_id)
        if username:
            normalized_username = str(username).strip().lstrip('@')
            if normalized_username:
                return f'{user_id} @{normalized_username}'
        return str(user_id)

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
        if not isinstance(value, str):
            return ''
        text = value
        text = ' '.join(text.splitlines())
        return text.strip()

    @classmethod
    def _sanitize_message_payload(cls, value: Any) -> Any:
        if isinstance(value, dict):
            cleaned: dict[Any, Any] = {}
            for key, nested_value in value.items():
                if cls._should_drop_message_key(key):
                    continue
                normalized_value = cls._sanitize_message_payload(nested_value)
                if normalized_value is cls._DROP_PAYLOAD_VALUE:
                    continue
                cleaned[key] = normalized_value
            return cleaned
        if isinstance(value, list):
            cleaned_items: list[Any] = []
            for item in value:
                normalized_value = cls._sanitize_message_payload(item)
                if normalized_value is cls._DROP_PAYLOAD_VALUE:
                    continue
                cleaned_items.append(normalized_value)
            return cleaned_items
        if isinstance(value, tuple):
            cleaned_items: list[Any] = []
            for item in value:
                normalized_value = cls._sanitize_message_payload(item)
                if normalized_value is cls._DROP_PAYLOAD_VALUE:
                    continue
                cleaned_items.append(normalized_value)
            return tuple(cleaned_items)
        if isinstance(value, (bytes, bytearray, memoryview)):
            return cls._DROP_PAYLOAD_VALUE
        return value

    @staticmethod
    def _should_drop_message_key(key: Any) -> bool:
        if not isinstance(key, str):
            return False
        normalized = key.lower()
        if 'file_reference' in normalized:
            return True
        if normalized == 'file_ref':
            return True
        return normalized.endswith('_file_ref')

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

    def _format_forward_reference(self, message: dict[str, Any]) -> str | None:
        fwd = message.get('fwd_from')
        if not isinstance(fwd, dict):
            return None
        source_id = None
        source_message_id = None
        from_id = fwd.get('from_id')
        if isinstance(from_id, dict):
            source_id = self._safe_int(from_id.get('user_id'))
            if source_id is None:
                source_id = self._safe_int(from_id.get('channel_id'))
            if source_id is None:
                source_id = self._safe_int(from_id.get('chat_id'))
        if source_id is None:
            source_id = self._safe_int(fwd.get('user_id'))
        if source_id is None:
            source_id = self._safe_int(fwd.get('channel_id'))
        if source_id is None:
            source_id = self._safe_int(fwd.get('chat_id'))
        saved_from_peer = fwd.get('saved_from_peer')
        if source_id is None and isinstance(saved_from_peer, dict):
            source_id = self._safe_int(saved_from_peer.get('user_id'))
            if source_id is None:
                source_id = self._safe_int(saved_from_peer.get('channel_id'))
            if source_id is None:
                source_id = self._safe_int(saved_from_peer.get('chat_id'))
        source_message_id = self._safe_int(fwd.get('channel_post'))
        if source_message_id is None:
            source_message_id = self._safe_int(fwd.get('saved_from_msg_id'))
        if source_id is None or source_message_id is None:
            return None
        return f'{source_id}-{source_message_id}'

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
