from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from datetime import timezone
from typing import Any

from telethon import events
from telethon.tl.types import PeerChannel
from telethon.tl.types import PeerChat

from .common import normalize_datetime
from .common import normalize_message_text
from .common import safe_int
from .config import MONITOR_DEFAULT_CHANNELS
from .config import MONITOR_USERS_CONTEXT_LIMIT
from .deepseek import DeepSeek
from .mediator import Mediator
from .storage import Storage
from .telegram import Telegram

logger = logging.getLogger(__name__)


class ChannelMonitoringService:
    def __init__(
        self,
        telegram: Telegram,
        deepseek: DeepSeek,
        storage: Storage,
        mediator: Mediator,
    ) -> None:
        self.telegram = telegram
        self.deepseek = deepseek
        self.storage = storage
        self.mediator = mediator
        self._handler_registered = False
        self._event_handler = self._on_new_message
        self._tasks: set[asyncio.Task[Any]] = set()
        self._inflight_keys: set[tuple[int, int, int]] = set()
        self._inflight_lock = asyncio.Lock()

    async def init(self) -> None:
        await self._bootstrap_default_channels()
        if self._handler_registered:
            return
        self.telegram.client.add_event_handler(
            self._event_handler,
            events.NewMessage(incoming=True),
        )
        self._handler_registered = True

    async def close(self) -> None:
        if self._handler_registered:
            self.telegram.client.remove_event_handler(self._event_handler)
            self._handler_registered = False
        if not self._tasks:
            return
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()

    async def _bootstrap_default_channels(self) -> None:
        for identifier in MONITOR_DEFAULT_CHANNELS:
            try:
                entity = await self.mediator.get_channel_entity_by_identifier(identifier)
            except Exception:
                logger.exception(
                    'Failed to bootstrap monitoring channel identifier=%s',
                    identifier,
                )
                continue
            try:
                channel = self.mediator.format_channel(entity)
                await self.storage.channels.upsert(channel)
            except Exception:
                logger.exception(
                    'Failed to save bootstrap monitoring channel identifier=%s',
                    identifier,
                )

    async def _on_new_message(self, event: Any) -> None:
        message = getattr(event, 'message', None)
        channel_id = self._extract_channel_id(message)
        if channel_id is None:
            return
        task = asyncio.create_task(self._process_message(channel_id, message))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _process_message(self, channel_id: int, message: Any) -> None:
        channel = await self.storage.channels.get(channel_id)
        if not channel or not bool(channel.get('monitoring_enabled')):
            return
        prompt_id = safe_int(channel.get('monitoring_prompt_id'))
        if prompt_id is None:
            return
        raw_message = message.to_dict() if message is not None else None
        if not isinstance(raw_message, dict):
            return
        normalized_message = self.mediator._sanitize_message_payload(raw_message)
        if not isinstance(normalized_message, dict):
            return
        message_id = safe_int(normalized_message.get('id'))
        if message_id is None:
            message_id = safe_int(getattr(message, 'id', None))
        if message_id is None:
            return
        normalized_message['id'] = message_id
        message_date = normalize_datetime(
            getattr(message, 'date', None) or normalized_message.get('date'),
        )
        if message_date is None:
            message_date = datetime.now(timezone.utc)
        normalized_message['date'] = message_date
        key = (channel_id, message_id, prompt_id)
        acquired = await self._acquire_processing_key(key)
        if not acquired:
            return
        request_payload: dict[str, Any] = {}
        try:
            processed = await self.storage.monitoring_runs.has_successful_result(
                channel_id,
                message_id,
                prompt_id,
            )
            if processed:
                return
            await self.storage.messages.upsert_many(channel_id, [normalized_message])
            sender_id = self.mediator._get_message_user_id(normalized_message)
            if sender_id is not None:
                await self.storage.users.ensure_users_exist([sender_id])
            prompt = await self.storage.prompts.get(prompt_id)
            prompt_text = str(prompt.get('text') if prompt else '').strip()
            if not prompt_text:
                raise RuntimeError(
                    f'Monitoring prompt {prompt_id} is not found or empty',
                )
            users = await self.storage.users.list_with_conclusions(
                MONITOR_USERS_CONTEXT_LIMIT,
            )
            request_payload = self._build_request_payload(
                channel=channel,
                message=normalized_message,
                users=users,
            )
            request_text = json.dumps(
                request_payload,
                ensure_ascii=False,
                separators=(',', ':'),
            )
            response_text = await self.deepseek.analyze_messages(
                prompt_text,
                [request_text],
            )
            if not response_text:
                raise RuntimeError('DeepSeek returned an empty monitoring response')
            await self.storage.monitoring_runs.save_success(
                channel_id=channel_id,
                message_id=message_id,
                prompt_id=prompt_id,
                request_payload=request_payload,
                response_text=response_text,
            )
            await self._try_update_user_conclusions(response_text)
            await self.storage.channels.set_monitoring_success(
                channel_id=channel_id,
                message_id=message_id,
                message_date=message_date,
            )
        except Exception as exc:
            logger.exception(
                'Failed to process monitored message '
                '(channel_id=%s, message_id=%s, prompt_id=%s)',
                channel_id,
                message_id,
                prompt_id,
            )
            error_text = str(exc).strip() or exc.__class__.__name__
            fallback_payload = request_payload or {
                'channel_id': channel_id,
                'message_id': message_id,
                'prompt_id': prompt_id,
            }
            try:
                await self.storage.monitoring_runs.save_error(
                    channel_id=channel_id,
                    message_id=message_id,
                    prompt_id=prompt_id,
                    request_payload=fallback_payload,
                    error=error_text,
                )
            except Exception:
                logger.exception(
                    'Failed to persist monitoring error '
                    '(channel_id=%s, message_id=%s, prompt_id=%s)',
                    channel_id,
                    message_id,
                    prompt_id,
                )
            await self.storage.channels.set_monitoring_error(channel_id, error_text)
        finally:
            await self._release_processing_key(key)

    async def _try_update_user_conclusions(self, analysis: str) -> None:
        try:
            conclusions = self.mediator._extract_user_conclusions(analysis)
        except Exception:
            logger.exception('Failed to parse monitoring DeepSeek output')
            return
        if not conclusions:
            return
        try:
            await self.storage.users.upsert_conclusions(conclusions)
        except Exception:
            logger.exception('Failed to persist conclusions from monitoring output')

    def _build_request_payload(
        self,
        channel: dict[str, Any],
        message: dict[str, Any],
        users: list[dict[str, Any]],
    ) -> dict[str, Any]:
        message_id = safe_int(message.get('id'))
        sender_id = self.mediator._get_message_user_id(message)
        reply_message_id = self.mediator._get_reply_message_id(message)
        message_text = normalize_message_text(message.get('message'))
        if not message_text:
            message_text = normalize_message_text(message.get('text'))
        message_date = normalize_datetime(message.get('date'))
        normalized_users: list[dict[str, Any]] = []
        for user in users:
            if not isinstance(user, dict):
                continue
            user_id = safe_int(user.get('id'))
            if user_id is None:
                continue
            normalized_users.append(
                {
                    'id': user_id,
                    'conclusion': user.get('conclusion'),
                }
            )
        return {
            'channel': {
                'id': safe_int(channel.get('id')),
                'title': channel.get('title'),
                'username': channel.get('username'),
                'link': channel.get('link'),
            },
            'message': {
                'id': message_id,
                'date': message_date.isoformat() if message_date else None,
                'sender_id': sender_id,
                'reply_to_message_id': reply_message_id,
                'text': message_text,
            },
            'users': normalized_users,
        }

    @staticmethod
    def _extract_channel_id(message: Any) -> int | None:
        if message is None:
            return None
        peer = getattr(message, 'peer_id', None)
        if isinstance(peer, PeerChannel):
            return int(peer.channel_id)
        if isinstance(peer, PeerChat):
            return int(peer.chat_id)
        return None

    async def _acquire_processing_key(self, key: tuple[int, int, int]) -> bool:
        async with self._inflight_lock:
            if key in self._inflight_keys:
                return False
            self._inflight_keys.add(key)
            return True

    async def _release_processing_key(self, key: tuple[int, int, int]) -> None:
        async with self._inflight_lock:
            self._inflight_keys.discard(key)
