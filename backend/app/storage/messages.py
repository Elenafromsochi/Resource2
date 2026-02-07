from datetime import datetime
from typing import Any

from pymongo import UpdateOne

from .base import BaseMongoRepository


class MessagesRepository(BaseMongoRepository):
    collection_name = 'messages'

    @property
    def collection(self):
        return self.db[self.collection_name]

    def _build_identity_filter(self, message: dict) -> dict[str, Any]:
        message_id = message.get('id')
        if message_id is None:
            return {}
        peer_id = message.get('peer_id')
        filter_doc: dict[str, Any] = {'id': message_id}
        if isinstance(peer_id, dict):
            if 'channel_id' in peer_id:
                filter_doc['peer_id.channel_id'] = peer_id.get('channel_id')
            elif 'chat_id' in peer_id:
                filter_doc['peer_id.chat_id'] = peer_id.get('chat_id')
            elif 'user_id' in peer_id:
                filter_doc['peer_id.user_id'] = peer_id.get('user_id')
            else:
                filter_doc['peer_id'] = peer_id
            return filter_doc
        filter_doc['peer_id'] = peer_id
        return filter_doc

    async def upsert_many(self, messages: list[dict]) -> dict[str, int]:
        if not messages:
            return {'processed': 0, 'upserted': 0, 'modified': 0, 'skipped': 0}
        operations: list[UpdateOne] = []
        skipped = 0
        for message in messages:
            filter_doc = self._build_identity_filter(message)
            if not filter_doc:
                skipped += 1
                continue
            operations.append(
                UpdateOne(
                    filter_doc,
                    {'$set': message},
                    upsert=True,
                )
            )
        if not operations:
            return {
                'processed': 0,
                'upserted': 0,
                'modified': 0,
                'skipped': skipped,
            }
        result = await self.collection.bulk_write(
            operations,
            ordered=False,
        )
        return {
            'processed': len(operations),
            'upserted': result.upserted_count,
            'modified': result.modified_count,
            'skipped': skipped,
        }

    async def list_by_channel_and_date(
        self,
        channel_id: int,
        date_from: datetime,
        date_to: datetime,
    ) -> list[dict[str, Any]]:
        if channel_id is None:
            return []
        filter_doc: dict[str, Any] = {
            'date': {'$gte': date_from, '$lte': date_to},
            '$or': [
                {'peer_id.channel_id': channel_id},
                {'peer_id.chat_id': channel_id},
            ],
        }
        cursor = self.collection.find(filter_doc).sort([('date', 1), ('id', 1)])
        return [doc async for doc in cursor]

    async def list_by_channel_and_ids(
        self,
        channel_id: int,
        message_ids: list[int],
    ) -> list[dict[str, Any]]:
        if channel_id is None:
            return []
        normalized: list[int] = []
        for message_id in message_ids or []:
            if message_id is None:
                continue
            try:
                normalized.append(int(message_id))
            except (TypeError, ValueError):
                continue
        if not normalized:
            return []
        filter_doc: dict[str, Any] = {
            'id': {'$in': normalized},
            '$or': [
                {'peer_id.channel_id': channel_id},
                {'peer_id.chat_id': channel_id},
            ],
        }
        cursor = self.collection.find(filter_doc).sort([('date', 1), ('id', 1)])
        return [doc async for doc in cursor]

    async def list_by_channel_and_ids(
        self,
        channel_id: int,
        message_ids: list[int],
    ) -> list[dict[str, Any]]:
        if channel_id is None or not message_ids:
            return []
        normalized: list[int] = []
        for message_id in message_ids:
            if message_id is None:
                continue
            try:
                normalized.append(int(message_id))
            except (TypeError, ValueError):
                continue
        if not normalized:
            return []
        filter_doc: dict[str, Any] = {
            'id': {'$in': normalized},
            '$or': [
                {'peer_id.channel_id': channel_id},
                {'peer_id.chat_id': channel_id},
            ],
        }
        cursor = self.collection.find(filter_doc).sort([('date', 1), ('id', 1)])
        return [doc async for doc in cursor]

    @staticmethod
    def _normalize_message_ids(message_ids: list[int] | None) -> list[int]:
        if not message_ids:
            return []
        normalized: list[int] = []
        for message_id in message_ids:
            if message_id is None:
                continue
            try:
                normalized.append(int(message_id))
            except (TypeError, ValueError):
                continue
        return normalized

    async def list_by_channel_and_ids(
        self,
        channel_id: int,
        message_ids: list[int],
    ) -> list[dict[str, Any]]:
        if channel_id is None:
            return []
        normalized = self._normalize_message_ids(message_ids)
        if not normalized:
            return []
        filter_doc: dict[str, Any] = {
            'id': {'$in': normalized},
            '$or': [
                {'peer_id.channel_id': channel_id},
                {'peer_id.chat_id': channel_id},
            ],
        }
        cursor = self.collection.find(filter_doc).sort([('date', 1), ('id', 1)])
        return [doc async for doc in cursor]

    @staticmethod
    def _normalize_user_ids(user_ids: list[int] | None) -> list[int]:
        if not user_ids:
            return []
        normalized: list[int] = []
        for user_id in user_ids:
            if user_id is None:
                continue
            try:
                normalized.append(int(user_id))
            except (TypeError, ValueError):
                continue
        return normalized

    def _build_user_message_stats_pipeline(
        self,
        user_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        match_doc: dict[str, Any] = {
            'channel_id': {'$ne': None},
            '$expr': {'$ne': ['$sender_id', '$channel_id']},
        }
        if user_ids:
            match_doc['sender_id'] = {'$in': user_ids}
        else:
            match_doc['sender_id'] = {'$ne': None}
        return [
            {
                '$project': {
                    'sender_id': {
                        '$ifNull': [
                            '$from_id.user_id',
                            '$sender_id',
                        ]
                    },
                    'channel_id': {
                        '$ifNull': [
                            '$peer_id.channel_id',
                            '$peer_id.chat_id',
                        ]
                    },
                }
            },
            {'$match': match_doc},
            {
                '$group': {
                    '_id': {
                        'user_id': '$sender_id',
                        'channel_id': '$channel_id',
                    },
                    'messages_count': {'$sum': 1},
                }
            },
            {
                '$group': {
                    '_id': '$_id.user_id',
                    'total': {'$sum': '$messages_count'},
                    'channels': {
                        '$push': {
                            'channel_id': '$_id.channel_id',
                            'messages_count': '$messages_count',
                        }
                    },
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'user_id': '$_id',
                    'total': 1,
                    'channels': 1,
                }
            },
        ]

    async def aggregate_user_message_stats(self) -> list[dict[str, Any]]:
        pipeline = self._build_user_message_stats_pipeline()
        cursor = await self.collection.aggregate(pipeline, allowDiskUse=True)
        return [doc async for doc in cursor]

    async def aggregate_user_message_stats_for_users(
        self,
        user_ids: list[int],
    ) -> list[dict[str, Any]]:
        normalized = self._normalize_user_ids(user_ids)
        if not normalized:
            return []
        pipeline = self._build_user_message_stats_pipeline(normalized)

        cursor = await self.collection.aggregate(pipeline, allowDiskUse=True)
        return [doc async for doc in cursor]
