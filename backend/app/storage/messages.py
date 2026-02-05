import asyncio
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
        result = await asyncio.to_thread(
            self.collection.bulk_write,
            operations,
            ordered=False,
        )
        return {
            'processed': len(operations),
            'upserted': result.upserted_count,
            'modified': result.modified_count,
            'skipped': skipped,
        }

    async def aggregate_user_message_stats(self) -> list[dict[str, Any]]:
        pipeline = [
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
            {
                '$match': {
                    'sender_id': {'$ne': None},
                    'channel_id': {'$ne': None},
                    '$expr': {'$ne': ['$sender_id', '$channel_id']},
                }
            },
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

        def run():
            return list(self.collection.aggregate(pipeline, allowDiskUse=True))

        return await asyncio.to_thread(run)
