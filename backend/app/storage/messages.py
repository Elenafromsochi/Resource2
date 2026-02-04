from .base import BaseMongoRepository


class MessagesRepository(BaseMongoRepository):
    collection_name = 'messages'

    @property
    def collection(self):
        return self.db[self.collection_name]

    async def insert_many(self, messages: list[dict]) -> int:
        if not messages:
            return 0
        result = await self.collection.insert_many(messages)
        return len(result.inserted_ids)
