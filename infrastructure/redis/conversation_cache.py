from __future__ import annotations
import json
import time

from redis.asyncio import Redis

from application.interfaces.conversation_cache import ConversationCache
from domain.entities import Message
from domain.value_objects import MessageRole
from shared.settings import settings


class RedisConversationCache(ConversationCache):
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def save_message(self, message: Message) -> None:
        key = f"conversation:{message.session_id}"
        member = json.dumps({
            "role": message.role.value,
            "content": message.content,
            "request_id": message.request_id,
        })
        await self._redis.zadd(name=key, mapping={member: time.time()})
        await self._redis.expire(name=key, time=settings.redis_ttl)

    async def get_history(self, session_id: str) -> list[Message]:
        key = f"conversation:{session_id}"
        raw = await self._redis.zrange(name=key, start=0, end=-1)
        return [
            Message(
                session_id=session_id,
                request_id=item["request_id"],
                role=MessageRole(item["role"]),
                content=item["content"],
            )
            for item in (json.loads(r) for r in raw)
        ]
