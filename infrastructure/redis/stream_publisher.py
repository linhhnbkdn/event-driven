from __future__ import annotations

from redis.asyncio import Redis

from application.interfaces.event_publisher import EventPublisher
from infrastructure.kafka.event_publisher import KafkaEventPublisher
from shared.schemas import ChatCompleted, ChatRequest, ChatResponse

_STREAM_TTL = 300  # 5 minutes — enough for late clients to catch up


class RedisStreamPublisher(EventPublisher):
    def __init__(self, kafka: KafkaEventPublisher, redis: Redis) -> None:
        self._kafka = kafka
        self._redis = redis

    async def publish_request(self, request: ChatRequest) -> None:
        await self._kafka.publish_request(request=request)

    async def publish_response(self, response: ChatResponse) -> None:
        await self._redis.xadd(
            name=f"stream:{response.request_id}",
            fields={
                "delta": response.delta,
                "finish_reason": response.finish_reason or "",
            },
        )

    async def publish_completed(self, completed: ChatCompleted) -> None:
        await self._redis.expire(name=f"stream:{completed.request_id}", time=_STREAM_TTL)
        await self._kafka.publish_completed(completed=completed)

    def flush(self) -> None:
        self._kafka.flush()
