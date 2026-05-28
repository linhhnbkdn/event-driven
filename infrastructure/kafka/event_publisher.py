from __future__ import annotations
import asyncio

from confluent_kafka import Producer

from application.interfaces.event_publisher import EventPublisher
from shared.schemas import ChatRequest, ChatResponse


class KafkaEventPublisher(EventPublisher):
    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    async def publish_request(self, request: ChatRequest) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: (
                self._producer.produce(
                    topic="chat.requests",
                    value=request.model_dump_json(),
                ),
                self._producer.flush(),
            ),
        )

    async def publish_response(self, response: ChatResponse) -> None:
        self._producer.produce(
            topic="chat.responses",
            value=response.model_dump_json(),
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()
