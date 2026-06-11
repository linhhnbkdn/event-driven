from __future__ import annotations
import asyncio
import logging

from confluent_kafka import Consumer, KafkaError, Producer
from redis.asyncio import Redis

from application.use_cases.process_chat_request import ProcessChatRequestUseCase
from consumer.handler import ChatRequestHandler
from infrastructure.kafka.event_publisher import KafkaEventPublisher
from infrastructure.llm.factory import create_llm_strategy
from infrastructure.redis.conversation_cache import RedisConversationCache
from infrastructure.redis.stream_publisher import RedisStreamPublisher
from shared.settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def run() -> None:
    redis = Redis.from_url(settings.redis_url)
    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "linger.ms": 0,
        "batch.size": 4096,
        "acks": "1",
    })

    use_case = ProcessChatRequestUseCase(
        generator=create_llm_strategy(),
        publisher=RedisStreamPublisher(
            kafka=KafkaEventPublisher(producer=producer),
            redis=redis,
        ),
        cache=RedisConversationCache(redis=redis),
    )
    handler = ChatRequestHandler(use_case=use_case)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "llm-worker",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["chat.requests"])
    logger.info("Consumer started — listening on chat.requests")
    loop = asyncio.get_running_loop()

    try:
        while True:
            msgs = await loop.run_in_executor(None, consumer.consume, 10, 0.1)
            valid = [m for m in msgs if not m.error()]
            if valid:
                await asyncio.gather(
                    *[handler.handle(raw_value=m.value()) for m in valid],
                )
    finally:
        consumer.close()
        await redis.aclose()
