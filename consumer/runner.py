from __future__ import annotations
import asyncio
import logging

from confluent_kafka import Consumer, KafkaError, Producer
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from application.use_cases.process_chat_request import ProcessChatRequestUseCase
from consumer.handler import ChatRequestHandler
from infrastructure.kafka.event_publisher import KafkaEventPublisher
from infrastructure.llm.factory import create_llm_strategy
from infrastructure.postgres.message_store import PostgresMessageStore
from infrastructure.redis.conversation_cache import RedisConversationCache
from shared.settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def run() -> None:
    redis = Redis.from_url(settings.redis_url)
    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

    use_case = ProcessChatRequestUseCase(
        generator=create_llm_strategy(),
        publisher=KafkaEventPublisher(producer=producer),
        cache=RedisConversationCache(redis=redis),
        store=PostgresMessageStore(session_factory=session_factory),
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
        await engine.dispose()
