from __future__ import annotations
import asyncio
import logging

from confluent_kafka import Consumer, KafkaError
from redis.asyncio import ConnectionPool, Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from application.use_cases.persist_session import PersistSessionUseCase
from consumer.persistence_handler import PersistenceHandler
from infrastructure.postgres.message_store import PostgresMessageStore
from infrastructure.redis.conversation_cache import RedisConversationCache
from shared.settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def run() -> None:
    pool = ConnectionPool.from_url(settings.redis_url, max_connections=40)
    redis = Redis(connection_pool=pool)
    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    use_case = PersistSessionUseCase(
        cache=RedisConversationCache(redis=redis),
        store=PostgresMessageStore(session_factory=session_factory),
    )
    handler = PersistenceHandler(use_case=use_case)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "persistence-worker",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["chat.completed"])
    logger.info("Persistence consumer started — listening on chat.completed")
    loop = asyncio.get_running_loop()
    # backpressure: cap concurrent Postgres writes to avoid connection pool exhaustion
    sem = asyncio.Semaphore(30)

    async def process(raw_value: bytes) -> None:
        async with sem:
            await handler.handle(raw_value=raw_value)

    try:
        while True:
            msgs = await loop.run_in_executor(None, consumer.consume, 50, 0.1)
            for m in msgs:
                if not m.error():
                    asyncio.create_task(process(raw_value=m.value()))
    finally:
        consumer.close()
        await redis.aclose()
        await pool.aclose()
        await engine.dispose()
