from __future__ import annotations
import asyncio
import json
import logging
from contextlib import asynccontextmanager

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import FastAPI
from redis.asyncio import Redis

from api import state
from api.routers import chat
from shared.settings import settings

logger = logging.getLogger(__name__)


def _ensure_topics() -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    topics = ["chat.requests", "chat.responses"]
    existing = admin.list_topics(timeout=10).topics
    to_create = [
        NewTopic(t, num_partitions=1, replication_factor=1,)
        for t in topics
        if t not in existing
    ]
    if to_create:
        futures = admin.create_topics(to_create)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Created topic: {topic}")
            except Exception as e:
                logger.warning(f"Topic {topic} may already exist: {e}")


async def _consume_responses() -> None:
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "fastapi-sse",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe(["chat.responses"])
    loop = asyncio.get_running_loop()
    try:
        while True:
            msgs = await loop.run_in_executor(None, consumer.consume, 100, 0.05)
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                data = json.loads(msg.value())
                request_id = data.get("request_id")
                if request_id and request_id in state.response_queues:
                    await state.response_queues[request_id].put(data)
    finally:
        consumer.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = Redis.from_url(settings.redis_url)
    app.state.producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    _ensure_topics()
    task = asyncio.create_task(_consume_responses())
    yield
    task.cancel()
    await app.state.redis.aclose()


app = FastAPI(title="Event-Driven Streaming", lifespan=lifespan)
app.include_router(chat.router)
