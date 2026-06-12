from __future__ import annotations
import logging
from contextlib import asynccontextmanager

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import FastAPI
from redis.asyncio import ConnectionPool, Redis

from api.routers import chat
from shared.settings import settings

logger = logging.getLogger(__name__)


def _ensure_topics() -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    topics = ["chat.requests", "chat.completed"]
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = ConnectionPool.from_url(settings.redis_url, max_connections=200)
    app.state.redis = Redis(connection_pool=pool)
    app.state.producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "linger.ms": 5,
        "batch.size": 16384,
        "acks": "1",
    })
    _ensure_topics()
    yield
    await app.state.redis.aclose()
    await pool.aclose()


app = FastAPI(title="Event-Driven Streaming", lifespan=lifespan)
app.include_router(chat.router)
