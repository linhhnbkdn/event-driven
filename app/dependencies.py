from __future__ import annotations
from typing import AsyncGenerator

from confluent_kafka import Producer
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from shared.settings import settings

engine = create_async_engine(settings.database_url)
_AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

_redis: Redis | None = None
_producer: Producer | None = None


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with _AsyncSessionLocal() as session:
        yield session


async def get_redis() -> Redis:
    return _redis


def get_producer() -> Producer:
    return _producer
