from __future__ import annotations

from confluent_kafka import Producer
from fastapi import Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from application.interfaces.conversation_cache import ConversationCache
from application.interfaces.event_publisher import EventPublisher
from application.use_cases.get_history import GetHistoryUseCase
from application.use_cases.send_message import SendMessageUseCase
from infrastructure.kafka.event_publisher import KafkaEventPublisher
from infrastructure.postgres.message_store import PostgresMessageStore
from infrastructure.redis.conversation_cache import RedisConversationCache
from shared.settings import settings

_engine = create_async_engine(settings.database_url)
_session_factory = async_sessionmaker(_engine, expire_on_commit=False)


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_producer(request: Request) -> Producer:
    return request.app.state.producer


def get_conversation_cache(request: Request) -> ConversationCache:
    return RedisConversationCache(redis=get_redis(request=request))


def get_event_publisher(request: Request) -> EventPublisher:
    return KafkaEventPublisher(producer=get_producer(request=request))


def get_send_message_use_case(request: Request) -> SendMessageUseCase:
    return SendMessageUseCase(publisher=get_event_publisher(request=request))


def get_history_use_case(request: Request) -> GetHistoryUseCase:
    return GetHistoryUseCase(cache=get_conversation_cache(request=request))
