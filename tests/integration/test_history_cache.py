from __future__ import annotations
import pytest
import fakeredis

from domain.entities import Message
from domain.value_objects import MessageRole
from infrastructure.redis.conversation_cache import RedisConversationCache


@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def cache(redis):
    return RedisConversationCache(redis=redis)


async def test_empty_history_returns_empty_list(cache):
    result = await cache.get_history(session_id="nonexistent")
    assert result == []


async def test_save_and_retrieve_message(cache):
    msg = Message(
        session_id="s1",
        request_id="r1",
        role=MessageRole.USER,
        content="hello",
    )
    await cache.save_message(message=msg)
    history = await cache.get_history(session_id="s1")
    assert len(history) == 1
    assert history[0] == msg


async def test_messages_ordered_by_save_time(cache):
    m1 = Message(session_id="s", request_id="r", role=MessageRole.USER, content="first")
    m2 = Message(session_id="s", request_id="r", role=MessageRole.ASSISTANT, content="second")
    await cache.save_message(message=m1)
    await cache.save_message(message=m2)
    history = await cache.get_history(session_id="s")
    assert [m.content for m in history] == ["first", "second"]


async def test_different_sessions_isolated(cache):
    await cache.save_message(
        message=Message(session_id="a", request_id="r1", role=MessageRole.USER, content="a"),
    )
    await cache.save_message(
        message=Message(session_id="b", request_id="r2", role=MessageRole.USER, content="b"),
    )
    assert len(await cache.get_history(session_id="a")) == 1
    assert len(await cache.get_history(session_id="b")) == 1


async def test_role_preserved_as_enum(cache):
    msg = Message(session_id="s", request_id="r", role=MessageRole.ASSISTANT, content="hi")
    await cache.save_message(message=msg)
    history = await cache.get_history(session_id="s")
    assert history[0].role == MessageRole.ASSISTANT
    assert isinstance(history[0].role, MessageRole)
