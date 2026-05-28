import pytest
import fakeredis
from app.services.history import get_history, save_message


@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


async def test_get_history_returns_empty_for_unknown_session(redis):
    result = await get_history(redis=redis, session_id="unknown")
    assert result == []


async def test_save_and_retrieve_single_message(redis):
    await save_message(
        redis=redis,
        session_id="sess1",
        role="user",
        content="hello",
        request_id="req1",
    )
    history = await get_history(redis=redis, session_id="sess1")
    assert len(history) == 1
    assert history[0]["role"] == "user"
    assert history[0]["content"] == "hello"
    assert history[0]["request_id"] == "req1"


async def test_messages_ordered_by_timestamp(redis):
    await save_message(redis=redis, session_id="s", role="user", content="first", request_id="r1")
    await save_message(redis=redis, session_id="s", role="assistant", content="second", request_id="r1")
    await save_message(redis=redis, session_id="s", role="user", content="third", request_id="r2")

    history = await get_history(redis=redis, session_id="s")
    assert [m["content"] for m in history] == ["first", "second", "third"]


async def test_different_sessions_are_isolated(redis):
    await save_message(redis=redis, session_id="a", role="user", content="from a", request_id="r1")
    await save_message(redis=redis, session_id="b", role="user", content="from b", request_id="r2")

    assert len(await get_history(redis=redis, session_id="a")) == 1
    assert len(await get_history(redis=redis, session_id="b")) == 1
