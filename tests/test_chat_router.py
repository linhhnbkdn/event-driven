import asyncio
import json
import pytest
from unittest.mock import MagicMock
from httpx import ASGITransport, AsyncClient

import fakeredis
from app.main import app
from app import dependencies, state


@pytest.fixture
def mock_producer():
    p = MagicMock()
    p.produce = MagicMock()
    p.flush = MagicMock()
    return p


@pytest.fixture
def fake_redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
async def client(mock_producer, fake_redis):
    app.dependency_overrides[dependencies.get_producer] = lambda: mock_producer
    app.dependency_overrides[dependencies.get_redis] = lambda: fake_redis
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c
    app.dependency_overrides.clear()


async def test_post_chat_returns_request_id(client, mock_producer):
    resp = await client.post("/chat", json={"session_id": "sess1", "content": "hello"})
    assert resp.status_code == 200
    data = resp.json()
    assert "request_id" in data
    assert len(data["request_id"]) == 36


async def test_post_chat_calls_kafka_producer(client, mock_producer):
    await client.post("/chat", json={"session_id": "sess1", "content": "hello"})
    assert mock_producer.produce.called
    call_kwargs = mock_producer.produce.call_args
    assert call_kwargs.kwargs["topic"] == "chat.requests"


async def test_history_empty_for_new_session(client):
    resp = await client.get("/history/nonexistent-session")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_stream_delivers_tokens_and_done(client):
    request_id = "stream-test-001"

    async def feed():
        await asyncio.sleep(0.05)
        q = state.response_queues.get(request_id)
        if q:
            await q.put({"request_id": request_id, "session_id": "s", "delta": "Hello", "finish_reason": None})
            await q.put({"request_id": request_id, "session_id": "s", "delta": " world", "finish_reason": None})
            await q.put({"request_id": request_id, "session_id": "s", "delta": "", "finish_reason": "stop"})

    asyncio.create_task(feed())

    lines = []
    async with client.stream("GET", f"/chat/stream/{request_id}") as resp:
        async for line in resp.aiter_lines():
            if line:
                lines.append(line)

    data_lines = [l for l in lines if l.startswith("data:")]
    assert any("[DONE]" in l for l in data_lines)
    token_lines = [l for l in data_lines if "[DONE]" not in l]
    tokens = [json.loads(l[len("data:"):].strip())["choices"][0]["delta"]["content"] for l in token_lines]
    assert tokens == ["Hello", " world"]
