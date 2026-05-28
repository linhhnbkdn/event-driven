from __future__ import annotations
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from httpx import ASGITransport, AsyncClient

from api.app import app
from api import state
from api.dependencies import get_history_use_case, get_send_message_use_case
from application.use_cases.get_history import GetHistoryUseCase
from application.use_cases.send_message import SendMessageUseCase
from domain.entities import Message
from domain.value_objects import MessageRole


@pytest.fixture
def mock_send_use_case():
    uc = AsyncMock(spec=SendMessageUseCase)
    uc.execute = AsyncMock(return_value="fixed-request-id-0001")
    return uc


@pytest.fixture
def mock_history_use_case():
    uc = AsyncMock(spec=GetHistoryUseCase)
    uc.execute = AsyncMock(return_value=[])
    return uc


@pytest.fixture
async def client(mock_send_use_case, mock_history_use_case):
    app.dependency_overrides[get_send_message_use_case] = lambda: mock_send_use_case
    app.dependency_overrides[get_history_use_case] = lambda: mock_history_use_case

    fake_redis = AsyncMock()
    fake_redis.aclose = AsyncMock()
    fake_producer = MagicMock()

    with (
        patch("api.app._ensure_topics"),
        patch("api.app._consume_responses", new_callable=lambda: lambda: AsyncMock(return_value=None)),
        patch("api.app.Redis") as mock_redis_cls,
        patch("api.app.Producer", return_value=fake_producer),
    ):
        mock_redis_cls.from_url.return_value = fake_redis
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c

    app.dependency_overrides.clear()


async def test_post_chat_returns_request_id(client, mock_send_use_case):
    resp = await client.post("/chat", json={"session_id": "s1", "content": "hello"})
    assert resp.status_code == 200
    assert resp.json()["request_id"] == "fixed-request-id-0001"
    mock_send_use_case.execute.assert_awaited_once_with(
        session_id="s1",
        content="hello",
    )


async def test_history_returns_empty_for_unknown_session(client):
    resp = await client.get("/history/unknown")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_history_serializes_messages(client, mock_history_use_case):
    mock_history_use_case.execute = AsyncMock(return_value=[
        Message(session_id="s1", request_id="r1", role=MessageRole.USER, content="hi"),
        Message(session_id="s1", request_id="r1", role=MessageRole.ASSISTANT, content="hello"),
    ])
    resp = await client.get("/history/s1")
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["role"] == "user"
    assert data[1]["role"] == "assistant"


async def test_stream_delivers_tokens_and_done(client):
    request_id = "stream-integ-001"

    async def feed():
        await asyncio.sleep(0.05)
        q = state.response_queues.get(request_id)
        if q:
            await q.put({"request_id": request_id, "session_id": "s", "delta": "Hi", "finish_reason": None})
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
    tokens = [json.loads(l[5:].strip())["choices"][0]["delta"]["content"] for l in token_lines]
    assert tokens == ["Hi"]
