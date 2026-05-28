from __future__ import annotations
import pytest
from unittest.mock import AsyncMock, MagicMock

from application.use_cases.send_message import SendMessageUseCase
from application.use_cases.get_history import GetHistoryUseCase
from application.use_cases.persist_session import PersistSessionUseCase
from application.use_cases.process_chat_request import ProcessChatRequestUseCase
from domain.entities import Message
from domain.value_objects import MessageRole
from shared.schemas import ChatCompleted, ChatRequest


async def test_send_message_returns_uuid_request_id():
    publisher = AsyncMock()
    publisher.publish_request = AsyncMock()
    use_case = SendMessageUseCase(publisher=publisher)
    request_id = await use_case.execute(session_id="s1", content="hello")
    assert len(request_id) == 36
    assert request_id.count("-") == 4
    publisher.publish_request.assert_awaited_once()


async def test_send_message_publishes_correct_session():
    publisher = AsyncMock()
    publisher.publish_request = AsyncMock()
    use_case = SendMessageUseCase(publisher=publisher)
    await use_case.execute(session_id="my-session", content="hi")
    call_arg = publisher.publish_request.call_args.kwargs["request"]
    assert call_arg.session_id == "my-session"
    assert call_arg.content == "hi"


async def test_get_history_delegates_to_cache():
    cache = AsyncMock()
    expected = [
        Message(session_id="s", request_id="r", role=MessageRole.USER, content="hi"),
    ]
    cache.get_history = AsyncMock(return_value=expected)
    use_case = GetHistoryUseCase(cache=cache)
    result = await use_case.execute(session_id="s")
    assert result == expected
    cache.get_history.assert_awaited_once_with(session_id="s")


def _make_publisher():
    publisher = AsyncMock()
    publisher.publish_response = AsyncMock()
    publisher.publish_completed = AsyncMock()
    publisher.flush = MagicMock()
    return publisher


async def test_process_request_publishes_token_per_word():
    async def fake_generate(content: str):
        yield "Hello"
        yield " world"

    generator = MagicMock()
    generator.generate = fake_generate
    publisher = _make_publisher()
    cache = AsyncMock()
    cache.save_message = AsyncMock()

    use_case = ProcessChatRequestUseCase(
        generator=generator,
        publisher=publisher,
        cache=cache,
    )
    request = ChatRequest(session_id="s1", content="test", request_id="req1")
    await use_case.execute(request=request)

    # 2 tokens + 1 done signal = 3 publish_response calls
    assert publisher.publish_response.await_count == 3
    done_call = publisher.publish_response.call_args_list[-1].kwargs["response"]
    assert done_call.finish_reason == "stop"
    publisher.flush.assert_called_once()


async def test_process_request_caches_user_and_assistant():
    async def fake_generate(content: str):
        yield "Hi"

    generator = MagicMock()
    generator.generate = fake_generate
    publisher = _make_publisher()
    cache = AsyncMock()
    cache.save_message = AsyncMock()

    use_case = ProcessChatRequestUseCase(
        generator=generator,
        publisher=publisher,
        cache=cache,
    )
    request = ChatRequest(session_id="s1", content="hello", request_id="req1")
    await use_case.execute(request=request)

    assert cache.save_message.await_count == 2
    saved_roles = [
        call.kwargs["message"].role
        for call in cache.save_message.call_args_list
    ]
    assert saved_roles == [MessageRole.USER, MessageRole.ASSISTANT]


async def test_process_request_publishes_completed_after_cache():
    async def fake_generate(content: str):
        yield "Hi"

    generator = MagicMock()
    generator.generate = fake_generate
    publisher = _make_publisher()
    cache = AsyncMock()
    cache.save_message = AsyncMock()

    use_case = ProcessChatRequestUseCase(
        generator=generator,
        publisher=publisher,
        cache=cache,
    )
    request = ChatRequest(session_id="s1", content="hello", request_id="req1")
    await use_case.execute(request=request)

    publisher.publish_completed.assert_awaited_once()
    call_arg = publisher.publish_completed.call_args.kwargs["completed"]
    assert call_arg.session_id == "s1"
    assert call_arg.request_id == "req1"


async def test_persist_session_filters_by_request_id_and_writes_store():
    msg_this = Message(session_id="s", request_id="req1", role=MessageRole.USER, content="hi")
    msg_old = Message(session_id="s", request_id="req0", role=MessageRole.USER, content="old")
    cache = AsyncMock()
    cache.get_history = AsyncMock(return_value=[msg_old, msg_this])
    store = AsyncMock()
    store.save_message = AsyncMock()

    use_case = PersistSessionUseCase(cache=cache, store=store)
    await use_case.execute(completed=ChatCompleted(session_id="s", request_id="req1"))

    # only msg_this matches request_id="req1"
    store.save_message.assert_awaited_once_with(message=msg_this)
