# Clean Architecture Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the existing flat `app/`+`worker/` layout into Clean Architecture layers: `domain` → `application` → `infrastructure/api/consumer`, fixing all clean code violations and adding Strategy Pattern for LLM providers.

**Architecture:** Innermost `domain/` has zero external imports (frozen dataclasses + MessageRole enum). `application/` defines interfaces as ABCs and use cases that depend only on those ABCs. `infrastructure/`, `api/`, and `consumer/` are outer adapters — only they import from libraries like Kafka, Redis, SQLAlchemy, FastAPI. Dependency rule enforced strictly.

**Tech Stack:** Python 3.13, uv, FastAPI, confluent-kafka, redis[asyncio], SQLAlchemy async, Alembic, pydantic-settings, httpx, pytest + fakeredis

---

## Migration Strategy

Build all new layers **while keeping old `app/` and `worker/` intact**. Migrate tests last. Delete old code only after all new tests pass. This ensures the repo is never broken mid-migration.

## File Map

### New files to create
```
domain/__init__.py
domain/entities.py
domain/value_objects.py

application/__init__.py
application/interfaces/__init__.py
application/interfaces/conversation_cache.py
application/interfaces/event_publisher.py
application/interfaces/message_store.py
application/interfaces/token_generator.py
application/use_cases/__init__.py
application/use_cases/send_message.py
application/use_cases/get_history.py
application/use_cases/process_chat_request.py

infrastructure/__init__.py
infrastructure/kafka/__init__.py
infrastructure/kafka/event_publisher.py
infrastructure/llm/__init__.py
infrastructure/llm/mock_strategy.py
infrastructure/llm/openai_strategy.py
infrastructure/llm/factory.py
infrastructure/postgres/__init__.py
infrastructure/postgres/models.py          ← moved from app/models.py
infrastructure/postgres/message_store.py
infrastructure/redis/__init__.py
infrastructure/redis/conversation_cache.py

api/__init__.py
api/app.py
api/dependencies.py
api/state.py
api/routers/__init__.py
api/routers/chat.py

consumer/__init__.py
consumer/handler.py
consumer/runner.py

tests/unit/__init__.py
tests/unit/test_domain.py
tests/unit/test_use_cases.py
tests/integration/__init__.py
tests/integration/test_chat_router.py
tests/integration/test_history_cache.py

run_api.py
run_worker.py
```

### Files to modify
```
shared/settings.py          ← add llm_provider, openai_api_key
alembic/env.py              ← update import to infrastructure.postgres.models
pyproject.toml              ← update packages list
Makefile                    ← update dev/worker targets
.env.example + .env         ← add LLM_PROVIDER=mock
```

### Files to delete (Task 15)
```
app/                        ← entire folder
worker/                     ← entire folder
tests/test_chat_router.py
tests/test_history.py
tests/test_mock_llm.py
tests/test_schemas.py
```

---

## Task 1: Update shared/settings.py

**Files:**
- Modify: `shared/settings.py`
- Modify: `.env.example`, `.env`

- [ ] **Step 1: Update shared/settings.py**

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379"
    database_url: str = "postgresql+asyncpg://app:app@localhost:5432/chatdb"
    redis_ttl: int = 86400
    llm_provider: str = "mock"
    openai_api_key: str = ""


settings = Settings()
```

- [ ] **Step 2: Add LLM_PROVIDER to .env.example and .env**

Append to both files:
```
LLM_PROVIDER=mock
OPENAI_API_KEY=
```

- [ ] **Step 3: Verify**

```bash
uv run python -c "from shared.settings import settings; print(settings.llm_provider)"
```
Expected: `mock`

- [ ] **Step 4: Commit**

```bash
git add shared/settings.py .env.example
git commit -m "feat: add llm_provider and openai_api_key to settings"
```

---

## Task 2: Domain Layer

**Files:**
- Create: `domain/__init__.py`
- Create: `domain/value_objects.py`
- Create: `domain/entities.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/unit/test_domain.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/__init__.py` — empty file.

`tests/unit/test_domain.py`:
```python
from __future__ import annotations
import pytest
from dataclasses import FrozenInstanceError

from domain.entities import Message, Session
from domain.value_objects import MessageRole


def test_message_role_user_is_string_user():
    assert MessageRole.USER == "user"


def test_message_role_assistant_is_string_assistant():
    assert MessageRole.ASSISTANT == "assistant"


def test_message_is_frozen():
    msg = Message(
        session_id="s",
        request_id="r",
        role=MessageRole.USER,
        content="hi",
    )
    with pytest.raises(FrozenInstanceError):
        object.__setattr__(msg, "content", "changed")


def test_message_equality():
    m1 = Message(session_id="s", request_id="r", role=MessageRole.USER, content="hi")
    m2 = Message(session_id="s", request_id="r", role=MessageRole.USER, content="hi")
    assert m1 == m2


def test_session_equality():
    assert Session(session_id="a") == Session(session_id="a")


def test_message_role_from_string():
    assert MessageRole("user") == MessageRole.USER
    assert MessageRole("assistant") == MessageRole.ASSISTANT
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/unit/test_domain.py -v
```
Expected: `ModuleNotFoundError: No module named 'domain'`

- [ ] **Step 3: Create domain/__init__.py (empty)**

- [ ] **Step 4: Create domain/value_objects.py**

```python
from __future__ import annotations
from enum import Enum


class MessageRole(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"
```

- [ ] **Step 5: Create domain/entities.py**

```python
from __future__ import annotations
from dataclasses import dataclass

from domain.value_objects import MessageRole


@dataclass(frozen=True)
class Message:
    session_id: str
    request_id: str
    role: MessageRole
    content: str


@dataclass(frozen=True)
class Session:
    session_id: str
```

- [ ] **Step 6: Run tests — must show 6 passed**

```bash
uv run pytest tests/unit/test_domain.py -v
```

- [ ] **Step 7: Commit**

```bash
git add domain/ tests/unit/
git commit -m "feat: domain layer — Message, Session, MessageRole"
```

---

## Task 3: Application Interfaces

**Files:**
- Create: `application/__init__.py`
- Create: `application/interfaces/__init__.py`
- Create: `application/interfaces/conversation_cache.py`
- Create: `application/interfaces/event_publisher.py`
- Create: `application/interfaces/message_store.py`
- Create: `application/interfaces/token_generator.py`

No tests needed — ABCs have no logic to test.

- [ ] **Step 1: Create application/__init__.py (empty)**

- [ ] **Step 2: Create application/interfaces/__init__.py (empty)**

- [ ] **Step 3: Create application/interfaces/conversation_cache.py**

```python
from __future__ import annotations
from abc import ABC, abstractmethod

from domain.entities import Message


class ConversationCache(ABC):
    @abstractmethod
    async def save_message(self, message: Message) -> None: ...

    @abstractmethod
    async def get_history(self, session_id: str) -> list[Message]: ...
```

- [ ] **Step 4: Create application/interfaces/event_publisher.py**

```python
from __future__ import annotations
from abc import ABC, abstractmethod

from shared.schemas import ChatRequest, ChatResponse


class EventPublisher(ABC):
    @abstractmethod
    async def publish_request(self, request: ChatRequest) -> None: ...

    @abstractmethod
    async def publish_response(self, response: ChatResponse) -> None: ...

    @abstractmethod
    def flush(self) -> None: ...
```

- [ ] **Step 5: Create application/interfaces/message_store.py**

```python
from __future__ import annotations
from abc import ABC, abstractmethod

from domain.entities import Message


class MessageStore(ABC):
    @abstractmethod
    async def save_message(self, message: Message) -> None: ...
```

- [ ] **Step 6: Create application/interfaces/token_generator.py**

```python
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import AsyncGenerator


class TokenGenerator(ABC):
    @abstractmethod
    async def generate(self, content: str) -> AsyncGenerator[str, None]: ...
```

- [ ] **Step 7: Verify all interfaces import cleanly**

```bash
uv run python -c "
from application.interfaces.conversation_cache import ConversationCache
from application.interfaces.event_publisher import EventPublisher
from application.interfaces.message_store import MessageStore
from application.interfaces.token_generator import TokenGenerator
print('ok')
"
```
Expected: `ok`

- [ ] **Step 8: Commit**

```bash
git add application/
git commit -m "feat: application interfaces — ConversationCache, EventPublisher, MessageStore, TokenGenerator"
```

---

## Task 4: Application Use Cases + Unit Tests

**Files:**
- Create: `application/use_cases/__init__.py`
- Create: `application/use_cases/send_message.py`
- Create: `application/use_cases/get_history.py`
- Create: `application/use_cases/process_chat_request.py`
- Create: `tests/unit/test_use_cases.py`

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_use_cases.py`:
```python
from __future__ import annotations
import pytest
from unittest.mock import AsyncMock, MagicMock

from application.use_cases.send_message import SendMessageUseCase
from application.use_cases.get_history import GetHistoryUseCase
from application.use_cases.process_chat_request import ProcessChatRequestUseCase
from domain.entities import Message
from domain.value_objects import MessageRole
from shared.schemas import ChatRequest


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


async def test_process_request_publishes_token_per_word():
    async def fake_generate(content: str):
        yield "Hello"
        yield " world"

    generator = MagicMock()
    generator.generate = fake_generate
    publisher = AsyncMock()
    publisher.publish_response = AsyncMock()
    publisher.flush = MagicMock()
    cache = AsyncMock()
    cache.save_message = AsyncMock()
    store = AsyncMock()
    store.save_message = AsyncMock()

    use_case = ProcessChatRequestUseCase(
        generator=generator,
        publisher=publisher,
        cache=cache,
        store=store,
    )
    request = ChatRequest(session_id="s1", content="test", request_id="req1")
    await use_case.execute(request=request)

    # 2 tokens + 1 done signal = 3 publish_response calls
    assert publisher.publish_response.await_count == 3
    # done signal has finish_reason="stop"
    done_call = publisher.publish_response.call_args_list[-1].kwargs["response"]
    assert done_call.finish_reason == "stop"
    publisher.flush.assert_called_once()


async def test_process_request_dual_writes_user_and_assistant():
    async def fake_generate(content: str):
        yield "Hi"

    generator = MagicMock()
    generator.generate = fake_generate
    publisher = AsyncMock()
    publisher.publish_response = AsyncMock()
    publisher.flush = MagicMock()
    cache = AsyncMock()
    cache.save_message = AsyncMock()
    store = AsyncMock()
    store.save_message = AsyncMock()

    use_case = ProcessChatRequestUseCase(
        generator=generator,
        publisher=publisher,
        cache=cache,
        store=store,
    )
    request = ChatRequest(session_id="s1", content="hello", request_id="req1")
    await use_case.execute(request=request)

    # 2 saves each: user + assistant
    assert cache.save_message.await_count == 2
    assert store.save_message.await_count == 2

    saved_roles = [
        call.kwargs["message"].role
        for call in cache.save_message.call_args_list
    ]
    assert saved_roles == [MessageRole.USER, MessageRole.ASSISTANT]
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/unit/test_use_cases.py -v
```
Expected: `ImportError` — use cases don't exist yet.

- [ ] **Step 3: Create application/use_cases/__init__.py (empty)**

- [ ] **Step 4: Create application/use_cases/send_message.py**

```python
from __future__ import annotations

from application.interfaces.event_publisher import EventPublisher
from shared.schemas import ChatRequest


class SendMessageUseCase:
    def __init__(self, publisher: EventPublisher) -> None:
        self._publisher = publisher

    async def execute(self, session_id: str, content: str) -> str:
        request = ChatRequest(session_id=session_id, content=content)
        await self._publisher.publish_request(request=request)
        return request.request_id
```

- [ ] **Step 5: Create application/use_cases/get_history.py**

```python
from __future__ import annotations

from application.interfaces.conversation_cache import ConversationCache
from domain.entities import Message


class GetHistoryUseCase:
    def __init__(self, cache: ConversationCache) -> None:
        self._cache = cache

    async def execute(self, session_id: str) -> list[Message]:
        return await self._cache.get_history(session_id=session_id)
```

- [ ] **Step 6: Create application/use_cases/process_chat_request.py**

```python
from __future__ import annotations

from application.interfaces.conversation_cache import ConversationCache
from application.interfaces.event_publisher import EventPublisher
from application.interfaces.message_store import MessageStore
from application.interfaces.token_generator import TokenGenerator
from domain.entities import Message
from domain.value_objects import MessageRole
from shared.schemas import ChatRequest, ChatResponse


class ProcessChatRequestUseCase:
    def __init__(
        self,
        generator: TokenGenerator,
        publisher: EventPublisher,
        cache: ConversationCache,
        store: MessageStore,
    ) -> None:
        self._generator = generator
        self._publisher = publisher
        self._cache = cache
        self._store = store

    async def execute(self, request: ChatRequest) -> None:
        full_response = await self._stream_tokens(request=request)
        await self._publisher.flush()
        await self._persist(request=request, full_response=full_response)

    async def _stream_tokens(self, request: ChatRequest) -> str:
        full_response = ""
        async for token in self._generator.generate(content=request.content):
            full_response += token
            await self._publisher.publish_response(
                response=ChatResponse(
                    request_id=request.request_id,
                    session_id=request.session_id,
                    delta=token,
                    finish_reason=None,
                ),
            )
        await self._publisher.publish_response(
            response=ChatResponse(
                request_id=request.request_id,
                session_id=request.session_id,
                delta="",
                finish_reason="stop",
            ),
        )
        return full_response

    async def _persist(self, request: ChatRequest, full_response: str) -> None:
        user_msg = Message(
            session_id=request.session_id,
            request_id=request.request_id,
            role=MessageRole.USER,
            content=request.content,
        )
        assistant_msg = Message(
            session_id=request.session_id,
            request_id=request.request_id,
            role=MessageRole.ASSISTANT,
            content=full_response,
        )
        await self._cache.save_message(message=user_msg)
        await self._cache.save_message(message=assistant_msg)
        await self._store.save_message(message=user_msg)
        await self._store.save_message(message=assistant_msg)
```

- [ ] **Step 7: Run tests — must show 5 passed**

```bash
uv run pytest tests/unit/test_use_cases.py -v
```

- [ ] **Step 8: Commit**

```bash
git add application/use_cases/ tests/unit/test_use_cases.py
git commit -m "feat: application use cases — SendMessage, GetHistory, ProcessChatRequest"
```

---

## Task 5: Infrastructure — Redis

**Files:**
- Create: `infrastructure/__init__.py`
- Create: `infrastructure/redis/__init__.py`
- Create: `infrastructure/redis/conversation_cache.py`
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/test_history_cache.py`

- [ ] **Step 1: Write the failing test**

`tests/integration/__init__.py` — empty file.

`tests/integration/test_history_cache.py`:
```python
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
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/integration/test_history_cache.py -v
```
Expected: `ImportError`.

- [ ] **Step 3: Create infrastructure/__init__.py and infrastructure/redis/__init__.py (both empty)**

- [ ] **Step 4: Create infrastructure/redis/conversation_cache.py**

```python
from __future__ import annotations
import json
import time

from redis.asyncio import Redis

from application.interfaces.conversation_cache import ConversationCache
from domain.entities import Message
from domain.value_objects import MessageRole
from shared.settings import settings


class RedisConversationCache(ConversationCache):
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def save_message(self, message: Message) -> None:
        key = f"conversation:{message.session_id}"
        member = json.dumps({
            "role": message.role.value,
            "content": message.content,
            "request_id": message.request_id,
        })
        await self._redis.zadd(name=key, mapping={member: time.time()})
        await self._redis.expire(name=key, time=settings.redis_ttl)

    async def get_history(self, session_id: str) -> list[Message]:
        key = f"conversation:{session_id}"
        raw = await self._redis.zrange(name=key, start=0, end=-1)
        return [
            Message(
                session_id=session_id,
                request_id=item["request_id"],
                role=MessageRole(item["role"]),
                content=item["content"],
            )
            for item in (json.loads(r) for r in raw)
        ]
```

- [ ] **Step 5: Run tests — must show 5 passed**

```bash
uv run pytest tests/integration/test_history_cache.py -v
```

- [ ] **Step 6: Commit**

```bash
git add infrastructure/ tests/integration/
git commit -m "feat: RedisConversationCache implements ConversationCache"
```

---

## Task 6: Infrastructure — PostgreSQL + Move Models + Update Alembic

**Files:**
- Create: `infrastructure/postgres/__init__.py`
- Create: `infrastructure/postgres/models.py`
- Create: `infrastructure/postgres/message_store.py`
- Modify: `alembic/env.py`

- [ ] **Step 1: Create infrastructure/postgres/__init__.py (empty)**

- [ ] **Step 2: Create infrastructure/postgres/models.py** (moved from app/models.py — identical content)

```python
from __future__ import annotations
from sqlalchemy import Column, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class Session(Base):
    __tablename__ = "sessions"

    session_id = Column(Text, primary_key=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())


class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Text, ForeignKey("sessions.session_id"), nullable=False)
    request_id = Column(Text, nullable=False)
    role = Column(Text, nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
```

- [ ] **Step 3: Create infrastructure/postgres/message_store.py**

```python
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from application.interfaces.message_store import MessageStore
from domain.entities import Message


class PostgresMessageStore(MessageStore):
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory

    async def save_message(self, message: Message) -> None:
        async with self._session_factory() as db:
            await db.execute(
                text("INSERT INTO sessions (session_id) VALUES (:sid) ON CONFLICT DO NOTHING"),
                {"sid": message.session_id},
            )
            await db.execute(
                text(
                    "INSERT INTO messages (session_id, request_id, role, content) "
                    "VALUES (:sid, :rid, :role, :content)"
                ),
                {
                    "sid": message.session_id,
                    "rid": message.request_id,
                    "role": message.role.value,
                    "content": message.content,
                },
            )
            await db.commit()
```

- [ ] **Step 4: Update alembic/env.py** — change the import line from `from app.models import Base` to `from infrastructure.postgres.models import Base`

Find this line in `alembic/env.py`:
```python
from app.models import Base
```
Replace with:
```python
from infrastructure.postgres.models import Base
```

- [ ] **Step 5: Verify alembic still works (postgres must be running)**

```bash
docker compose up -d postgres
uv run alembic current
```
Expected: shows current revision without errors.

- [ ] **Step 6: Verify import**

```bash
uv run python -c "from infrastructure.postgres.message_store import PostgresMessageStore; print('ok')"
```
Expected: `ok`

- [ ] **Step 7: Commit**

```bash
git add infrastructure/postgres/ alembic/env.py
git commit -m "feat: PostgresMessageStore + move ORM models to infrastructure"
```

---

## Task 7: Infrastructure — Kafka

**Files:**
- Create: `infrastructure/kafka/__init__.py`
- Create: `infrastructure/kafka/event_publisher.py`

- [ ] **Step 1: Create infrastructure/kafka/__init__.py (empty)**

- [ ] **Step 2: Create infrastructure/kafka/event_publisher.py**

```python
from __future__ import annotations
import asyncio

from confluent_kafka import Producer

from application.interfaces.event_publisher import EventPublisher
from shared.schemas import ChatRequest, ChatResponse


class KafkaEventPublisher(EventPublisher):
    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    async def publish_request(self, request: ChatRequest) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: (
                self._producer.produce(
                    topic="chat.requests",
                    value=request.model_dump_json(),
                ),
                self._producer.flush(),
            ),
        )

    async def publish_response(self, response: ChatResponse) -> None:
        self._producer.produce(
            topic="chat.responses",
            value=response.model_dump_json(),
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()
```

- [ ] **Step 3: Verify import**

```bash
uv run python -c "from infrastructure.kafka.event_publisher import KafkaEventPublisher; print('ok')"
```
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add infrastructure/kafka/
git commit -m "feat: KafkaEventPublisher implements EventPublisher"
```

---

## Task 8: Infrastructure — LLM Strategy Pattern

**Files:**
- Create: `infrastructure/llm/__init__.py`
- Create: `infrastructure/llm/mock_strategy.py`
- Create: `infrastructure/llm/openai_strategy.py`
- Create: `infrastructure/llm/factory.py`

- [ ] **Step 1: Create infrastructure/llm/__init__.py (empty)**

- [ ] **Step 2: Create infrastructure/llm/mock_strategy.py**

```python
from __future__ import annotations
import asyncio
import random
from typing import AsyncGenerator

from application.interfaces.token_generator import TokenGenerator

_RESPONSES = [
    "Xin chào! Tôi là một AI trợ lý. Tôi có thể giúp gì cho bạn?",
    "Đây là một hệ thống event-driven streaming sử dụng Kafka và FastAPI.",
    "Latency là thời gian để một packet đi từ sender đến receiver.",
    "Throughput là lượng data thực sự được truyền thành công mỗi giây.",
    "Redis sorted sets rất phù hợp để lưu conversation history theo thứ tự thời gian.",
]


class MockLLMStrategy(TokenGenerator):
    async def generate(self, content: str) -> AsyncGenerator[str, None]:
        response = random.choice(_RESPONSES)
        words = response.split()
        for i, word in enumerate(words):
            prefix = "" if i == 0 else " "
            yield prefix + word
            await asyncio.sleep(0.05)
```

- [ ] **Step 3: Create infrastructure/llm/openai_strategy.py**

```python
from __future__ import annotations
from typing import AsyncGenerator

from application.interfaces.token_generator import TokenGenerator


class OpenAIStrategy(TokenGenerator):
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    async def generate(self, content: str) -> AsyncGenerator[str, None]:
        raise NotImplementedError("OpenAI integration not yet implemented")
        yield  # make this an async generator
```

- [ ] **Step 4: Create infrastructure/llm/factory.py**

```python
from __future__ import annotations

from application.interfaces.token_generator import TokenGenerator
from shared.settings import settings


def create_llm_strategy() -> TokenGenerator:
    match settings.llm_provider:
        case "mock":
            from infrastructure.llm.mock_strategy import MockLLMStrategy
            return MockLLMStrategy()
        case "openai":
            from infrastructure.llm.openai_strategy import OpenAIStrategy
            return OpenAIStrategy(api_key=settings.openai_api_key)
        case _:
            raise ValueError(f"Unknown LLM provider: {settings.llm_provider}")
```

- [ ] **Step 5: Verify factory creates mock strategy**

```bash
uv run python -c "
from infrastructure.llm.factory import create_llm_strategy
s = create_llm_strategy()
print(type(s).__name__)
"
```
Expected: `MockLLMStrategy`

- [ ] **Step 6: Commit**

```bash
git add infrastructure/llm/
git commit -m "feat: LLM Strategy Pattern — MockLLMStrategy, OpenAIStrategy stub, factory"
```

---

## Task 9: API Layer

**Files:**
- Create: `api/__init__.py`
- Create: `api/state.py`
- Create: `api/dependencies.py`
- Create: `api/routers/__init__.py`
- Create: `api/routers/chat.py`
- Create: `api/app.py`

- [ ] **Step 1: Create api/__init__.py (empty)**

- [ ] **Step 2: Create api/state.py**

```python
from __future__ import annotations
import asyncio

response_queues: dict[str, asyncio.Queue] = {}
```

- [ ] **Step 3: Create api/routers/__init__.py (empty)**

- [ ] **Step 4: Create api/dependencies.py**

```python
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
```

- [ ] **Step 5: Create api/routers/chat.py**

```python
from __future__ import annotations
import asyncio
import json

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api import state
from api.dependencies import get_history_use_case, get_send_message_use_case
from application.use_cases.get_history import GetHistoryUseCase
from application.use_cases.send_message import SendMessageUseCase

router = APIRouter()


class ChatBody(BaseModel):
    session_id: str
    content: str


@router.post("/chat")
async def post_chat(
    body: ChatBody,
    use_case: SendMessageUseCase = Depends(get_send_message_use_case),
) -> dict:
    request_id = await use_case.execute(
        session_id=body.session_id,
        content=body.content,
    )
    return {"request_id": request_id}


@router.get("/chat/stream/{request_id}")
async def stream_response(request_id: str) -> StreamingResponse:
    queue: asyncio.Queue = asyncio.Queue()
    state.response_queues[request_id] = queue

    async def event_generator():
        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    yield "data: [DONE]\n\n"
                    break
                if data.get("finish_reason") == "stop":
                    yield "data: [DONE]\n\n"
                    break
                content = data.get("delta", "")
                payload = json.dumps({
                    "choices": [{"delta": {"content": content}, "finish_reason": None}],
                })
                yield f"data: {payload}\n\n"
        finally:
            state.response_queues.pop(request_id, None)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.get("/history/{session_id}")
async def get_conversation_history(
    session_id: str,
    use_case: GetHistoryUseCase = Depends(get_history_use_case),
) -> list[dict]:
    messages = await use_case.execute(session_id=session_id)
    return [
        {"role": m.role.value, "content": m.content, "request_id": m.request_id}
        for m in messages
    ]
```

- [ ] **Step 6: Create api/app.py**

```python
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
            msg = await loop.run_in_executor(None, consumer.poll, 0.1)
            if msg is None:
                continue
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
```

- [ ] **Step 7: Verify app imports cleanly**

```bash
uv run python -c "from api.app import app; print('ok')"
```
Expected: `ok`

- [ ] **Step 8: Commit**

```bash
git add api/
git commit -m "feat: API layer — FastAPI app, dependencies via app.state, chat router"
```

---

## Task 10: Consumer Layer

**Files:**
- Create: `consumer/__init__.py`
- Create: `consumer/handler.py`
- Create: `consumer/runner.py`

- [ ] **Step 1: Create consumer/__init__.py (empty)**

- [ ] **Step 2: Create consumer/handler.py**

```python
from __future__ import annotations
import logging

from application.use_cases.process_chat_request import ProcessChatRequestUseCase
from shared.schemas import ChatRequest

logger = logging.getLogger(__name__)


class ChatRequestHandler:
    def __init__(self, use_case: ProcessChatRequestUseCase) -> None:
        self._use_case = use_case

    async def handle(self, raw_value: bytes) -> None:
        request = ChatRequest.model_validate_json(raw_value)
        logger.info(f"Handling request {request.request_id} for session {request.session_id}")
        await self._use_case.execute(request=request)
```

- [ ] **Step 3: Create consumer/runner.py**

```python
from __future__ import annotations
import asyncio
import logging

from confluent_kafka import Consumer, KafkaError, Producer
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from application.use_cases.process_chat_request import ProcessChatRequestUseCase
from consumer.handler import ChatRequestHandler
from infrastructure.kafka.event_publisher import KafkaEventPublisher
from infrastructure.llm.factory import create_llm_strategy
from infrastructure.postgres.message_store import PostgresMessageStore
from infrastructure.redis.conversation_cache import RedisConversationCache
from shared.settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def run() -> None:
    redis = Redis.from_url(settings.redis_url)
    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

    use_case = ProcessChatRequestUseCase(
        generator=create_llm_strategy(),
        publisher=KafkaEventPublisher(producer=producer),
        cache=RedisConversationCache(redis=redis),
        store=PostgresMessageStore(session_factory=session_factory),
    )
    handler = ChatRequestHandler(use_case=use_case)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "llm-worker",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["chat.requests"])
    logger.info("Consumer started — listening on chat.requests")
    loop = asyncio.get_running_loop()

    try:
        while True:
            msg = await loop.run_in_executor(None, consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue
            await handler.handle(raw_value=msg.value())
    finally:
        consumer.close()
        await redis.aclose()
        await engine.dispose()
```

- [ ] **Step 4: Verify imports**

```bash
uv run python -c "from consumer.runner import run; print('ok')"
```
Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add consumer/
git commit -m "feat: consumer layer — ChatRequestHandler, runner with DI wiring"
```

---

## Task 11: Entry Points + pyproject.toml Update

**Files:**
- Create: `run_api.py`
- Create: `run_worker.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Create run_api.py**

```python
from __future__ import annotations
import uvicorn

if __name__ == "__main__":
    uvicorn.run("api.app:app", host="0.0.0.0", port=8000, reload=True)
```

- [ ] **Step 2: Create run_worker.py**

```python
from __future__ import annotations
import asyncio

from consumer.runner import run

if __name__ == "__main__":
    asyncio.run(run())
```

- [ ] **Step 3: Update pyproject.toml** — replace `packages` list in `[tool.hatch.build.targets.wheel]`

Find:
```toml
[tool.hatch.build.targets.wheel]
packages = ["app", "shared", "worker"]
```
Replace with:
```toml
[tool.hatch.build.targets.wheel]
packages = ["domain", "application", "infrastructure", "api", "consumer", "shared"]
```

- [ ] **Step 4: Run uv sync to reinstall with updated packages**

```bash
uv sync
```

- [ ] **Step 5: Verify entry points import cleanly**

```bash
uv run python -c "import run_api; print('api ok')"
uv run python -c "import run_worker; print('worker ok')"
```
Expected: `api ok`, `worker ok`

- [ ] **Step 6: Commit**

```bash
git add run_api.py run_worker.py pyproject.toml uv.lock
git commit -m "feat: entry points run_api.py and run_worker.py, update pyproject packages"
```

---

## Task 12: Integration Tests for API Router

**Files:**
- Create: `tests/integration/test_chat_router.py`

- [ ] **Step 1: Write the failing tests**

`tests/integration/test_chat_router.py`:
```python
from __future__ import annotations
import asyncio
import json
import pytest
from unittest.mock import AsyncMock

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
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/integration/test_chat_router.py -v
```
Expected: tests import correctly but may fail on app startup details.

- [ ] **Step 3: Run tests — must show 4 passed**

```bash
uv run pytest tests/integration/test_chat_router.py -v
```

- [ ] **Step 4: Run all new tests together**

```bash
uv run pytest tests/unit/ tests/integration/ -v
```
Expected: all pass (unit + integration, old tests/ root still intact).

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_chat_router.py
git commit -m "feat: integration tests for API router using new architecture"
```

---

## Task 13: Delete Old Code + Final Cleanup

**Files:**
- Delete: `app/` (entire folder)
- Delete: `worker/` (entire folder)
- Delete: `tests/test_chat_router.py`, `tests/test_history.py`, `tests/test_mock_llm.py`, `tests/test_schemas.py`
- Modify: `Makefile`
- Modify: `README.md`

- [ ] **Step 1: Run new test suite to confirm it passes before deleting**

```bash
uv run pytest tests/unit/ tests/integration/ -v
```
Expected: all pass. If any fail, fix before proceeding.

- [ ] **Step 2: Delete old folders and root-level tests**

```bash
git rm -r app/ worker/
git rm tests/test_chat_router.py tests/test_history.py tests/test_mock_llm.py tests/test_schemas.py
```

- [ ] **Step 3: Update Makefile** — replace `dev` and `worker` targets

Find:
```makefile
dev:
	uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

worker:
	uv run python -m worker.main
```
Replace with:
```makefile
dev:
	uv run python run_api.py

worker:
	uv run python run_worker.py
```

- [ ] **Step 4: Update README.md** — replace project structure section

Find the `## Project structure` section and replace with:
```markdown
## Project structure

```
domain/         — entities (Message, Session), value objects (MessageRole)
application/
  interfaces/   — ABCs: ConversationCache, EventPublisher, MessageStore, TokenGenerator
  use_cases/    — SendMessage, GetHistory, ProcessChatRequest
infrastructure/
  kafka/        — KafkaEventPublisher
  llm/          — Strategy: MockLLMStrategy, OpenAIStrategy, factory
  redis/        — RedisConversationCache
  postgres/     — PostgresMessageStore, ORM models
api/            — FastAPI (Backend role): app.py, dependencies, routers
consumer/       — Kafka consumer (Consumer role): handler, runner
shared/         — cross-cutting: Kafka schemas, pydantic-settings
tests/
  unit/         — use cases tested with mock ABCs
  integration/  — routers + cache with fakeredis/httpx
```
```

Also update the quickstart `make dev` and `make worker` descriptions if needed.

- [ ] **Step 5: Run full test suite**

```bash
uv run pytest -v
```
Expected: all tests pass (domain + use cases + history cache + chat router).

- [ ] **Step 6: Verify entry points still work**

```bash
timeout 3 uv run python run_worker.py || true
```
Expected: prints `Consumer started — listening on chat.requests` before timeout.

- [ ] **Step 7: Commit**

```bash
git add Makefile README.md
git commit -m "refactor: complete Clean Architecture migration — remove old app/ worker/ layers"
```
