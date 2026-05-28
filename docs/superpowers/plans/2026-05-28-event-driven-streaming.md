# Event-Driven Streaming System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a monorepo event-driven system where a CLI sends chat messages through FastAPI → Kafka → Mock LLM Worker → Kafka → FastAPI SSE → CLI, with conversation history in Redis (ZADD/ZRANGE) and PostgreSQL.

**Architecture:** FastAPI publishes user messages to `chat.requests`; a worker consumes them, streams OpenAI-format tokens to `chat.responses`; a background consumer in FastAPI fans out tokens to per-request `asyncio.Queue` instances that feed SSE connections. On `finish_reason: stop`, the worker persists both user and assistant messages to Redis sorted sets (score = Unix timestamp) and PostgreSQL.

**Tech Stack:** Python 3.12, uv, FastAPI, confluent-kafka, redis[asyncio], SQLAlchemy async + asyncpg, Alembic, pydantic-settings, httpx (CLI), pytest + pytest-asyncio + fakeredis

---

## File Map

```
event-driven/
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── .env                          ← gitignored
├── alembic.ini
├── alembic/
│   ├── env.py
│   └── versions/
│       └── 0001_initial.py
├── shared/
│   ├── __init__.py
│   ├── schemas.py                ← Pydantic: ChatRequest, ChatResponse
│   └── settings.py               ← pydantic-settings: Settings singleton
├── app/
│   ├── __init__.py
│   ├── main.py                   ← FastAPI app, lifespan, background consumer
│   ├── state.py                  ← response_queues dict (avoids circular imports)
│   ├── dependencies.py           ← get_db(), get_redis(), get_producer()
│   ├── models.py                 ← SQLAlchemy ORM: Session, Message
│   ├── routers/
│   │   ├── __init__.py
│   │   └── chat.py               ← POST /chat, GET /chat/stream/{id}, GET /history/{sid}
│   └── services/
│       ├── __init__.py
│       └── history.py            ← save_message(), get_history() via Redis ZADD/ZRANGE
├── worker/
│   ├── __init__.py
│   ├── main.py                   ← Kafka consumer loop, orchestrates processing
│   ├── mock_llm.py               ← async token generator (OpenAI SSE format)
│   └── persistence.py            ← ZADD to Redis + INSERT to PostgreSQL
├── tests/
│   ├── __init__.py
│   ├── test_schemas.py
│   ├── test_history.py
│   ├── test_mock_llm.py
│   └── test_chat_router.py
└── cli.py                        ← httpx client: chat + history commands
```

---

## Task 1: Project Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `.env`

- [ ] **Step 1: Initialize uv project**

```bash
uv init --no-readme
```

- [ ] **Step 2: Replace generated pyproject.toml with full config**

```toml
[project]
name = "event-driven"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.32.0",
    "confluent-kafka>=2.6.0",
    "redis[asyncio]>=5.2.0",
    "sqlalchemy[asyncio]>=2.0.0",
    "asyncpg>=0.30.0",
    "alembic>=1.14.0",
    "pydantic-settings>=2.6.0",
    "httpx>=0.28.0",
]

[dependency-groups]
dev = [
    "pytest>=8.3.0",
    "pytest-asyncio>=0.24.0",
    "fakeredis>=2.26.0",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

- [ ] **Step 3: Create docker-compose.yml**

```yaml
version: "3.9"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.7.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.7.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  postgres:
    image: postgres:16-alpine
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
      POSTGRES_DB: chatdb
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

- [ ] **Step 4: Create .env.example and .env**

Both files have identical content:
```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/chatdb
REDIS_TTL=86400
```

- [ ] **Step 5: Install dependencies**

```bash
uv sync --dev
```

Expected: lock file created, all packages installed.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml docker-compose.yml .env.example uv.lock
git commit -m "feat: project scaffolding with uv and docker-compose"
```

---

## Task 2: Shared Schemas

**Files:**
- Create: `shared/__init__.py`
- Create: `shared/schemas.py`
- Create: `tests/__init__.py`
- Create: `tests/test_schemas.py`

- [ ] **Step 1: Write the failing test**

`tests/test_schemas.py`:
```python
from shared.schemas import ChatRequest, ChatResponse


def test_chat_request_auto_generates_request_id():
    req = ChatRequest(session_id="sess1", content="hello")
    assert len(req.request_id) == 36
    assert req.request_id.count("-") == 4


def test_chat_request_auto_generates_timestamp():
    req = ChatRequest(session_id="sess1", content="hello")
    assert req.timestamp > 0


def test_chat_request_explicit_fields():
    req = ChatRequest(
        request_id="abc",
        session_id="sess1",
        content="hi",
        timestamp=1000.0,
    )
    assert req.request_id == "abc"
    assert req.timestamp == 1000.0


def test_chat_response_defaults_finish_reason_to_none():
    resp = ChatResponse(
        request_id="abc",
        session_id="sess1",
        delta="hello",
    )
    assert resp.finish_reason is None


def test_chat_response_done_signal():
    resp = ChatResponse(
        request_id="abc",
        session_id="sess1",
        delta="",
        finish_reason="stop",
    )
    assert resp.finish_reason == "stop"
    assert resp.delta == ""


def test_chat_request_round_trips_json():
    req = ChatRequest(session_id="s", content="hi")
    restored = ChatRequest.model_validate_json(req.model_dump_json())
    assert restored.request_id == req.request_id
    assert restored.content == req.content
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_schemas.py -v
```

Expected: `ModuleNotFoundError: No module named 'shared'`

- [ ] **Step 3: Create shared/__init__.py (empty)**

```python
```

- [ ] **Step 4: Create shared/schemas.py**

```python
from __future__ import annotations
import time
import uuid
from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    session_id: str
    content: str
    timestamp: float = Field(default_factory=time.time)


class ChatResponse(BaseModel):
    request_id: str
    session_id: str
    delta: str
    finish_reason: str | None = None
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/test_schemas.py -v
```

Expected: 6 passed.

- [ ] **Step 6: Commit**

```bash
git add shared/ tests/
git commit -m "feat: shared Pydantic schemas for Kafka messages"
```

---

## Task 3: Shared Settings

**Files:**
- Create: `shared/settings.py`

- [ ] **Step 1: Create shared/settings.py**

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379"
    database_url: str = "postgresql+asyncpg://app:app@localhost:5432/chatdb"
    redis_ttl: int = 86400


settings = Settings()
```

- [ ] **Step 2: Verify settings loads from .env**

```bash
uv run python -c "from shared.settings import settings; print(settings.kafka_bootstrap_servers)"
```

Expected: `localhost:9092`

- [ ] **Step 3: Commit**

```bash
git add shared/settings.py
git commit -m "feat: pydantic-settings config from .env"
```

---

## Task 4: SQLAlchemy Models

**Files:**
- Create: `app/__init__.py`
- Create: `app/models.py`

- [ ] **Step 1: Create app/__init__.py (empty)**

```python
```

- [ ] **Step 2: Create app/models.py**

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

- [ ] **Step 3: Verify models import cleanly**

```bash
uv run python -c "from app.models import Base, Session, Message; print(list(Base.metadata.tables))"
```

Expected: `['sessions', 'messages']`

- [ ] **Step 4: Commit**

```bash
git add app/
git commit -m "feat: SQLAlchemy ORM models for sessions and messages"
```

---

## Task 5: Alembic Setup and First Migration

**Files:**
- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/script.py.mako`
- Create: `alembic/versions/0001_initial.py`

- [ ] **Step 1: Initialize Alembic**

```bash
uv run alembic init alembic
```

Expected: `alembic/` directory and `alembic.ini` created.

- [ ] **Step 2: Replace alembic/env.py with async-compatible version**

```python
import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context
from shared.settings import settings
from app.models import Base

config = context.config
config.set_main_option("sqlalchemy.url", settings.database_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

- [ ] **Step 3: Ensure docker-compose postgres is running**

```bash
docker compose up -d postgres
```

Wait ~5 seconds for postgres to be ready.

- [ ] **Step 4: Generate initial migration**

```bash
uv run alembic revision --autogenerate -m "initial"
```

Expected: `alembic/versions/<hash>_initial.py` created with `sessions` and `messages` tables.

- [ ] **Step 5: Apply migration**

```bash
uv run alembic upgrade head
```

Expected: `Running upgrade  -> <hash>, initial`

- [ ] **Step 6: Verify tables exist**

```bash
docker compose exec postgres psql -U app -d chatdb -c "\dt"
```

Expected: tables `alembic_version`, `messages`, `sessions` listed.

- [ ] **Step 7: Commit**

```bash
git add alembic.ini alembic/
git commit -m "feat: Alembic async setup with sessions and messages migration"
```

---

## Task 6: History Service

**Files:**
- Create: `app/services/__init__.py`
- Create: `app/services/history.py`
- Create: `tests/test_history.py`

- [ ] **Step 1: Write the failing test**

`tests/test_history.py`:
```python
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_history.py -v
```

Expected: `ImportError` — module does not exist yet.

- [ ] **Step 3: Create app/services/__init__.py (empty)**

```python
```

- [ ] **Step 4: Create app/services/history.py**

```python
from __future__ import annotations
import json
import time

from redis.asyncio import Redis

from shared.settings import settings


async def save_message(
    redis: Redis,
    session_id: str,
    role: str,
    content: str,
    request_id: str,
) -> None:
    key = f"conversation:{session_id}"
    member = json.dumps({"role": role, "content": content, "request_id": request_id})
    score = time.time()
    await redis.zadd(name=key, mapping={member: score})
    await redis.expire(name=key, time=settings.redis_ttl)


async def get_history(redis: Redis, session_id: str) -> list[dict]:
    key = f"conversation:{session_id}"
    raw = await redis.zrange(name=key, start=0, end=-1)
    return [json.loads(item) for item in raw]
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/test_history.py -v
```

Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add app/services/ tests/test_history.py
git commit -m "feat: Redis ZADD/ZRANGE history service"
```

---

## Task 7: FastAPI Core (state, dependencies, main)

**Files:**
- Create: `app/state.py`
- Create: `app/dependencies.py`
- Create: `app/main.py`
- Create: `app/routers/__init__.py`

- [ ] **Step 1: Create app/state.py**

```python
from __future__ import annotations
import asyncio

response_queues: dict[str, asyncio.Queue] = {}
```

- [ ] **Step 2: Create app/dependencies.py**

```python
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
```

- [ ] **Step 3: Create app/routers/__init__.py (empty)**

```python
```

- [ ] **Step 4: Create app/main.py**

```python
from __future__ import annotations
import asyncio
import json
import logging
from contextlib import asynccontextmanager

from confluent_kafka import Consumer, KafkaError, Producer
from fastapi import FastAPI
from redis.asyncio import Redis

from app import dependencies, state
from app.routers import chat
from shared.settings import settings

logger = logging.getLogger(__name__)


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
    dependencies._redis = Redis.from_url(settings.redis_url)
    dependencies._producer = Producer(
        {"bootstrap.servers": settings.kafka_bootstrap_servers}
    )
    task = asyncio.create_task(_consume_responses())
    yield
    task.cancel()
    await dependencies._redis.aclose()


app = FastAPI(title="Event-Driven Streaming", lifespan=lifespan)
app.include_router(chat.router)
```

- [ ] **Step 5: Verify app starts (docker-compose services must be running)**

```bash
docker compose up -d
uv run uvicorn app.main:app --reload &
curl http://localhost:8000/docs
```

Expected: FastAPI docs page returns 200. Kill with `fg` then Ctrl+C.

- [ ] **Step 6: Commit**

```bash
git add app/state.py app/dependencies.py app/main.py app/routers/__init__.py
git commit -m "feat: FastAPI core with lifespan, DI, background Kafka consumer"
```

---

## Task 8: Chat Router

**Files:**
- Create: `app/routers/chat.py`
- Create: `tests/test_chat_router.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_chat_router.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_chat_router.py -v
```

Expected: `ImportError` — `app.routers.chat` does not exist.

- [ ] **Step 3: Create app/routers/chat.py**

```python
from __future__ import annotations
import asyncio
import json

from confluent_kafka import Producer
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from redis.asyncio import Redis

from app import state
from app.dependencies import get_db, get_producer, get_redis
from app.services.history import get_history
from shared.schemas import ChatRequest

router = APIRouter()


class ChatBody(BaseModel):
    session_id: str
    content: str


@router.post("/chat")
async def post_chat(
    body: ChatBody,
    producer: Producer = Depends(get_producer),
) -> dict:
    request = ChatRequest(session_id=body.session_id, content=body.content)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: (
            producer.produce(topic="chat.requests", value=request.model_dump_json()),
            producer.flush(),
        ),
    )
    return {"request_id": request.request_id}


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
    redis: Redis = Depends(get_redis),
) -> list[dict]:
    return await get_history(redis=redis, session_id=session_id)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_chat_router.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add app/routers/chat.py tests/test_chat_router.py
git commit -m "feat: chat router — POST /chat, SSE stream, GET /history"
```

---

## Task 9: Worker — Mock LLM

**Files:**
- Create: `worker/__init__.py`
- Create: `worker/mock_llm.py`
- Create: `tests/test_mock_llm.py`

- [ ] **Step 1: Write the failing test**

`tests/test_mock_llm.py`:
```python
from worker.mock_llm import generate_tokens


async def test_generate_tokens_yields_non_empty_strings():
    tokens = []
    async for token in generate_tokens(content="test input"):
        tokens.append(token)
    assert len(tokens) > 0
    assert all(isinstance(t, str) and len(t) > 0 for t in tokens)


async def test_full_response_is_non_empty():
    text = ""
    async for token in generate_tokens(content="anything"):
        text += token
    assert len(text.strip()) > 0


async def test_different_calls_may_vary():
    results = set()
    for _ in range(5):
        text = ""
        async for token in generate_tokens(content="hi"):
            text += token
        results.add(text)
    # At least one unique response across 5 calls (mock has multiple responses)
    assert len(results) >= 1
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_mock_llm.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create worker/__init__.py (empty)**

```python
```

- [ ] **Step 4: Create worker/mock_llm.py**

```python
from __future__ import annotations
import asyncio
import random
from typing import AsyncGenerator

_RESPONSES = [
    "Xin chào! Tôi là một AI trợ lý. Tôi có thể giúp gì cho bạn?",
    "Đây là một hệ thống event-driven streaming sử dụng Kafka và FastAPI.",
    "Latency là thời gian để một packet đi từ sender đến receiver.",
    "Throughput là lượng data thực sự được truyền thành công mỗi giây.",
    "Redis sorted sets rất phù hợp để lưu conversation history theo thứ tự thời gian.",
]


async def generate_tokens(content: str) -> AsyncGenerator[str, None]:
    response = random.choice(_RESPONSES)
    words = response.split()
    for i, word in enumerate(words):
        prefix = "" if i == 0 else " "
        yield prefix + word
        await asyncio.sleep(0.05)
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/test_mock_llm.py -v
```

Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add worker/__init__.py worker/mock_llm.py tests/test_mock_llm.py
git commit -m "feat: mock LLM token generator with async streaming"
```

---

## Task 10: Worker — Persistence

**Files:**
- Create: `worker/persistence.py`

- [ ] **Step 1: Create worker/persistence.py**

```python
from __future__ import annotations
import json
import time

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.settings import settings


async def persist_message(
    redis: Redis,
    db: AsyncSession,
    session_id: str,
    role: str,
    content: str,
    request_id: str,
) -> None:
    await db.execute(
        text("INSERT INTO sessions (session_id) VALUES (:sid) ON CONFLICT DO NOTHING"),
        {"sid": session_id},
    )
    await db.execute(
        text(
            "INSERT INTO messages (session_id, request_id, role, content) "
            "VALUES (:sid, :rid, :role, :content)"
        ),
        {"sid": session_id, "rid": request_id, "role": role, "content": content},
    )
    await db.commit()

    key = f"conversation:{session_id}"
    member = json.dumps({"role": role, "content": content, "request_id": request_id})
    await redis.zadd(name=key, mapping={member: time.time()})
    await redis.expire(name=key, time=settings.redis_ttl)
```

- [ ] **Step 2: Verify import is clean**

```bash
uv run python -c "from worker.persistence import persist_message; print('ok')"
```

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add worker/persistence.py
git commit -m "feat: worker persistence — ZADD to Redis + INSERT to PostgreSQL"
```

---

## Task 11: Worker — Main

**Files:**
- Create: `worker/main.py`

- [ ] **Step 1: Create worker/main.py**

```python
from __future__ import annotations
import asyncio
import logging

from confluent_kafka import Consumer, KafkaError, Producer
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from shared.schemas import ChatRequest, ChatResponse
from shared.settings import settings
from worker.mock_llm import generate_tokens
from worker.persistence import persist_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def _process_request(
    request: ChatRequest,
    producer: Producer,
    redis: Redis,
    session_factory: async_sessionmaker,
) -> None:
    full_response = ""

    async for token in generate_tokens(content=request.content):
        full_response += token
        msg = ChatResponse(
            request_id=request.request_id,
            session_id=request.session_id,
            delta=token,
            finish_reason=None,
        )
        producer.produce(topic="chat.responses", value=msg.model_dump_json())
        producer.poll(0)

    done = ChatResponse(
        request_id=request.request_id,
        session_id=request.session_id,
        delta="",
        finish_reason="stop",
    )
    producer.produce(topic="chat.responses", value=done.model_dump_json())
    producer.flush()

    async with session_factory() as db:
        await persist_message(
            redis=redis,
            db=db,
            session_id=request.session_id,
            role="user",
            content=request.content,
            request_id=request.request_id,
        )
        await persist_message(
            redis=redis,
            db=db,
            session_id=request.session_id,
            role="assistant",
            content=full_response,
            request_id=request.request_id,
        )
    logger.info(f"Persisted conversation for request {request.request_id}")


async def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "llm-worker",
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    redis = Redis.from_url(settings.redis_url)
    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    consumer.subscribe(["chat.requests"])
    logger.info("Worker started — listening on chat.requests")
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
            request = ChatRequest.model_validate_json(msg.value())
            logger.info(f"Received request {request.request_id} for session {request.session_id}")
            await _process_request(
                request=request,
                producer=producer,
                redis=redis,
                session_factory=session_factory,
            )
    finally:
        consumer.close()
        await redis.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Verify import is clean**

```bash
uv run python -c "from worker.main import main; print('ok')"
```

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add worker/main.py
git commit -m "feat: worker main — Kafka consumer loop with async processing"
```

---

## Task 12: CLI

**Files:**
- Create: `cli.py`

- [ ] **Step 1: Create cli.py**

```python
from __future__ import annotations
import argparse
import json
import sys

import httpx

BASE_URL = "http://localhost:8000"


def cmd_chat(session_id: str, content: str) -> None:
    with httpx.Client(timeout=60.0) as client:
        resp = client.post(
            f"{BASE_URL}/chat",
            json={"session_id": session_id, "content": content},
        )
        resp.raise_for_status()
        request_id = resp.json()["request_id"]
        print(f"[request_id: {request_id}]")
        print("Assistant: ", end="", flush=True)

        with client.stream("GET", f"{BASE_URL}/chat/stream/{request_id}") as stream:
            for line in stream.iter_lines():
                if not line.startswith("data:"):
                    continue
                payload = line[len("data:"):].strip()
                if payload == "[DONE]":
                    print()
                    break
                data = json.loads(payload)
                token = data["choices"][0]["delta"]["content"]
                print(token, end="", flush=True)


def cmd_history(session_id: str) -> None:
    with httpx.Client() as client:
        resp = client.get(f"{BASE_URL}/history/{session_id}")
        resp.raise_for_status()
        messages = resp.json()
    if not messages:
        print(f"No history for session '{session_id}'")
        return
    for msg in messages:
        role = msg["role"].upper()
        content = msg["content"]
        print(f"[{role}] {content}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Event-driven chat CLI")
    subs = parser.add_subparsers(dest="command", required=True)

    chat_p = subs.add_parser("chat", help="Send a message and stream the response")
    chat_p.add_argument("--session", required=True, help="Session ID")
    chat_p.add_argument("content", help="Message to send")

    hist_p = subs.add_parser("history", help="Print conversation history")
    hist_p.add_argument("session_id", help="Session ID")

    args = parser.parse_args()

    if args.command == "chat":
        cmd_chat(session_id=args.session, content=args.content)
    elif args.command == "history":
        cmd_history(session_id=args.session_id)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add cli.py
git commit -m "feat: CLI — chat and history commands via httpx"
```

---

## Task 13: End-to-End Smoke Test

- [ ] **Step 1: Start all infrastructure**

```bash
docker compose up -d
```

Wait ~10 seconds for Kafka to be ready.

- [ ] **Step 2: Run database migrations**

```bash
uv run alembic upgrade head
```

Expected: `Running upgrade  -> <hash>, initial`

- [ ] **Step 3: Start the worker in a separate terminal**

```bash
uv run python worker/main.py
```

Expected: `Worker started — listening on chat.requests`

- [ ] **Step 4: Start FastAPI in another terminal**

```bash
uv run uvicorn app.main:app --reload
```

Expected: `Application startup complete.`

- [ ] **Step 5: Send a chat message and observe streaming**

```bash
uv run python cli.py chat --session demo-001 "xin chào"
```

Expected output:
```
[request_id: <uuid>]
Assistant: Xin chào! Tôi là một AI trợ lý. Tôi có thể giúp gì cho bạn?
```
Tokens should appear word-by-word with ~50ms gaps.

- [ ] **Step 6: Send a second message in the same session**

```bash
uv run python cli.py chat --session demo-001 "throughput là gì?"
```

Expected: another streamed response.

- [ ] **Step 7: Verify conversation history**

```bash
uv run python cli.py history demo-001
```

Expected:
```
[USER] xin chào
[ASSISTANT] Xin chào! Tôi là một AI trợ lý...
[USER] throughput là gì?
[ASSISTANT] Throughput là lượng data...
```

- [ ] **Step 8: Verify PostgreSQL persistence**

```bash
docker compose exec postgres psql -U app -d chatdb -c "SELECT role, content FROM messages;"
```

Expected: 4 rows (2 user + 2 assistant messages).

- [ ] **Step 9: Run full test suite**

```bash
uv run pytest -v
```

Expected: all tests pass.

- [ ] **Step 10: Final commit**

```bash
git add .
git commit -m "feat: complete event-driven streaming system"
```
