# Clean Architecture Restructure — Design Spec

**Date:** 2026-05-28
**Scope:** Full restructure of event-driven streaming repo to Clean Architecture + fix all clean code violations

---

## 1. Goal

Restructure the codebase into Clean Architecture layers (domain → application → infrastructure/api/consumer) with proper separation of concerns, Strategy Pattern for LLM providers, and all clean code violations fixed.

---

## 2. Architecture Layers & Dependency Rule

```
domain/          ← innermost, no external imports
    ↑
application/     ← depends only on domain
    ↑
infrastructure/  ← depends on application interfaces
api/             ← depends on application use cases
consumer/        ← depends on application use cases
```

Outer layers depend on inner layers. Inner layers never import from outer layers.

---

## 3. Folder Structure

```
event-driven/
├── domain/
│   ├── __init__.py
│   ├── entities.py              ← Message, Session (frozen dataclasses)
│   └── value_objects.py         ← MessageRole(str, Enum)
│
├── application/
│   ├── __init__.py
│   ├── interfaces/
│   │   ├── __init__.py
│   │   ├── conversation_cache.py   ← ABC: save_message(), get_history()
│   │   ├── message_store.py        ← ABC: save_message()
│   │   └── token_generator.py      ← ABC: generate() → AsyncGenerator (Strategy interface)
│   └── use_cases/
│       ├── __init__.py
│       ├── send_message.py          ← publish ChatRequest to event bus
│       ├── get_history.py           ← read from ConversationCache
│       └── process_chat_request.py  ← stream tokens + dual-write cache+store
│
├── infrastructure/
│   ├── __init__.py
│   ├── kafka/
│   │   ├── __init__.py
│   │   └── event_publisher.py       ← produce to chat.requests / chat.responses
│   ├── llm/
│   │   ├── __init__.py
│   │   ├── mock_strategy.py         ← MockLLMStrategy(TokenGenerator)
│   │   ├── openai_strategy.py       ← OpenAIStrategy(TokenGenerator) — stub
│   │   └── factory.py               ← create_llm_strategy(provider) → TokenGenerator
│   ├── redis/
│   │   ├── __init__.py
│   │   └── conversation_cache.py    ← RedisConversationCache(ConversationCache ABC)
│   └── postgres/
│       ├── __init__.py
│       └── message_store.py         ← PostgresMessageStore(MessageStore ABC)
│
├── api/                             ← Backend role
│   ├── __init__.py
│   ├── app.py                       ← FastAPI instance + lifespan
│   ├── dependencies.py              ← wire use cases ↔ infrastructure, no mutable globals
│   ├── state.py                     ← response_queues dict
│   └── routers/
│       ├── __init__.py
│       └── chat.py                  ← POST /chat, GET /chat/stream/{id}, GET /history/{sid}
│
├── consumer/                        ← Consumer role
│   ├── __init__.py
│   ├── handler.py                   ← parse message, call ProcessChatRequestUseCase
│   └── runner.py                    ← Kafka poll loop, dependency wiring
│
├── shared/
│   ├── __init__.py
│   ├── schemas.py                   ← Kafka wire format (ChatRequest, ChatResponse)
│   └── settings.py                  ← pydantic-settings + llm_provider field
│
├── tests/
│   ├── __init__.py
│   ├── unit/
│   │   ├── __init__.py
│   │   ├── test_send_message.py     ← mock EventPublisher
│   │   ├── test_get_history.py      ← mock ConversationCache
│   │   └── test_process_request.py  ← mock TokenGenerator + mocks
│   └── integration/
│       ├── __init__.py
│       ├── test_chat_router.py      ← fakeredis + httpx ASGITransport
│       └── test_history_cache.py    ← fakeredis
│
├── alembic/
├── run_api.py                       ← entry: uv run python run_api.py
├── run_worker.py                    ← entry: uv run python run_worker.py
├── cli.py
├── pyproject.toml
└── docker-compose.yml
```

---

## 4. Domain Layer

```python
# domain/value_objects.py
from enum import Enum

class MessageRole(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"
```

```python
# domain/entities.py
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

No imports from application, infrastructure, or any external library.

---

## 5. Application Interfaces (ABCs)

```python
# application/interfaces/conversation_cache.py
from abc import ABC, abstractmethod
from domain.entities import Message

class ConversationCache(ABC):
    @abstractmethod
    async def save_message(self, message: Message) -> None: ...

    @abstractmethod
    async def get_history(self, session_id: str) -> list[Message]: ...
```

```python
# application/interfaces/message_store.py
from abc import ABC, abstractmethod
from domain.entities import Message

class MessageStore(ABC):
    @abstractmethod
    async def save_message(self, message: Message) -> None: ...
```

```python
# application/interfaces/token_generator.py
from abc import ABC, abstractmethod
from typing import AsyncGenerator

class TokenGenerator(ABC):
    @abstractmethod
    async def generate(self, content: str) -> AsyncGenerator[str, None]: ...
```

---

## 6. Application Use Cases

```python
# application/use_cases/send_message.py
class SendMessageUseCase:
    def __init__(self, publisher: KafkaEventPublisher) -> None: ...
    async def execute(self, session_id: str, content: str) -> str:
        # creates ChatRequest, publishes to chat.requests, returns request_id
```

```python
# application/use_cases/get_history.py
class GetHistoryUseCase:
    def __init__(self, cache: ConversationCache) -> None: ...
    async def execute(self, session_id: str) -> list[Message]: ...
```

```python
# application/use_cases/process_chat_request.py
class ProcessChatRequestUseCase:
    def __init__(
        self,
        generator: TokenGenerator,
        publisher: KafkaEventPublisher,
        cache: ConversationCache,
        store: MessageStore,
    ) -> None: ...
    async def execute(self, request: ChatRequest) -> None:
        # 1. stream tokens → publish each to chat.responses
        # 2. send finish_reason=stop
        # 3. dual-write Message(user) + Message(assistant) to cache + store
```

Use cases depend only on ABCs — never on infrastructure implementations.

---

## 7. Infrastructure — Strategy Pattern for LLM

```python
# infrastructure/llm/factory.py
from shared.settings import settings
from application.interfaces.token_generator import TokenGenerator

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

```python
# shared/settings.py — add fields
llm_provider: str = "mock"          # "mock" | "openai"
openai_api_key: str = ""            # only needed when llm_provider="openai"
```

`MockLLMStrategy` và `OpenAIStrategy` đều implement `TokenGenerator` ABC — swap provider chỉ cần đổi `LLM_PROVIDER` trong `.env`.

---

## 8. Infrastructure Implementations

**RedisConversationCache** implements `ConversationCache`:
- `save_message()` → `ZADD conversation:{session_id} {timestamp} {json}`
- `get_history()` → `ZRANGE conversation:{session_id} 0 -1` → parse JSON → `list[Message]`
- TTL reset on every save

**PostgresMessageStore** implements `MessageStore`:
- `save_message()` → upsert session + insert message row

**KafkaEventPublisher**:
- `publish_request(ChatRequest)` → produce to `chat.requests`
- `publish_response(ChatResponse)` → produce to `chat.responses`
- Wraps `confluent_kafka.Producer` với `asyncio.get_running_loop().run_in_executor`

---

## 9. API Layer (Backend Role)

`api/dependencies.py` — wires infrastructure vào use cases via FastAPI `Depends()`. **Không dùng mutable module-level globals**. Dùng `app.state` của FastAPI thay thế:

```python
# api/app.py lifespan sets:
app.state.redis = Redis.from_url(...)
app.state.producer = Producer(...)

# api/dependencies.py reads from request.app.state:
async def get_redis(request: Request) -> Redis:
    return request.app.state.redis
```

`api/routers/chat.py` — chỉ gọi use cases, không biết gì về Kafka/Redis trực tiếp.

---

## 10. Consumer Layer (Consumer Role)

```
consumer/runner.py   ← Kafka poll loop, khởi tạo ProcessChatRequestUseCase
consumer/handler.py  ← parse raw Kafka message → gọi use_case.execute()
```

```python
# run_worker.py
import asyncio
from consumer.runner import run

if __name__ == "__main__":
    asyncio.run(run())
```

---

## 11. Entry Points

```python
# run_api.py
import uvicorn
from api.app import app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

```python
# run_worker.py
import asyncio
from consumer.runner import run

if __name__ == "__main__":
    asyncio.run(run())
```

Makefile targets remain: `make dev` → `uv run python run_api.py`, `make worker` → `uv run python run_worker.py`.

---

## 12. Clean Code Fixes Applied

| Issue | Fix |
|---|---|
| DRY: duplicate Redis ZADD in history.py + persistence.py | Merged vào `RedisConversationCache.save_message()` |
| SRP: `_process_request` làm 3 việc | Split thành `ProcessChatRequestUseCase` với 3 injected dependencies |
| Type: `role` là raw `str` | `MessageRole(str, Enum)` — enforced ở domain layer |
| Mutable globals `_redis`, `_producer` | Replaced bằng `app.state` (FastAPI built-in) |
| Late import ở cuối `main.py` | Resolved vì `api/app.py` → `api/routers/chat.py` không còn circular |
| `get_db` import thừa trong `chat.py` | Removed |
| `role` string literal rải rác | `MessageRole.USER`, `MessageRole.ASSISTANT` |

---

## 13. Test Strategy

**Unit tests** (`tests/unit/`): test use cases với mock ABCs — không cần Kafka/Redis/Postgres.

**Integration tests** (`tests/integration/`): test routers với `fakeredis` + `httpx.AsyncClient(ASGITransport)`.

Existing 17 tests → migrate sang structure mới, không mất coverage.
