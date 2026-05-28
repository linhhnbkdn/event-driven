# Event-Driven Streaming

LLM-style token streaming qua Kafka, FastAPI SSE, Redis, PostgreSQL.

```
CLI ──POST /chat──► FastAPI (api/) ──► Kafka[chat.requests]
                                              │
                                    Consumer (consumer/)
                                    LLM Strategy (infrastructure/llm/)
                                              │
CLI ◄──SSE stream──  FastAPI ◄──── Kafka[chat.responses]
                        │
                   Redis ZADD/ZRANGE  ←  ConversationCache
                   PostgreSQL         ←  MessageStore
```

## Architecture

Clean Architecture — dependency rule: outer layers depend on inner, never reverse.

```
domain/          ← innermost, no external imports
    ↑ depends on
application/     ← use cases + interfaces (ABCs)
    ↑ depends on
infrastructure/  ← Kafka, Redis, Postgres, LLM adapters
api/             ← FastAPI adapter (Backend role)
consumer/        ← Kafka consumer adapter (Consumer role)
```

## Stack

| Component | Technology |
|---|---|
| API | FastAPI + uvicorn |
| Message broker | Confluent Kafka |
| Cache | Redis sorted sets |
| Database | PostgreSQL + SQLAlchemy async |
| Migrations | Alembic |
| Package manager | uv |

## Quickstart

```bash
# 1. Install dependencies
uv sync --dev

# 2. Start infrastructure
make up

# 3. Apply migrations
make migrate

# 4. Start worker + API (two terminals)
make worker
make dev

# 5. Chat
make chat SESSION=demo-001 MSG="xin chào"
make history SESSION=demo-001
```

## Make targets

```
make up          — docker compose up -d
make down        — docker compose down
make migrate     — alembic upgrade head
make dev         — python run_api.py
make worker      — python run_worker.py
make test        — pytest -v
make chat        — SESSION=<id> MSG=<text>
make history     — SESSION=<id>
make psql        — psql into chatdb
make logs        — docker compose logs -f
```

## API

| Method | Path | Description |
|---|---|---|
| POST | `/chat` | Submit message → returns `{request_id}` |
| GET | `/chat/stream/{request_id}` | SSE token stream (OpenAI format) |
| GET | `/history/{session_id}` | Conversation history from Redis |

### SSE format

```
data: {"choices":[{"delta":{"content":"Xin"},"finish_reason":null}]}

data: {"choices":[{"delta":{"content":" chào"},"finish_reason":null}]}

data: [DONE]
```

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

## Swapping LLM Provider

The LLM layer uses the **Strategy Pattern** — swap provider by changing one env var:

```bash
# Mock (default, no API key needed)
LLM_PROVIDER=mock

# OpenAI
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...
```

Adding a new provider: implement `TokenGenerator` ABC in `infrastructure/llm/`, add a case to `infrastructure/llm/factory.py`.

## Environment variables

See `.env.example`. Default values work with `docker compose up`.

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/chatdb
REDIS_TTL=86400
LLM_PROVIDER=mock
OPENAI_API_KEY=
```
