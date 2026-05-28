# Event-Driven Streaming

LLM-style token streaming qua Kafka, FastAPI SSE, Redis, PostgreSQL.

```
CLI ──POST /chat──► FastAPI ──► Kafka[chat.requests]
                                       │
                               Mock LLM Worker
                                       │
CLI ◄──SSE stream──  FastAPI ◄── Kafka[chat.responses]
                        │
                   Redis ZADD/ZRANGE (history cache)
                   PostgreSQL (persistent storage)
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

## Environment variables

See `.env.example`. Default values work with `docker compose up`.

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/chatdb
REDIS_TTL=86400
```
