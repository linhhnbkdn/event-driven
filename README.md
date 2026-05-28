# Event-Driven Streaming

LLM-style token streaming qua Kafka, FastAPI SSE, Redis, PostgreSQL.

## High-level flow

```
CLI ──POST /chat──► FastAPI ──► Kafka[chat.requests]
                                       │
                              ┌────────┴─────────┐
                              │  Group 1: worker  │
                              │  (run_worker.py)  │
                              └────────┬─────────┘
                                       │ tokens
                              Kafka[chat.responses] ──► FastAPI SSE ──► CLI
                                       │
                                  ZADD Redis
                                       │
                              Kafka[chat.completed]
                                       │
                              ┌────────┴──────────────┐
                              │  Group 2: persistence  │
                              │  (run_persistence.py)  │
                              └────────┬──────────────┘
                                       │ ZRANGE Redis → INSERT
                                  PostgreSQL
```

## Sequence diagram

```
┌─────┐     ┌─────────┐    ┌──────────────┐   ┌──────────────────┐   ┌───────┐   ┌──────────┐
│ CLI │     │ FastAPI │    │  Kafka       │   │  Group 1         │   │ Redis │   │ Postgres │
│     │     │ api/    │    │              │   │  worker          │   │       │   │          │
└──┬──┘     └────┬────┘    └──────┬───────┘   └────────┬─────────┘   └───┬───┘   └────┬─────┘
   │             │                │                    │                  │             │
   │ POST /chat  │                │                    │                  │             │
   │────────────►│                │                    │                  │             │
   │             │ produce()      │                    │                  │             │
   │             │───────────────►│ chat.requests      │                  │             │
   │ {request_id}│                │                    │                  │             │
   │◄────────────│                │                    │                  │             │
   │             │                │                    │                  │             │
   │ GET /stream │                │                    │                  │             │
   │────────────►│                │                    │                  │             │
   │  SSE open   │                │                    │                  │             │
   │             │                │   consume()        │                  │             │
   │             │                │ ──────────────────►│                  │             │
   │             │                │                    │ generate token_1 │             │
   │             │ chat.responses │ ◄──────────────────│                  │             │
   │◄────────────│◄───────────────│                    │                  │             │
   │  token_1    │                │                    │ generate token_2 │             │
   │             │ chat.responses │ ◄──────────────────│                  │             │
   │◄────────────│◄───────────────│                    │                  │             │
   │  token_2    │                │                    │ finish_reason    │             │
   │             │ chat.responses │ ◄──────────────────│  =stop           │             │
   │◄────────────│◄───────────────│                    │                  │             │
   │  [DONE]     │                │                    │                  │             │
   │             │                │                    │ ZADD user_msg    │             │
   │             │                │                    │─────────────────►│             │
   │             │                │                    │ ZADD asst_msg    │             │
   │             │                │                    │─────────────────►│             │
   │             │                │                    │                  │             │
   │             │                │ chat.completed     │                  │             │
   │             │                │ ◄──────────────────│{session,request} │             │
   │             │                │                    │                  │             │
   │             │                │   ┌─────────────────────────────────────────────┐  │
   │             │                │   │  Group 2: persistence consumer              │  │
   │             │                │   │  consume(chat.completed)                    │  │
   │             │                │   │  ZRANGE Redis → filter request_id           │  │
   │             │                │   │  INSERT Postgres                ────────────┼─►│
   │             │                │   └─────────────────────────────────────────────┘  │
└──┴──┘     └────┴────┘    └──────┴───────┘   └────────┴─────────┘   └───┴───┘   └────┴─────┘
```

**Tại sao tách 2 consumer groups:**
- Group 1 không bị block bởi DB write → streaming latency thấp hơn
- Group 2 xử lý persistence async, có thể batch nhiều completed events cùng lúc
- Scale độc lập: tăng instance Group 1 để xử lý nhiều chat hơn, tăng Group 2 để xử lý DB write

## Architecture

Clean Architecture — dependency rule: outer layers depend on inner, never reverse.

```
domain/          ← innermost, no external imports
    ↑ depends on
application/     ← use cases + interfaces (ABCs)
    ↑ depends on
infrastructure/  ← Kafka, Redis, Postgres, LLM adapters
api/             ← FastAPI adapter (Backend role)
consumer/        ← Kafka consumer adapters (Streaming + Persistence roles)
```

## Stack

| Component | Technology |
|---|---|
| API | FastAPI + uvicorn |
| Message broker | Confluent Kafka (3 topics) |
| Cache | Redis sorted sets (ZADD/ZRANGE) |
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

# 4. Start all 3 services (3 terminals)
make dev          # FastAPI backend
make worker       # Group 1: streaming consumer
make persistence  # Group 2: persistence consumer

# 5. Chat
make chat SESSION=demo-001 MSG="xin chào"
make history SESSION=demo-001
```

## Kafka topics

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `chat.requests` | FastAPI | Group 1 | Incoming chat messages |
| `chat.responses` | Group 1 | FastAPI | Token stream (SSE fan-out) |
| `chat.completed` | Group 1 | Group 2 | Trigger persistence after stream done |

## Make targets

```
make up          — docker compose up -d
make down        — docker compose down
make migrate     — alembic upgrade head
make dev         — python run_api.py
make worker      — python run_worker.py       (Group 1: streaming)
make persistence — python run_persistence.py  (Group 2: persistence)
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
| GET | `/history/{session_id}` | Conversation history from Redis (fast) |
| GET | `/history/{session_id}/db` | Conversation history from PostgreSQL (persistent) |

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
  use_cases/    — SendMessage, GetHistory, ProcessChatRequest, PersistSession
infrastructure/
  kafka/        — KafkaEventPublisher (chat.requests, chat.responses, chat.completed)
  llm/          — Strategy: MockLLMStrategy, OpenAIStrategy, factory
  redis/        — RedisConversationCache (ZADD/ZRANGE, TTL 24h)
  postgres/     — PostgresMessageStore, ORM models
api/            — FastAPI (Backend role): app.py, dependencies, routers
consumer/       — runner.py (Group 1), persistence_runner.py (Group 2)
shared/         — Kafka schemas (ChatRequest, ChatResponse, ChatCompleted), settings
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
