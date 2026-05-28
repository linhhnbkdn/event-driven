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
make dev         — uvicorn app.main:app --reload
make worker      — python -m worker.main
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
shared/         — Pydantic schemas + pydantic-settings
app/            — FastAPI service
  routers/      — chat endpoints
  services/     — history (Redis ZADD/ZRANGE)
  dependencies.py — DI: get_db, get_redis, get_producer
  state.py      — response_queues fan-out dict
worker/         — Mock LLM consumer
  mock_llm.py   — async token generator
  persistence.py — Redis + PostgreSQL write
alembic/        — DB migrations
tests/          — unit tests (fakeredis, httpx ASGITransport)
cli.py          — httpx CLI client
```

## Environment variables

See `.env.example`. Default values work with `docker compose up`.

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/chatdb
REDIS_TTL=86400
```
