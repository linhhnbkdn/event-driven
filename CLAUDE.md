# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Dependencies
uv sync --dev

# Infrastructure
make up           # docker compose up -d (Kafka + Zookeeper + Redis + Postgres)
make down
make logs

# Database
make migrate      # alembic upgrade head (runs inside api container)

# Run services (3 terminals, or use docker compose)
make dev          # FastAPI on :8000
make worker       # Group 1 consumer: LLM streaming
make persistence  # Group 2 consumer: DB persistence

# Tests
uv run pytest -v                          # all tests
uv run pytest tests/unit/ -v             # unit only
uv run pytest tests/integration/ -v      # integration only
uv run pytest -v -k test_send_message    # single test

# Interact
make chat SESSION=demo-001 MSG="hello"
make history SESSION=demo-001            # from Redis
uv run python cli.py history demo-001   # or postgres: add --db flag

# Load test
uv run locust -f locustfile.py
```

## Architecture

Clean Architecture with strict dependency direction: `domain ← application ← infrastructure/api/consumer`.

**Three processes** run independently and communicate via Kafka:

1. **`api/`** (FastAPI) — receives HTTP requests, publishes to `chat.requests`, fans out SSE tokens from `chat.responses` via in-memory `asyncio.Queue` per `request_id` (`api/state.py`).
2. **`consumer/runner.py`** (Group 1: `llm-worker`) — consumes `chat.requests`, streams tokens from LLM, publishes each token to `chat.responses`, saves user+assistant messages to Redis, then publishes to `chat.completed`.
3. **`consumer/persistence_runner.py`** (Group 2: `persistence-worker`) — consumes `chat.completed`, reads that request's messages from Redis via `ZRANGE`, writes them to PostgreSQL.

**Kafka topics:**
- `chat.requests` — FastAPI → Group 1
- `chat.responses` — Group 1 → FastAPI SSE fan-out
- `chat.completed` — Group 1 → Group 2

**Redis** stores messages as sorted sets keyed by `session_id` (score = timestamp). TTL 24h. Used as the fast history source; Postgres is the durable backup.

**LLM Strategy Pattern** (`infrastructure/llm/`): swap provider via `LLM_PROVIDER` env var (`mock` default, `openai`). Add new providers by implementing `TokenGenerator` ABC and registering in `factory.py`.

## Key Design Decisions

- The FastAPI process also runs a background Kafka consumer (`_consume_responses`) for `chat.responses`, routing tokens into per-request `asyncio.Queue`s. This is how SSE works without a separate pub/sub layer.
- `PersistSessionUseCase` filters Redis history by `request_id` before writing to Postgres — so only the current exchange is persisted per event, not the full session history.
- Kafka producer configs differ intentionally: API uses `linger.ms=5` (batch-tolerant), worker uses `linger.ms=0` (latency-sensitive for streaming).
- Topic auto-creation happens at FastAPI startup via `AdminClient` — no manual Kafka setup needed.

## Testing

- **Unit tests** (`tests/unit/`) mock all ABCs with `AsyncMock`/`MagicMock` — no infrastructure needed.
- **Integration tests** (`tests/integration/`) use `fakeredis` and `httpx.AsyncClient` with the FastAPI app — no real Redis or Kafka needed.
- `pytest-asyncio` with `asyncio_mode = "auto"` — all async test functions work without decorators.

## Environment

All defaults in `.env.example` work with `docker compose up`. Override for local dev (no Docker):
```
KAFKA_BOOTSTRAP_SERVERS=localhost:29092   # external listener port
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/chatdb
LLM_PROVIDER=mock
```
