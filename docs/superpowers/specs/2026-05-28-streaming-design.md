# Event-Driven Streaming System — Design Spec

**Date:** 2026-05-28
**Stack:** FastAPI · Confluent Kafka · Redis · PostgreSQL · Alembic · CLI · uv

---

## 1. Goal

Build a monorepo event-driven streaming system that simulates LLM-style token streaming via Kafka. A CLI acts as the frontend, calling FastAPI endpoints that coordinate message flow through Kafka topics. Conversation history is cached in Redis and persisted in PostgreSQL.

---

## 2. Architecture

```
CLI (cli.py)
  │
  │ POST /chat  →  FastAPI  →  Kafka[chat.requests]
  │                                   │
  │                            Mock LLM Worker
  │                                   │
  │ GET /chat/stream/{id}  ←  FastAPI ← Kafka[chat.responses]
  │
  │ GET /history/{session_id}  →  FastAPI  →  Redis ZRANGE
```

### Data Flow

1. CLI sends `POST /chat` with `{session_id, content}`.
2. FastAPI publishes to `chat.requests`, returns `{request_id}`.
3. CLI opens SSE: `GET /chat/stream/{request_id}`.
4. Worker consumes `chat.requests`, produces tokens one-by-one to `chat.responses`.
5. FastAPI consumes `chat.responses`, filters by `request_id`, forwards as SSE to CLI.
6. On `finish_reason: stop`, worker persists full message to Redis (ZADD) and PostgreSQL.

---

## 3. Kafka Topics

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `chat.requests` | FastAPI | Worker | Incoming user messages |
| `chat.responses` | Worker | FastAPI | Token-by-token streaming responses |

### Message Schemas

**chat.requests:**
```json
{
  "request_id": "uuid4",
  "session_id": "abc123",
  "content": "xin chào",
  "timestamp": 1716900000.123
}
```

**chat.responses (streaming):**
```json
{
  "request_id": "uuid4",
  "session_id": "abc123",
  "delta": "hello",
  "finish_reason": null
}
```

**chat.responses (done):**
```json
{
  "request_id": "uuid4",
  "session_id": "abc123",
  "delta": "",
  "finish_reason": "stop"
}
```

---

## 4. SSE Format (OpenAI-compatible)

Each token forwarded to CLI:
```
data: {"choices":[{"delta":{"content":"Xin"},"finish_reason":null}]}

data: {"choices":[{"delta":{"content":" chào"},"finish_reason":null}]}

data: [DONE]
```

---

## 5. Redis

- **Key:** `conversation:{session_id}`
- **Type:** Sorted Set
- **Score:** Unix timestamp (float)
- **Member:** JSON string `{"role":"user"|"assistant","content":"...","request_id":"..."}`
- **TTL:** 24 hours (reset on each ZADD)

Commands used:
- `ZADD conversation:{sid} {timestamp} {json}` — add message
- `ZRANGE conversation:{sid} 0 -1` — fetch full history in order

---

## 6. PostgreSQL Schema

```sql
CREATE TABLE sessions (
    session_id  TEXT PRIMARY KEY,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE messages (
    id          SERIAL PRIMARY KEY,
    session_id  TEXT REFERENCES sessions(session_id),
    request_id  TEXT,
    role        TEXT CHECK (role IN ('user', 'assistant')),
    content     TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
```

Managed by **Alembic** (`alembic upgrade head`).

---

## 7. Project Structure (Monorepo)

```
event-driven/
├── docker-compose.yml         ← Kafka + Zookeeper + Redis + PostgreSQL
├── alembic.ini
├── alembic/
│   └── versions/
├── pyproject.toml             ← uv project (replaces requirements.txt)
├── uv.lock
│
├── shared/
│   ├── __init__.py
│   ├── schemas.py             ← Pydantic models for Kafka messages
│   └── settings.py            ← pydantic-settings, reads from .env
│
├── app/                       ← FastAPI service
│   ├── main.py
│   ├── dependencies.py        ← get_db(), get_redis(), get_producer()
│   ├── routers/
│   │   └── chat.py            ← POST /chat, GET /chat/stream/{id}, GET /history/{sid}
│   └── services/
│       └── history.py         ← Redis ZRANGE wrapper
│
├── worker/                    ← Mock LLM consumer
│   ├── main.py                ← Kafka consumer entrypoint
│   ├── mock_llm.py            ← Token generator (OpenAI SSE format)
│   └── persistence.py         ← ZADD to Redis + INSERT to PostgreSQL
│
└── cli.py                     ← HTTP client, renders SSE to terminal
```

---

## 8. FastAPI DI Pattern

All infrastructure clients injected via `Depends()`:

```python
# dependencies.py
async def get_db() -> AsyncGenerator[AsyncSession, None]: ...
async def get_redis() -> Redis: ...
async def get_producer() -> Producer: ...  # confluent_kafka.Producer
```

Routers declare dependencies explicitly — no global state.

---

## 9. API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/chat` | Submit message, returns `{request_id}` |
| `GET` | `/chat/stream/{request_id}` | SSE stream of tokens |
| `GET` | `/history/{session_id}` | Full conversation from Redis ZRANGE |

---

## 10. CLI Usage

```bash
# Send a message and stream the response
python cli.py chat --session abc123 "xin chào"

# View conversation history
python cli.py history abc123
```

---

## 11. Infrastructure (docker-compose)

Services: `zookeeper`, `kafka`, `redis`, `postgres`

App and worker run locally:
```bash
uv run uvicorn app.main:app --reload
uv run python worker/main.py
```

---

## 12. Key Libraries

| Purpose | Library |
|---|---|
| FastAPI async | `fastapi`, `uvicorn` |
| Kafka client | `confluent-kafka` |
| Redis | `redis[asyncio]` |
| PostgreSQL ORM | `sqlalchemy[asyncio]`, `asyncpg` |
| Migrations | `alembic` |
| Settings | `pydantic-settings` |
| CLI HTTP client | `httpx` |
