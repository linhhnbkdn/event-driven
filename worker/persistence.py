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
