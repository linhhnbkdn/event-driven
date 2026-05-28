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
    member = json.dumps({"role": role, "content": content, "request_id": request_id,})
    score = time.time()
    await redis.zadd(name=key, mapping={member: score,})
    await redis.expire(name=key, time=settings.redis_ttl,)


async def get_history(redis: Redis, session_id: str) -> list[dict]:
    key = f"conversation:{session_id}"
    raw = await redis.zrange(name=key, start=0, end=-1,)
    return [json.loads(item,) for item in raw]
