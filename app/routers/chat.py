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
