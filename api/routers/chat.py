from __future__ import annotations
import json

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from redis.asyncio import Redis

from api.dependencies import get_history_use_case, get_message_store, get_redis, get_send_message_use_case
from application.interfaces.message_store import MessageStore
from application.use_cases.get_history import GetHistoryUseCase
from application.use_cases.send_message import SendMessageUseCase

router = APIRouter()


class ChatBody(BaseModel):
    session_id: str
    content: str


@router.post("/chat")
async def post_chat(
    body: ChatBody,
    use_case: SendMessageUseCase = Depends(get_send_message_use_case),
) -> dict:
    request_id = await use_case.execute(
        session_id=body.session_id,
        content=body.content,
    )
    return {"request_id": request_id}


@router.get("/chat/stream/{request_id}")
async def stream_response(
    request_id: str,
    redis: Redis = Depends(get_redis),
) -> StreamingResponse:
    async def event_generator():
        stream_key = f"stream:{request_id}"
        last_id = "0"
        while True:
            results = await redis.xread({stream_key: last_id}, block=30000, count=100)
            if not results:
                yield "data: [DONE]\n\n"
                return
            for _stream, messages in results:
                for msg_id, fields in messages:
                    last_id = msg_id
                    finish_reason = fields.get(b"finish_reason", b"").decode()
                    if finish_reason == "stop":
                        yield "data: [DONE]\n\n"
                        return
                    delta = fields.get(b"delta", b"").decode()
                    payload = json.dumps({
                        "choices": [{"delta": {"content": delta}, "finish_reason": None}],
                    })
                    yield f"data: {payload}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.get("/history/{session_id}")
async def get_conversation_history(
    session_id: str,
    use_case: GetHistoryUseCase = Depends(get_history_use_case),
) -> list[dict]:
    messages = await use_case.execute(session_id=session_id)
    return [
        {"role": m.role.value, "content": m.content, "request_id": m.request_id}
        for m in messages
    ]


@router.get("/history/{session_id}/db")
async def get_conversation_history_from_db(
    session_id: str,
    store: MessageStore = Depends(get_message_store),
) -> list[dict]:
    messages = await store.get_history(session_id=session_id)
    return [
        {"role": m.role.value, "content": m.content, "request_id": m.request_id}
        for m in messages
    ]
