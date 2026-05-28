from __future__ import annotations
import asyncio
import json

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api import state
from api.dependencies import get_history_use_case, get_send_message_use_case
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
    use_case: GetHistoryUseCase = Depends(get_history_use_case),
) -> list[dict]:
    messages = await use_case.execute(session_id=session_id)
    return [
        {"role": m.role.value, "content": m.content, "request_id": m.request_id}
        for m in messages
    ]
