from __future__ import annotations
import time
import uuid
from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    session_id: str
    content: str
    timestamp: float = Field(default_factory=time.time)


class ChatResponse(BaseModel):
    request_id: str
    session_id: str
    delta: str
    finish_reason: str | None = None


class ChatCompleted(BaseModel):
    session_id: str
    request_id: str
