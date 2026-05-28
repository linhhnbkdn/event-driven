from __future__ import annotations
from dataclasses import dataclass

from domain.value_objects import MessageRole


@dataclass(frozen=True)
class Message:
    session_id: str
    request_id: str
    role: MessageRole
    content: str


@dataclass(frozen=True)
class Session:
    session_id: str
