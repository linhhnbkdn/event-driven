from __future__ import annotations
from abc import ABC, abstractmethod

from domain.entities import Message


class ConversationCache(ABC):
    @abstractmethod
    async def save_message(self, message: Message) -> None: ...

    @abstractmethod
    async def get_history(self, session_id: str) -> list[Message]: ...
