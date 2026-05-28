from __future__ import annotations
from abc import ABC, abstractmethod

from domain.entities import Message


class MessageStore(ABC):
    @abstractmethod
    async def save_message(self, message: Message) -> None: ...
