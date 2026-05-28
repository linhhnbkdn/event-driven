from __future__ import annotations
from abc import ABC, abstractmethod

from shared.schemas import ChatCompleted, ChatRequest, ChatResponse


class EventPublisher(ABC):
    @abstractmethod
    async def publish_request(self, request: ChatRequest) -> None: ...

    @abstractmethod
    async def publish_response(self, response: ChatResponse) -> None: ...

    @abstractmethod
    async def publish_completed(self, completed: ChatCompleted) -> None: ...

    @abstractmethod
    def flush(self) -> None: ...
