from __future__ import annotations
from abc import ABC, abstractmethod

from shared.schemas import ChatRequest, ChatResponse


class EventPublisher(ABC):
    @abstractmethod
    async def publish_request(self, request: ChatRequest) -> None: ...

    @abstractmethod
    async def publish_response(self, response: ChatResponse) -> None: ...

    @abstractmethod
    def flush(self) -> None: ...
