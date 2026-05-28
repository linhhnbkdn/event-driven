from __future__ import annotations

from application.interfaces.event_publisher import EventPublisher
from shared.schemas import ChatRequest


class SendMessageUseCase:
    def __init__(self, publisher: EventPublisher) -> None:
        self._publisher = publisher

    async def execute(self, session_id: str, content: str) -> str:
        request = ChatRequest(session_id=session_id, content=content)
        await self._publisher.publish_request(request=request)
        return request.request_id
