from __future__ import annotations

from application.interfaces.conversation_cache import ConversationCache
from domain.entities import Message


class GetHistoryUseCase:
    def __init__(self, cache: ConversationCache) -> None:
        self._cache = cache

    async def execute(self, session_id: str) -> list[Message]:
        return await self._cache.get_history(session_id=session_id)
