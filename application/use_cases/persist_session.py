from __future__ import annotations

from application.interfaces.conversation_cache import ConversationCache
from application.interfaces.message_store import MessageStore
from shared.schemas import ChatCompleted


class PersistSessionUseCase:
    def __init__(
        self,
        cache: ConversationCache,
        store: MessageStore,
    ) -> None:
        self._cache = cache
        self._store = store

    async def execute(self, completed: ChatCompleted) -> None:
        messages = await self._cache.get_history(session_id=completed.session_id)
        new_messages = [m for m in messages if m.request_id == completed.request_id]
        for message in new_messages:
            await self._store.save_message(message=message)
