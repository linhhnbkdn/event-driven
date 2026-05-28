from __future__ import annotations
import logging

from application.use_cases.persist_session import PersistSessionUseCase
from shared.schemas import ChatCompleted

logger = logging.getLogger(__name__)


class PersistenceHandler:
    def __init__(self, use_case: PersistSessionUseCase) -> None:
        self._use_case = use_case

    async def handle(self, raw_value: bytes) -> None:
        completed = ChatCompleted.model_validate_json(raw_value)
        logger.info(f"Persisting session={completed.session_id} request={completed.request_id}")
        await self._use_case.execute(completed=completed)
