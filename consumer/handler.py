from __future__ import annotations
import logging

from application.use_cases.process_chat_request import ProcessChatRequestUseCase
from shared.schemas import ChatRequest

logger = logging.getLogger(__name__)


class ChatRequestHandler:
    def __init__(self, use_case: ProcessChatRequestUseCase) -> None:
        self._use_case = use_case

    async def handle(self, raw_value: bytes) -> None:
        request = ChatRequest.model_validate_json(raw_value)
        logger.info(f"Handling request {request.request_id} for session {request.session_id}")
        await self._use_case.execute(request=request)
