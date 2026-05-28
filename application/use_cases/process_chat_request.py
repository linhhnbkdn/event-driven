from __future__ import annotations

from application.interfaces.conversation_cache import ConversationCache
from application.interfaces.event_publisher import EventPublisher
from application.interfaces.token_generator import TokenGenerator
from domain.entities import Message
from domain.value_objects import MessageRole
from shared.schemas import ChatCompleted, ChatRequest, ChatResponse


class ProcessChatRequestUseCase:
    def __init__(
        self,
        generator: TokenGenerator,
        publisher: EventPublisher,
        cache: ConversationCache,
    ) -> None:
        self._generator = generator
        self._publisher = publisher
        self._cache = cache

    async def execute(self, request: ChatRequest) -> None:
        full_response = await self._stream_tokens(request=request)
        self._publisher.flush()
        await self._cache_messages(request=request, full_response=full_response)
        await self._publisher.publish_completed(
            completed=ChatCompleted(
                session_id=request.session_id,
                request_id=request.request_id,
            ),
        )

    async def _stream_tokens(self, request: ChatRequest) -> str:
        full_response = ""
        async for token in self._generator.generate(content=request.content):
            full_response += token
            await self._publisher.publish_response(
                response=ChatResponse(
                    request_id=request.request_id,
                    session_id=request.session_id,
                    delta=token,
                    finish_reason=None,
                ),
            )
        await self._publisher.publish_response(
            response=ChatResponse(
                request_id=request.request_id,
                session_id=request.session_id,
                delta="",
                finish_reason="stop",
            ),
        )
        return full_response

    async def _cache_messages(self, request: ChatRequest, full_response: str) -> None:
        await self._cache.save_message(
            message=Message(
                session_id=request.session_id,
                request_id=request.request_id,
                role=MessageRole.USER,
                content=request.content,
            ),
        )
        await self._cache.save_message(
            message=Message(
                session_id=request.session_id,
                request_id=request.request_id,
                role=MessageRole.ASSISTANT,
                content=full_response,
            ),
        )
