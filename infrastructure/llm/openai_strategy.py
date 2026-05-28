from __future__ import annotations
from typing import AsyncGenerator

from application.interfaces.token_generator import TokenGenerator


class OpenAIStrategy(TokenGenerator):
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    async def generate(self, content: str) -> AsyncGenerator[str, None]:
        raise NotImplementedError("OpenAI integration not yet implemented")
        yield  # make this an async generator
