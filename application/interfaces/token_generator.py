from __future__ import annotations
from abc import ABC, abstractmethod
from typing import AsyncGenerator


class TokenGenerator(ABC):
    @abstractmethod
    async def generate(self, content: str) -> AsyncGenerator[str, None]: ...
