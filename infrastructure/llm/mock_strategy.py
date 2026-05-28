from __future__ import annotations
import asyncio
import random
from typing import AsyncGenerator

from application.interfaces.token_generator import TokenGenerator

_RESPONSES = [
    "Xin chào! Tôi là một AI trợ lý. Tôi có thể giúp gì cho bạn?",
    "Đây là một hệ thống event-driven streaming sử dụng Kafka và FastAPI.",
    "Latency là thời gian để một packet đi từ sender đến receiver.",
    "Throughput là lượng data thực sự được truyền thành công mỗi giây.",
    "Redis sorted sets rất phù hợp để lưu conversation history theo thứ tự thời gian.",
]


class MockLLMStrategy(TokenGenerator):
    async def generate(self, content: str) -> AsyncGenerator[str, None]:
        response = random.choice(_RESPONSES)
        words = response.split()
        for i, word in enumerate(words):
            prefix = "" if i == 0 else " "
            yield prefix + word
            await asyncio.sleep(0.05)
