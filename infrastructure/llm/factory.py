from __future__ import annotations

from application.interfaces.token_generator import TokenGenerator
from shared.settings import settings


def create_llm_strategy() -> TokenGenerator:
    match settings.llm_provider:
        case "mock":
            from infrastructure.llm.mock_strategy import MockLLMStrategy
            return MockLLMStrategy()
        case "openai":
            from infrastructure.llm.openai_strategy import OpenAIStrategy
            return OpenAIStrategy(api_key=settings.openai_api_key)
        case _:
            raise ValueError(f"Unknown LLM provider: {settings.llm_provider}")
