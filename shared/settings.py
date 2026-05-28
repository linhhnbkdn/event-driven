from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379"
    database_url: str = "postgresql+asyncpg://app:app@localhost:5432/chatdb"
    redis_ttl: int = 86400
    llm_provider: str = "mock"
    openai_api_key: str = ""


settings = Settings()
