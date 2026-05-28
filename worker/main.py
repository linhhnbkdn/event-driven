from __future__ import annotations
import asyncio
import logging

from confluent_kafka import Consumer, KafkaError, Producer
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from shared.schemas import ChatRequest, ChatResponse
from shared.settings import settings
from worker.mock_llm import generate_tokens
from worker.persistence import persist_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def _process_request(
    request: ChatRequest,
    producer: Producer,
    redis: Redis,
    session_factory: async_sessionmaker,
) -> None:
    full_response = ""

    async for token in generate_tokens(content=request.content):
        full_response += token
        msg = ChatResponse(
            request_id=request.request_id,
            session_id=request.session_id,
            delta=token,
            finish_reason=None,
        )
        producer.produce(topic="chat.responses", value=msg.model_dump_json())
        producer.poll(0)

    done = ChatResponse(
        request_id=request.request_id,
        session_id=request.session_id,
        delta="",
        finish_reason="stop",
    )
    producer.produce(topic="chat.responses", value=done.model_dump_json())
    producer.flush()

    async with session_factory() as db:
        await persist_message(
            redis=redis,
            db=db,
            session_id=request.session_id,
            role="user",
            content=request.content,
            request_id=request.request_id,
        )
        await persist_message(
            redis=redis,
            db=db,
            session_id=request.session_id,
            role="assistant",
            content=full_response,
            request_id=request.request_id,
        )
    logger.info(f"Persisted conversation for request {request.request_id}")


async def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "llm-worker",
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    redis = Redis.from_url(settings.redis_url)
    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    consumer.subscribe(["chat.requests"])
    logger.info("Worker started — listening on chat.requests")
    loop = asyncio.get_running_loop()

    try:
        while True:
            msg = await loop.run_in_executor(None, consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue
            request = ChatRequest.model_validate_json(msg.value())
            logger.info(f"Received request {request.request_id} for session {request.session_id}")
            await _process_request(
                request=request,
                producer=producer,
                redis=redis,
                session_factory=session_factory,
            )
    finally:
        consumer.close()
        await redis.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
