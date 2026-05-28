from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from application.interfaces.message_store import MessageStore
from domain.entities import Message


class PostgresMessageStore(MessageStore):
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory

    async def save_message(self, message: Message) -> None:
        async with self._session_factory() as db:
            await db.execute(
                text("INSERT INTO sessions (session_id) VALUES (:sid) ON CONFLICT DO NOTHING"),
                {"sid": message.session_id},
            )
            await db.execute(
                text(
                    "INSERT INTO messages (session_id, request_id, role, content) "
                    "VALUES (:sid, :rid, :role, :content)"
                ),
                {
                    "sid": message.session_id,
                    "rid": message.request_id,
                    "role": message.role.value,
                    "content": message.content,
                },
            )
            await db.commit()
