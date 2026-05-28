from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from application.interfaces.message_store import MessageStore
from domain.entities import Message
from domain.value_objects import MessageRole


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

    async def get_history(self, session_id: str) -> list[Message]:
        async with self._session_factory() as db:
            result = await db.execute(
                text(
                    "SELECT request_id, role, content FROM messages "
                    "WHERE session_id = :sid ORDER BY id ASC"
                ),
                {"sid": session_id},
            )
            return [
                Message(
                    session_id=session_id,
                    request_id=row.request_id,
                    role=MessageRole(row.role),
                    content=row.content,
                )
                for row in result.fetchall()
            ]
