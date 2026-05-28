from __future__ import annotations
from dataclasses import FrozenInstanceError

from domain.value_objects import MessageRole


class FrozenDescriptor:
    """Descriptor that prevents attribute assignment using object.__setattr__."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.private_name = f'__{name}'

    def __get__(self, obj: object, objtype: type | None = None) -> object:
        if obj is None:
            return self
        return object.__getattribute__(obj, self.private_name)

    def __set__(self, obj: object, value: object) -> None:
        raise FrozenInstanceError(f"cannot assign to field {self.name!r}")

    def __delete__(self, obj: object) -> None:
        raise FrozenInstanceError(f"cannot delete field {self.name!r}")


class Message:
    """Immutable message entity with frozen attribute access."""

    session_id = FrozenDescriptor('session_id')
    request_id = FrozenDescriptor('request_id')
    role = FrozenDescriptor('role')
    content = FrozenDescriptor('content')

    def __init__(
        self,
        session_id: str,
        request_id: str,
        role: MessageRole,
        content: str,
    ) -> None:
        object.__setattr__(self, '__session_id', session_id,)
        object.__setattr__(self, '__request_id', request_id,)
        object.__setattr__(self, '__role', role,)
        object.__setattr__(self, '__content', content,)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Message):
            return False
        return (
            self.session_id == other.session_id
            and self.request_id == other.request_id
            and self.role == other.role
            and self.content == other.content
        )

    def __hash__(self) -> int:
        return hash((self.session_id, self.request_id, self.role, self.content,))

    def __repr__(self) -> str:
        return (
            f"Message("
            f"session_id={self.session_id!r}, "
            f"request_id={self.request_id!r}, "
            f"role={self.role!r}, "
            f"content={self.content!r}"
            f")"
        )


class Session:
    """Immutable session entity with frozen attribute access."""

    session_id = FrozenDescriptor('session_id')

    def __init__(self, session_id: str,) -> None:
        object.__setattr__(self, '__session_id', session_id,)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Session):
            return False
        return self.session_id == other.session_id

    def __hash__(self) -> int:
        return hash((self.session_id,))

    def __repr__(self) -> str:
        return f"Session(session_id={self.session_id!r})"
