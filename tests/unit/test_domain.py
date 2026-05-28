from __future__ import annotations
import pytest
from dataclasses import FrozenInstanceError
from domain.entities import Message, Session
from domain.value_objects import MessageRole


def test_message_role_user_is_string_user():
    assert MessageRole.USER == "user"

def test_message_role_assistant_is_string_assistant():
    assert MessageRole.ASSISTANT == "assistant"

def test_message_is_frozen():
    msg = Message(session_id="s", request_id="r", role=MessageRole.USER, content="hi")
    with pytest.raises(FrozenInstanceError):
        object.__setattr__(msg, "content", "changed")

def test_message_equality():
    m1 = Message(session_id="s", request_id="r", role=MessageRole.USER, content="hi")
    m2 = Message(session_id="s", request_id="r", role=MessageRole.USER, content="hi")
    assert m1 == m2

def test_session_equality():
    assert Session(session_id="a") == Session(session_id="a")

def test_message_role_from_string():
    assert MessageRole("user") == MessageRole.USER
    assert MessageRole("assistant") == MessageRole.ASSISTANT
