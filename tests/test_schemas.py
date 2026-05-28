from shared.schemas import ChatRequest, ChatResponse


def test_chat_request_auto_generates_request_id():
    req = ChatRequest(session_id="sess1", content="hello")
    assert len(req.request_id) == 36
    assert req.request_id.count("-") == 4


def test_chat_request_auto_generates_timestamp():
    req = ChatRequest(session_id="sess1", content="hello")
    assert req.timestamp > 0


def test_chat_request_explicit_fields():
    req = ChatRequest(
        request_id="abc",
        session_id="sess1",
        content="hi",
        timestamp=1000.0,
    )
    assert req.request_id == "abc"
    assert req.timestamp == 1000.0


def test_chat_response_defaults_finish_reason_to_none():
    resp = ChatResponse(
        request_id="abc",
        session_id="sess1",
        delta="hello",
    )
    assert resp.finish_reason is None


def test_chat_response_done_signal():
    resp = ChatResponse(
        request_id="abc",
        session_id="sess1",
        delta="",
        finish_reason="stop",
    )
    assert resp.finish_reason == "stop"
    assert resp.delta == ""


def test_chat_request_round_trips_json():
    req = ChatRequest(session_id="s", content="hi")
    restored = ChatRequest.model_validate_json(req.model_dump_json())
    assert restored.request_id == req.request_id
    assert restored.content == req.content
