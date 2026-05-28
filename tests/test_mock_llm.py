from worker.mock_llm import generate_tokens


async def test_generate_tokens_yields_non_empty_strings():
    tokens = []
    async for token in generate_tokens(content="test input"):
        tokens.append(token)
    assert len(tokens) > 0
    assert all(isinstance(t, str) and len(t) > 0 for t in tokens)


async def test_full_response_is_non_empty():
    text = ""
    async for token in generate_tokens(content="anything"):
        text += token
    assert len(text.strip()) > 0


async def test_different_calls_may_vary():
    results = set()
    for _ in range(5):
        text = ""
        async for token in generate_tokens(content="hi"):
            text += token
        results.add(text)
    assert len(results) >= 1
