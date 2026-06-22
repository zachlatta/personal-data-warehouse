from __future__ import annotations

import pytest

from personal_data_warehouse_claude_desktop.api import (
    ClaudeAiApiError,
    ClaudeAiClient,
    ClaudeAiRateLimitError,
)


class FakeResponse:
    def __init__(self, status_code: int, payload=None, text: str = "", headers=None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = headers or {}

    def json(self):
        return self._payload


class FakeSession:
    """Returns queued responses and records each GET (url, params)."""

    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = responses
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, headers=None, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        return self._responses.pop(0)


def _client(session: FakeSession, **kwargs) -> ClaudeAiClient:
    return ClaudeAiClient(
        cookie_header="sessionKey=sk-test",
        org_id="org-1",
        session=session,
        sleep=lambda _s: None,
        **kwargs,
    )


def test_iter_conversations_pages_until_short_page() -> None:
    page1 = [{"uuid": f"c{i}", "updated_at": "2026-06-01T00:00:00Z"} for i in range(3)]
    page2 = [{"uuid": "c3", "updated_at": "2026-06-02T00:00:00Z"}]  # short -> stop
    session = FakeSession([FakeResponse(200, page1), FakeResponse(200, page2)])
    client = _client(session)

    convs = list(client.iter_conversations(page_size=3))

    assert [c["uuid"] for c in convs] == ["c0", "c1", "c2", "c3"]
    # offsets advanced: 0 then 3
    assert [params["offset"] for _, params in session.calls] == ["0", "3"]
    assert all(params["limit"] == "3" for _, params in session.calls)


def test_iter_conversations_stops_on_empty_page() -> None:
    full = [{"uuid": f"c{i}"} for i in range(2)]
    session = FakeSession([FakeResponse(200, full), FakeResponse(200, [])])
    client = _client(session, )
    convs = list(client.iter_conversations(page_size=2))
    assert [c["uuid"] for c in convs] == ["c0", "c1"]


def test_get_conversation_returns_tree() -> None:
    conv = {"uuid": "c1", "name": "Hello", "chat_messages": [{"uuid": "m1"}]}
    session = FakeSession([FakeResponse(200, conv)])
    client = _client(session)
    result = client.get_conversation("c1")
    assert result["name"] == "Hello"
    # asked for the full tree with messages rendering
    _, params = session.calls[0]
    assert params["tree"] == "True"
    assert params["rendering_mode"] == "messages"


def test_auth_failure_raises_clear_error_without_retry() -> None:
    session = FakeSession([FakeResponse(403, text="forbidden")])
    client = _client(session)
    with pytest.raises(ClaudeAiApiError) as exc:
        client.get_conversation("c1")
    assert "expired" in str(exc.value) or "403" in str(exc.value)
    assert len(session.calls) == 1  # not retried


def test_server_error_is_retried_then_succeeds() -> None:
    conv = {"uuid": "c1"}
    session = FakeSession([FakeResponse(500), FakeResponse(200, conv)])
    client = _client(session, max_retries=3)
    result = client.get_conversation("c1")
    assert result["uuid"] == "c1"
    assert len(session.calls) == 2


def test_rate_limit_raises_without_retry() -> None:
    # A 429 is throttling, not a transient blip: surface it immediately (like the
    # ChatGPT backend client) so the poller defers rather than hammering it.
    session = FakeSession(
        [FakeResponse(429, headers={"Retry-After": "12"}), FakeResponse(200, {"uuid": "c1"})]
    )
    client = _client(session, max_retries=3)
    with pytest.raises(ClaudeAiRateLimitError) as exc:
        client.get_conversation("c1")
    assert exc.value.retry_after_seconds == 12.0
    assert len(session.calls) == 1  # not retried
    # It is also a ClaudeAiApiError so existing broad handlers still catch it.
    assert isinstance(exc.value, ClaudeAiApiError)


def test_rate_limit_without_retry_after_header() -> None:
    session = FakeSession([FakeResponse(429)])
    client = _client(session)
    with pytest.raises(ClaudeAiRateLimitError) as exc:
        client.get_conversation("c1")
    assert exc.value.retry_after_seconds is None
