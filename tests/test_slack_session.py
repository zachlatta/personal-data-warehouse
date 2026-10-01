"""The server-side half of the pasted Slack web session, used for Slack writes."""

from __future__ import annotations


def test_the_session_helper_puts_the_given_user_agent_on_the_wire(monkeypatch):
    import urllib.request

    from personal_data_warehouse import slack_session

    sent = {}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b'{"ok": true}'

    def fake_urlopen(request, timeout):
        sent["ua"] = request.get_header("User-agent")
        sent["cookie"] = request.get_header("Cookie")
        return _Response()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert slack_session._slack_post(
        "auth.test", token="xoxc-t", cookie_header="d=xoxd-c", user_agent="Mozilla/5.0 Chrome/140"
    ) == {"ok": True}
    assert sent == {"ua": "Mozilla/5.0 Chrome/140", "cookie": "d=xoxd-c"}


class _Response:
    def __init__(self, body: bytes):
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self._body


def test_the_session_client_polls_with_the_pasted_session(monkeypatch):
    """Zach's choice (2026-10-01): poll with his pasted web session, which Slack
    rate-limits far less than the workspace OAuth token. It sends the token, the
    `d` cookie and the browser's own User-Agent, nothing else."""
    import json
    import urllib.parse
    import urllib.request

    from personal_data_warehouse.slack_sync import SlackSessionApiClient

    sent = {}

    def fake_urlopen(request, timeout):
        sent["url"] = request.full_url
        sent["form"] = dict(urllib.parse.parse_qsl(request.data.decode()))
        sent["cookie"] = request.get_header("Cookie")
        sent["ua"] = request.get_header("User-agent")
        return _Response(json.dumps({"ok": True, "messages": []}).encode())

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    client = SlackSessionApiClient(token="xoxc-t", cookie="xoxd-c", user_agent="Mozilla/5.0 Chrome/140")
    assert client.call("conversations.history", channel="D1", limit=200, inclusive=True) == {
        "ok": True, "messages": []
    }
    assert sent["url"] == "https://slack.com/api/conversations.history"
    assert sent["form"] == {"token": "xoxc-t", "channel": "D1", "limit": "200", "inclusive": "true"}
    assert sent["cookie"] == "d=xoxd-c" and sent["ua"] == "Mozilla/5.0 Chrome/140"


def test_the_session_client_raises_the_sync_runners_own_errors(monkeypatch):
    import io
    import json
    import urllib.error
    import urllib.request

    import pytest

    from personal_data_warehouse.slack_sync import (
        SlackApiCallError,
        SlackRateLimitedError,
        SlackSessionApiClient,
    )

    responses = []

    def fake_urlopen(request, timeout):
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return _Response(item)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    client = SlackSessionApiClient(token="xoxc-t", cookie="xoxd-c", user_agent="UA")

    responses.append(urllib.error.HTTPError("u", 429, "rate", {"Retry-After": "7"}, io.BytesIO(b"")))
    with pytest.raises(SlackRateLimitedError) as limited:
        client.call("conversations.history", channel="D1")
    assert limited.value.retry_after == 7

    responses.append(json.dumps({"ok": False, "error": "invalid_auth"}).encode())
    with pytest.raises(SlackApiCallError) as failed:
        client.call("auth.test")
    assert failed.value.code == "invalid_auth"


def test_a_rejected_session_falls_back_to_the_oauth_token_for_the_rest_of_the_pass():
    """A dead or signed-out session must cost speed, never the sync."""
    import pytest

    from personal_data_warehouse.slack_sync import (
        SlackApiCallError,
        SlackRateLimitedError,
        SlackSessionFallbackClient,
    )

    calls = []

    class _Primary:
        def call(self, method, **params):
            calls.append(("session", method))
            if method == "auth.test":
                raise SlackApiCallError("auth.test failed: invalid_auth", code="invalid_auth")
            raise AssertionError("the session must not be used again after it was rejected")

    class _Fallback:
        def call(self, method, **params):
            calls.append(("oauth", method))
            return {"ok": True}

    warnings = []

    class _Log:
        def warning(self, *a, **k):
            warnings.append(a)

    client = SlackSessionFallbackClient(_Primary(), _Fallback(), logger=_Log())
    assert client.call("auth.test") == {"ok": True}
    assert client.call("conversations.history", channel="D1") == {"ok": True}
    assert calls == [("session", "auth.test"), ("oauth", "auth.test"), ("oauth", "conversations.history")]
    assert warnings

    class _Limited:
        def call(self, method, **params):
            raise SlackRateLimitedError(retry_after=3)

    with pytest.raises(SlackRateLimitedError):
        SlackSessionFallbackClient(_Limited(), _Fallback(), logger=_Log()).call("conversations.history")
