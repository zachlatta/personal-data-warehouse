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
