from __future__ import annotations

import pytest

from personal_data_warehouse.chatgpt_backend import ChatGPTAuthError
from personal_data_warehouse.chatgpt_cookies import BrowserProfile, CapturedSession, ChatGPTCookieError
import personal_data_warehouse_chatgpt.cli as cli

_BRAVE = BrowserProfile("brave", "Brave", "BraveSoftware/Brave-Browser", "Brave Safe Storage", "Brave")


class FakeClient:
    def __init__(self, *, session_credential, session, **kwargs):
        self.credential = session_credential

    def fetch_auth_session(self):
        if self.credential == "bad":
            raise ChatGPTAuthError("session expired")
        return {"user": {"email": "user@example.com"}, "accessToken": "tok"}


class FakeIngest:
    def __init__(self):
        self.published = None

    def publish_chatgpt_session(self, *, account, session_token, session_key, source_browser):
        self.published = {
            "account": account,
            "session_token": session_token,
            "session_key": session_key,
            "source_browser": source_browser,
        }
        return {"token_sha256": "deadbeefcafe1234"}


def _patch_common(monkeypatch, *, credential="header", display="Google Chrome"):
    monkeypatch.setattr(cli, "ensure_browser", lambda prefer=None, auto_install=True: _BRAVE)
    monkeypatch.setattr(
        cli,
        "ensure_logged_in",
        lambda profile: CapturedSession(
            browser=display, cookie_header=credential, cookie_count=3, has_session_token=True
        ),
    )
    monkeypatch.setattr(cli, "ChatGPTBackendClient", FakeClient)


def test_publish_session_happy_path(monkeypatch, capsys):
    _patch_common(monkeypatch)
    fake_ingest = FakeIngest()
    monkeypatch.setattr(cli, "ingest_client_from_env", lambda: fake_ingest)

    code = cli.main(["publish-session", "--account", "user@example.com"])
    assert code == 0
    assert fake_ingest.published["account"] == "user@example.com"
    assert fake_ingest.published["session_token"] == "header"
    assert fake_ingest.published["source_browser"] == "Google Chrome"
    out = capsys.readouterr().out
    assert "Signed in as user@example.com" in out
    assert "Published ChatGPT session" in out


def test_publish_session_uses_account_fallback(monkeypatch):
    _patch_common(monkeypatch)
    fake_ingest = FakeIngest()
    monkeypatch.setattr(cli, "ingest_client_from_env", lambda: fake_ingest)
    monkeypatch.delenv("CHATGPT_ACCOUNT", raising=False)
    monkeypatch.delenv("AGENT_SESSIONS_ACCOUNT", raising=False)
    monkeypatch.delenv("APPLE_MESSAGES_ACCOUNT", raising=False)
    monkeypatch.delenv("VOICE_MEMOS_ACCOUNT", raising=False)
    monkeypatch.setenv("GMAIL_ACCOUNTS", "primary@example.com, secondary@example.com")

    code = cli.main(["publish-session"])

    assert code == 0
    assert fake_ingest.published["account"] == "primary@example.com"


def test_dry_run_validates_without_publishing(monkeypatch, capsys):
    _patch_common(monkeypatch)
    called = {"n": 0}

    def _should_not_run():
        called["n"] += 1
        raise AssertionError("must not publish on dry-run")

    monkeypatch.setattr(cli, "ingest_client_from_env", _should_not_run)
    code = cli.main(["publish-session", "--account", "a@b.com", "--dry-run"])
    assert code == 0
    assert called["n"] == 0
    assert "--dry-run: not publishing" in capsys.readouterr().out


def test_rejected_session_exits_nonzero(monkeypatch, capsys):
    _patch_common(monkeypatch, credential="bad")
    monkeypatch.setattr(cli, "ingest_client_from_env", lambda: FakeIngest())
    code = cli.main(["publish-session", "--account", "a@b.com"])
    assert code == 1
    assert "rejected it" in capsys.readouterr().err


def test_missing_account_errors(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.delenv("CHATGPT_ACCOUNT", raising=False)
    monkeypatch.delenv("AGENT_SESSIONS_ACCOUNT", raising=False)
    monkeypatch.delenv("APPLE_MESSAGES_ACCOUNT", raising=False)
    monkeypatch.delenv("VOICE_MEMOS_ACCOUNT", raising=False)
    monkeypatch.delenv("GMAIL_ACCOUNTS", raising=False)
    with pytest.raises(SystemExit):
        cli.main(["publish-session"])


def test_cookie_discovery_failure_exits_nonzero(monkeypatch, capsys):
    monkeypatch.setattr(cli, "ensure_browser", lambda prefer=None, auto_install=True: _BRAVE)

    def _raise(profile):
        raise ChatGPTCookieError("no session found")

    monkeypatch.setattr(cli, "ensure_logged_in", _raise)
    code = cli.main(["publish-session", "--account", "a@b.com"])
    assert code == 1
    assert "no session found" in capsys.readouterr().err
