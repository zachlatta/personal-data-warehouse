from __future__ import annotations

import pytest

from personal_data_warehouse.chatgpt_backend import ChatGPTAuthError
from personal_data_warehouse.chatgpt_cookies import BrowserProfile, CapturedSession, ChatGPTCookieError
import personal_data_warehouse_chatgpt.cli as cli

_BRAVE = BrowserProfile("brave", "Brave", "BraveSoftware/Brave-Browser", "Brave Safe Storage", "Brave")


class FakeClient:
    #: Epoch the fake's token dies; tests override to exercise the warning.
    expiry = 0.0

    def __init__(self, *, session_credential, session=None, **kwargs):
        self.credential = session_credential
        self.access_token_expiry = 0.0

    def fetch_auth_session(self):
        if self.credential == "bad":
            raise ChatGPTAuthError("session expired")
        self.access_token_expiry = type(self).expiry
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


def test_publish_session_reports_a_near_expiry_session(monkeypatch, capsys):
    # The hourly LaunchAgent republishes the same browser session all week, so
    # the only useful early warning is the token's own exp: say it out loud
    # while there is still time to sign in again.
    import time

    _patch_common(monkeypatch)
    monkeypatch.setattr(cli, "ingest_client_from_env", lambda: FakeIngest())
    monkeypatch.setattr(FakeClient, "expiry", time.time() + 6 * 3600)

    code = cli.main(["publish-session", "--account", "a@b.com"])

    assert code == 0  # publishing a short-lived session still beats no session
    captured = capsys.readouterr()
    assert "expires in" in captured.err
    assert "Published ChatGPT session" in captured.out


def test_non_interactive_never_installs_or_prompts(monkeypatch, capsys):
    # Under launchd there is no one to answer a prompt and no reason to install
    # a browser; a missing session must fail fast instead of hanging the agent.
    seen = {}

    def _ensure_browser(prefer=None, auto_install=True):
        seen["auto_install"] = auto_install
        return _BRAVE

    def _discover(*, browser=None):
        seen["browser"] = browser
        raise ChatGPTCookieError("no session found")

    monkeypatch.setattr(cli, "ensure_browser", _ensure_browser)
    monkeypatch.setattr(cli, "discover_chatgpt_session", _discover)
    monkeypatch.setattr(
        cli, "ensure_logged_in", lambda profile: pytest.fail("must not prompt in non-interactive mode")
    )

    code = cli.main(["publish-session", "--account", "a@b.com", "--non-interactive"])

    assert code == 1
    assert seen == {"auto_install": False, "browser": "brave"}
    assert "no session found" in capsys.readouterr().err
