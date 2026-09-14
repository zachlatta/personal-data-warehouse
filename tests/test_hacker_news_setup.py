from __future__ import annotations

import json

from personal_data_warehouse import hacker_news_setup as mod
from personal_data_warehouse.hacker_news_session import CapturedHackerNewsSession, HackerNewsSessionCaptureError


class FakeClient:
    def __init__(self, *, ok, **_kw):
        self._ok = ok

    def check_login(self, *, user_id):
        return self._ok


class FakeIngest:
    def __init__(self):
        self.published = []

    def publish_hacker_news_session(self, **kwargs):
        self.published.append(kwargs)
        return {"token_sha256": "deadbeef"}


def _capture(user_id="zachlatta"):
    return CapturedHackerNewsSession(browser="Chrome", cookie_header=f"user={user_id}&s3cret", user_id=user_id)


def test_dry_run_validates_and_never_publishes(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "capture_hacker_news_session", lambda browser=None: _capture())
    monkeypatch.setattr(mod, "HackerNewsClient", lambda session_cookie: FakeClient(ok=True))
    monkeypatch.setattr(mod, "ingest_client_from_env", lambda: (_ for _ in ()).throw(AssertionError("must not publish")))
    monkeypatch.delenv("HACKER_NEWS_ACCOUNT", raising=False)
    assert mod.main(["--dry-run"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["published"] is False
    assert report["validated"] is True
    assert report["account"] == "zachlatta"
    assert "s3cret" not in json.dumps(report)


def test_a_dead_cookie_is_refused_before_publishing(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "capture_hacker_news_session", lambda browser=None: _capture())
    monkeypatch.setattr(mod, "HackerNewsClient", lambda session_cookie: FakeClient(ok=False))
    assert mod.main([]) == 1
    assert "login page" in capsys.readouterr().err


def test_publish_sends_the_cookie_under_the_hn_username(monkeypatch, capsys) -> None:
    ingest = FakeIngest()
    monkeypatch.setattr(mod, "capture_hacker_news_session", lambda browser=None: _capture())
    monkeypatch.setattr(mod, "HackerNewsClient", lambda session_cookie: FakeClient(ok=True))
    monkeypatch.setattr(mod, "ingest_client_from_env", lambda: ingest)
    assert mod.main([]) == 0
    assert ingest.published == [
        {"account": "zachlatta", "session_token": "user=zachlatta&s3cret", "session_key": "default", "source_browser": "Chrome"}
    ]
    assert json.loads(capsys.readouterr().out)["published"] is True


def test_publishing_another_users_cookie_under_this_account_is_refused(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "capture_hacker_news_session", lambda browser=None: _capture(user_id="someoneelse"))
    assert mod.main(["--account", "zachlatta", "--skip-check"]) == 2
    assert "someoneelse" in capsys.readouterr().err


def test_no_login_in_any_browser_exits_2(monkeypatch, capsys) -> None:
    def boom(browser=None):
        raise HackerNewsSessionCaptureError("nothing")

    monkeypatch.setattr(mod, "capture_hacker_news_session", boom)
    assert mod.main([]) == 2
    assert "nothing" in capsys.readouterr().err
