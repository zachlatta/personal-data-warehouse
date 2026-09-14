from __future__ import annotations

import json

import pytest

from personal_data_warehouse import hacker_news_session as mod
from personal_data_warehouse.hacker_news_session import (
    HackerNewsSessionCaptureError,
    capture_hacker_news_session,
    user_id_from_cookie,
)


class FakeProfile:
    def __init__(self, key: str, display_name: str) -> None:
        self.key = key
        self.display_name = display_name


def install_fake_browser(monkeypatch, cookies_by_browser: dict[str, dict[str, str]]) -> None:
    profiles = [FakeProfile(key, key.title()) for key in cookies_by_browser]
    monkeypatch.setattr(mod, "BROWSERS", tuple(profiles))
    monkeypatch.setattr(mod, "browser_by_key", lambda key: next((p for p in profiles if p.key == key), None))
    monkeypatch.setattr(mod, "_cookie_dbs", lambda profile: [f"/fake/{profile.key}/Cookies"])
    monkeypatch.setattr(mod, "_safe_storage_key", lambda profile: b"key")
    monkeypatch.setattr(
        mod, "read_cookies_for_host", lambda db, key, host: cookies_by_browser[str(db).split("/")[2]]
    )


def test_captures_the_user_cookie_and_reads_the_username_out_of_it(monkeypatch) -> None:
    install_fake_browser(monkeypatch, {"chrome": {"user": "zachlatta&s3cret"}})
    captured = capture_hacker_news_session()
    assert captured.user_id == "zachlatta"
    assert captured.cookie_header == "user=zachlatta&s3cret"
    assert captured.browser == "Chrome"


def test_a_browser_without_the_login_is_skipped_for_one_that_has_it(monkeypatch) -> None:
    install_fake_browser(monkeypatch, {"chrome": {}, "brave": {"user": "zachlatta&tok"}})
    assert capture_hacker_news_session().browser == "Brave"


def test_no_login_anywhere_says_what_to_do(monkeypatch) -> None:
    install_fake_browser(monkeypatch, {"chrome": {}})
    with pytest.raises(HackerNewsSessionCaptureError, match="news.ycombinator.com"):
        capture_hacker_news_session()


def test_an_unknown_browser_lists_the_valid_ones(monkeypatch) -> None:
    install_fake_browser(monkeypatch, {"chrome": {}})
    with pytest.raises(HackerNewsSessionCaptureError, match="chrome"):
        capture_hacker_news_session(browser="netscape")


def test_the_redacted_report_never_carries_the_token(monkeypatch) -> None:
    install_fake_browser(monkeypatch, {"chrome": {"user": "zachlatta&super-secret"}})
    report = json.dumps(capture_hacker_news_session().redacted())
    assert "super-secret" not in report
    assert "token_sha256" in report


def test_user_id_from_cookie() -> None:
    assert user_id_from_cookie("zachlatta&abc") == "zachlatta"
    assert user_id_from_cookie("") == ""
