"""Tests for capturing the app.whoop.com browser session."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import base64
import json

import pytest

from personal_data_warehouse import whoop_private_session as mod
from personal_data_warehouse.whoop_private_session import (
    ACCESS_TOKEN_COOKIE,
    REFRESH_TOKEN_COOKIE,
    WhoopSessionCaptureError,
    access_token_expiry,
    capture_whoop_session,
)

NOW = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)


def jwt_expiring_at(expires: datetime) -> str:
    claims = base64.urlsafe_b64encode(json.dumps({"exp": int(expires.timestamp())}).encode()).decode().rstrip("=")
    return f"header.{claims}.signature"


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
        mod,
        "read_cookies_for_host",
        lambda db, key, host: cookies_by_browser[str(db).split("/")[2]],
    )


def test_captures_both_tokens_and_reads_the_jwt_expiry(monkeypatch) -> None:
    expires = NOW + timedelta(hours=24)
    install_fake_browser(monkeypatch, {"chrome": {ACCESS_TOKEN_COOKIE: jwt_expiring_at(expires), REFRESH_TOKEN_COOKIE: "refresh"}})

    captured = capture_whoop_session(now=NOW)

    assert captured.refresh_token == "refresh"
    assert captured.access_expires_at == expires
    # The refresh token is opaque, so its 30-day life is assumed until the
    # first refresh reports the real number.
    assert captured.refresh_expires_at == NOW + timedelta(days=30)


def test_a_malformed_access_token_falls_back_to_24h_rather_than_failing() -> None:
    assert access_token_expiry("not-a-jwt", now=NOW) == NOW + timedelta(hours=24)


def test_capture_skips_a_browser_holding_only_half_a_session(monkeypatch) -> None:
    """A stale profile with one cookie must not shadow a good one."""
    install_fake_browser(
        monkeypatch,
        {
            "chrome": {ACCESS_TOKEN_COOKIE: jwt_expiring_at(NOW)},
            "brave": {ACCESS_TOKEN_COOKIE: jwt_expiring_at(NOW), REFRESH_TOKEN_COOKIE: "refresh"},
        },
    )

    captured = capture_whoop_session(now=NOW)

    assert captured.browser == "Brave"


def test_no_session_anywhere_says_what_to_do(monkeypatch) -> None:
    install_fake_browser(monkeypatch, {"chrome": {}})

    with pytest.raises(WhoopSessionCaptureError, match="app.whoop.com"):
        capture_whoop_session(now=NOW)


def test_an_unknown_browser_lists_the_valid_ones(monkeypatch) -> None:
    install_fake_browser(monkeypatch, {"chrome": {}})

    with pytest.raises(WhoopSessionCaptureError, match="chrome"):
        capture_whoop_session(browser="netscape", now=NOW)


def test_the_redacted_report_never_carries_a_token(monkeypatch) -> None:
    install_fake_browser(
        monkeypatch,
        {"chrome": {ACCESS_TOKEN_COOKIE: jwt_expiring_at(NOW), REFRESH_TOKEN_COOKIE: "super-secret-refresh"}},
    )

    report = json.dumps(capture_whoop_session(now=NOW).redacted())

    assert "super-secret-refresh" not in report
    assert "refresh_token_sha256" in report


def test_the_fingerprint_tracks_the_refresh_token(monkeypatch) -> None:
    """The sync uses this to tell a replacement credential from a rejected one."""
    install_fake_browser(monkeypatch, {"chrome": {ACCESS_TOKEN_COOKIE: jwt_expiring_at(NOW), REFRESH_TOKEN_COOKIE: "a"}})
    first = capture_whoop_session(now=NOW).fingerprint()
    install_fake_browser(monkeypatch, {"chrome": {ACCESS_TOKEN_COOKIE: jwt_expiring_at(NOW), REFRESH_TOKEN_COOKIE: "b"}})
    second = capture_whoop_session(now=NOW).fingerprint()

    assert first != second


def test_publish_uses_the_real_ingest_client_factory() -> None:
    """`--dry-run` never constructs the client, so nothing caught a bad factory.

    The first real publish failed with AttributeError because the CLI called
    `IngestClient.from_env()`, which does not exist -- the module-level factory
    is `ingest_client_from_env`. This asserts the name the CLI actually binds.
    """
    from personal_data_warehouse import whoop_private_setup

    assert hasattr(whoop_private_setup, "ingest_client_from_env")
    assert callable(whoop_private_setup.ingest_client_from_env)
