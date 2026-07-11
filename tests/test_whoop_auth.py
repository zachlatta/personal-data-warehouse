from __future__ import annotations

import pytest

from personal_data_warehouse.config import WhoopConfig
from personal_data_warehouse.whoop_auth import (
    WhoopOAuthError,
    authorization_code_from_callback_url,
    authorization_url,
    refresh_whoop_token,
)


def _config() -> WhoopConfig:
    return WhoopConfig(
        account="test@example.com",
        token_json="{}",
        client_id="client-id",
        client_secret="client-secret",
        redirect_uri="http://localhost:8080/callback",
    )


def test_authorization_url_contains_read_only_scopes_and_state() -> None:
    url = authorization_url(config=_config(), state="12345678")

    assert "client_id=client-id" in url
    assert "state=12345678" in url
    assert "read%3Aprofile" in url
    assert "read%3Aworkout" in url
    assert "offline" in url


def test_refresh_whoop_token_requests_offline_scope_and_returns_rotated_token(monkeypatch) -> None:
    captured = {}

    class Response:
        status_code = 200

        def json(self):
            return {
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 3600,
                "scope": "offline read:profile",
            }

    def post(url, *, data, timeout):
        captured.update({"url": url, "data": data, "timeout": timeout})
        return Response()

    monkeypatch.setattr("personal_data_warehouse.whoop_auth.requests.post", post)

    token = refresh_whoop_token(
        {"access_token": "old-access", "refresh_token": "old-refresh"},
        client_id="client-id",
        client_secret="client-secret",
        token_url="https://whoop.example/token",
        timeout=12,
    )

    assert captured["data"] == {
        "grant_type": "refresh_token",
        "refresh_token": "old-refresh",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "scope": "offline",
    }
    assert captured["timeout"] == 12
    assert token["access_token"] == "new-access"
    assert token["refresh_token"] == "new-refresh"
    assert token["expires_at"] > 0


def test_refresh_whoop_token_requires_rotated_refresh_token(monkeypatch) -> None:
    class Response:
        status_code = 200

        def json(self):
            return {"access_token": "new-access", "expires_in": 3600}

    monkeypatch.setattr("personal_data_warehouse.whoop_auth.requests.post", lambda *_args, **_kwargs: Response())

    with pytest.raises(WhoopOAuthError, match="rotated refresh_token"):
        refresh_whoop_token(
            {"refresh_token": "old-refresh"},
            client_id="client-id",
            client_secret="client-secret",
            token_url="https://whoop.example/token",
        )


def test_authorization_code_from_callback_url_validates_state_and_redirect() -> None:
    callback = "http://localhost:8080/callback?code=one-time-code&state=12345678"

    assert authorization_code_from_callback_url(
        callback,
        config=_config(),
        expected_state="12345678",
    ) == "one-time-code"

    with pytest.raises(WhoopOAuthError, match="state mismatch"):
        authorization_code_from_callback_url(
            callback,
            config=_config(),
            expected_state="different",
        )

    with pytest.raises(WhoopOAuthError, match="does not match"):
        authorization_code_from_callback_url(
            "http://localhost:9999/callback?code=one-time-code&state=12345678",
            config=_config(),
            expected_state="12345678",
        )
