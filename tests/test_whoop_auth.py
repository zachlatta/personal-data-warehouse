from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from personal_data_warehouse.config import WhoopConfig
from personal_data_warehouse.whoop_auth import (
    WhoopOAuthError,
    authorization_code_from_callback_url,
    authorization_url,
    install_whoop_token,
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


def test_refresh_whoop_token_carries_status_code_on_http_error(monkeypatch) -> None:
    # The status code is how callers distinguish a dead refresh token (4xx,
    # needs a manual re-auth) from a transient token-endpoint outage (5xx).
    class Response:
        status_code = 400

        def json(self):
            return {"error": "invalid_grant"}

    monkeypatch.setattr("personal_data_warehouse.whoop_auth.requests.post", lambda *a, **k: Response())

    with pytest.raises(WhoopOAuthError, match="HTTP 400") as excinfo:
        refresh_whoop_token(
            {"access_token": "old-access", "refresh_token": "old-refresh"},
            client_id="client-id",
            client_secret="client-secret",
            token_url="https://whoop.example/token",
            timeout=12,
        )

    assert excinfo.value.status_code == 400


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


def test_install_whoop_token_replaces_the_serialized_database_authority() -> None:
    calls = []

    class Settings:
        postgres_database_url = "postgresql://warehouse"
        whoop = type("Whoop", (), {"account": "test@example.com"})()

    class Warehouse:
        def ensure_whoop_tables(self):
            calls.append(("ensure",))

        def replace_whoop_oauth_token(self, **kwargs):
            calls.append(("replace", kwargs))

        def close(self):
            calls.append(("close",))

    installed_at = datetime(
        2026,
        8,
        14,
        12,
        tzinfo=UTC,
    )
    token = {"access_token": "new", "refresh_token": "new-refresh"}

    install_whoop_token(
        settings=Settings(),
        token=token,
        warehouse_factory=lambda _settings: Warehouse(),
        now=lambda: installed_at,
    )

    assert calls[0] == ("ensure",)
    operation, kwargs = calls[1]
    assert operation == "replace"
    assert kwargs["account"] == "test@example.com"
    assert json.loads(kwargs["token_json"]) == token
    assert kwargs["updated_at"] == installed_at
    assert calls[2] == ("close",)


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
