from __future__ import annotations

import argparse
import base64
import json
import os
import secrets
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from personal_data_warehouse.config import DEFAULT_WHOOP_SCOPES, WhoopConfig, load_settings
from personal_data_warehouse.gmail_auth import update_env_file


class WhoopOAuthError(RuntimeError):
    pass


def encode_whoop_token_env(token_json: str | dict[str, Any]) -> str:
    if isinstance(token_json, dict):
        token_json = json.dumps(token_json, sort_keys=True, separators=(",", ":"))
    return base64.b64encode(token_json.encode("utf-8")).decode("ascii")


def whoop_token_json_from_env() -> str:
    token_json = os.getenv("WHOOP_TOKEN_JSON", "").strip()
    if token_json:
        return token_json
    encoded = os.getenv("WHOOP_TOKEN_JSON_B64", "").strip()
    if encoded:
        return base64.b64decode(encoded).decode("utf-8")
    raise RuntimeError("WHOOP_TOKEN_JSON or WHOOP_TOKEN_JSON_B64 must be set")


def refresh_whoop_token(
    token: dict[str, Any],
    *,
    client_id: str = "",
    client_secret: str = "",
    token_url: str = "",
    timeout: int = 30,
) -> dict[str, Any]:
    refresh_token = str(token.get("refresh_token") or "")
    if not refresh_token:
        raise WhoopOAuthError("WHOOP token has no refresh_token; re-run the WHOOP OAuth flow")

    if not client_id or not client_secret or not token_url:
        settings = load_settings(
            require_postgres=False,
            require_gmail=False,
            require_whoop_client_secrets=True,
        )
        if settings.whoop is None:
            raise WhoopOAuthError("WHOOP_CLIENT_ID and WHOOP_CLIENT_SECRET must be set")
        client_id = client_id or settings.whoop.client_id
        client_secret = client_secret or settings.whoop.client_secret
        token_url = token_url or settings.whoop.token_url
        timeout = settings.whoop.request_timeout_seconds

    response = requests.post(
        token_url,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "offline",
        },
        timeout=timeout,
    )
    if response.status_code >= 400:
        raise WhoopOAuthError(f"WHOOP token refresh failed with HTTP {response.status_code}")
    refreshed = _oauth_token_response(response, operation="refresh")
    if not refreshed.get("refresh_token"):
        raise WhoopOAuthError("WHOOP token refresh response did not include rotated refresh_token")
    return _token_with_expires_at(refreshed)


def exchange_authorization_code(
    *,
    code: str,
    config: WhoopConfig,
    timeout: int | None = None,
) -> dict[str, Any]:
    response = requests.post(
        config.token_url,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": config.redirect_uri,
            "client_id": config.client_id,
            "client_secret": config.client_secret,
        },
        timeout=timeout or config.request_timeout_seconds,
    )
    if response.status_code >= 400:
        raise WhoopOAuthError(f"WHOOP token exchange failed with HTTP {response.status_code}")
    token = _oauth_token_response(response, operation="exchange")
    if "offline" in config.scopes and not token.get("refresh_token"):
        raise WhoopOAuthError("WHOOP token exchange did not return refresh_token for offline scope")
    return _token_with_expires_at(token)


def authorization_url(*, config: WhoopConfig, state: str) -> str:
    return config.auth_url + "?" + urlencode(
        {
            "response_type": "code",
            "client_id": config.client_id,
            "redirect_uri": config.redirect_uri,
            "scope": " ".join(config.scopes or DEFAULT_WHOOP_SCOPES),
            "state": state,
        }
    )


def authorization_code_from_callback_url(
    callback_url: str,
    *,
    config: WhoopConfig,
    expected_state: str,
) -> str:
    parsed = urlparse(callback_url.strip())
    expected = urlparse(config.redirect_uri)
    if (parsed.scheme, parsed.hostname, parsed.port, parsed.path) != (
        expected.scheme,
        expected.hostname,
        expected.port,
        expected.path,
    ):
        raise WhoopOAuthError("WHOOP OAuth callback URL does not match WHOOP_REDIRECT_URI")
    params = parse_qs(parsed.query)
    if (params.get("state") or [""])[0] != expected_state:
        raise WhoopOAuthError("WHOOP OAuth callback state mismatch")
    if params.get("error"):
        error = (params.get("error_description") or params.get("error") or ["WHOOP OAuth error"])[0]
        raise WhoopOAuthError(f"WHOOP authorization failed: {error}")
    code = (params.get("code") or [""])[0]
    if not code:
        raise WhoopOAuthError("WHOOP OAuth callback did not include an authorization code")
    return code


def run_oauth_flow(
    *,
    write_env: bool = False,
    open_browser: bool = True,
    manual: bool = False,
    input_fn=input,
) -> dict[str, str]:
    settings = load_settings(
        require_postgres=False,
        require_gmail=False,
        require_whoop_client_secrets=True,
    )
    if settings.whoop is None:
        raise RuntimeError("WHOOP settings were not loaded")
    config = settings.whoop
    parsed_redirect = urlparse(config.redirect_uri)
    if parsed_redirect.scheme != "http" or parsed_redirect.hostname not in {"localhost", "127.0.0.1"}:
        raise WhoopOAuthError(
            "personal-data-warehouse-whoop-auth can only listen on an http://localhost redirect URI; "
            "set WHOOP_REDIRECT_URI to the localhost URL registered in the WHOOP Developer Dashboard"
        )
    if not parsed_redirect.port:
        raise WhoopOAuthError("WHOOP_REDIRECT_URI must include an explicit localhost port, e.g. http://localhost:8080/callback")

    state = secrets.token_urlsafe(8)[:8]
    auth_url = authorization_url(config=config, state=state)
    print("Open this WHOOP authorization URL if your browser does not open automatically:")
    print(auth_url)

    if manual:
        if open_browser:
            webbrowser.open(auth_url)
        callback_url = input_fn(
            "After WHOOP redirects, paste the complete localhost callback URL here: "
        )
        code = authorization_code_from_callback_url(
            callback_url,
            config=config,
            expected_state=state,
        )
    else:
        callback = _OAuthCallbackServer((parsed_redirect.hostname, parsed_redirect.port), parsed_redirect.path or "/", state)
        thread = threading.Thread(target=callback.handle_request, daemon=True)
        thread.start()
        if open_browser:
            webbrowser.open(auth_url)
        thread.join(timeout=300)
        callback.server_close()
        if callback.error:
            raise WhoopOAuthError(f"WHOOP authorization failed: {callback.error}")
        if not callback.code:
            raise WhoopOAuthError("Timed out waiting for WHOOP OAuth redirect")
        code = callback.code

    token = exchange_authorization_code(code=code, config=config)
    env_values = {"WHOOP_TOKEN_JSON_B64": encode_whoop_token_env(token)}
    if write_env:
        update_env_file(Path(".env"), env_values)
        print("Updated .env with WHOOP_TOKEN_JSON_B64")
    else:
        for key, value in env_values.items():
            print(f"{key}={value}")
    print("Generated WHOOP OAuth token env var")
    return env_values


def _oauth_token_response(response: requests.Response, *, operation: str) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise WhoopOAuthError(f"WHOOP token {operation} returned invalid JSON") from exc
    if not isinstance(payload, dict) or not payload.get("access_token"):
        raise WhoopOAuthError(f"WHOOP token {operation} response did not include access_token")
    return dict(payload)


def _token_with_expires_at(token: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(token)
    if "expires_at" not in normalized and normalized.get("expires_in") is not None:
        try:
            import time

            normalized["expires_at"] = int(time.time()) + int(normalized["expires_in"])
        except (TypeError, ValueError):
            pass
    return normalized


class _OAuthCallbackServer(HTTPServer):
    def __init__(self, server_address, callback_path: str, expected_state: str) -> None:
        self.callback_path = callback_path
        self.expected_state = expected_state
        self.code = ""
        self.error = ""
        super().__init__(server_address, _OAuthCallbackHandler)


class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    server: _OAuthCallbackServer

    def do_GET(self) -> None:  # noqa: N802 - stdlib callback name
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if parsed.path != self.server.callback_path:
            self._respond(404, "Unexpected WHOOP OAuth callback path")
            return
        state = (params.get("state") or [""])[0]
        if state != self.server.expected_state:
            self.server.error = "state mismatch"
            self._respond(400, "WHOOP OAuth state mismatch; you can close this tab.")
            return
        if params.get("error"):
            self.server.error = (params.get("error_description") or params.get("error") or ["WHOOP OAuth error"])[0]
            self._respond(400, "WHOOP authorization failed; you can close this tab.")
            return
        self.server.code = (params.get("code") or [""])[0]
        self._respond(200, "WHOOP authorization complete; you can close this tab and return to the terminal.")

    def log_message(self, format: str, *args) -> None:  # noqa: A002 - stdlib signature
        return

    def _respond(self, status: int, message: str) -> None:
        body = message.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser(description="Authorize WHOOP for the personal data warehouse.")
    parser.add_argument(
        "--write-env",
        action="store_true",
        help="Update the local .env file with WHOOP_TOKEN_JSON_B64 instead of only printing it.",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Print the authorization URL without attempting to open a browser.",
    )
    parser.add_argument(
        "--manual",
        action="store_true",
        help="Paste the final callback URL instead of listening on localhost (useful when authorizing in a browser on another computer).",
    )
    args = parser.parse_args()
    run_oauth_flow(write_env=args.write_env, open_browser=not args.no_browser, manual=args.manual)


if __name__ == "__main__":
    main()
