from __future__ import annotations

import base64
import json
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from personal_data_warehouse.config import Settings, env_slug


def load_google_credentials(
    *,
    email_address: str,
    settings: Settings,
    scopes: tuple[str, ...],
    service_name: str,
    request_timeout_seconds: int = 120,
) -> Credentials:
    token_json = google_token_json_from_env(email_address)
    if not token_json:
        raise RuntimeError(
            f"No {service_name} OAuth token for {email_address}. "
            f"Set GOOGLE_{env_slug(email_address)}_TOKEN_JSON_B64 or "
            f"GMAIL_{env_slug(email_address)}_TOKEN_JSON_B64."
        )

    credentials = Credentials.from_authorized_user_info(json.loads(token_json), scopes)
    if credentials.valid:
        return credentials

    if not credentials.expired or not credentials.refresh_token:
        raise RuntimeError(
            f"Stored {service_name} OAuth token for {email_address} cannot be refreshed. "
            f"Update GOOGLE_{env_slug(email_address)}_TOKEN_JSON_B64 or "
            f"GMAIL_{env_slug(email_address)}_TOKEN_JSON_B64 and authorize again."
        )

    credentials.refresh(TimeoutRequest(timeout_seconds=request_timeout_seconds))
    return credentials


class TimeoutRequest:
    def __init__(self, *, timeout_seconds: int) -> None:
        self._request = Request()
        self._timeout_seconds = timeout_seconds

    def __call__(self, url, method="GET", body=None, headers=None, timeout=120, **kwargs):
        return self._request(
            url,
            method=method,
            body=body,
            headers=headers,
            timeout=min(timeout, self._timeout_seconds) if timeout else self._timeout_seconds,
            **kwargs,
        )


def google_token_json_from_env(email_address: str) -> str | None:
    slug = env_slug(email_address)
    for name in (
        f"GOOGLE_{slug}_TOKEN_JSON",
        f"GOOGLE_TOKEN_JSON_{slug}",
        f"GMAIL_{slug}_TOKEN_JSON",
        f"GMAIL_TOKEN_JSON_{slug}",
    ):
        value = os.getenv(name)
        if value:
            return value
    for name in (
        f"GOOGLE_{slug}_TOKEN_JSON_B64",
        f"GOOGLE_TOKEN_JSON_B64_{slug}",
        f"GMAIL_{slug}_TOKEN_JSON_B64",
        f"GMAIL_TOKEN_JSON_B64_{slug}",
    ):
        value = os.getenv(name)
        if value:
            return base64.b64decode(value).decode("utf-8")
    return None
