"""Capture the app.whoop.com browser session for the ``whoop_private`` source.

WHOOP requires MFA, so there is no unattended password login. The web app keeps
its session in ordinary Chrome cookies on ``.whoop.com``, so instead of
implementing the login we capture what the browser already holds -- the same
approach the ChatGPT poller uses, and the reason MFA is not a blocker here.

The captured pair is a 24-hour access token and a 30-day refresh token. Because
every refresh issues a NEW refresh token (see ``whoop_private_api``), a sync that
runs more often than monthly slides the window forward forever and this capture
never has to be repeated. It exists for first setup and for repair.

Nothing here logs or returns a token value; callers get fingerprints and
expiries so a run log can prove freshness without leaking the credential.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
import base64
import hashlib
import json

from personal_data_warehouse.chatgpt_cookies import (
    BROWSERS,
    ChatGPTCookieError,
    _cookie_dbs,
    _safe_storage_key,
    browser_by_key,
    read_cookies_for_host,
)

WHOOP_COOKIE_HOST_SUFFIX = "whoop.com"
ACCESS_TOKEN_COOKIE = "whoop-auth-token"
REFRESH_TOKEN_COOKIE = "whoop-auth-refresh-token"

# Cognito's access token is a 24h JWT and the refresh token is a 30-day opaque
# string. The refresh token carries no readable expiry, so we assume the
# documented 30 days from capture; the server corrects it on the first refresh,
# which returns refresh_token_expires_in explicitly.
ASSUMED_REFRESH_TOKEN_LIFETIME = timedelta(days=30)


class WhoopSessionCaptureError(RuntimeError):
    """No logged-in app.whoop.com session was found in a local browser."""


@dataclass(frozen=True)
class CapturedWhoopSession:
    browser: str
    access_token: str
    refresh_token: str
    access_expires_at: datetime
    refresh_expires_at: datetime

    def fingerprint(self) -> str:
        """A stable, non-secret identity for this credential."""
        return hashlib.sha256(self.refresh_token.encode("utf-8")).hexdigest()

    def redacted(self) -> dict[str, Any]:
        return {
            "browser": self.browser,
            "access_expires_at": self.access_expires_at.isoformat(),
            "refresh_expires_at": self.refresh_expires_at.isoformat(),
            "refresh_token_sha256": self.fingerprint(),
        }


def access_token_expiry(token: str, *, now: datetime) -> datetime:
    """Read ``exp`` out of the Cognito JWT without verifying it.

    We are not authenticating the token, only scheduling around it, so an
    unverifiable or malformed token falls back to the documented 24 hours
    rather than failing the capture.
    """
    parts = token.split(".")
    if len(parts) == 3:
        try:
            padded = parts[1] + "=" * (-len(parts[1]) % 4)
            claims = json.loads(base64.urlsafe_b64decode(padded))
            expires = claims.get("exp")
            if isinstance(expires, int):
                return datetime.fromtimestamp(expires, tz=UTC)
        except Exception:  # noqa: BLE001 - a bad JWT is not a capture failure
            pass
    return now + timedelta(hours=24)


def capture_whoop_session(
    *,
    browser: str | None = None,
    now: datetime | None = None,
) -> CapturedWhoopSession:
    """Find a logged-in app.whoop.com session in a local Chrome-family browser."""
    moment = now or datetime.now(UTC)
    if browser:
        profile = browser_by_key(browser)
        if profile is None:
            valid = ", ".join(b.key for b in BROWSERS)
            raise WhoopSessionCaptureError(f"unknown browser {browser!r}; valid: {valid}")
        candidates = [profile]
    else:
        candidates = list(BROWSERS)

    problems: list[str] = []
    for profile in candidates:
        dbs = _cookie_dbs(profile)
        if not dbs:
            continue
        try:
            key = _safe_storage_key(profile)
        except ChatGPTCookieError as exc:
            problems.append(f"{profile.display_name}: {exc}")
            continue
        for db in dbs:
            try:
                cookies = read_cookies_for_host(db, key, WHOOP_COOKIE_HOST_SUFFIX)
            except Exception as exc:  # noqa: BLE001 - keep trying other profiles
                problems.append(f"{profile.display_name}: {type(exc).__name__}")
                continue
            access = cookies.get(ACCESS_TOKEN_COOKIE, "")
            refresh = cookies.get(REFRESH_TOKEN_COOKIE, "")
            if access and refresh:
                return CapturedWhoopSession(
                    browser=profile.display_name,
                    access_token=access,
                    refresh_token=refresh,
                    access_expires_at=access_token_expiry(access, now=moment),
                    refresh_expires_at=moment + ASSUMED_REFRESH_TOKEN_LIFETIME,
                )
            if access or refresh:
                problems.append(f"{profile.display_name}: partial whoop.com session (one cookie of two)")

    detail = "; ".join(problems) if problems else "no Chrome-family browser held a whoop.com session"
    raise WhoopSessionCaptureError(
        "could not find a logged-in app.whoop.com session in a local browser. "
        "Open Chrome, log in to app.whoop.com (MFA included), then retry. " + detail
    )
