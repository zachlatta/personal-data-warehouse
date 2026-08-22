"""Minimal client for ChatGPT's (consumer) backend API.

A captured chatgpt.com web session, the browser ``Cookie`` header, primarily
``__Secure-next-auth.session-token`` plus any Cloudflare cookies, is exchanged
at ``/api/auth/session`` for a short-lived bearer ``accessToken``, which then
authorizes ``/backend-api/conversations`` (paginated, newest-first) and
``/backend-api/conversation/<id>`` (the full message tree).

This is an unofficial API, so the client is deliberately small and fails loudly:
any 401/403 (or a session response without an ``accessToken``) raises
``ChatGPTAuthError`` so the caller can surface "re-publish the session" rather
than silently degrade.

Cloudflare fronts chatgpt.com and bot-challenges non-browser TLS fingerprints,
so the default transport is a ``curl_cffi`` session impersonating Chrome (the
same treatment ``personal_data_warehouse_claude_desktop.api`` needed after the
2026-07-12 claude.ai block). Measured 2026-08-22 from the production host with
one identical cookie: plain ``requests``/``urllib`` got ``403`` with
``cf-mitigated: challenge``, ``curl_cffi`` got ``200``. The User-Agent below
must stay in step with the impersonation target.
"""

from __future__ import annotations

import base64
from collections.abc import Callable, Iterator
from dataclasses import dataclass
import json
import time
from typing import Any

from curl_cffi import requests as cffi_requests

DEFAULT_BASE_URL = "https://chatgpt.com"
# The Chrome build curl_cffi impersonates at the TLS layer; the UA header
# matches so the two fingerprints tell Cloudflare the same story.
DEFAULT_IMPERSONATE = "chrome131"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
DEFAULT_PAGE_SIZE = 28
DEFAULT_TIMEOUT_SECONDS = 30.0


#: How long before the hard expiry a human should be told to sign in again.
#: The token lives exactly 10 days, so two days is ~one weekend of slack.
TOKEN_WARN_WITHIN_SECONDS = 2 * 86400


def token_expiry_warning(
    expiry: float,
    *,
    now: float,
    warn_within_seconds: float = TOKEN_WARN_WITHIN_SECONDS,
) -> str | None:
    """Warn that the published session is about to lapse, or already has.

    ChatGPT mints the ``accessToken`` only during a browser sign-in and stamps it
    with a hard 10-day ``exp``; replaying the captured cookie returns that same
    cached token forever, so no amount of polling renews it. That makes the
    lapse perfectly predictable, and therefore worth announcing *before* the
    ingest stops rather than after. Returns ``None`` when there is plenty of
    life left, or when ``expiry`` is 0.0 (an opaque token whose expiry we cannot
    read - never invent an alarm from a missing fact).
    """
    if expiry <= 0:
        return None
    remaining = expiry - now
    if remaining > warn_within_seconds:
        return None
    action = (
        "Sign out and back into chatgpt.com in the capture browser, then run "
        "`pdw chatgpt publish-session` - a token is only minted at sign-in and lives 10 days."
    )
    if remaining <= 0:
        return f"ChatGPT session expired {_days(-remaining)} ago. {action}"
    return f"ChatGPT session expires in {_days(remaining)}. {action}"


def _days(seconds: float) -> str:
    days = seconds / 86400.0
    if days >= 1:
        return f"{days:.1f} days"
    return f"{seconds / 3600.0:.1f} hours"


class ChatGPTAuthError(RuntimeError):
    """The stored session is missing, expired, or rejected; re-publish needed."""


class ChatGPTBackendError(RuntimeError):
    """A non-auth backend failure (network, 5xx, malformed payload)."""


class ChatGPTRateLimitError(ChatGPTBackendError):
    """The backend returned 429; stop this poll and continue on the next tick."""

    def __init__(self, message: str, *, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


@dataclass(frozen=True)
class ConversationRef:
    id: str
    title: str
    create_time: float
    update_time: float


class ChatGPTBackendClient:
    def __init__(
        self,
        *,
        session_credential: str,
        session: Any | None = None,
        base_url: str = DEFAULT_BASE_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        now: Callable[[], float] = time.time,
    ) -> None:
        if not session_credential:
            raise ChatGPTAuthError("no ChatGPT session credential configured")
        self._cookie_header = _cookie_header(session_credential)
        self._session = (
            session if session is not None else cffi_requests.Session(impersonate=DEFAULT_IMPERSONATE)
        )
        self._base = base_url.rstrip("/")
        self._user_agent = user_agent
        self._timeout = timeout
        self._now = now
        self._access_token: str = ""
        self._access_expiry: float = 0.0

    @property
    def access_token_expiry(self) -> float:
        """Epoch seconds the current access token dies, or 0.0 before a fetch.

        The bearer is minted only by a browser sign-in and carries a hard 10-day
        ``exp`` that no server-side call can extend, so callers surface this to
        warn a human *before* the credential lapses.
        """
        return self._access_expiry

    # --- auth ---------------------------------------------------------------

    def fetch_auth_session(self) -> dict[str, Any]:
        """Exchange the cookie for an access token; return the session payload.

        Raises ``ChatGPTAuthError`` when the session is rejected or logged out.
        Used both internally (before backend calls) and by ``publish-session`` to
        validate a freshly-captured cookie and report the signed-in account.
        """
        response = self._session.get(
            f"{self._base}/api/auth/session",
            headers={
                "Cookie": self._cookie_header,
                "User-Agent": self._user_agent,
                "Accept": "application/json",
            },
            timeout=self._timeout,
        )
        if response.status_code in (401, 403):
            raise ChatGPTAuthError(
                f"/api/auth/session returned {response.status_code}: session expired"
            )
        if response.status_code == 429:
            raise ChatGPTRateLimitError(
                "/api/auth/session returned 429",
                retry_after_seconds=_retry_after_seconds(response),
            )
        if response.status_code >= 400:
            raise ChatGPTBackendError(f"/api/auth/session returned {response.status_code}")
        try:
            data = response.json()
        except ValueError as exc:  # pragma: no cover - defensive
            raise ChatGPTBackendError("auth session response was not JSON") from exc
        token = str((data or {}).get("accessToken") or "")
        if not token:
            # A logged-out session returns ``{}``; treat that as auth failure.
            raise ChatGPTAuthError("auth session response had no accessToken: session expired")
        now = self._now()
        token_exp = _jwt_exp(token)
        if token_exp is not None and token_exp <= now:
            # ``/api/auth/session`` returns a *cached, already-expired* access
            # token when the underlying browser session can no longer refresh it
            # (the JWT lasts ~10 days; the session cookie ~90). Sending it just
            # earns an opaque backend 401, so fail here with the auth-expiry
            # classification callers act on (``publish-session`` refuses to
            # publish a dead session; the poller marks it expired and skips).
            raise ChatGPTAuthError("auth session accessToken is expired: session expired")
        self._access_token = token
        # The JWT ``exp`` is authoritative for the bearer we send; fall back to
        # the session ``expires`` only for opaque (non-JWT) tokens.
        self._access_expiry = (
            token_exp if token_exp is not None else _parse_expiry(data.get("expires"), now=now)
        )
        return data if isinstance(data, dict) else {}

    def _ensure_access_token(self) -> None:
        if self._access_token and self._now() < self._access_expiry - 60:
            return
        self.fetch_auth_session()

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Cookie": self._cookie_header,
            "User-Agent": self._user_agent,
            "Accept": "application/json",
        }

    def _get_json(self, path: str) -> Any:
        self._ensure_access_token()
        response = self._session.get(
            f"{self._base}{path}", headers=self._auth_headers(), timeout=self._timeout
        )
        if response.status_code in (401, 403):
            raise ChatGPTAuthError(f"{path} returned {response.status_code}: session expired")
        if response.status_code == 429:
            raise ChatGPTRateLimitError(
                f"{path} returned 429",
                retry_after_seconds=_retry_after_seconds(response),
            )
        if response.status_code >= 400:
            raise ChatGPTBackendError(f"{path} returned {response.status_code}")
        try:
            return response.json()
        except ValueError as exc:
            raise ChatGPTBackendError(f"{path} response was not JSON") from exc

    # --- data ---------------------------------------------------------------

    def iter_conversation_refs(self, *, page_size: int = DEFAULT_PAGE_SIZE) -> Iterator[ConversationRef]:
        """Yield conversation references newest-first; the caller stops early."""
        offset = 0
        seen = 0
        while True:
            data = self._get_json(
                f"/backend-api/conversations?offset={offset}&limit={page_size}&order=updated"
            )
            items = (data or {}).get("items") or []
            if not items:
                return
            for item in items:
                yield ConversationRef(
                    id=str(item.get("id") or ""),
                    title=str(item.get("title") or ""),
                    create_time=_epoch(item.get("create_time")),
                    update_time=_epoch(item.get("update_time")),
                )
            seen += len(items)
            offset += len(items)
            total = (data or {}).get("total")
            if isinstance(total, int) and seen >= total:
                return

    def get_conversation(self, conversation_id: str) -> dict[str, Any]:
        data = self._get_json(f"/backend-api/conversation/{conversation_id}")
        if not isinstance(data, dict):
            raise ChatGPTBackendError(f"conversation {conversation_id} response was not an object")
        # The detail endpoint omits the id; backfill it so the normalizer keys on it.
        data.setdefault("conversation_id", conversation_id)
        return data


def _cookie_header(credential: str) -> str:
    credential = credential.strip()
    if "=" in credential:
        # Already a full Cookie header (one or more name=value pairs).
        return credential
    return f"__Secure-next-auth.session-token={credential}"


def _jwt_exp(token: str) -> float | None:
    """Return a JWT access token's ``exp`` (epoch seconds), or ``None``.

    The ChatGPT ``accessToken`` is an RS256 JWT whose ``exp`` (~10 days) is far
    shorter than the NextAuth *session* ``expires`` (~90 days), and only the JWT
    ``exp`` bounds how long the bearer actually authorizes backend calls. Returns
    ``None`` for opaque (non-JWT) tokens so callers fall back to the session
    expiry. Best-effort: any decode failure yields ``None``.
    """
    parts = token.split(".")
    if len(parts) != 3:
        return None
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
    except (ValueError, TypeError, json.JSONDecodeError):
        return None
    exp = claims.get("exp") if isinstance(claims, dict) else None
    return float(exp) if isinstance(exp, (int, float)) else None


def _parse_expiry(expires: Any, *, now: float) -> float:
    """Best-effort parse of the ISO-8601 ``expires`` field into an epoch float."""
    if isinstance(expires, (int, float)):
        return float(expires)
    if isinstance(expires, str) and expires:
        from datetime import datetime

        try:
            return datetime.fromisoformat(expires.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    # Unknown expiry: assume a short validity so we re-fetch the token soon.
    return now + 300.0


def _retry_after_seconds(response: Any) -> float | None:
    headers = getattr(response, "headers", {}) or {}
    raw = headers.get("Retry-After") if hasattr(headers, "get") else None
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None


def _epoch(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        from datetime import datetime

        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0
    return 0.0
