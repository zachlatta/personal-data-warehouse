"""Minimal client for claude.ai's conversation API.

The Claude Desktop app talks to the same private claude.ai endpoints the web app
uses. We reuse the desktop app's login cookies (see :mod:`.cookies`) to:

* page through ``/chat_conversations`` for the conversation list (each summary
  carries an ``updated_at`` we use as the incremental cursor), and
* fetch ``/chat_conversations/{uuid}?tree=True&rendering_mode=messages`` for a
  conversation's full message tree.

This is an unofficial endpoint, so we send browser-like headers and retry
transient failures, but we deliberately keep the surface tiny.

Cloudflare in front of claude.ai bot-challenges non-browser TLS fingerprints
(every request from plain ``requests``/``curl`` gets a 403 "Just a moment..."
interstitial, observed from 2026-07-12), so the default transport is a
``curl_cffi`` session that impersonates Chrome's TLS fingerprint. The
User-Agent below must stay in step with the impersonation target.
"""

from __future__ import annotations

from collections.abc import Iterator
import time
from typing import Any

from curl_cffi import requests as cffi_requests

DEFAULT_BASE_URL = "https://claude.ai"
DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_PAGE_SIZE = 50
# The Chrome build curl_cffi impersonates at the TLS layer; the UA header
# matches so the two fingerprints tell the same story to Cloudflare.
DEFAULT_IMPERSONATE = "chrome131"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


class ClaudeAiApiError(RuntimeError):
    """Raised when a claude.ai API call fails after retries."""


class ClaudeAiRateLimitError(ClaudeAiApiError):
    """The backend returned 429; stop this poll and continue on the next tick.

    Mirrors ``ChatGPTRateLimitError``: a 429 is not retried inline (that would
    only hammer an already-throttled endpoint); instead the poller stops
    gracefully and the keepalive sensor retries on its next tick.
    """

    def __init__(self, message: str, *, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class ClaudeAiClient:
    def __init__(
        self,
        *,
        cookie_header: str,
        org_id: str,
        base_url: str = DEFAULT_BASE_URL,
        session: Any | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        user_agent: str = DEFAULT_USER_AGENT,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.5,
        sleep=time.sleep,
    ) -> None:
        if not cookie_header:
            raise ValueError("cookie_header is required")
        if not org_id:
            raise ValueError("org_id is required")
        self._cookie_header = cookie_header
        self._base_url = base_url.rstrip("/")
        self._org_id = org_id
        self._session = session or cffi_requests.Session(impersonate=DEFAULT_IMPERSONATE)
        self._timeout = timeout
        self._user_agent = user_agent
        self._max_retries = max(1, max_retries)
        self._retry_backoff_seconds = retry_backoff_seconds
        self._sleep = sleep

    def _headers(self, *, referer: str) -> dict[str, str]:
        return {
            "Cookie": self._cookie_header,
            "User-Agent": self._user_agent,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer,
            "Origin": self._base_url,
            "anthropic-client-platform": "web_claude_ai",
        }

    def _get(self, path: str, *, referer: str, params: dict[str, str] | None = None) -> Any:
        url = f"{self._base_url}{path}"
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                response = self._session.get(
                    url,
                    headers=self._headers(referer=referer),
                    params=params,
                    timeout=self._timeout,
                )
            except cffi_requests.exceptions.RequestException as exc:
                last_error = exc
            else:
                if response.status_code == 200:
                    return response.json()
                # 401/403 are auth/cloudflare and will not improve on retry, so
                # fail fast with a clear message.
                if response.status_code in (401, 403):
                    raise ClaudeAiApiError(
                        f"claude.ai returned {response.status_code} for {path}; "
                        "the desktop login may have expired or Cloudflare is blocking - "
                        "open the Claude Desktop app and sign in again"
                    )
                # 429 means throttled: don't retry inline (that only hammers an
                # already-rate-limited endpoint); surface it so the poller stops
                # and the keepalive sensor retries on its next tick.
                if response.status_code == 429:
                    raise ClaudeAiRateLimitError(
                        f"claude.ai returned 429 for {path}",
                        retry_after_seconds=_retry_after_seconds(response),
                    )
                if response.status_code < 500:
                    raise ClaudeAiApiError(
                        f"claude.ai returned {response.status_code} for {path}: {response.text[:200]}"
                    )
                # 5xx is transient and worth retrying.
                last_error = ClaudeAiApiError(
                    f"claude.ai returned {response.status_code} for {path}"
                )
            if attempt < self._max_retries - 1:
                self._sleep(self._retry_backoff_seconds * (attempt + 1))
        raise ClaudeAiApiError(f"claude.ai request to {path} failed after {self._max_retries} attempts: {last_error}")

    def iter_conversations(self, *, page_size: int = DEFAULT_PAGE_SIZE) -> Iterator[dict[str, Any]]:
        """Yield conversation summaries newest-first, paging until exhausted."""
        offset = 0
        page_size = max(1, page_size)
        while True:
            page = self._get(
                f"/api/organizations/{self._org_id}/chat_conversations",
                referer=f"{self._base_url}/recents",
                params={"limit": str(page_size), "offset": str(offset)},
            )
            if not isinstance(page, list) or not page:
                return
            for summary in page:
                if isinstance(summary, dict):
                    yield summary
            if len(page) < page_size:
                return
            offset += len(page)

    def account_email(self) -> str:
        """Return the verified email of the account this session belongs to.

        Used to detect a credential that was captured from the *wrong* claude.ai
        login (the failure mode that silently synced a near-empty account): the
        ``sessionKey`` authenticates whatever account the desktop app was logged
        into, which need not be the account label the credential is keyed by.
        Best-effort - the caller treats an empty string as "could not verify".
        """
        data = self._get("/api/account", referer=f"{self._base_url}/")
        if not isinstance(data, dict):
            return ""
        account = data.get("account")
        if not isinstance(account, dict):
            account = data
        email = account.get("email_address") or account.get("email") or ""
        return str(email).strip()

    def get_conversation(self, conversation_id: str) -> dict[str, Any]:
        """Fetch a conversation's full message tree."""
        conversation = self._get(
            f"/api/organizations/{self._org_id}/chat_conversations/{conversation_id}",
            referer=f"{self._base_url}/chat/{conversation_id}",
            params={
                "tree": "True",
                "rendering_mode": "messages",
                "render_all_tools": "true",
            },
        )
        if not isinstance(conversation, dict):
            raise ClaudeAiApiError(f"unexpected conversation payload for {conversation_id}")
        return conversation


def resolve_sync_account(*, session_email: str, credential_label: str, configured: str) -> tuple[str, str]:
    """Pick the account to sync and tag events under, and report where it came from.

    Precedence: the live session's verified email (authoritative - it is the
    account the ``sessionKey`` actually belongs to) > the stored credential's
    label > the env-configured fallback. Returns ``(account, source)`` with
    ``source`` one of ``"session"``, ``"credential"``, ``"config"`` or ``"none"``.
    """
    for value, source in (
        (session_email, "session"),
        (credential_label, "credential"),
        (configured, "config"),
    ):
        cleaned = (value or "").strip()
        if cleaned:
            return cleaned, source
    return "", "none"


def _retry_after_seconds(response: Any) -> float | None:
    headers = getattr(response, "headers", {}) or {}
    raw = headers.get("Retry-After") if hasattr(headers, "get") else None
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None
