"""Capture the news.ycombinator.com login for the ``hacker_news`` source.

HN keeps its login in one cookie, ``user`` (``<username>&<token>``), on
``news.ycombinator.com``. The lists HN shows only to the logged-in user
(upvoted, hidden) need it; everything else the source reads is public. As
with ChatGPT and WHOOP the credential is read out of a local Chrome-family
browser rather than obtained by logging in from a server.

Nothing here returns the token in a report: callers get the username and a
fingerprint, so a run log can prove which credential was published.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib

from personal_data_warehouse.chatgpt_cookies import (
    BROWSERS,
    ChatGPTCookieError,
    _cookie_dbs,
    _safe_storage_key,
    browser_by_key,
    read_cookies_for_host,
)

HN_COOKIE_HOST_SUFFIX = "news.ycombinator.com"
HN_LOGIN_COOKIE = "user"


class HackerNewsSessionCaptureError(RuntimeError):
    """No logged-in news.ycombinator.com session was found in a local browser."""


@dataclass(frozen=True)
class CapturedHackerNewsSession:
    browser: str
    #: The full ``Cookie:`` header value the poller sends (today: ``user=...``).
    cookie_header: str
    #: The username embedded in the cookie, so a publish can refuse the wrong account.
    user_id: str

    def fingerprint(self) -> str:
        return hashlib.sha256(self.cookie_header.encode("utf-8")).hexdigest()

    def redacted(self) -> dict[str, str]:
        return {
            "browser": self.browser,
            "user_id": self.user_id,
            "token_sha256": self.fingerprint(),
        }


def user_id_from_cookie(value: str) -> str:
    """The username half of HN's ``user`` cookie (``name&token``)."""
    return value.split("&", 1)[0].strip() if value else ""


def capture_hacker_news_session(*, browser: str | None = None) -> CapturedHackerNewsSession:
    """Find a logged-in news.ycombinator.com session in a local Chrome-family browser."""
    if browser:
        profile = browser_by_key(browser)
        if profile is None:
            valid = ", ".join(b.key for b in BROWSERS)
            raise HackerNewsSessionCaptureError(f"unknown browser {browser!r}; valid: {valid}")
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
                cookies = read_cookies_for_host(db, key, HN_COOKIE_HOST_SUFFIX)
            except Exception as exc:  # noqa: BLE001 - keep trying other profiles
                problems.append(f"{profile.display_name}: {type(exc).__name__}")
                continue
            value = cookies.get(HN_LOGIN_COOKIE, "")
            user_id = user_id_from_cookie(value)
            if value and user_id:
                return CapturedHackerNewsSession(
                    browser=profile.display_name,
                    cookie_header=f"{HN_LOGIN_COOKIE}={value}",
                    user_id=user_id,
                )

    detail = "; ".join(problems) if problems else "no Chrome-family browser held a news.ycombinator.com login"
    raise HackerNewsSessionCaptureError(
        "could not find a logged-in news.ycombinator.com session in a local browser. "
        "Open Chrome, log in to news.ycombinator.com, then retry. " + detail
    )
