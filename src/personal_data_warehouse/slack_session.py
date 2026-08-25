"""Capture the Slack desktop/browser session for high-frequency Slack sync.

Slack's public Web API cannot tell us *which* conversations changed:
``conversations.list`` returns no last-message marker at all (only ``updated``,
which tracks topic/member edits). The only way to find new messages with an app
token is to call ``conversations.history`` per conversation -- 950 calls every
five minutes against a measured budget of ~39 calls/minute. The pipeline
therefore spends its time asleep on 429s while holding the shared Slack lock,
which starves every backfill stage.

Slack's own client answers that question in a single request (``client.counts``),
but only for a real logged-in session. That session is two pieces which are
useless apart: an ``xoxc-`` token kept in the app's localStorage, and the ``d``
cookie. This module captures both from what is already on the machine -- the
same approach as ``chatgpt_cookies`` and ``whoop_private_session``, and it reuses
their AES/keychain machinery rather than re-implementing it.

Nothing here logs or returns a secret; callers get a fingerprint plus identity so
a run log can prove freshness without leaking the credential.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
import hashlib
import re

from personal_data_warehouse.chatgpt_cookies import (
    BrowserProfile,
    ChatGPTCookieError,
    _cookie_dbs,
    _safe_storage_key,
    read_cookies_for_host,
)

SLACK_COOKIE_HOST_SUFFIX = "slack.com"
SESSION_COOKIE = "d"

# The Slack desktop app is an Electron Chromium profile, so the cookie store and
# the Safe Storage keychain entry have exactly the browser shape and the existing
# decryption path works unchanged. It is the preferred source because it stays
# signed in indefinitely, where a browser tab may not.
SLACK_APP_PROFILE = BrowserProfile(
    "slack-app",
    "Slack",
    "Slack",
    "Slack Safe Storage",
    "Slack",
    "/Applications/Slack.app",
    "slack",
)

# Deliberately narrow: `xoxc-` only (a user *client* token). `xoxb-`/`xoxp-` are
# different token types that do not work with the client endpoints, and a length
# floor keeps truncated fragments out of the candidate list.
_TOKEN_RE = re.compile(r"xoxc-[0-9a-zA-Z-]{10,}")


class SlackSessionCaptureError(RuntimeError):
    """No usable logged-in Slack session was found on this machine."""


@dataclass(frozen=True)
class CapturedSlackSession:
    source: str
    token: str
    cookie_d: str
    team_id: str
    enterprise_id: str
    user_id: str
    team_url: str
    cookie_expires_at: datetime | None

    def fingerprint(self) -> str:
        """A stable, non-secret identity for this credential."""
        return hashlib.sha256(self.token.encode("utf-8")).hexdigest()

    def cookie_header(self) -> str:
        return f"{SESSION_COOKIE}={self.cookie_d}"

    def redacted(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "team_id": self.team_id,
            "enterprise_id": self.enterprise_id,
            "user_id": self.user_id,
            "team_url": self.team_url,
            "cookie_expires_at": self.cookie_expires_at.isoformat() if self.cookie_expires_at else None,
            "token_sha256": self.fingerprint(),
        }


def _scan_bytes(data: bytes) -> set[str]:
    found = {match.group(0) for match in _TOKEN_RE.finditer(data.decode("latin-1"))}
    # Chromium stores some localStorage values as UTF-16LE, where a byte-level
    # search for "xoxc-" finds nothing because every character is NUL-separated.
    # Both alignments are tried: LevelDB records carry a one-byte type prefix, so
    # the UTF-16 payload is as likely to start on an odd offset as an even one.
    for offset in (0, 1):
        found |= {
            match.group(0)
            for match in _TOKEN_RE.finditer(data[offset:].decode("utf-16-le", errors="ignore"))
        }
    return found


def scan_local_storage_for_tokens(store_root: Path) -> list[str]:
    """Every ``xoxc-`` token in a Chromium localStorage tree, newest file first.

    Slack rewrites ``localConfig_v2`` on each sign-in, so stale tokens from
    previous logins stay behind in older ``.ldb`` files. Nothing on disk reliably
    marks which is live, so this only *orders* candidates by file mtime; the
    caller decides by asking Slack.
    """
    leveldb = store_root / "Local Storage" / "leveldb"
    if not leveldb.is_dir():
        return []
    ordered: list[str] = []
    files = sorted(
        (p for p in leveldb.iterdir() if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in files:
        try:
            data = path.read_bytes()
        except OSError:  # pragma: no cover - unreadable sidecar files
            continue
        for token in sorted(_scan_bytes(data)):
            if token not in ordered:
                ordered.append(token)
    return ordered


def capture_slack_session(
    *,
    store_root: Path,
    cookies: Mapping[str, str],
    cookie_expires_at: datetime | None,
    source: str,
    auth_test: Callable[..., Mapping[str, Any]],
) -> CapturedSlackSession:
    """Pick the token Slack accepts, given a store and its cookies."""
    cookie_d = str(cookies.get(SESSION_COOKIE) or "")
    if not cookie_d:
        raise SlackSessionCaptureError(
            "found no `d` session cookie for slack.com; the xoxc token alone is not a session "
            "(sign in to Slack on this machine, then retry)"
        )
    candidates = scan_local_storage_for_tokens(store_root)
    if not candidates:
        raise SlackSessionCaptureError(f"found no xoxc- token in {store_root}/Local Storage")

    header = f"{SESSION_COOKIE}={cookie_d}"
    last_error = ""
    for token in candidates:
        payload = auth_test(token=token, cookie_header=header)
        if payload.get("ok"):
            # On Enterprise Grid the client session authenticates against the ORG,
            # so auth.test returns an `E...` id here where the app token returns
            # the workspace `T...` id that every warehouse row is keyed by.
            # Storing the org id as team_id would fork the whole dataset, so the
            # two are kept apart and the caller resolves the workspace from
            # base_slack.teams.enterprise_id.
            reported = str(payload.get("team_id") or "")
            is_enterprise = reported.startswith("E")
            return CapturedSlackSession(
                source=source,
                token=token,
                cookie_d=cookie_d,
                team_id="" if is_enterprise else reported,
                enterprise_id=reported if is_enterprise else "",
                user_id=str(payload.get("user_id") or ""),
                team_url=str(payload.get("url") or ""),
                cookie_expires_at=cookie_expires_at,
            )
        last_error = str(payload.get("error") or "unknown_error")
    raise SlackSessionCaptureError(
        f"found {len(candidates)} xoxc- token(s) but no working one (last error: {last_error}); "
        "the stored session has probably been signed out"
    )


__all__ = [
    "CapturedSlackSession",
    "SLACK_APP_PROFILE",
    "SlackSessionCaptureError",
    "capture_slack_session",
    "scan_local_storage_for_tokens",
]


def _slack_post(method: str, *, token: str, cookie_header: str, form: Mapping[str, str] | None = None) -> dict[str, Any]:
    """POST to Slack with a *client* session (token + `d` cookie).

    Both parts are required together: the token alone returns `not_authed`, and
    the cookie alone has nothing to authorise. The browser-shaped headers are not
    decoration -- Slack's client endpoints reject requests that do not look like
    the web app.
    """
    import json as _json
    import urllib.error
    import urllib.parse
    import urllib.request

    body = urllib.parse.urlencode({"token": token, **(form or {})}).encode("utf-8")
    request = urllib.request.Request(
        f"https://slack.com/api/{method}",
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Cookie": cookie_header,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Slack_SSB/4.36.140",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return _json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": f"http_{exc.code}"}
    except (OSError, ValueError) as exc:  # pragma: no cover - network dependent
        return {"ok": False, "error": str(exc)}


def slack_auth_test(*, token: str, cookie_header: str) -> dict[str, Any]:
    return _slack_post("auth.test", token=token, cookie_header=cookie_header)


def discover_slack_session(*, source: str | None = None) -> CapturedSlackSession:
    """Capture a Slack session from the Slack app, or a browser as a fallback."""
    profiles = [SLACK_APP_PROFILE]
    if source:
        profiles = [p for p in profiles if p.key == source]
        if not profiles:
            raise SlackSessionCaptureError(f"unknown Slack session source {source!r}")

    errors: list[str] = []
    for profile in profiles:
        try:
            key = _safe_storage_key(profile)
        except ChatGPTCookieError as exc:
            errors.append(str(exc))
            continue
        for db in _cookie_dbs(profile):
            try:
                cookies = read_cookies_for_host(db, key, SLACK_COOKIE_HOST_SUFFIX)
            except Exception as exc:  # pragma: no cover - per-store failure
                errors.append(f"{profile.key}: {exc}")
                continue
            if SESSION_COOKIE not in cookies:
                continue
            return capture_slack_session(
                store_root=db.parent,
                cookies=cookies,
                cookie_expires_at=_cookie_expiry(db, SESSION_COOKIE),
                source=profile.key,
                auth_test=slack_auth_test,
            )
        errors.append(f"{profile.key}: no slack.com `d` cookie in any cookie store")
    raise SlackSessionCaptureError("; ".join(errors) or "no Slack session found")


def _cookie_expiry(db_path: Path, name: str) -> datetime | None:
    """Read a cookie's expiry without decrypting anything."""
    import shutil
    import sqlite3
    import tempfile
    from datetime import UTC, timedelta

    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "Cookies"
        shutil.copy(db_path, local)
        connection = sqlite3.connect(str(local))
        try:
            row = connection.execute(
                "SELECT expires_utc FROM cookies WHERE host_key LIKE ? AND name = ? LIMIT 1",
                (f"%{SLACK_COOKIE_HOST_SUFFIX}", name),
            ).fetchone()
        finally:
            connection.close()
    if not row or not row[0]:
        return None
    # Chromium timestamps are microseconds since 1601-01-01.
    return datetime(1601, 1, 1, tzinfo=UTC) + timedelta(microseconds=int(row[0]))


def probe_client_counts(session: CapturedSlackSession) -> dict[str, Any]:
    """Ask Slack, in ONE request, what has changed across every conversation.

    This is the whole point of capturing a client session: it replaces the
    ~950 per-conversation ``conversations.history`` polls the app token forces.
    Returns a non-secret summary of what came back.
    """
    payload = _slack_post(
        "client.counts",
        token=session.token,
        cookie_header=session.cookie_header(),
        form={"thread_counts_by_channel": "true", "org_wide_aware": "true"},
    )
    if not payload.get("ok"):
        return {"ok": False, "error": str(payload.get("error") or "unknown_error")}

    summary: dict[str, Any] = {"ok": True}
    total = 0
    with_latest = 0
    for bucket in ("channels", "ims", "mpims"):
        entries = payload.get(bucket) or []
        summary[bucket] = len(entries)
        total += len(entries)
        for entry in entries:
            if isinstance(entry, Mapping) and entry.get("latest"):
                with_latest += 1
    summary["total_conversations"] = total
    summary["with_latest_marker"] = with_latest
    return summary


def main(argv: list[str] | None = None) -> int:
    """Capture the local Slack session and prove client.counts works.

    Prints only non-secret identity plus the probe result, so the output is safe
    to paste. Publishing to the warehouse is deliberately a later step: this has
    to earn the keychain grant first.
    """
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m personal_data_warehouse.slack_session",
        description="Capture the local Slack session and check what it can see (read-only, publishes nothing).",
    )
    parser.add_argument("--source", help="Force a session source (default: the Slack desktop app).")
    args = parser.parse_args(argv)

    try:
        session = discover_slack_session(source=args.source)
    except SlackSessionCaptureError as exc:
        print(f"could not capture a Slack session: {exc}")
        return 1

    print("captured Slack session (nothing secret below):")
    for key, value in session.redacted().items():
        print(f"  {key}: {value}")

    probe = probe_client_counts(session)
    if not probe.get("ok"):
        print(f"\nclient.counts FAILED: {probe.get('error')}")
        print("the session is valid but cannot answer 'what changed' in one call; tell the agent this.")
        return 2

    print("\nclient.counts OK -- one request returned:")
    for key in ("channels", "ims", "mpims", "total_conversations", "with_latest_marker"):
        if key in probe:
            print(f"  {key}: {probe[key]}")
    print("\nThis is the call that replaces ~950 per-conversation polls. Paste this output back.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
