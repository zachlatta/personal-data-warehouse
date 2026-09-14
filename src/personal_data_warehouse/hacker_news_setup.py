"""``pdw hn publish-session`` -- capture and publish the news.ycombinator.com login.

Run this on the Mac whose Chrome is signed in to Hacker News. It reads the
``user`` cookie locally, checks it against a login-only page, and publishes
it to the app's HMAC-signed endpoint so the server-side sync can read the
upvoted and hidden lists. The cookie never touches a log line or stdout.

HN's login cookie is long-lived, so this is first-time setup and repair after
a sign-out; /pipelines reads ``action_required`` when it is needed again.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from personal_data_warehouse.hacker_news_api import HackerNewsClient
from personal_data_warehouse.hacker_news_session import (
    HackerNewsSessionCaptureError,
    capture_hacker_news_session,
)
from personal_data_warehouse.ingest_client import ingest_client_from_env


def _resolve_account(explicit: str | None, captured_user_id: str) -> str:
    """The HN username: explicit flag, then HACKER_NEWS_ACCOUNT, then the cookie's own."""
    account = (explicit or os.getenv("HACKER_NEWS_ACCOUNT") or captured_user_id or "").strip()
    if not account:
        raise SystemExit("no account resolved; pass --account <hn-username>")
    return account


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pdw hn publish-session", description=__doc__)
    parser.add_argument("--account", default=None, help="HN username the session belongs to")
    parser.add_argument("--session-key", default="default")
    parser.add_argument("--browser", default=None, help="chrome, brave, edge, arc, chromium, vivaldi")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="capture and validate without publishing (verifies cookie decryption)",
    )
    parser.add_argument("--skip-check", action="store_true", help="do not validate the cookie against HN")
    args = parser.parse_args(argv)

    try:
        captured = capture_hacker_news_session(browser=args.browser)
    except HackerNewsSessionCaptureError as error:
        print(f"hn publish-session: {error}", file=sys.stderr)
        return 2

    account = _resolve_account(args.account, captured.user_id)
    report: dict[str, Any] = dict(captured.redacted())
    report["account"] = account
    if captured.user_id != account:
        print(
            f"hn publish-session: the browser is signed in as {captured.user_id!r} but the account is "
            f"{account!r}; refusing to publish one user's cookie under another's name.",
            file=sys.stderr,
        )
        return 2

    if not args.skip_check:
        client = HackerNewsClient(session_cookie=captured.cookie_header)
        if not client.check_login(user_id=account):
            print(
                "hn publish-session: news.ycombinator.com answered a login page for this cookie; "
                "sign in again in that browser and retry.",
                file=sys.stderr,
            )
            return 1
        report["validated"] = True

    if args.dry_run:
        report["published"] = False
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    ingest = ingest_client_from_env()
    ack = ingest.publish_hacker_news_session(
        account=account,
        session_token=captured.cookie_header,
        session_key=args.session_key,
        source_browser=captured.browser,
    )
    report["published"] = True
    report["acknowledgement"] = dict(ack)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
