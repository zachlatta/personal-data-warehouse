"""``pdw whoop publish-session`` -- capture and publish the WHOOP browser session.

Run this on the Mac whose Chrome holds an app.whoop.com login. It reads the
session cookies locally, then publishes them to the app's HMAC-signed ingest
endpoint so the server-side poller can authenticate. The tokens never touch
Drive, a log line, or this process's stdout.

Because every refresh rotates the refresh token and slides its 30-day window
forward, a healthy sync never needs this command again -- it is for first setup
and for repair after the session is revoked or left idle past 30 days.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
import argparse
import json
import sys

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.ingest_client import IngestClient
from personal_data_warehouse.whoop_private_session import (
    WhoopSessionCaptureError,
    capture_whoop_session,
)


def _resolve_account(explicit: str | None) -> str:
    if explicit:
        return explicit
    settings = load_settings(require_gmail=False)
    for candidate in (
        getattr(getattr(settings, "whoop", None), "account", None),
        getattr(settings, "voice_memos_account", None),
    ):
        if candidate:
            return str(candidate)
    raise SystemExit(
        "no account resolved; pass --account (it keys the credential and the sync cursors)"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pdw whoop publish-session", description=__doc__)
    parser.add_argument("--account", default=None, help="account label keying the credential")
    parser.add_argument("--session-key", default="default")
    parser.add_argument("--browser", default=None, help="chrome, brave, edge, arc, chromium, vivaldi")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="capture and report without publishing (verifies cookie decryption)",
    )
    args = parser.parse_args(argv)

    try:
        captured = capture_whoop_session(browser=args.browser)
    except WhoopSessionCaptureError as error:
        print(f"whoop publish-session: {error}", file=sys.stderr)
        return 2

    now = datetime.now(UTC)
    report: dict[str, Any] = dict(captured.redacted())
    report["access_token_valid_for_hours"] = round(
        (captured.access_expires_at - now).total_seconds() / 3600, 1
    )

    if captured.access_expires_at <= now:
        # Not fatal: the poller refreshes on first use. Say so rather than
        # letting a stale-looking report read as a failure.
        report["note"] = "access token already expired; the server refreshes it on first use"

    if args.dry_run:
        report["published"] = False
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    account = _resolve_account(args.account)
    client = IngestClient.from_env()
    ack = client.publish_whoop_private_session(
        account=account,
        access_token=captured.access_token,
        refresh_token=captured.refresh_token,
        access_expires_at=captured.access_expires_at.isoformat(),
        refresh_expires_at=captured.refresh_expires_at.isoformat(),
        session_key=args.session_key,
        source_browser=captured.browser,
    )
    report["published"] = True
    report["account"] = account
    report["acknowledgement"] = dict(ack)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
