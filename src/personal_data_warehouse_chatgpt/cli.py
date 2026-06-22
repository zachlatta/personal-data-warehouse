"""``pdw chatgpt`` - client-side setup for server-side ChatGPT ingestion.

The only subcommand is ``publish-session``: it captures the chatgpt.com web
session from a local Chrome-family browser, validates it against ChatGPT, and
publishes it to the warehouse (HMAC-signed, stored in Postgres) so the
server-side Dagster poller can sync conversations. This is the one manual,
interactive step a person runs, and re-runs whenever the server reports the
session expired.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

from personal_data_warehouse.chatgpt_backend import ChatGPTAuthError, ChatGPTBackendClient
from personal_data_warehouse.chatgpt_cookies import ChatGPTCookieError
from personal_data_warehouse.chatgpt_setup import ensure_browser, ensure_logged_in
from personal_data_warehouse.ingest_client import ingest_client_from_env


def _first_csv_env(name: str) -> str:
    raw = os.getenv(name, "")
    for part in raw.split(","):
        value = part.strip()
        if value:
            return value
    return ""


def _resolve_account(explicit: str | None) -> str:
    account = (
        explicit
        or os.getenv("CHATGPT_ACCOUNT")
        or os.getenv("AGENT_SESSIONS_ACCOUNT")
        or os.getenv("APPLE_MESSAGES_ACCOUNT")
        or os.getenv("VOICE_MEMOS_ACCOUNT")
        or _first_csv_env("GMAIL_ACCOUNTS")
        or ""
    ).strip()
    if not account:
        raise SystemExit(
            "no ChatGPT account configured: pass --account or set CHATGPT_ACCOUNT/GMAIL_ACCOUNTS "
            "(this only labels/keys the stored session)."
        )
    return account


def _publish_session(args: argparse.Namespace) -> int:
    import requests

    account = _resolve_account(args.account)

    try:
        profile = ensure_browser(prefer=args.browser, auto_install=not args.no_install)
        captured = ensure_logged_in(profile)
    except ChatGPTCookieError as exc:
        print(f"pdw chatgpt: {exc}", file=sys.stderr)
        return 1

    client = ChatGPTBackendClient(
        session_credential=captured.cookie_header, session=requests.Session()
    )
    try:
        session_payload = client.fetch_auth_session()
    except ChatGPTAuthError as exc:
        print(
            f"pdw chatgpt: found a session in {captured.browser} but ChatGPT rejected it ({exc}). "
            "Log into chatgpt.com in that browser and retry.",
            file=sys.stderr,
        )
        return 1

    user = session_payload.get("user") if isinstance(session_payload.get("user"), dict) else {}
    signed_in = str(user.get("email") or user.get("name") or "unknown account")
    print(
        f"Found ChatGPT session in {captured.browser} "
        f"({captured.cookie_count} chatgpt.com cookies). Signed in as {signed_in}."
    )

    if args.dry_run:
        print("--dry-run: not publishing the session.")
        return 0

    try:
        ingest = ingest_client_from_env()
    except ValueError as exc:
        print(
            f"pdw chatgpt: cannot publish; warehouse upload is not configured: {exc}. "
            "Run `pdw login` (or set PDW_API_URL + PDW_SECRET_TOKEN).",
            file=sys.stderr,
        )
        return 1

    ack = ingest.publish_chatgpt_session(
        account=account,
        session_token=captured.cookie_header,
        session_key=args.session_key,
        source_browser=captured.browser,
    )
    token_sha = str(ack.get("token_sha256", ""))[:12]
    print(
        f"Published ChatGPT session for {account} (key={args.session_key}, "
        f"browser={captured.browser}, token_sha256={token_sha}...). "
        "The server-side poller will pick it up on its next tick."
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pdw chatgpt", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    publish = sub.add_parser(
        "publish-session",
        help="Capture the chatgpt.com session from a local browser and publish it to the warehouse.",
    )
    publish.add_argument(
        "--browser",
        default=None,
        help="Force a specific browser (chrome, brave, edge, arc, chromium, vivaldi). Default: auto-detect.",
    )
    publish.add_argument(
        "--no-install",
        action="store_true",
        help="Do not auto-install a browser if none is found (error instead).",
    )
    publish.add_argument(
        "--account",
        default=None,
        help="Account label/key for the stored session (default: $CHATGPT_ACCOUNT/fallback).",
    )
    publish.add_argument(
        "--session-key",
        default="default",
        help="Session key, for multiple ChatGPT accounts (default: 'default').",
    )
    publish.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report the session without publishing it.",
    )
    publish.set_defaults(func=_publish_session)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
