"""Publish the local Slack client session to the warehouse.

Run on the Mac that is signed in to Slack:

    pdw slack publish-session [--dry-run]

The capture itself lives in ``slack_session``; this adds the two things that
need the warehouse rather than the laptop -- resolving the workspace id and
posting the credential over the same signed ingest path every other uploader
uses.
"""

from __future__ import annotations

from typing import Any
import json
import os

from personal_data_warehouse.ingest_client import ingest_client_from_env
from personal_data_warehouse.slack_session import (
    SlackSessionCaptureError,
    discover_slack_session,
    probe_client_counts,
)


def _resolve_account(explicit: str | None) -> str:
    """The label the credential is stored under.

    SLACK_ACCOUNTS comes first and the generic personal-email fallbacks come
    last, because the *sync* looks this credential up by its own Slack account
    label. Publishing under a different label stores a credential that reads
    healthy everywhere and that the sync can never find, so it would silently
    fall back to polling forever.
    """
    for candidate in (
        explicit,
        os.getenv("SLACK_ACCOUNT"),
        (os.getenv("SLACK_ACCOUNTS") or "").split(",")[0].strip() or None,
        os.getenv("AGENT_SESSIONS_ACCOUNT"),
        os.getenv("APPLE_MESSAGES_ACCOUNT"),
        (os.getenv("GMAIL_ACCOUNTS") or "").split(",")[0].strip() or None,
    ):
        if candidate:
            return candidate
    return "zrl"


def _workspace_ids_for_enterprise(enterprise_id: str) -> list[str]:
    """Workspace ids the warehouse already knows under this org.

    Asked through the app's read-only SQL tool -- the same static-bearer HTTP
    API `pdw sql` uses -- rather than a direct database connection, because this
    runs on a laptop that has neither the warehouse credential nor a route to
    Postgres.
    """
    import requests

    from personal_data_warehouse.relations import relation

    base_url = os.getenv("PDW_API_URL") or os.getenv("MCP_BASE_URL") or ""
    token = os.getenv("PDW_SECRET_TOKEN") or os.getenv("MCP_SECRET_TOKEN") or ""
    if not base_url or not token:
        raise SlackSessionCaptureError(
            "PDW_API_URL/PDW_SECRET_TOKEN are not set; run `pdw login` first"
        )
    teams = relation("slack_teams")
    escaped = enterprise_id.replace("'", "''")
    response = requests.post(
        f"{base_url.rstrip('/')}/api/tools/sql",
        json={
            "question": "Which workspace does this Slack enterprise session belong to?",
            "sql": (
                f"SELECT team_id FROM {teams.schema}.{teams.name} "
                f"WHERE enterprise_id = '{escaped}' ORDER BY team_id"
            ),
            # The server's name for newline-delimited JSON; an unknown format
            # silently falls back to csv, so this string matters.
            "format": "ndjson",
        },
        headers={"Authorization": f"Bearer {os.getenv('PDW_CLIENT_NAME') or 'pdw'}:{token}"},
        timeout=60,
    )
    response.raise_for_status()
    data = response.json().get("data") or {}
    if data.get("error"):
        raise SlackSessionCaptureError(f"workspace lookup failed: {data['error']}")
    found: list[str] = []
    for line in (data.get("rows") or "").splitlines():
        line = line.strip()
        if line:
            team_id = json.loads(line).get("team_id")
            if team_id:
                found.append(str(team_id))
    return found


def resolve_team_id(*, team_id: str, enterprise_id: str) -> str:
    """The workspace id this credential's writes must be keyed by.

    A client session on Enterprise Grid only knows the org, but every warehouse
    row is keyed by the workspace. Guessing is not acceptable here: keying to the
    wrong id forks the dataset silently, so an ambiguous or unknown org raises.
    """
    if team_id:
        return team_id
    if not enterprise_id:
        raise SlackSessionCaptureError("the session reported neither a workspace nor an enterprise id")
    candidates = _workspace_ids_for_enterprise(enterprise_id)
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise SlackSessionCaptureError(
            f"no workspace in the warehouse belongs to enterprise {enterprise_id}; "
            "run a Slack sync first, or pass --team-id explicitly"
        )
    raise SlackSessionCaptureError(
        f"enterprise {enterprise_id} covers more than one workspace ({', '.join(candidates)}); "
        "pass --team-id to say which one this credential is for"
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="pdw slack publish-session",
        description="Capture the local Slack client session and publish it to the warehouse.",
    )
    parser.add_argument("--account", help="Account label the credential is stored under.")
    parser.add_argument("--session-key", default="default")
    parser.add_argument("--source", help="Force a session source (default: the Slack desktop app).")
    parser.add_argument("--team-id", default="", help="Workspace id, when the org covers several.")
    parser.add_argument("--dry-run", action="store_true", help="Capture and validate without publishing.")
    args = parser.parse_args(argv)

    report: dict[str, Any] = {"published": False}
    try:
        session = discover_slack_session(source=args.source)
    except SlackSessionCaptureError as exc:
        print(json.dumps({"error": str(exc)}, indent=2))
        return 1
    report["session"] = session.redacted()

    probe = probe_client_counts(session)
    report["client_counts"] = probe
    if not probe.get("ok"):
        # A session that cannot answer "what changed" is useless for the sync,
        # so publishing it would only produce a credential that looks healthy.
        report["error"] = "client.counts failed; not publishing"
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2

    if args.dry_run:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    try:
        team_id = args.team_id or resolve_team_id(
            team_id=session.team_id, enterprise_id=session.enterprise_id
        )
    except SlackSessionCaptureError as exc:
        report["error"] = str(exc)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 3

    account = _resolve_account(args.account)
    client = ingest_client_from_env()
    ack = client.publish_slack_session(
        account=account,
        session_token=session.token,
        session_cookie=session.cookie_d,
        team_id=team_id,
        enterprise_id=session.enterprise_id,
        user_id=session.user_id,
        team_url=session.team_url,
        cookie_expires_at=session.cookie_expires_at.isoformat() if session.cookie_expires_at else "",
        session_key=args.session_key,
        source_app=session.source,
    )
    report["published"] = True
    report["account"] = account
    report["team_id"] = team_id
    report["acknowledgement"] = dict(ack)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
