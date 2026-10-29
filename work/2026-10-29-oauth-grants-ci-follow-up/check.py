"""Read-only bounded operational checks for commit 0ff92eb follow-up.

Collects non-sensitive grant metadata, HTTP status counts on /mcp and
/oauth/token, and GitHub Actions check-run results for 0ff92eb. No secrets,
token identifiers, or email addresses are written.
"""

from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import urllib.request
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).parent
COMMIT_SHA = "0ff92ebbadc9c188cae1898fd7c543d758f40cc6"
DEPLOY_START_UTC = "2026-10-28T14:46:56Z"


def redact_client(raw: str) -> str:
    cleaned = raw.strip().strip('"')
    return re.sub(r"\([^)]*@[^)]*\)", "(account)", cleaned)


def query_warehouse_grants() -> None:
    host = os.environ["AUDIT_HOST"]
    container = os.environ["WAREHOUSE_CONTAINER"]
    sql = """
SET statement_timeout='30s';
SET max_parallel_workers_per_gather=0;
BEGIN READ ONLY;
SELECT
    kind,
    scope,
    regexp_replace(client_name, '\\([^)]*@[^)]*\\)', '(account)', 'g') AS client_label,
    created_by,
    created_at,
    last_used_at,
    expires_at,
    idle_timeout_seconds,
    revoked_at
FROM private.app_access_tokens
WHERE kind = 'oauth'
ORDER BY created_at ASC;
ROLLBACK;
""".strip()
    result = subprocess.run(
        [
            "ssh",
            host,
            f"sudo -n docker exec -i {container} psql -X -v ON_ERROR_STOP=1 -U postgres --csv",
        ],
        input=sql,
        text=True,
        capture_output=True,
        check=True,
    )
    lines = [
        line
        for line in result.stdout.splitlines()
        if line not in ("SET", "BEGIN", "ROLLBACK")
    ]
    (ROOT / "oauth_grants.csv").write_text("\n".join(lines) + "\n")


def query_loki_mcp_and_oauth() -> None:
    loki_script = os.environ["LOKI_SCRIPT"]
    selector = os.environ["APP_LOKI_SELECTOR"]
    result = subprocess.run(
        [
            loki_script,
            "--format-logs",
            "--since",
            "15h",
            "--limit",
            "5000",
            f'{selector} |~ "(path=/mcp|path=/oauth/token|oauth grant created)"',
        ],
        text=True,
        capture_output=True,
        check=True,
    )
    http_counts: Counter[tuple[str, str, int, str]] = Counter()
    grant_events = 0
    earliest = None
    latest = None
    for line in result.stdout.splitlines():
        m_time = re.search(r"time=(\S+)", line)
        if not m_time:
            continue
        ts = m_time.group(1)
        if ts < DEPLOY_START_UTC:
            continue
        if "oauth grant created" in line:
            grant_events += 1
            continue
        m_path = re.search(r"path=(\S+)", line)
        m_method = re.search(r"method=(\S+)", line)
        m_status = re.search(r"status=(\d+)", line)
        m_client = re.search(r'client=("[^"]*"|\S+)', line)
        if not (m_path and m_method and m_status and m_client):
            continue
        path = m_path.group(1)
        if path not in ("/mcp", "/oauth/token"):
            continue
        if earliest is None or ts < earliest:
            earliest = ts
        if latest is None or ts > latest:
            latest = ts
        http_counts[
            (
                path,
                m_method.group(1),
                int(m_status.group(1)),
                redact_client(m_client.group(1)),
            )
        ] += 1

    with (ROOT / "mcp_http_counts.csv").open("w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(
            ["path", "method", "status", "client_label", "requests", "window_start_utc", "window_end_utc", "grant_creation_logs"]
        )
        for (path, method, status, client_label), count in sorted(
            http_counts.items(), key=lambda item: (item[0][0], -item[1], item[0][2])
        ):
            writer.writerow(
                [
                    path,
                    method,
                    status,
                    client_label,
                    count,
                    earliest or "",
                    latest or "",
                    grant_events,
                ]
            )


def query_github_ci() -> None:
    url = f"https://api.github.com/repos/zachlatta/personal-data-warehouse/commits/{COMMIT_SHA}/check-runs"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    rows = []
    for run in payload.get("check_runs", []):
        html_url = run.get("html_url", "")
        m_wf = re.search(r"/actions/runs/(\d+)/job/", html_url)
        wf_id = m_wf.group(1) if m_wf else ""
        rows.append(
            (
                wf_id,
                run["id"],
                run["name"],
                run["status"],
                run["conclusion"],
                run["started_at"],
                run["completed_at"],
            )
        )
    rows.sort(key=lambda r: (r[0], r[5], r[1]))
    with (ROOT / "ci_check_runs.csv").open("w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(
            [
                "workflow_run_id",
                "check_run_id",
                "name",
                "status",
                "conclusion",
                "started_at",
                "completed_at",
            ]
        )
        writer.writerows(rows)


if __name__ == "__main__":
    query_warehouse_grants()
    query_loki_mcp_and_oauth()
    query_github_ci()
