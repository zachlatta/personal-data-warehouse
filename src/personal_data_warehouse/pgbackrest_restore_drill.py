"""Record a verified pgBackRest restore drill in ``ops.pgbackrest_health``.

A backup that has not been restored is a hypothesis. The backup loop can say a
backup EXISTS; only a human (or an agent) who restored one into a throwaway
cluster and counted it can say it RESTORES, and until 2026-08-28 the only
record of that was a commit message. This is the recorder:

    uv run python -m personal_data_warehouse.pgbackrest_restore_drill record \\
        --label 20260827-032703F_20260827-050637D \\
        --restored-url postgres://.../postgres \\
        --note "restored into a fresh volume on mew, promoted, counted"

``--restored-url`` counts ``timeline.events`` in the RESTORED cluster (so the
number is measured, not typed); ``--rows`` is the escape hatch for a drill
counted by hand. The record lands in the production warehouse named by
``POSTGRES_DATABASE_URL`` and shows on ``marts_ops.pgbackrest_health`` /
``/pipelines`` as ``restore_status``; older than
``PGBACKREST_RESTORE_DRILL_STALE_SECONDS`` it reads ``attention``.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime

import psycopg2

from .postgres import PostgresWarehouse
from .relations import expand_relations

RESTORED_ROW_COUNT_SQL = "SELECT count(*) FROM @timeline_events"


def count_restored_rows(restored_url: str) -> int:
    """Count timeline.events in the restored cluster. Raises if unreachable."""
    with psycopg2.connect(restored_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(expand_relations(RESTORED_ROW_COUNT_SQL))
            (count,) = cursor.fetchone()
    return int(count)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    record = sub.add_parser("record", help="record a restore that was performed and counted")
    record.add_argument("--stanza", default=os.getenv("PGBACKREST_STANZA", "pdw"))
    record.add_argument("--label", required=True, help="the pgBackRest backup label that was restored")
    record.add_argument("--restored-url", help="connection URL of the RESTORED cluster; timeline.events is counted there")
    record.add_argument("--rows", type=int, help="rows counted by hand, when --restored-url is not reachable")
    record.add_argument("--note", default="", help="where it was restored and what was checked")
    record.add_argument(
        "--verified-at",
        help="ISO-8601 time the drill completed (default: now); for recording a drill after the fact",
    )
    record.add_argument("--database-url", default=os.getenv("POSTGRES_DATABASE_URL", ""))
    return parser


def run_record(args: argparse.Namespace, *, out=sys.stdout) -> int:
    if bool(args.restored_url) == (args.rows is not None):
        raise SystemExit("record: give exactly one of --restored-url (measured) or --rows (counted by hand)")
    rows = count_restored_rows(args.restored_url) if args.restored_url else int(args.rows)
    verified_at = (
        datetime.fromisoformat(args.verified_at).astimezone(UTC) if args.verified_at else None
    )
    warehouse = PostgresWarehouse(args.database_url)
    warehouse.ensure_pipeline_health_tables()
    warehouse.record_pgbackrest_restore_drill(
        stanza=args.stanza,
        label=args.label,
        rows=rows,
        note=args.note,
        verified_at=verified_at,
    )
    print(
        f"recorded restore drill for stanza {args.stanza}: label {args.label}, {rows:,} rows"
        f" ({'measured' if args.restored_url else 'counted by hand'})",
        file=out,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "record":
        return run_record(args)
    raise SystemExit(f"unknown command {args.command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
