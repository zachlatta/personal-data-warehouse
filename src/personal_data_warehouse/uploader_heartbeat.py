"""Post one uploader run's verdict to the warehouse.

Run by ``bin/_pdw-upload-lib.sh`` (``pdw_post_heartbeat``) after every
remote-device uploader run, with the exit code the wrapper observed. It is the
only in-warehouse heartbeat those uploaders have: their data tables go quiet
both when nothing changed and when macOS silently revoked Full Disk Access,
and only a run record can tell the two apart on /pipelines.

Best effort by design: a failure here is logged and exits non-zero, but the
wrapper never lets it change the uploader's own exit code.
"""

from __future__ import annotations

import argparse
import logging
import socket
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from personal_data_warehouse.ingest_client import ingest_client_from_env

logger = logging.getLogger(__name__)


def default_device() -> str:
    return socket.gethostname().split(".")[0] or "unknown"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="record an uploader run in the warehouse")
    parser.add_argument("--pipeline", required=True, help="pipeline id(s) as registered in pipeline_health, comma-separated")
    parser.add_argument("--device", default=default_device())
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("--duration-seconds", type=int, default=0)
    parser.add_argument("--ran-at", default="", help="ISO-8601; defaults to now")
    parser.add_argument("--error", default="")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ran_at = args.ran_at or datetime.now(tz=UTC).isoformat()
    pipelines = [p.strip() for p in str(args.pipeline).split(",") if p.strip()]
    if not pipelines:
        print("uploader_heartbeat: --pipeline is required", file=sys.stderr)
        return 2
    try:
        client = ingest_client_from_env()
    except ValueError as error:
        print(f"uploader_heartbeat: {error}", file=sys.stderr)
        return 2
    failures = 0
    for pipeline in pipelines:
        try:
            client.post_heartbeat(
                pipeline=pipeline,
                device=args.device,
                ran_at=ran_at,
                exit_code=args.exit_code,
                duration_seconds=args.duration_seconds,
                error=args.error,
            )
        except Exception as error:  # noqa: BLE001 - never fail the uploader's run
            failures += 1
            print(f"uploader_heartbeat: {pipeline}: {error}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
