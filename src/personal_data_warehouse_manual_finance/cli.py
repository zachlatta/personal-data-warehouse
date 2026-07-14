"""`pdw ingest manual-finance` — upload finance documents through the app."""

from __future__ import annotations

import argparse
import fcntl
import sys
from contextlib import contextmanager
from pathlib import Path

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.ingest_client import ingest_client_from_env
from personal_data_warehouse_manual_finance.state import ManualFinanceUploadState, default_state_file
from personal_data_warehouse_manual_finance.sync import ManualFinanceUploadRunner


class CliLogger:
    def info(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)

    def warning(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Upload finance documents (statements, valuations, exports) through the app "
            "ingest API. Folder organization is preserved: each file's path relative to "
            "the upload root becomes its original_path hint, and its top folder the "
            "object key's account segment."
        )
    )
    parser.add_argument("paths", nargs="+", type=Path, help="Files or directories to upload")
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Base directory original_path is computed against (default: each directory argument, or a bare file's parent)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Maximum files to upload this run; 0 means no limit")
    parser.add_argument("--mode", choices=("incremental", "full"), default="incremental", help="Upload mode")
    parser.add_argument("--state-file", type=Path, default=default_state_file(), help="Incremental upload state path")
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=default_state_file().with_suffix(".lock"),
        help="Nonblocking lock path used to avoid overlapping runs",
    )
    args = parser.parse_args(argv)

    settings = load_settings(
        require_postgres=False,
        require_gmail=False,
        require_manual_finance=True,
    )
    if settings.manual_finance is None:
        raise RuntimeError("Manual finance uploads are not configured")

    logger = CliLogger()
    state = ManualFinanceUploadState.open(args.state_file, account=settings.manual_finance.account)

    with exclusive_lock(args.lock_file) as acquired:
        if not acquired:
            print("Manual finance upload skipped: another uploader run is active")
            return
        try:
            summary = ManualFinanceUploadRunner(
                account=settings.manual_finance.account,
                paths=args.paths,
                root=args.root,
                ingest_client=ingest_client_from_env(),
                logger=logger,
                limit=args.limit or None,
                mode=args.mode,
                upload_state=state,
            ).sync()
        finally:
            state.close()

    print(
        "Manual finance upload complete: "
        f"seen={summary.files_seen} "
        f"ignored={summary.files_ignored} "
        f"selected={summary.files_selected} "
        f"uploaded={summary.files_uploaded} "
        f"skipped={summary.files_skipped}"
    )


@contextmanager
def exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
