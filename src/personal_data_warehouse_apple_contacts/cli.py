from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
from pathlib import Path
import os
import sys

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.ingest_client import ingest_client_from_env
from personal_data_warehouse_voice_memos.network import (
    NetworkPolicy,
    is_transient_upload_error,
    preflight_app_ingest,
)
from personal_data_warehouse_apple_contacts.state import AppleContactsUploadState, default_state_file
from personal_data_warehouse_apple_contacts.sync import AppleContactsUploadRunner


class CliLogger:
    def info(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)

    def warning(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local macOS Apple/iCloud Contacts through the app ingest API.")
    parser.add_argument("--mode", choices=("incremental", "full"), default="incremental")
    parser.add_argument("--state-file", type=Path, default=default_state_file())
    parser.add_argument("--lock-file", type=Path, default=default_state_file().with_suffix(".lock"))
    parser.add_argument("--limit", type=int, default=None, help="Maximum changed contacts to upload; 0 means unlimited")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be greater than or equal to 0")

    settings = load_settings(require_postgres=False, require_gmail=False, require_apple_contacts=True)
    if settings.apple_contacts is None:
        raise RuntimeError("Apple Contacts sync is not configured")
    state = AppleContactsUploadState.open(
        args.state_file,
        account=settings.apple_contacts.account,
        store_path=settings.apple_contacts.store_path,
    )
    try:
        with exclusive_lock(args.lock_file) as acquired:
            if not acquired:
                print("Apple Contacts upload skipped: another uploader run is active")
                return
            summary = AppleContactsUploadRunner(
                account=settings.apple_contacts.account,
                store_path=settings.apple_contacts.store_path,
                ingest_client=ingest_client_from_env(),
                logger=CliLogger(),
                upload_state=state,
                mode=args.mode,
                limit=args.limit or None,
                before_upload_check=build_before_upload_check(),
            ).sync()
    except Exception as exc:
        if is_transient_exception(exc):
            print(f"Apple Contacts upload skipped after transient network failure: {exc}")
            return
        raise
    finally:
        state.close()

    print(
        "Apple Contacts upload complete: "
        f"seen={summary.contacts_seen} selected={summary.contacts_selected} "
        f"skipped={summary.contacts_skipped} deleted={summary.contacts_deleted} "
        f"deferred={summary.contacts_deferred} batches={summary.batches_uploaded}"
    )


def build_before_upload_check():
    policy = NetworkPolicy.from_env(
        prefix="APPLE_CONTACTS_UPLOAD",
        fallback_prefix="VOICE_MEMOS_UPLOAD",
    )
    timeout_seconds = float(os.getenv("APPLE_CONTACTS_UPLOAD_PREFLIGHT_TIMEOUT_SECONDS", "5"))

    def before_upload_check() -> str | None:
        decision = policy.check()
        if not decision.allowed:
            return decision.reason
        preflight = preflight_app_ingest(timeout_seconds=timeout_seconds)
        return None if preflight.allowed else preflight.reason

    return before_upload_check


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


def is_transient_exception(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, Exception) and is_transient_upload_error(current):
            return True
        current = current.__cause__ or current.__context__
    return False


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
