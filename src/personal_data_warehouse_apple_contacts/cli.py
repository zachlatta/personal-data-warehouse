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
    preflight_app_ingest,
)
from personal_data_warehouse_apple_contacts.mutation_worker import (
    apple_contacts_mutations_enabled,
    process_apple_contacts_mutations,
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
    parser.add_argument("--no-mutations", action="store_true", help="Skip applying approved Apple Contacts mutations in this run")
    parser.add_argument("--mutations-only", action="store_true", help="Apply approved Apple Contacts mutations and skip the upload stage")
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
            # Mutations run BEFORE the upload stage on purpose: a card this run writes is
            # then picked up by the same run's scan, so an approved edit reaches the
            # warehouse in one cycle instead of waiting five minutes for the next one.
            if not args.no_mutations and apple_contacts_mutations_enabled():
                print(run_apple_contacts_mutations())
            if args.mutations_only:
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
    finally:
        state.close()

    print(
        "Apple Contacts upload complete: "
        f"seen={summary.contacts_seen} selected={summary.contacts_selected} "
        f"skipped={summary.contacts_skipped} deleted={summary.contacts_deleted} "
        f"deferred={summary.contacts_deferred} batches={summary.batches_uploaded}"
    )


def run_apple_contacts_mutations() -> str:
    """Apply approved Apple Contacts mutations, never failing the upload run.

    The uploader is the source of truth for card *data*; mutations are a rider on it. A
    warehouse that is unreachable, or a Contacts.app that refuses one edit, must not stop
    the address book from syncing -- so every failure here is reported and swallowed.
    """

    try:
        from personal_data_warehouse.config import load_settings as _load_settings
        from personal_data_warehouse.warehouse import warehouse_from_settings

        settings = _load_settings(require_postgres=True, require_gmail=False)
        warehouse = warehouse_from_settings(settings)
    except Exception as error:  # noqa: BLE001 - reported, never fatal to the upload
        return f"Apple Contacts mutations skipped: warehouse unavailable ({error})"
    try:
        return process_apple_contacts_mutations(warehouse=warehouse).describe()
    except Exception as error:  # noqa: BLE001 - reported, never fatal to the upload
        return f"Apple Contacts mutations failed: {error}"
    finally:
        warehouse.close()


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


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
