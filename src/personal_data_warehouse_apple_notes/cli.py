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
from personal_data_warehouse_apple_notes.state import AppleNotesUploadState, default_state_file
from personal_data_warehouse_apple_notes.sync import AppleNotesUploadRunner


class CliLogger:
    def info(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)

    def warning(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local macOS Apple Notes revisions through the app ingest API.")
    parser.add_argument("--mode", choices=("incremental", "full"), default="incremental", help="Upload mode")
    parser.add_argument("--state-file", type=Path, default=default_state_file(), help="Incremental upload state path")
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=default_state_file().with_suffix(".lock"),
        help="Nonblocking lock path used to avoid overlapping LaunchAgent runs",
    )
    parser.add_argument(
        "--network-diagnostics",
        action="store_true",
        help="Print network guard diagnostics and exit",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum changed or deleted notes to upload in this run; 0 means unlimited",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of parallel upload workers; 0 uses APPLE_NOTES_UPLOAD_WORKERS or 4",
    )
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be greater than or equal to 0")
    if args.workers < 0:
        parser.error("--workers must be greater than or equal to 0")

    if args.network_diagnostics:
        print_network_diagnostics()
        return

    settings = load_settings(
        require_postgres=False,
        require_gmail=False,
        require_apple_notes=True,
    )
    if settings.apple_notes is None:
        raise RuntimeError("Apple Notes sync is not configured")

    logger = CliLogger()
    state = AppleNotesUploadState.load(
        args.state_file,
        account=settings.apple_notes.account,
        store_path=settings.apple_notes.store_path,
    )
    preflight_timeout_seconds = float(os.getenv("APPLE_NOTES_UPLOAD_PREFLIGHT_TIMEOUT_SECONDS", "5"))
    workers = args.workers or int(os.getenv("APPLE_NOTES_UPLOAD_WORKERS", "4"))
    workers = max(1, workers)

    try:
        with exclusive_lock(args.lock_file) as acquired:
            if not acquired:
                print("Apple Notes upload skipped: another uploader run is active")
                return
            try:
                summary = AppleNotesUploadRunner(
                    account=settings.apple_notes.account,
                    store_path=settings.apple_notes.store_path,
                    ingest_client=ingest_client_from_env(),
                    logger=logger,
                    upload_state=state,
                    mode=args.mode,
                    limit=args.limit or None,
                    workers=workers,
                    state_save_callback=lambda: state.save(args.state_file),
                    before_upload_check=build_before_upload_check(preflight_timeout_seconds=preflight_timeout_seconds),
                ).sync()
            finally:
                state.save(args.state_file)
    except Exception as exc:
        if is_transient_exception(exc):
            print(f"Apple Notes upload skipped after transient network failure: {exc}")
            return
        raise

    print(
        "Apple Notes upload complete: "
        f"seen={summary.notes_seen} "
        f"selected={summary.notes_selected} "
        f"revisions={summary.revisions_uploaded} "
        f"skipped={summary.notes_skipped} "
        f"deferred={summary.notes_deferred} "
        f"deleted={summary.notes_deleted} "
        f"attachments={summary.attachments_uploaded} "
        f"missing={summary.attachments_missing}"
    )


def build_before_upload_check(*, preflight_timeout_seconds: float):
    policy = NetworkPolicy.from_env(
        prefix="APPLE_NOTES_UPLOAD",
        fallback_prefix="VOICE_MEMOS_UPLOAD",
    )

    def before_upload_check() -> str | None:
        decision = policy.check()
        if not decision.allowed:
            return decision.reason
        preflight = preflight_app_ingest(timeout_seconds=preflight_timeout_seconds)
        if not preflight.allowed:
            return preflight.reason
        return None

    return before_upload_check


def print_network_diagnostics() -> None:
    policy = NetworkPolicy.from_env(prefix="APPLE_NOTES_UPLOAD", fallback_prefix="VOICE_MEMOS_UPLOAD")
    context = policy.context()
    decision = policy.check()
    if context is None:
        print("Network: no default route")
        print(f"Decision: {'allowed' if decision.allowed else 'blocked'} ({decision.reason})")
        return
    print(f"Default interface: {context.interface}")
    print(f"Hardware port: {context.hardware_port}")
    if context.hardware_port.lower() == "wi-fi":
        probe = policy.ssid_probe(context.interface)
        print(f"Wi-Fi SSID: {probe.ssid or '<unavailable>'}")
        print(f"SSID source: {probe.source}")
        if probe.detail:
            print(f"SSID detail: {probe.detail}")
    print(f"Decision: {'allowed' if decision.allowed else 'blocked'} ({decision.reason})")


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
