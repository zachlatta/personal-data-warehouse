from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
from pathlib import Path
import os
import sys

from personal_data_warehouse.config import load_settings
from personal_data_warehouse_voice_memos.cli import build_google_drive_service
from personal_data_warehouse_voice_memos.google_drive_storage import GoogleDriveObjectStore, is_transient_google_error
from personal_data_warehouse_voice_memos.network import NetworkPolicy, preflight_google_drive
from personal_data_warehouse_apple_messages.state import AppleMessagesUploadState, default_state_file
from personal_data_warehouse_apple_messages.sync import AppleMessagesUploadRunner


class CliLogger:
    def info(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)

    def warning(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local macOS Apple Messages batches to Google Drive.")
    parser.add_argument("--mode", choices=("incremental", "full"), default="incremental", help="Upload mode")
    parser.add_argument("--state-file", type=Path, default=default_state_file(), help="Incremental upload state path")
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=default_state_file().with_suffix(".lock"),
        help="Nonblocking lock path used to avoid overlapping LaunchAgent runs",
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum manifest records to upload; 0 means unlimited")
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of parallel attachment upload workers; 0 uses APPLE_MESSAGES_UPLOAD_WORKERS or 4",
    )
    parser.add_argument("--network-diagnostics", action="store_true", help="Print network guard diagnostics and exit")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be greater than or equal to 0")
    if args.workers < 0:
        parser.error("--workers must be greater than or equal to 0")

    if args.network_diagnostics:
        print_network_diagnostics()
        return

    settings = load_settings(require_postgres=False, require_gmail=False, require_apple_messages=True)
    if settings.apple_messages is None:
        raise RuntimeError("Apple Messages sync is not configured")
    if settings.apple_messages.storage_backend != "google_drive":
        raise RuntimeError(f"Unsupported Apple Messages storage backend: {settings.apple_messages.storage_backend}")

    logger = CliLogger()
    request_timeout_seconds = int(os.getenv("APPLE_MESSAGES_UPLOAD_REQUEST_TIMEOUT_SECONDS", "30"))
    preflight_timeout_seconds = float(os.getenv("APPLE_MESSAGES_UPLOAD_PREFLIGHT_TIMEOUT_SECONDS", "5"))
    workers = args.workers or settings.apple_messages.upload_workers
    workers = max(1, workers)
    state = AppleMessagesUploadState.open(
        args.state_file,
        account=settings.apple_messages.account,
        store_path=settings.apple_messages.store_path,
    )
    try:
        with exclusive_lock(args.lock_file) as acquired:
            if not acquired:
                print("Apple Messages upload skipped: another uploader run is active")
                return
            summary = AppleMessagesUploadRunner(
                account=settings.apple_messages.account,
                store_path=settings.apple_messages.store_path,
                object_store_factory=lambda: build_google_drive_object_store(
                    account=settings.apple_messages.google_drive_account,
                    settings=settings,
                    folder_id=settings.apple_messages.google_drive_folder_id,
                    request_timeout_seconds=request_timeout_seconds,
                ),
                logger=logger,
                upload_state=state,
                mode=args.mode,
                limit=args.limit or None,
                attachment_bytes_per_run=settings.apple_messages.attachment_bytes_per_run,
                attachment_count_per_run=settings.apple_messages.attachment_count_per_run,
                workers=workers,
                before_upload_check=build_before_upload_check(preflight_timeout_seconds=preflight_timeout_seconds),
            ).sync()
    except Exception as exc:
        if is_transient_exception(exc):
            print(f"Apple Messages upload skipped after transient network failure: {exc}")
            return
        raise
    finally:
        state.close()

    print(
        "Apple Messages upload complete: "
        f"messages={summary.messages_seen} "
        f"attachments={summary.attachments_seen} "
        f"selected={summary.records_selected} "
        f"skipped={summary.records_skipped} "
        f"batches={summary.batches_uploaded} "
        f"attachment_uploads={summary.attachments_uploaded} "
        f"attachment_deferred={summary.attachments_deferred}"
    )


def build_google_drive_object_store(*, account: str, settings, folder_id: str, request_timeout_seconds: int):
    service = build_google_drive_service(
        account=account,
        settings=settings,
        request_timeout_seconds=request_timeout_seconds,
    )
    return GoogleDriveObjectStore(
        folder_id=folder_id,
        service=service,
        source="apple_messages",
        legacy_sources=(),
        audio_kind="apple_message_attachment",
        metadata_kind="apple_message_export_batch",
    )


def build_before_upload_check(*, preflight_timeout_seconds: float):
    policy = NetworkPolicy.from_env(
        prefix="APPLE_MESSAGES_UPLOAD",
        fallback_prefix="VOICE_MEMOS_UPLOAD",
    )

    def before_upload_check() -> str | None:
        decision = policy.check()
        if not decision.allowed:
            return decision.reason
        preflight = preflight_google_drive(timeout_seconds=preflight_timeout_seconds)
        if not preflight.allowed:
            return preflight.reason
        return None

    return before_upload_check


def print_network_diagnostics() -> None:
    policy = NetworkPolicy.from_env(prefix="APPLE_MESSAGES_UPLOAD", fallback_prefix="VOICE_MEMOS_UPLOAD")
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
        if isinstance(current, Exception) and is_transient_google_error(current):
            return True
        current = current.__cause__ or current.__context__
    return False


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
