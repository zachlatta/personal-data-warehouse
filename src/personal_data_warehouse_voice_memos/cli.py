from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
from pathlib import Path
import os
import sys

import google_auth_httplib2
import httplib2
from googleapiclient.discovery import build

from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE, load_settings
from personal_data_warehouse.google_auth import load_google_credentials
from personal_data_warehouse_voice_memos.google_drive_storage import GoogleDriveObjectStore, is_transient_google_error
from personal_data_warehouse_voice_memos.network import NetworkPolicy, preflight_google_drive
from personal_data_warehouse_voice_memos.state import VoiceMemosUploadState, default_state_file
from personal_data_warehouse_voice_memos.sync import VoiceMemosUploadRunner


class CliLogger:
    def info(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)

    def warning(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local macOS Voice Memos files to Google Drive.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum recordings to upload; 0 means no limit")
    parser.add_argument("--workers", type=int, default=0, help="Number of parallel upload workers; 0 picks a mode-specific default")
    parser.add_argument("--mode", choices=("incremental", "full"), default="incremental", help="Upload mode")
    parser.add_argument("--state-file", type=Path, default=default_state_file(), help="Incremental upload state path")
    parser.add_argument(
        "--min-file-age-seconds",
        type=int,
        default=120,
        help="Do not upload recordings modified more recently than this",
    )
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=default_state_file().with_suffix(".lock"),
        help="Nonblocking lock path used to avoid overlapping cron runs",
    )
    parser.add_argument(
        "--network-diagnostics",
        action="store_true",
        help="Print network guard diagnostics and exit",
    )
    args = parser.parse_args()

    if args.network_diagnostics:
        print_network_diagnostics()
        return

    settings = load_settings(
        require_clickhouse=False,
        require_gmail=False,
        require_voice_memos=True,
    )
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos sync is not configured")
    if settings.voice_memos.storage_backend != "google_drive":
        raise RuntimeError(f"Unsupported Voice Memos storage backend: {settings.voice_memos.storage_backend}")

    logger = CliLogger()
    state = VoiceMemosUploadState.load(
        args.state_file,
        account=settings.voice_memos.account,
        recordings_path=settings.voice_memos.recordings_path,
    )
    request_timeout_seconds = int(os.getenv("VOICE_MEMOS_UPLOAD_REQUEST_TIMEOUT_SECONDS", "30"))
    preflight_timeout_seconds = float(os.getenv("VOICE_MEMOS_UPLOAD_PREFLIGHT_TIMEOUT_SECONDS", "5"))
    workers = args.workers or (1 if args.mode == "incremental" else 8)

    try:
        with exclusive_lock(args.lock_file) as acquired:
            if not acquired:
                print("Voice Memos upload skipped: another uploader run is active")
                return
            try:
                summary = VoiceMemosUploadRunner(
                    account=settings.voice_memos.account,
                    recordings_path=settings.voice_memos.recordings_path,
                    extensions=settings.voice_memos.extensions,
                    object_store_factory=lambda: GoogleDriveObjectStore(
                        folder_id=settings.voice_memos.google_drive_folder_id,
                        service=build_google_drive_service(
                            account=settings.voice_memos.account,
                            settings=settings,
                            request_timeout_seconds=request_timeout_seconds,
                        ),
                    ),
                    logger=logger,
                    limit=args.limit or None,
                    workers=workers,
                    mode=args.mode,
                    upload_state=state,
                    min_file_age_seconds=args.min_file_age_seconds if args.mode == "incremental" else 0,
                    before_upload_check=build_before_upload_check(preflight_timeout_seconds=preflight_timeout_seconds),
                ).sync()
            finally:
                state.save(args.state_file)
    except Exception as exc:
        if is_transient_exception(exc):
            print(f"Voice Memos upload skipped after transient network failure: {exc}")
            return
        raise

    print(
        "Voice Memos upload complete: "
        f"seen={summary.recordings_seen} "
        f"selected={summary.recordings_selected} "
        f"uploaded={summary.recordings_uploaded} "
        f"skipped={summary.recordings_skipped} "
        f"deferred={summary.recordings_deferred} "
        f"metadata={summary.metadata_uploaded}"
    )


def build_google_drive_service(*, account: str, settings, request_timeout_seconds: int = 30):
    credentials = load_google_credentials(
        email_address=account,
        settings=settings,
        scopes=(GOOGLE_DRIVE_SCOPE,),
        service_name="Google Drive",
        request_timeout_seconds=request_timeout_seconds,
    )
    http = google_auth_httplib2.AuthorizedHttp(credentials, http=httplib2.Http(timeout=request_timeout_seconds))
    return build("drive", "v3", http=http, cache_discovery=False)


def build_before_upload_check(*, preflight_timeout_seconds: float):
    policy = NetworkPolicy.from_env()

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
    policy = NetworkPolicy.from_env()
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
