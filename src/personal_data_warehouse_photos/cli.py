"""`pdw ingest apple-photos` — upload Apple Photos originals through the app."""

from __future__ import annotations

import argparse
import fcntl
import os
import sys
from contextlib import contextmanager
from pathlib import Path

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.ingest_client import ingest_client_from_env
from personal_data_warehouse_voice_memos.network import (
    NetworkPolicy,
    is_transient_upload_error,
    preflight_app_ingest,
)
from personal_data_warehouse_photos.state import PhotosUploadState, default_state_file
from personal_data_warehouse_photos.sync import PhotosUploadRunner


class CliLogger:
    def info(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)

    def warning(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local Apple Photos originals through the app ingest API.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum files to upload this run; 0 means no limit")
    parser.add_argument("--mode", choices=("incremental", "full"), default="incremental", help="Upload mode")
    parser.add_argument("--state-file", type=Path, default=default_state_file(), help="Incremental upload state path")
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=default_state_file().with_suffix(".lock"),
        help="Nonblocking lock path used to avoid overlapping scheduled runs",
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
        require_postgres=False,
        require_gmail=False,
        require_photos=True,
    )
    if settings.photos is None:
        raise RuntimeError("Photo sync is not configured")

    logger = CliLogger()
    state = PhotosUploadState.open(
        args.state_file,
        account=settings.photos.account,
        library_path=settings.photos.library_path,
    )
    preflight_timeout_seconds = float(os.getenv("PHOTOS_UPLOAD_PREFLIGHT_TIMEOUT_SECONDS", "5"))

    try:
        with exclusive_lock(args.lock_file) as acquired:
            if not acquired:
                print("Photo upload skipped: another uploader run is active")
                return
            try:
                summary = PhotosUploadRunner(
                    account=settings.photos.account,
                    library_path=settings.photos.library_path,
                    ingest_client=ingest_client_from_env(),
                    logger=logger,
                    limit=args.limit or None,
                    mode=args.mode,
                    upload_state=state,
                    before_upload_check=build_before_upload_check(
                        preflight_timeout_seconds=preflight_timeout_seconds
                    ),
                ).sync()
            finally:
                state.close()
    except Exception as exc:
        if is_transient_exception(exc):
            print(f"Photo upload skipped after transient network failure: {exc}")
            return
        raise

    print(
        "Photo upload complete: "
        f"assets={summary.assets_seen} "
        f"files={summary.files_seen} "
        f"present={summary.files_present} "
        f"missing={summary.files_missing} "
        f"selected={summary.files_selected} "
        f"uploaded={summary.files_uploaded} "
        f"skipped={summary.files_skipped} "
        f"oversize_deferred={summary.files_deferred_oversize}"
    )


def build_before_upload_check(*, preflight_timeout_seconds: float):
    policy = NetworkPolicy.from_env()

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
        if isinstance(current, Exception) and is_transient_upload_error(current):
            return True
        current = current.__cause__ or current.__context__
    return False


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
