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
from personal_data_warehouse_voice_memos.state import VoiceMemosUploadState, default_state_file
from personal_data_warehouse_voice_memos.sync import VoiceMemosUploadRunner
from personal_data_warehouse_voice_memos.writeback import VoiceMemosWritebackRunner


class CliLogger:
    def info(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)

    def warning(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local macOS Voice Memos files through the app ingest API.")
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
    parser.add_argument(
        "--no-writeback",
        action="store_true",
        help="Skip the enriched-title write-back into the Voice Memos app after uploading",
    )
    parser.add_argument(
        "--writeback-only",
        action="store_true",
        help="Run only the enriched-title write-back, skipping the upload phase",
    )
    parser.add_argument(
        "--writeback-dry-run",
        action="store_true",
        help="Log the renames write-back would apply without writing to the Voice Memos store",
    )
    parser.add_argument(
        "--writeback-limit",
        type=int,
        default=0,
        help="Maximum renames to apply per run; 0 means no limit",
    )
    args = parser.parse_args()

    if args.network_diagnostics:
        print_network_diagnostics()
        return

    settings = load_settings(
        require_postgres=False,
        require_gmail=False,
        require_voice_memos=True,
    )
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos sync is not configured")

    logger = CliLogger()
    state = VoiceMemosUploadState.load(
        args.state_file,
        account=settings.voice_memos.account,
        recordings_path=settings.voice_memos.recordings_path,
    )
    preflight_timeout_seconds = float(os.getenv("VOICE_MEMOS_UPLOAD_PREFLIGHT_TIMEOUT_SECONDS", "5"))
    workers = args.workers or (1 if args.mode == "incremental" else 8)

    run_upload = not args.writeback_only
    run_title_writeback = args.writeback_only or (not args.no_writeback and writeback_enabled_from_env())

    summary = None
    writeback_summary = None
    try:
        with exclusive_lock(args.lock_file) as acquired:
            if not acquired:
                print("Voice Memos upload skipped: another uploader run is active")
                return
            if run_upload:
                try:
                    summary = VoiceMemosUploadRunner(
                        account=settings.voice_memos.account,
                        recordings_path=settings.voice_memos.recordings_path,
                        extensions=settings.voice_memos.extensions,
                        ingest_client=ingest_client_from_env(),
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
            if run_title_writeback:
                writeback_summary = run_writeback(
                    settings=settings,
                    logger=logger,
                    dry_run=args.writeback_dry_run,
                    limit=args.writeback_limit or None,
                    upload_state=state,
                )
    except Exception as exc:
        if is_transient_exception(exc):
            print(f"Voice Memos run skipped after transient network failure: {exc}")
            return
        raise

    if summary is not None:
        print(
            "Voice Memos upload complete: "
            f"seen={summary.recordings_seen} "
            f"selected={summary.recordings_selected} "
            f"uploaded={summary.recordings_uploaded} "
            f"skipped={summary.recordings_skipped} "
            f"deferred={summary.recordings_deferred} "
            f"metadata={summary.metadata_uploaded}"
        )
    if writeback_summary is not None:
        print(
            "Voice Memos write-back complete: "
            f"local={writeback_summary.local_recordings} "
            f"auto_named={writeback_summary.auto_named} "
            f"titles={writeback_summary.enriched_titles} "
            f"planned={writeback_summary.planned} "
            f"renamed={writeback_summary.renamed} "
            f"skipped={writeback_summary.skipped}"
            f"{' (dry run)' if writeback_summary.dry_run else ''}"
        )


def writeback_enabled_from_env(getenv=os.getenv) -> bool:
    raw = (getenv("VOICE_MEMOS_WRITEBACK_ENABLED") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def run_writeback(*, settings, logger, dry_run: bool, limit: int | None, upload_state=None):
    base_url = (os.getenv("PDW_API_URL") or os.getenv("MCP_BASE_URL") or "").strip()
    secret_token = (os.getenv("PDW_SECRET_TOKEN") or os.getenv("MCP_SECRET_TOKEN") or "").strip()
    if not base_url or not secret_token:
        raise RuntimeError(
            "Voice Memos write-back requires PDW_API_URL and PDW_SECRET_TOKEN (or their MCP_* aliases)"
        )
    client_name = (os.getenv("PDW_CLIENT_NAME") or "").strip() or "pdw"
    # The uploader's state caches each filename's audio sha; the write-back
    # uses it to re-identify memos whose filenames drifted (timezone rebases).
    sha_by_filename = None
    if upload_state is not None:
        sha_by_filename = {
            filename: entry.content_sha256
            for filename, entry in upload_state.entries.items()
            if entry.content_sha256
        }
    return VoiceMemosWritebackRunner(
        recordings_path=settings.voice_memos.recordings_path,
        account=settings.voice_memos.account,
        base_url=base_url,
        secret_token=secret_token,
        client_name=client_name,
        logger=logger,
        limit=limit,
        dry_run=dry_run,
        sha_by_filename=sha_by_filename,
    ).run()


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
