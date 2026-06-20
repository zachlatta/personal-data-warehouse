from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
from pathlib import Path
import sys

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.objectstore.google_drive import is_transient_google_error
from personal_data_warehouse_voice_memos.network import NetworkPolicy, preflight_google_drive
from personal_data_warehouse_agent_sessions.state import AgentSessionsUploadState, default_state_file
from personal_data_warehouse_agent_sessions.sync import AgentSessionsUploadRunner, UploadBlockedError


class CliLogger:
    def info(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)

    def warning(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local AI agent CLI session transcripts to Google Drive.")
    parser.add_argument("--mode", choices=("incremental", "full"), default="incremental", help="Upload mode")
    parser.add_argument("--state-file", type=Path, default=default_state_file(), help="Incremental upload state path")
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=default_state_file().with_suffix(".lock"),
        help="Nonblocking lock path used to avoid overlapping LaunchAgent runs",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum lines to upload this run; 0 means unlimited. Bounds the first backfill.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20000,
        help="Maximum lines per uploaded batch file",
    )
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be greater than or equal to 0")
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")

    settings = load_settings(require_postgres=False, require_gmail=False, require_agent_sessions=True)
    if settings.agent_sessions is None:
        raise RuntimeError("Agent sessions sync is not configured")
    config = settings.agent_sessions
    if config.storage_backend != "google_drive":
        raise RuntimeError(f"Unsupported agent sessions storage backend: {config.storage_backend}")

    logger = CliLogger()
    state = AgentSessionsUploadState.open(args.state_file, account=config.account)
    try:
        with exclusive_lock(args.lock_file) as acquired:
            if not acquired:
                print("Agent sessions upload skipped: another uploader run is active")
                return
            summary = AgentSessionsUploadRunner(
                account=config.account,
                device=config.device,
                claude_projects_dir=config.claude_projects_dir,
                codex_sessions_dir=config.codex_sessions_dir,
                openclaw_sessions_dir=config.openclaw_sessions_dir,
                object_store_factory=lambda: build_object_store(
                    google_drive_spec(
                        folder_id=config.google_drive_folder_id,
                        account=config.google_drive_account,
                        source="agent_sessions",
                        blob_kind="agent_sessions_blob",
                        metadata_kind="agent_sessions_export_batch",
                    ),
                    settings=settings,
                ),
                logger=logger,
                upload_state=state,
                mode=args.mode,
                limit=args.limit,
                batch_size=args.batch_size,
                before_upload_check=build_before_upload_check(),
            ).sync()
    except UploadBlockedError as exc:
        print(f"Agent sessions upload skipped: {exc}")
        return
    except Exception as exc:
        if is_transient_exception(exc):
            print(f"Agent sessions upload skipped after transient network failure: {exc}")
            return
        raise
    finally:
        state.close()

    print(
        "Agent sessions upload complete: "
        f"files={summary.files_seen} "
        f"new={summary.files_with_new_lines} "
        f"lines={summary.lines_selected} "
        f"skipped={summary.lines_skipped} "
        f"batches={summary.batches_uploaded}"
    )


def build_before_upload_check(*, preflight_timeout_seconds: float = 5.0):
    policy = NetworkPolicy.from_env(prefix="AGENT_SESSIONS_UPLOAD", fallback_prefix="VOICE_MEMOS_UPLOAD")

    def before_upload_check() -> str | None:
        decision = policy.check()
        if not decision.allowed:
            return decision.reason
        preflight = preflight_google_drive(timeout_seconds=preflight_timeout_seconds)
        if not preflight.allowed:
            return preflight.reason
        return None

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
        if isinstance(current, Exception) and is_transient_google_error(current):
            return True
        current = current.__cause__ or current.__context__
    return False


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
