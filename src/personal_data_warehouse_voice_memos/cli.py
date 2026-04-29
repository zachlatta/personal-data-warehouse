from __future__ import annotations

import argparse

from googleapiclient.discovery import build

from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE, load_settings
from personal_data_warehouse.google_auth import load_google_credentials
from personal_data_warehouse_voice_memos.google_drive_storage import GoogleDriveObjectStore
from personal_data_warehouse_voice_memos.sync import VoiceMemosUploadRunner


class CliLogger:
    def info(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)

    def warning(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local macOS Voice Memos files to Google Drive.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum recordings to upload; 0 means no limit")
    parser.add_argument("--workers", type=int, default=8, help="Number of parallel upload workers")
    args = parser.parse_args()

    settings = load_settings(
        require_clickhouse=False,
        require_gmail=False,
        require_voice_memos=True,
    )
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos sync is not configured")
    if settings.voice_memos.storage_backend != "google_drive":
        raise RuntimeError(f"Unsupported Voice Memos storage backend: {settings.voice_memos.storage_backend}")

    summary = VoiceMemosUploadRunner(
        account=settings.voice_memos.account,
        recordings_path=settings.voice_memos.recordings_path,
        extensions=settings.voice_memos.extensions,
        object_store_factory=lambda: GoogleDriveObjectStore(
            folder_id=settings.voice_memos.google_drive_folder_id,
            service=build_google_drive_service(account=settings.voice_memos.account, settings=settings),
        ),
        logger=CliLogger(),
        limit=args.limit or None,
        workers=args.workers,
    ).sync()
    print(
        "Voice Memos upload complete: "
        f"seen={summary.recordings_seen} "
        f"uploaded={summary.recordings_uploaded} "
        f"skipped={summary.recordings_skipped} "
        f"metadata={summary.metadata_uploaded}"
    )


def build_google_drive_service(*, account: str, settings):
    credentials = load_google_credentials(
        email_address=account,
        settings=settings,
        scopes=(GOOGLE_DRIVE_SCOPE,),
        service_name="Google Drive",
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


if __name__ == "__main__":
    main()
