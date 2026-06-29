from __future__ import annotations

import argparse
from pathlib import Path
import sys

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse_whatsapp.client import WhatsAppClientRunner
from personal_data_warehouse.warehouse import warehouse_from_settings
from personal_data_warehouse_whatsapp.session_store import PostgresWhatsAppSessionStore
from personal_data_warehouse_whatsapp.state import WhatsAppUploadState, default_state_file


class CliLogger:
    def info(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)

    def warning(self, message: str, *args) -> None:
        print(message % args if args else message, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the WhatsApp linked-device client: pair via QR/phone code, receive "
            "messages, and write batches + media straight to object storage (Drive). In "
            "production this runs as the whatsapp_client Dagster job; this CLI is for pairing "
            "and debugging."
        )
    )
    parser.add_argument("--run-seconds", type=int, default=0, help="Run window; 0 uses WHATSAPP_CLIENT_RUN_SECONDS")
    parser.add_argument("--session-file", type=Path, default=None, help="Runtime session cache path; canonical session bytes live in Postgres")
    parser.add_argument("--state-file", type=Path, default=default_state_file(), help="Incremental upload state path")
    parser.add_argument("--pair-phone", default="", help="Phone number for pairing-code login instead of QR")
    parser.add_argument("--qr-output-dir", type=Path, default=None, help="Directory for refreshed QR PNG/HTML pairing files")
    args = parser.parse_args()
    if args.run_seconds < 0:
        parser.error("--run-seconds must be greater than or equal to 0")

    settings = load_settings(require_gmail=False, require_whatsapp=True)
    if settings.whatsapp is None:
        raise RuntimeError("WhatsApp sync is not configured")

    session_path = args.session_file or Path(settings.whatsapp.session_path)
    warehouse = warehouse_from_settings(settings)

    def build_whatsapp_object_store():
        # Same spec the whatsapp_drive_ingest reader builds, so the client writes
        # byte/tag-identical objects into the folder the reader promotes from.
        return build_object_store(
            google_drive_spec(
                folder_id=settings.whatsapp.google_drive_folder_id,
                account=settings.whatsapp.google_drive_account,
                source="whatsapp",
                blob_kind="whatsapp_media_item",
                metadata_kind="whatsapp_export_batch",
            ),
            settings=settings,
        )

    session_store = PostgresWhatsAppSessionStore(
        warehouse=warehouse,
        account=settings.whatsapp.account,
        session_key=settings.whatsapp.session_key,
        configured_client_id=settings.whatsapp.client_id,
    )
    client_id = session_store.restore_to_path(session_path)
    state = WhatsAppUploadState.open(
        args.state_file,
        account=settings.whatsapp.account,
        store_path=f"postgres:{settings.whatsapp.session_key}",
    )
    try:
        summary = WhatsAppClientRunner(
            account=settings.whatsapp.account,
            session_path=session_path,
            object_store_factory=build_whatsapp_object_store,
            upload_state=state,
            logger=CliLogger(),
            run_seconds=args.run_seconds or settings.whatsapp.client_run_seconds,
            flush_interval_seconds=settings.whatsapp.flush_interval_seconds,
            media_bytes_per_flush=settings.whatsapp.media_bytes_per_flush,
            media_count_per_flush=settings.whatsapp.media_count_per_flush,
            pair_phone=args.pair_phone or settings.whatsapp.pair_phone,
            client_id=client_id,
            session_snapshot_callback=lambda: session_store.snapshot_from_path(session_path, client_id=client_id),
            qr_output_dir=args.qr_output_dir,
            download_history_media=settings.whatsapp.download_history_media,
        ).run()
    finally:
        state.close()
        warehouse.close()

    print(
        "WhatsApp client run complete: "
        f"messages={summary['messages_received']} "
        f"history={summary['history_messages_received']} "
        f"selected={summary['records_selected']} "
        f"batches={summary['batches_uploaded']} "
        f"media={summary['media_uploaded']}"
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
