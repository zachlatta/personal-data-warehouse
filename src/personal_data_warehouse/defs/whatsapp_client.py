"""Run the WhatsApp linked-device client inside the Dagster deployment.

The client needs a persistent connection to receive messages, but Dagster's
run monitoring caps run duration (max_runtime_seconds in docker/dagster.yaml).
So the client runs in bounded windows (WHATSAPP_CLIENT_RUN_SECONDS, default 3h)
and a keepalive sensor relaunches it whenever no run is active. WhatsApp
queues messages for offline linked devices, so the seconds between windows
lose nothing.

First-time pairing: the client is enabled by default once WhatsApp is
configured. Optionally set WHATSAPP_PAIR_PHONE for a pairing code instead of a
QR, then watch the whatsapp_client run logs for the QR / pairing code. Set
WHATSAPP_CLIENT_ENABLED=0 when the client needs to be paused.
"""

from __future__ import annotations

from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    sensor,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

WHATSAPP_CLIENT_POSTGRES_LOCK_ID = 8_407_112_442
WHATSAPP_CLIENT_SENSOR_INTERVAL_SECONDS = 60


def _whatsapp_object_store(settings):
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


@asset(group_name="whatsapp")
def whatsapp_client(context) -> MaterializeResult:
    # Heavy native deps (goneonize, libmagic) load on import; keep them out of
    # code-location load by importing here.
    from personal_data_warehouse_whatsapp.client import WhatsAppClientRunner
    from personal_data_warehouse_whatsapp.session_store import PostgresWhatsAppSessionStore
    from personal_data_warehouse_whatsapp.state import WhatsAppUploadState, default_state_file

    settings = load_settings(require_gmail=False, require_whatsapp=True)
    if settings.whatsapp is None:
        raise RuntimeError("WhatsApp sync is not configured")

    # Two concurrent connections on one session corrupt the device state;
    # the advisory lock makes a second run a no-op instead.
    with exclusive_sync_lock(
        name="whatsapp_client",
        postgres_lock_id=WHATSAPP_CLIENT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping WhatsApp client because another run is already active")
            summary = None
        else:
            session_path = Path(settings.whatsapp.session_path)
            warehouse = warehouse_from_settings(settings)
            session_store = PostgresWhatsAppSessionStore(
                warehouse=warehouse,
                account=settings.whatsapp.account,
                session_key=settings.whatsapp.session_key,
                configured_client_id=settings.whatsapp.client_id,
            )
            client_id = session_store.restore_to_path(session_path)
            state = WhatsAppUploadState.open(
                default_state_file(),
                account=settings.whatsapp.account,
                store_path=f"postgres:{settings.whatsapp.session_key}",
            )
            try:
                summary = WhatsAppClientRunner(
                    account=settings.whatsapp.account,
                    session_path=session_path,
                    object_store_factory=lambda: _whatsapp_object_store(settings),
                    upload_state=state,
                    logger=context.log,
                    run_seconds=settings.whatsapp.client_run_seconds,
                    flush_interval_seconds=settings.whatsapp.flush_interval_seconds,
                    media_bytes_per_flush=settings.whatsapp.media_bytes_per_flush,
                    media_count_per_flush=settings.whatsapp.media_count_per_flush,
                    pair_phone=settings.whatsapp.pair_phone,
                    client_id=client_id,
                    session_snapshot_callback=lambda: session_store.snapshot_from_path(session_path, client_id=client_id),
                    download_history_media=settings.whatsapp.download_history_media,
                ).run()
            finally:
                state.close()
                warehouse.close()

    return MaterializeResult(
        metadata={
            key: MetadataValue.int(summary.get(key, 0) if summary else 0)
            for key in (
                "messages_received",
                "history_messages_received",
                "chats_received",
                "contacts_received",
                "records_selected",
                "records_skipped",
                "batches_uploaded",
                "media_uploaded",
                "media_bytes_uploaded",
            )
        }
    )


whatsapp_client_job = define_asset_job(
    "whatsapp_client_job",
    selection=[whatsapp_client],
)


@sensor(
    job=whatsapp_client_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=WHATSAPP_CLIENT_SENSOR_INTERVAL_SECONDS,
)
def whatsapp_client_keepalive_sensor(context):
    active = skip_if_job_in_progress(context, job_name="whatsapp_client_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_whatsapp=True)
    except ValueError as exc:
        return SkipReason(f"WhatsApp is not configured: {exc}")
    if settings.whatsapp is None or not settings.whatsapp.client_enabled:
        return SkipReason(
            "WhatsApp client is disabled by WHATSAPP_CLIENT_ENABLED=0; unset it or set "
            "WHATSAPP_CLIENT_ENABLED=1 to start it."
        )

    return RunRequest(tags={"whatsapp_trigger": "keepalive"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[whatsapp_client],
        jobs=[whatsapp_client_job],
        sensors=[whatsapp_client_keepalive_sensor],
    )
