from __future__ import annotations

import time

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

from personal_data_warehouse.config import (
    GmailAccount,
    Settings,
    load_settings,
)
from personal_data_warehouse.warehouse import warehouse_from_settings
from personal_data_warehouse.gmail_sync import (
    GMAIL_ATTACHMENT_STORAGE_KIND,
    GMAIL_ATTACHMENT_STORAGE_METADATA_KIND,
    GMAIL_ATTACHMENT_STORAGE_SOURCE,
    GmailPollConfig,
    GmailSyncRunner,
)
from personal_data_warehouse.objectstore import ObjectStore, build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import (
    finished_job_runs,
    keepalive_crash_backoff_skip,
    skip_if_job_in_progress,
)
from personal_data_warehouse.timeline_fast_lane import land_sources_on_timeline


def build_attachment_object_store_factory(*, settings: Settings, logger):
    if not settings.gmail_attachment_storage_enabled:
        logger.info(
            "Gmail attachment blob storage is disabled "
            "(set GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID or VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID to enable)"
        )
        return None

    folder_id = settings.gmail_attachment_google_drive_folder_id
    drive_account = settings.gmail_attachment_google_drive_account

    def factory(account: GmailAccount) -> ObjectStore:
        upload_account = drive_account or account.email_address
        return build_object_store(
            google_drive_spec(
                folder_id=folder_id,
                account=upload_account,
                source=GMAIL_ATTACHMENT_STORAGE_SOURCE,
                blob_kind=GMAIL_ATTACHMENT_STORAGE_KIND,
                metadata_kind=GMAIL_ATTACHMENT_STORAGE_METADATA_KIND,
            ),
            settings=settings,
        )

    logger.info(
        "Gmail attachment blob storage is enabled via Google Drive folder %s (upload account: %s)",
        folder_id,
        drive_account or "<source mailbox>",
    )
    return factory


@asset(group_name="gmail")
def gmail_mailbox_sync(context) -> MaterializeResult:
    """Poll every mailbox's history every ~15 s for one bounded window.

    No retry policy: the keepalive sensor relaunches a finished or failed run
    within a tick, and the history cursor makes the relaunch pick up exactly
    where the last write left off.
    """
    settings = load_settings(require_gmail_client_secrets=False)
    config = GmailPollConfig.from_env()
    attachment_object_store_factory = build_attachment_object_store_factory(
        settings=settings,
        logger=context.log,
    )
    warehouse = warehouse_from_settings(settings)
    fast_lane = {"calls": 0, "rows": 0, "errors": 0}

    def land_on_timeline() -> None:
        # Land this source's timeline rows in the tick that wrote them instead
        # of waiting for the five-minute timeline_sync schedule.
        result = land_sources_on_timeline(
            postgres_url=settings.postgres_database_url or "",
            sources=["gmail"],
            logger=context.log,
        )
        fast_lane["calls"] += 1
        fast_lane["rows"] += int(result.get("rows", 0))
        fast_lane["errors"] += len(result.get("errors") or {})

    try:
        summary = GmailSyncRunner(
            settings=settings,
            warehouse=warehouse,
            logger=context.log,
            attachment_object_store_factory=attachment_object_store_factory,
        ).run(config=config, on_messages_written=land_on_timeline)
    finally:
        warehouse.close()

    return MaterializeResult(
        metadata={
            "timeline_fast_lane": MetadataValue.json(fast_lane),
            "lock_acquired": summary.lock_acquired,
            "ticks": summary.ticks,
            "failed_ticks": summary.failed_ticks,
            "poll_interval_seconds": config.poll_interval_seconds,
            "window_seconds": config.window_seconds,
            "mailbox_count": len(settings.gmail_accounts),
            "messages_written": summary.messages_written,
            "deleted_messages": summary.deleted_messages,
            "reconciled_messages": summary.reconciled_messages,
            "full_syncs": summary.full_syncs,
            "attachments_written": summary.attachments_written,
            "attachments_stored": summary.attachments_stored,
            "attachment_text_chars": summary.attachment_text_chars,
            "attachment_backfill_candidates": summary.attachment_backfill_candidates,
            "attachment_backfill_rows_written": summary.attachment_backfill_rows_written,
        }
    )


gmail_mailbox_sync_job = define_asset_job(
    "gmail_mailbox_sync_job",
    selection=[gmail_mailbox_sync],
    # A run-time cap below the global 4-hour run-monitoring one: on 2026-09-09
    # seven short jobs hung in their step subprocess for 3.5 hours after a
    # deploy and starved the five-minute syncs of run slots (see
    # tests/test_dagster_job_runtime_caps.py). The poll window
    # (GMAIL_POLL_WINDOW_SECONDS, 45 min) stays under it with room for a
    # stale-cursor full sync that starts near the end of a window.
    tags={"dagster/max_runtime": "3600"},
)

GMAIL_KEEPALIVE_SENSOR_INTERVAL_SECONDS = 30


# Gmail used to be a five-minute cron job: ~5 s of work per run, and a mean
# landing latency of ~2.5 minutes that was all clock (measured 2026-10-04). A
# poll loop that holds one run open and asks history every 15 s lands mail in
# ~15 s instead, and this sensor keeps exactly one such run alive.
@sensor(
    job=gmail_mailbox_sync_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=GMAIL_KEEPALIVE_SENSOR_INTERVAL_SECONDS,
)
def gmail_mailbox_sync_keepalive_sensor(context):
    active = skip_if_job_in_progress(context, job_name="gmail_mailbox_sync_job")
    if isinstance(active, SkipReason):
        return active
    crash_skip = keepalive_crash_backoff_skip(
        finished_job_runs(context.instance, job_name="gmail_mailbox_sync_job"),
        job_name="gmail_mailbox_sync_job",
        now=time.time(),
        hint="ops.gmail_sync_state.error names the failing mailbox.",
    )
    if crash_skip is not None:
        return crash_skip
    return RunRequest(tags={"gmail_trigger": "keepalive"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[gmail_mailbox_sync],
        jobs=[gmail_mailbox_sync_job],
        sensors=[gmail_mailbox_sync_keepalive_sensor],
    )
