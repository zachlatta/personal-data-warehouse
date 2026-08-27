from __future__ import annotations

from datetime import UTC, datetime

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
    define_asset_job,
    definitions,
    schedule,
)

from personal_data_warehouse.alice_voice_recordings_drive_ingest import (
    AliceVoiceRecordingsDriveIngestRunner,
    iter_archive_payloads,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.gmail_sync import build_gmail_service
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse_alice_voice_recordings.api import AliceApiClient
from personal_data_warehouse_alice_voice_recordings.gmail_recovery import (
    AliceGmailRecoveryRunner,
    load_alice_gmail_transcript_emails,
)
from personal_data_warehouse_alice_voice_recordings.sync import SOURCE, AliceVoiceRecordingsImportRunner
from personal_data_warehouse.warehouse import warehouse_from_settings

ALICE_VOICE_RECORDINGS_IMPORT_POSTGRES_LOCK_ID = 7_403_111_845
ALICE_VOICE_RECORDINGS_DRIVE_INGEST_POSTGRES_LOCK_ID = 7_403_111_854


#: Absence is the epoch here, as everywhere in the warehouse -- a poll that has
#: never succeeded carries the sentinel rather than NULL.
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _alice_state_row(
    *,
    account: str,
    status: str,
    error: str,
    recordings_seen: int,
    now: datetime,
    last_success_at: datetime | None = None,
) -> dict[str, object]:
    """One Alice poll's verdict, shaped for ops.alice_voice_recordings_sync_state.

    ``last_success_at`` stays at the epoch on a failure so that "it ran" and "it
    worked" remain separate facts -- the same split ``pdw_record_run`` keeps for
    the device uploaders, and the reason a chronically failing poller cannot
    look healthy just by continuing to fire.
    """
    return {
        "account": account,
        "last_sync_type": "incremental",
        "status": status,
        "error": error,
        "recordings_seen": recordings_seen,
        "last_success_at": last_success_at or _EPOCH,
        "updated_at": now,
        "sync_version": int(now.timestamp() * 1000),
    }


def alice_object_store(config, settings):
    return build_object_store(
        google_drive_spec(
            folder_id=config.google_drive_folder_id,
            account=config.google_drive_account,
            source=SOURCE,
            blob_kind="voice_recording_audio",
            metadata_kind="voice_recording_metadata",
            request_timeout_seconds=config.request_timeout_seconds,
        ),
        settings=settings,
    )


@asset(
    group_name="alice_voice_recordings",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def alice_voice_recordings_import(context) -> MaterializeResult:
    # require_postgres: the poll now records its own run state, which is this
    # pipeline's only heartbeat.
    settings = load_settings(
        require_postgres=True,
        require_gmail=False,
        require_alice_voice_recordings=True,
    )
    if settings.alice_voice_recordings is None:
        raise RuntimeError("Alice voice recordings import is not configured")
    config = settings.alice_voice_recordings

    with exclusive_sync_lock(
        name="alice_voice_recordings_import",
        postgres_lock_id=ALICE_VOICE_RECORDINGS_IMPORT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Alice voice recordings import because another run is already active")
            # Deliberately no heartbeat: a skip is not evidence the poller
            # works, and stamping here would let an overlap storm hold the
            # pipeline green while no poll ever completes. The run that holds
            # the lock stamps it.
            summary = None
        else:
            client = AliceApiClient(
                key_id=config.key_id,
                secret_key=config.secret_key,
                base_url=config.base_url,
                timeout_seconds=config.request_timeout_seconds,
            )
            # This asset is the POLLER, so it is the only place that can say
            # whether the daily poll happened and whether it worked -- the two
            # facts /pipelines needs and could not get from data freshness,
            # because Zach records a few times a year. Stamped here rather than
            # in the downstream ingest asset, which Dagster never reaches when
            # this one raises, i.e. in exactly the case worth recording.
            warehouse = warehouse_from_settings(settings)
            warehouse.ensure_alice_voice_recordings_tables()
            try:
                summary = AliceVoiceRecordingsImportRunner(
                    account=config.account,
                    upload_requests=client.iter_recordings(),
                    object_store=alice_object_store(config, settings),
                    logger=context.log,
                    mode="incremental",
                    stage="library",
                ).sync()
            except Exception as error:
                warehouse.upsert_alice_voice_recordings_sync_state(
                    _alice_state_row(
                        account=config.account,
                        status="failed",
                        error=str(error),
                        recordings_seen=0,
                        now=datetime.now(tz=UTC),
                    )
                )
                # Re-raised: the run stays red in Dagster AND the reason is on
                # /pipelines. Swallowing it would trade one signal for the other.
                raise
            polled_at = datetime.now(tz=UTC)
            warehouse.upsert_alice_voice_recordings_sync_state(
                _alice_state_row(
                    account=config.account,
                    status="ok",
                    error="",
                    recordings_seen=summary.upload_requests_seen,
                    now=polled_at,
                    last_success_at=polled_at,
                )
            )

    return MaterializeResult(
        metadata={
            "recordings_seen": MetadataValue.int(summary.upload_requests_seen if summary else 0),
            "recordings_uploaded": MetadataValue.int(summary.recordings_uploaded if summary else 0),
            "recordings_skipped": MetadataValue.int(summary.recordings_skipped if summary else 0),
            "metadata_uploaded": MetadataValue.int(summary.metadata_uploaded if summary else 0),
            "bytes_uploaded": MetadataValue.int(summary.bytes_uploaded if summary else 0),
            "bytes_skipped": MetadataValue.int(summary.bytes_skipped if summary else 0),
        }
    )


@asset(
    group_name="alice_voice_recordings",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def alice_voice_recordings_gmail_recovery(context) -> MaterializeResult:
    settings = load_settings(
        require_gmail=False,
        require_alice_voice_recordings=True,
    )
    if settings.alice_voice_recordings is None:
        raise RuntimeError("Alice voice recordings import is not configured")
    config = settings.alice_voice_recordings

    with exclusive_sync_lock(
        name="alice_voice_recordings_gmail_recovery",
        postgres_lock_id=ALICE_VOICE_RECORDINGS_IMPORT_POSTGRES_LOCK_ID + 1,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Alice Gmail recovery because another run is already active")
            summary = None
        else:
            warehouse = warehouse_from_settings(settings)
            emails = load_alice_gmail_transcript_emails(
                warehouse=warehouse,
                accounts=[account.email_address for account in settings.gmail_accounts],
            )
            gmail_services = {
                account.email_address: build_gmail_service(account=account, settings=settings)
                for account in settings.gmail_accounts
                if any(email.account == account.email_address for email in emails)
            }
            summary = AliceGmailRecoveryRunner(
                emails=emails,
                object_store=alice_object_store(config, settings),
                gmail_services_by_account=gmail_services,
                logger=context.log,
                stage="library",
            ).sync()

    return MaterializeResult(
        metadata={
            "emails_seen": MetadataValue.int(summary.emails_seen if summary else 0),
            "emails_archived": MetadataValue.int(summary.emails_archived if summary else 0),
            "emails_skipped": MetadataValue.int(summary.emails_skipped if summary else 0),
            "attachments_seen": MetadataValue.int(summary.attachments_seen if summary else 0),
            "attachments_uploaded": MetadataValue.int(summary.attachments_uploaded if summary else 0),
            "metadata_uploaded": MetadataValue.int(summary.metadata_uploaded if summary else 0),
            "bytes_uploaded": MetadataValue.int(summary.bytes_uploaded if summary else 0),
        }
    )


@asset(
    deps=[alice_voice_recordings_import, alice_voice_recordings_gmail_recovery],
    group_name="alice_voice_recordings",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def alice_voice_recordings_drive_ingest(context) -> MaterializeResult:
    """Materialize the immutable Alice Drive archive into canonical tables."""
    settings = load_settings(
        require_gmail=False,
        require_alice_voice_recordings=True,
    )
    if settings.alice_voice_recordings is None:
        raise RuntimeError("Alice voice recordings import is not configured")
    config = settings.alice_voice_recordings

    with exclusive_sync_lock(
        name="alice_voice_recordings_drive_ingest",
        postgres_lock_id=ALICE_VOICE_RECORDINGS_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Alice Drive ingest because another run is already active")
            summary = None
        else:
            object_store = alice_object_store(config, settings)
            summary = AliceVoiceRecordingsDriveIngestRunner(
                warehouse=warehouse_from_settings(settings),
                metadata_source=lambda: iter_archive_payloads(object_store=object_store),
                logger=context.log,
            ).sync()

    return MaterializeResult(
        metadata={
            "metadata_seen": MetadataValue.int(summary.metadata_seen if summary else 0),
            "recordings_written": MetadataValue.int(summary.recordings_written if summary else 0),
            "artifacts_written": MetadataValue.int(summary.artifacts_written if summary else 0),
        }
    )


alice_voice_recordings_import_job = define_asset_job(
    "alice_voice_recordings_import_job",
    selection=[
        alice_voice_recordings_import,
        alice_voice_recordings_gmail_recovery,
        alice_voice_recordings_drive_ingest,
    ],
)


@schedule(
    cron_schedule="17 4 * * *",
    job=alice_voice_recordings_import_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def alice_voice_recordings_import_daily(context):
    return skip_if_job_active(context, job_name="alice_voice_recordings_import_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[
            alice_voice_recordings_import,
            alice_voice_recordings_gmail_recovery,
            alice_voice_recordings_drive_ingest,
        ],
        jobs=[alice_voice_recordings_import_job],
        schedules=[alice_voice_recordings_import_daily],
    )
