from __future__ import annotations

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


def _alice_object_store(config, settings):
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
    settings = load_settings(
        require_postgres=False,
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
            summary = None
        else:
            client = AliceApiClient(
                key_id=config.key_id,
                secret_key=config.secret_key,
                base_url=config.base_url,
                timeout_seconds=config.request_timeout_seconds,
            )
            summary = AliceVoiceRecordingsImportRunner(
                account=config.account,
                upload_requests=client.iter_recordings(),
                object_store=_alice_object_store(config, settings),
                logger=context.log,
                mode="incremental",
                stage="library",
            ).sync()

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
                object_store=_alice_object_store(config, settings),
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


alice_voice_recordings_import_job = define_asset_job(
    "alice_voice_recordings_import_job",
    selection=[alice_voice_recordings_import, alice_voice_recordings_gmail_recovery],
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
        assets=[alice_voice_recordings_import, alice_voice_recordings_gmail_recovery],
        jobs=[alice_voice_recordings_import_job],
        schedules=[alice_voice_recordings_import_daily],
    )
