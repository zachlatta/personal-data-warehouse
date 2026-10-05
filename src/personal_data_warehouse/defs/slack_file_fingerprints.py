"""Dagster wiring for the Slack image fingerprint backfill.

Each image is hashed from Slack's own thumbnail, fetched with the workspace
token (slack_file_fingerprints.SlackThumbnailFetcher), because Slack's audit log
records every full download and flagged this backfill's ~6,500 a day as
``excessive_downloads`` every three hours until 2026-10-05. Only an image with
no thumbnail is downloaded in full, a few a run.

Deliberately a *schedule*, not a backlog sensor: the backlog is ~750k images,
drained in bounded hourly slices.
"""

from __future__ import annotations

import os

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
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.slack_file_fingerprints import (
    DEFAULT_MAX_FULL_DOWNLOADS,
    SlackFileFingerprintRunner,
    SlackThumbnailFetcher,
)
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

SLACK_FILE_FINGERPRINTS_POSTGRES_LOCK_ID = 8_407_112_473
SLACK_FILE_FINGERPRINT_LIMIT_ENV = "SLACK_FILE_FINGERPRINT_LIMIT"
SLACK_FILE_FINGERPRINT_RUN_SECONDS_ENV = "SLACK_FILE_FINGERPRINT_RUN_SECONDS"
SLACK_FILE_FINGERPRINT_SPACING_SECONDS_ENV = "SLACK_FILE_FINGERPRINT_SPACING_SECONDS"
SLACK_FILE_FINGERPRINT_MAX_FULL_DOWNLOADS_ENV = "SLACK_FILE_FINGERPRINT_MAX_FULL_DOWNLOADS"

#: One hourly slice of thumbnails, about one a second, so a run is ~10 minutes
#: and never a burst.
DEFAULT_LIMIT = 600
DEFAULT_RUN_SECONDS = 1500
DEFAULT_SPACING_SECONDS = 1.0


def slack_file_fingerprint_limit() -> int:
    return int(os.getenv(SLACK_FILE_FINGERPRINT_LIMIT_ENV, str(DEFAULT_LIMIT)))


def slack_file_fingerprint_run_seconds() -> float:
    return float(os.getenv(SLACK_FILE_FINGERPRINT_RUN_SECONDS_ENV, str(DEFAULT_RUN_SECONDS)))


def slack_file_fingerprint_spacing_seconds() -> float:
    return float(os.getenv(SLACK_FILE_FINGERPRINT_SPACING_SECONDS_ENV, str(DEFAULT_SPACING_SECONDS)))


def slack_file_fingerprint_max_full_downloads() -> int:
    return int(os.getenv(SLACK_FILE_FINGERPRINT_MAX_FULL_DOWNLOADS_ENV, str(DEFAULT_MAX_FULL_DOWNLOADS)))


def slack_tokens_by_account(settings) -> dict[str, str]:
    return {
        str(account.account): str(account.token)
        for account in getattr(settings, "slack_accounts", ()) or ()
        if getattr(account, "token", "")
    }


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def slack_file_fingerprints(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_slack=False)
    tokens = slack_tokens_by_account(settings)
    if not tokens:
        context.log.warning("Skipping Slack file fingerprints: no Slack account token is configured")
        return MaterializeResult(metadata={"skipped": MetadataValue.text("no Slack token configured")})

    warehouse = warehouse_from_settings(settings)
    summary = None
    try:
        with exclusive_sync_lock(
            name="slack_file_fingerprints",
            postgres_lock_id=SLACK_FILE_FINGERPRINTS_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                context.log.warning(
                    "Skipping Slack file fingerprints because another run is already active"
                )
            else:
                summary = SlackFileFingerprintRunner(
                    warehouse=warehouse,
                    fetcher=SlackThumbnailFetcher(tokens=tokens),
                    logger=context.log,
                    limit=slack_file_fingerprint_limit(),
                    max_run_seconds=slack_file_fingerprint_run_seconds(),
                    download_spacing_seconds=slack_file_fingerprint_spacing_seconds(),
                    max_full_downloads=slack_file_fingerprint_max_full_downloads(),
                ).run()
    finally:
        warehouse.close()

    return MaterializeResult(
        metadata={
            "candidates": MetadataValue.int(summary.candidates if summary else 0),
            "fingerprinted": MetadataValue.int(summary.fingerprinted if summary else 0),
            "undecodable": MetadataValue.int(summary.undecodable if summary else 0),
            "too_large": MetadataValue.int(summary.too_large if summary else 0),
            "missing": MetadataValue.int(summary.missing if summary else 0),
            "failed": MetadataValue.int(summary.failed if summary else 0),
            "full_downloads": MetadataValue.int(summary.full_downloads if summary else 0),
            "megabytes_downloaded": MetadataValue.float(
                round((summary.bytes_downloaded if summary else 0) / 1_048_576, 1)
            ),
            # Surfaced rather than raised: being throttled is the expected way a
            # slice ends, not a failure.
            "rate_limited": MetadataValue.bool(bool(summary and summary.rate_limited)),
        }
    )


slack_file_fingerprints_job = define_asset_job(
    "slack_file_fingerprints_job",
    selection=[slack_file_fingerprints],
)


@schedule(
    # :19 keeps it clear of slack_sync's staged runs and of photo_identity (:29).
    cron_schedule="19 * * * *",
    job=slack_file_fingerprints_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_file_fingerprints_hourly(context):
    return skip_if_job_active(context, job_name="slack_file_fingerprints_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[slack_file_fingerprints],
        jobs=[slack_file_fingerprints_job],
        schedules=[slack_file_fingerprints_hourly],
    )
