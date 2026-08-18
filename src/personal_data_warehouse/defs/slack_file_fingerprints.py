"""Dagster wiring for the Slack image fingerprint backfill.

Bytes come from the app's existing ``get_object`` tool rather than a second
Slack client: ``app/internal/objectstore/slack.go`` already resolves a Slack
file id through ``files.info`` across every configured workspace token and
handles Slack's 200-with-an-HTML-login-page answer. This asset therefore needs
no Slack credential at all.

Deliberately a *schedule*, not a backlog sensor. The photos identity pipeline
uses a sensor because its backlog is small, bursty, and local; this one has a
~905k-image / ~552 GB backlog that is drained over weeks against a rate-limited
third-party API. A sensor that fires whenever work exists would simply run it
continuously. An hourly bounded slice is the whole design.
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
    AppObjectFetcher,
    SlackFileFingerprintRunner,
)
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

SLACK_FILE_FINGERPRINTS_POSTGRES_LOCK_ID = 8_407_112_473
SLACK_FILE_FINGERPRINT_LIMIT_ENV = "SLACK_FILE_FINGERPRINT_LIMIT"
SLACK_FILE_FINGERPRINT_RUN_SECONDS_ENV = "SLACK_FILE_FINGERPRINT_RUN_SECONDS"

#: One hourly slice. Chosen to be obviously bounded rather than fast: at this
#: rate the backlog drains over months, which is the correct trade against a
#: rate-limited API that this repo has already been throttled by once.
DEFAULT_LIMIT = 300
DEFAULT_RUN_SECONDS = 900


def slack_file_fingerprint_limit() -> int:
    return int(os.getenv(SLACK_FILE_FINGERPRINT_LIMIT_ENV, str(DEFAULT_LIMIT)))


def slack_file_fingerprint_run_seconds() -> float:
    return float(os.getenv(SLACK_FILE_FINGERPRINT_RUN_SECONDS_ENV, str(DEFAULT_RUN_SECONDS)))


def app_credentials() -> tuple[str, str]:
    """The app URL + token this backfill fetches bytes through.

    Slack file bytes come from the app's get_object tool, which already owns
    Slack file resolution and already holds the workspace tokens, so this
    process needs no Slack credential of its own.
    """
    base_url = (os.getenv("PDW_API_URL") or os.getenv("MCP_BASE_URL") or "").strip()
    secret_token = (os.getenv("PDW_SECRET_TOKEN") or os.getenv("MCP_SECRET_TOKEN") or "").strip()
    return base_url, secret_token


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def slack_file_fingerprints(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_slack=False)
    base_url, secret_token = app_credentials()
    if not base_url or not secret_token:
        context.log.warning(
            "Skipping Slack file fingerprints: PDW_API_URL / PDW_SECRET_TOKEN are not set"
        )
        return MaterializeResult(
            metadata={"skipped": MetadataValue.text("app credentials not configured")}
        )

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
                    fetcher=AppObjectFetcher(base_url=base_url, secret_token=secret_token),
                    logger=context.log,
                    limit=slack_file_fingerprint_limit(),
                    max_run_seconds=slack_file_fingerprint_run_seconds(),
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
