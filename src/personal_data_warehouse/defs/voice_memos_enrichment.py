from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

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

from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.voice_memos_enrichment import (
    DEFAULT_ENRICHMENT_LOOKBACK_WEEKS,
    OpenAIResponsesClient,
    VoiceMemosEnrichmentRunner,
)

VOICE_MEMOS_ENRICHMENT_POSTGRES_LOCK_ID = 7_403_111_841
DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE = 0


@asset(
    group_name="voice_memos",
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def voice_memos_enrichment(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_openai=True)
    if settings.openai is None:
        raise RuntimeError("OpenAI is not configured")

    batch_size = int(
        os.getenv(
            "VOICE_MEMOS_ENRICHMENT_BATCH_SIZE",
            str(DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE),
        )
    )
    lookback_weeks = int(os.getenv("VOICE_MEMOS_ENRICHMENT_LOOKBACK_WEEKS", str(DEFAULT_ENRICHMENT_LOOKBACK_WEEKS)))
    if lookback_weeks < 1:
        raise ValueError("VOICE_MEMOS_ENRICHMENT_LOOKBACK_WEEKS must be at least 1")
    recorded_after = datetime.now(tz=UTC) - timedelta(weeks=lookback_weeks)
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")
    with exclusive_sync_lock(
        name="voice_memos_enrichment",
        postgres_lock_id=VOICE_MEMOS_ENRICHMENT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Voice Memos enrichment because another run is already active")
            summary = None
        else:
            summary = VoiceMemosEnrichmentRunner(
                warehouse=warehouse,
                client=OpenAIResponsesClient(
                    api_key=settings.openai.api_key,
                    model=settings.openai.model,
                    base_url=settings.openai.base_url,
                    timeout_seconds=settings.openai.timeout_seconds,
                    reasoning_effort=settings.openai.reasoning_effort,
                ),
                logger=context.log,
            ).sync(limit=batch_size if batch_size > 0 else None, recorded_after=recorded_after)

    return MaterializeResult(
        metadata={
            "recordings_seen": MetadataValue.int(summary.recordings_seen if summary else 0),
            "recordings_enriched": MetadataValue.int(summary.recordings_enriched if summary else 0),
            "recordings_failed": MetadataValue.int(summary.recordings_failed if summary else 0),
            "lookback_weeks": MetadataValue.int(lookback_weeks),
        }
    )


voice_memos_enrichment_job = define_asset_job(
    "voice_memos_enrichment_job",
    selection=[voice_memos_enrichment],
)


@schedule(
    cron_schedule="17 * * * *",
    job=voice_memos_enrichment_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def voice_memos_enrichment_hourly(context):
    return skip_if_job_active(context, job_name="voice_memos_enrichment_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[voice_memos_enrichment],
        jobs=[voice_memos_enrichment_job],
        schedules=[voice_memos_enrichment_hourly],
    )
