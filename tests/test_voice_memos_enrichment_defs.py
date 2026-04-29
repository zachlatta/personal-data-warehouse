from __future__ import annotations

from dagster import AssetKey, Definitions

from personal_data_warehouse.defs.calendar_sync import calendar_event_sync
from personal_data_warehouse.defs.voice_memos_drive_ingest import voice_memos_drive_ingest
from personal_data_warehouse.defs.voice_memos_enrichment import (
    DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE,
    voice_memos_enrichment,
    voice_memos_enrichment_job,
    voice_memos_enrichment_hourly,
)
from personal_data_warehouse.defs.voice_memos_transcription import voice_memos_transcription
from personal_data_warehouse.voice_memos_enrichment import DEFAULT_ENRICHMENT_LOOKBACK_WEEKS


def test_voice_memos_enrichment_job_selects_asset() -> None:
    assert voice_memos_enrichment_job.name == "voice_memos_enrichment_job"


def test_voice_memos_enrichment_defaults_to_all_recent_transcripts() -> None:
    assert DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE == 0
    assert DEFAULT_ENRICHMENT_LOOKBACK_WEEKS == 12


def test_voice_memos_enrichment_depends_on_transcription_and_calendar() -> None:
    assert voice_memos_enrichment.asset_deps[AssetKey("voice_memos_enrichment")] == {
        AssetKey("calendar_event_sync"),
        AssetKey("voice_memos_transcription"),
    }


def test_voice_memos_enrichment_job_selects_full_upstream_lineage() -> None:
    repository = Definitions(
        assets=[
            calendar_event_sync,
            voice_memos_drive_ingest,
            voice_memos_transcription,
            voice_memos_enrichment,
        ],
    ).get_repository_def()

    assert voice_memos_enrichment_job.selection.resolve(repository.asset_graph) == {
        AssetKey("calendar_event_sync"),
        AssetKey("voice_memos_drive_ingest"),
        AssetKey("voice_memos_transcription"),
        AssetKey("voice_memos_enrichment"),
    }


def test_voice_memos_enrichment_schedule_runs_hourly() -> None:
    assert voice_memos_enrichment_hourly.cron_schedule == "17 * * * *"
    assert voice_memos_enrichment_hourly.default_status.value == "RUNNING"
