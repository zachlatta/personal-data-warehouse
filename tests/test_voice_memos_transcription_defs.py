from __future__ import annotations

from dagster import AssetKey, Definitions

from personal_data_warehouse.defs.voice_memos_drive_ingest import voice_memos_drive_ingest
from personal_data_warehouse.defs.voice_memos_transcription import (
    DEFAULT_VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE,
    voice_memos_transcription,
    voice_memos_transcription_job,
    voice_memos_transcription_every_fifteen_minutes,
)


def test_voice_memos_transcription_job_selects_asset() -> None:
    assert voice_memos_transcription_job.name == "voice_memos_transcription_job"
    assert DEFAULT_VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE == 3


def test_voice_memos_transcription_depends_on_drive_ingest() -> None:
    assert voice_memos_transcription.asset_deps[AssetKey("voice_memos_transcription")] == {
        AssetKey("voice_memos_drive_ingest")
    }


def test_voice_memos_transcription_job_selects_upstream_ingest() -> None:
    repository = Definitions(
        assets=[voice_memos_drive_ingest, voice_memos_transcription],
    ).get_repository_def()

    assert voice_memos_transcription_job.selection.resolve(repository.asset_graph) == {
        AssetKey("voice_memos_drive_ingest"),
        AssetKey("voice_memos_transcription"),
    }


def test_voice_memos_transcription_schedule_runs_every_fifteen_minutes() -> None:
    assert voice_memos_transcription_every_fifteen_minutes.cron_schedule == "*/15 * * * *"
    assert voice_memos_transcription_every_fifteen_minutes.default_status.value == "RUNNING"
