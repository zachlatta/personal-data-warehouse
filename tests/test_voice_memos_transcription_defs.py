from __future__ import annotations

from personal_data_warehouse.defs.voice_memos_transcription import (
    DEFAULT_VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE,
    voice_memos_transcription_job,
    voice_memos_transcription_every_fifteen_minutes,
)


def test_voice_memos_transcription_job_selects_asset() -> None:
    assert voice_memos_transcription_job.name == "voice_memos_transcription_job"
    assert DEFAULT_VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE == 3


def test_voice_memos_transcription_schedule_runs_every_fifteen_minutes() -> None:
    assert voice_memos_transcription_every_fifteen_minutes.cron_schedule == "*/15 * * * *"
    assert voice_memos_transcription_every_fifteen_minutes.default_status.value == "RUNNING"
