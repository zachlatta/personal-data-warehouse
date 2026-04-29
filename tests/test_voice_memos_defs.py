from __future__ import annotations

from personal_data_warehouse.defs.voice_memos_drive_ingest import voice_memos_drive_ingest_every_five_minutes


def test_voice_memos_drive_ingest_schedule_runs_every_five_minutes() -> None:
    assert voice_memos_drive_ingest_every_five_minutes.cron_schedule == "*/5 * * * *"
    assert voice_memos_drive_ingest_every_five_minutes.default_status.value == "RUNNING"
