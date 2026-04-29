from __future__ import annotations

from dagster import AssetKey, DagsterInstance, Definitions, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs.voice_memos_drive_ingest import voice_memos_drive_ingest
from personal_data_warehouse.defs import voice_memos_transcription as voice_memos_transcription_defs
from personal_data_warehouse.defs.voice_memos_transcription import (
    DEFAULT_VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE,
    voice_memos_transcription,
    voice_memos_transcription_job,
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


def test_voice_memos_transcription_backlog_sensor_runs_every_minute() -> None:
    sensor = voice_memos_transcription_defs.voice_memos_transcription_backlog_sensor

    assert sensor.minimum_interval_seconds == 60
    assert sensor.default_status.value == "RUNNING"


def test_voice_memos_transcription_backlog_sensor_skips_when_backlog_is_empty(monkeypatch) -> None:
    fake_warehouse = FakeWarehouse([])
    monkeypatch.setattr(
        voice_memos_transcription_defs,
        "load_settings",
        lambda **_kwargs: FakeSettings(),
    )
    monkeypatch.setattr(
        voice_memos_transcription_defs,
        "ClickHouseWarehouse",
        lambda _url: fake_warehouse,
    )

    with DagsterInstance.ephemeral() as instance:
        result = voice_memos_transcription_defs.voice_memos_transcription_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "No untranscribed Voice Memos" in result.skip_message
    assert fake_warehouse.provider == "assemblyai"
    assert fake_warehouse.limit == 1


def test_voice_memos_transcription_backlog_sensor_launches_when_backlog_exists(monkeypatch) -> None:
    monkeypatch.setattr(
        voice_memos_transcription_defs,
        "load_settings",
        lambda **_kwargs: FakeSettings(),
    )
    monkeypatch.setattr(
        voice_memos_transcription_defs,
        "ClickHouseWarehouse",
        lambda _url: FakeWarehouse([{"recording_id": "memo-1"}]),
    )

    with DagsterInstance.ephemeral() as instance:
        result = voice_memos_transcription_defs.voice_memos_transcription_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, RunRequest)
    assert result.tags == {"voice_memos_trigger": "transcription_backlog"}


class FakeSettings:
    clickhouse_url = "clickhouse://example"


class FakeWarehouse:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.provider = None
        self.limit = None

    def load_untranscribed_voice_memo_files(self, *, provider: str, limit: int):
        self.provider = provider
        self.limit = limit
        return self.rows
