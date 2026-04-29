from __future__ import annotations

from dagster import AssetKey, DagsterInstance, Definitions, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs.calendar_sync import calendar_event_sync
from personal_data_warehouse.defs import voice_memos_enrichment as voice_memos_enrichment_defs
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


def test_voice_memos_enrichment_backlog_sensor_runs_every_minute() -> None:
    sensor = voice_memos_enrichment_defs.voice_memos_enrichment_backlog_sensor

    assert sensor.minimum_interval_seconds == 60
    assert sensor.default_status.value == "RUNNING"


def test_voice_memos_enrichment_backlog_sensor_skips_when_backlog_is_empty(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        voice_memos_enrichment_defs,
        "load_settings",
        lambda **_kwargs: FakeSettings(),
    )
    monkeypatch.setattr(
        voice_memos_enrichment_defs,
        "ClickHouseWarehouse",
        lambda _url: object(),
    )
    monkeypatch.setattr(
        voice_memos_enrichment_defs,
        "load_enrichment_candidates",
        lambda *args, **kwargs: calls.append((args, kwargs)) or [],
    )

    with DagsterInstance.ephemeral() as instance:
        result = voice_memos_enrichment_defs.voice_memos_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "No unenriched Voice Memos" in result.skip_message
    assert calls[0][1]["limit"] == 1
    assert calls[0][1]["model"] == "gpt-test"


def test_voice_memos_enrichment_backlog_sensor_launches_when_backlog_exists(monkeypatch) -> None:
    monkeypatch.setattr(
        voice_memos_enrichment_defs,
        "load_settings",
        lambda **_kwargs: FakeSettings(),
    )
    monkeypatch.setattr(
        voice_memos_enrichment_defs,
        "ClickHouseWarehouse",
        lambda _url: object(),
    )
    monkeypatch.setattr(
        voice_memos_enrichment_defs,
        "load_enrichment_candidates",
        lambda *args, **kwargs: [{"recording_id": "memo-1"}],
    )

    with DagsterInstance.ephemeral() as instance:
        result = voice_memos_enrichment_defs.voice_memos_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, RunRequest)
    assert result.tags == {"voice_memos_trigger": "enrichment_backlog"}


class FakeSettings:
    clickhouse_url = "clickhouse://example"
    openai = type("FakeOpenAIConfig", (), {"model": "gpt-test"})()
