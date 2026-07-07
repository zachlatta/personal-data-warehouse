from __future__ import annotations

from dagster import AssetKey, DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs import (
    whatsapp_audio_transcription as whatsapp_audio_transcription_defs,
)
from personal_data_warehouse.defs.whatsapp_audio_transcription import (
    DEFAULT_WHATSAPP_AUDIO_TRANSCRIPTION_BATCH_SIZE,
    whatsapp_audio_transcription,
    whatsapp_audio_transcription_batch_size,
    whatsapp_audio_transcription_hourly,
    whatsapp_audio_transcription_job,
    whatsapp_audio_transcription_max_error_attempts,
)
from personal_data_warehouse.audio_attachment_enrichment import WHATSAPP_AUDIO_SOURCE


def test_whatsapp_audio_transcription_job_selects_asset() -> None:
    assert whatsapp_audio_transcription_job.name == "whatsapp_audio_transcription_job"


def test_whatsapp_audio_transcription_depends_on_drive_ingest() -> None:
    assert whatsapp_audio_transcription.asset_deps[AssetKey("whatsapp_audio_transcription")] == {
        AssetKey("whatsapp_drive_ingest"),
    }


def test_whatsapp_audio_transcription_schedule_runs_hourly() -> None:
    assert whatsapp_audio_transcription_hourly.cron_schedule == "13 * * * *"
    assert whatsapp_audio_transcription_hourly.default_status.value == "RUNNING"


def test_whatsapp_audio_transcription_backlog_sensor_runs_every_two_minutes() -> None:
    sensor = whatsapp_audio_transcription_defs.whatsapp_audio_transcription_backlog_sensor

    assert sensor.minimum_interval_seconds == 120
    assert sensor.default_status.value == "RUNNING"


def test_whatsapp_audio_transcription_batch_size_defaults(monkeypatch) -> None:
    monkeypatch.delenv("WHATSAPP_AUDIO_TRANSCRIPTION_BATCH_SIZE", raising=False)
    assert whatsapp_audio_transcription_batch_size() == DEFAULT_WHATSAPP_AUDIO_TRANSCRIPTION_BATCH_SIZE

    monkeypatch.setenv("WHATSAPP_AUDIO_TRANSCRIPTION_BATCH_SIZE", "7")
    assert whatsapp_audio_transcription_batch_size() == 7


def test_whatsapp_audio_transcription_max_error_attempts_env(monkeypatch) -> None:
    monkeypatch.delenv("WHATSAPP_AUDIO_TRANSCRIPTION_MAX_ERROR_ATTEMPTS", raising=False)
    assert whatsapp_audio_transcription_max_error_attempts() == 3

    monkeypatch.setenv("WHATSAPP_AUDIO_TRANSCRIPTION_MAX_ERROR_ATTEMPTS", "9")
    assert whatsapp_audio_transcription_max_error_attempts() == 9


def test_whatsapp_audio_transcription_backlog_sensor_skips_when_backlog_is_empty(monkeypatch) -> None:
    calls = []
    warehouse = FakeWarehouse()
    monkeypatch.setattr(whatsapp_audio_transcription_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(whatsapp_audio_transcription_defs, "warehouse_from_settings", lambda _settings: warehouse)
    monkeypatch.setattr(
        whatsapp_audio_transcription_defs,
        "has_audio_enrichment_candidate",
        lambda *args, **kwargs: calls.append((args, kwargs)) or False,
    )

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_audio_transcription_defs.whatsapp_audio_transcription_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "No WhatsApp voice messages" in result.skip_message
    assert calls[0][1]["source"] is WHATSAPP_AUDIO_SOURCE
    assert calls[0][1]["prompt_version"] == WHATSAPP_AUDIO_SOURCE.prompt_version
    assert warehouse.closed


def test_whatsapp_audio_transcription_backlog_sensor_launches_when_backlog_exists(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(whatsapp_audio_transcription_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(whatsapp_audio_transcription_defs, "warehouse_from_settings", lambda _settings: warehouse)
    monkeypatch.setattr(
        whatsapp_audio_transcription_defs,
        "has_audio_enrichment_candidate",
        lambda *args, **kwargs: True,
    )

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_audio_transcription_defs.whatsapp_audio_transcription_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, RunRequest)
    assert result.tags == {"whatsapp_audio_trigger": "transcription_backlog"}
    assert warehouse.closed


def test_whatsapp_audio_transcription_backlog_sensor_skips_when_tables_missing(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(whatsapp_audio_transcription_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(whatsapp_audio_transcription_defs, "warehouse_from_settings", lambda _settings: warehouse)

    def boom(*_args, **_kwargs):
        raise RuntimeError('relation "whatsapp_media_items" does not exist')

    monkeypatch.setattr(whatsapp_audio_transcription_defs, "has_audio_enrichment_candidate", boom)

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_audio_transcription_defs.whatsapp_audio_transcription_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "not ready yet" in result.skip_message
    assert warehouse.closed


class FakeSettings:
    whatsapp = object()
    assemblyai = type(
        "FakeAssemblyAIConfig",
        (),
        {
            "api_key": "key",
            "base_url": "https://api.assemblyai.com",
            "poll_interval_seconds": 5,
            "timeout_seconds": 1800,
            "min_speakers_expected": None,
            "max_speakers_expected": None,
        },
    )()
    agent = type(
        "FakeAgentConfig",
        (),
        {"provider": "codex", "model": "gpt-agent"},
    )()


class FakeWarehouse:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True
