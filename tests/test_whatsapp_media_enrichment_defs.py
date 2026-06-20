from __future__ import annotations

from dagster import AssetKey, DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs import whatsapp_media_enrichment as whatsapp_media_enrichment_defs
from personal_data_warehouse.defs.whatsapp_media_enrichment import (
    DEFAULT_WHATSAPP_MEDIA_ENRICHMENT_BATCH_SIZE,
    whatsapp_media_enrichment,
    whatsapp_media_enrichment_batch_size,
    whatsapp_media_enrichment_hourly,
    whatsapp_media_enrichment_job,
    whatsapp_media_enrichment_max_error_attempts,
)
from personal_data_warehouse.file_attachment_enrichment import WHATSAPP_SOURCE


def test_whatsapp_media_enrichment_job_selects_asset() -> None:
    assert whatsapp_media_enrichment_job.name == "whatsapp_media_enrichment_job"


def test_whatsapp_media_enrichment_depends_on_drive_ingest() -> None:
    assert whatsapp_media_enrichment.asset_deps[AssetKey("whatsapp_media_enrichment")] == {
        AssetKey("whatsapp_drive_ingest"),
    }


def test_whatsapp_media_enrichment_schedule_runs_hourly() -> None:
    assert whatsapp_media_enrichment_hourly.cron_schedule == "47 * * * *"
    assert whatsapp_media_enrichment_hourly.default_status.value == "RUNNING"


def test_whatsapp_media_enrichment_backlog_sensor_runs_every_two_minutes() -> None:
    sensor = whatsapp_media_enrichment_defs.whatsapp_media_enrichment_backlog_sensor

    assert sensor.minimum_interval_seconds == 120
    assert sensor.default_status.value == "RUNNING"


def test_whatsapp_media_enrichment_batch_size_defaults(monkeypatch) -> None:
    monkeypatch.delenv("WHATSAPP_MEDIA_ENRICHMENT_BATCH_SIZE", raising=False)
    assert whatsapp_media_enrichment_batch_size() == DEFAULT_WHATSAPP_MEDIA_ENRICHMENT_BATCH_SIZE

    monkeypatch.setenv("WHATSAPP_MEDIA_ENRICHMENT_BATCH_SIZE", "7")
    assert whatsapp_media_enrichment_batch_size() == 7


def test_whatsapp_media_enrichment_max_error_attempts_env(monkeypatch) -> None:
    monkeypatch.delenv("WHATSAPP_MEDIA_ENRICHMENT_MAX_ERROR_ATTEMPTS", raising=False)
    assert whatsapp_media_enrichment_max_error_attempts() == 3

    monkeypatch.setenv("WHATSAPP_MEDIA_ENRICHMENT_MAX_ERROR_ATTEMPTS", "9")
    assert whatsapp_media_enrichment_max_error_attempts() == 9


def test_whatsapp_media_enrichment_backlog_sensor_skips_when_backlog_is_empty(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(whatsapp_media_enrichment_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(whatsapp_media_enrichment_defs, "warehouse_from_settings", lambda _settings: object())
    monkeypatch.setattr(
        whatsapp_media_enrichment_defs,
        "has_file_enrichment_candidate",
        lambda *args, **kwargs: calls.append((args, kwargs)) or False,
    )

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_media_enrichment_defs.whatsapp_media_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "No WhatsApp media" in result.skip_message
    assert calls[0][1]["source"] is WHATSAPP_SOURCE
    assert calls[0][1]["prompt_version"] == WHATSAPP_SOURCE.prompt_version


def test_whatsapp_media_enrichment_backlog_sensor_launches_when_backlog_exists(monkeypatch) -> None:
    monkeypatch.setattr(whatsapp_media_enrichment_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(whatsapp_media_enrichment_defs, "warehouse_from_settings", lambda _settings: object())
    monkeypatch.setattr(
        whatsapp_media_enrichment_defs,
        "has_file_enrichment_candidate",
        lambda *args, **kwargs: True,
    )

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_media_enrichment_defs.whatsapp_media_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, RunRequest)
    assert result.tags == {"whatsapp_media_trigger": "enrichment_backlog"}


def test_whatsapp_media_enrichment_backlog_sensor_skips_when_tables_missing(monkeypatch) -> None:
    monkeypatch.setattr(whatsapp_media_enrichment_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(whatsapp_media_enrichment_defs, "warehouse_from_settings", lambda _settings: object())

    def boom(*_args, **_kwargs):
        raise RuntimeError('relation "whatsapp_media_items" does not exist')

    monkeypatch.setattr(whatsapp_media_enrichment_defs, "has_file_enrichment_candidate", boom)

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_media_enrichment_defs.whatsapp_media_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "not ready yet" in result.skip_message


class FakeSettings:
    whatsapp = object()
    agent = type(
        "FakeAgentConfig",
        (),
        {"provider": "codex", "model": "gpt-agent"},
    )()
