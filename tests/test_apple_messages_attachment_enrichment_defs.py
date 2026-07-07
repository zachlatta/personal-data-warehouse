from __future__ import annotations

from dagster import AssetKey, DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs import (
    apple_messages_attachment_enrichment as apple_messages_attachment_enrichment_defs,
)
from personal_data_warehouse.defs.apple_messages_attachment_enrichment import (
    DEFAULT_APPLE_MESSAGES_ATTACHMENT_ENRICHMENT_BATCH_SIZE,
    apple_messages_attachment_enrichment,
    apple_messages_attachment_enrichment_batch_size,
    apple_messages_attachment_enrichment_hourly,
    apple_messages_attachment_enrichment_job,
    apple_messages_attachment_enrichment_max_error_attempts,
)
from personal_data_warehouse.file_attachment_enrichment import APPLE_MESSAGES_SOURCE


def test_apple_messages_attachment_enrichment_job_selects_asset() -> None:
    assert apple_messages_attachment_enrichment_job.name == "apple_messages_attachment_enrichment_job"


def test_apple_messages_attachment_enrichment_depends_on_drive_ingest() -> None:
    assert apple_messages_attachment_enrichment.asset_deps[AssetKey("apple_messages_attachment_enrichment")] == {
        AssetKey("apple_messages_drive_ingest"),
    }


def test_apple_messages_attachment_enrichment_schedule_runs_hourly() -> None:
    assert apple_messages_attachment_enrichment_hourly.cron_schedule == "53 * * * *"
    assert apple_messages_attachment_enrichment_hourly.default_status.value == "RUNNING"


def test_apple_messages_attachment_enrichment_backlog_sensor_runs_every_two_minutes() -> None:
    sensor = apple_messages_attachment_enrichment_defs.apple_messages_attachment_enrichment_backlog_sensor

    assert sensor.minimum_interval_seconds == 120
    assert sensor.default_status.value == "RUNNING"


def test_apple_messages_attachment_enrichment_batch_size_defaults(monkeypatch) -> None:
    monkeypatch.delenv("APPLE_MESSAGES_ATTACHMENT_ENRICHMENT_BATCH_SIZE", raising=False)
    assert (
        apple_messages_attachment_enrichment_batch_size()
        == DEFAULT_APPLE_MESSAGES_ATTACHMENT_ENRICHMENT_BATCH_SIZE
    )

    monkeypatch.setenv("APPLE_MESSAGES_ATTACHMENT_ENRICHMENT_BATCH_SIZE", "7")
    assert apple_messages_attachment_enrichment_batch_size() == 7


def test_apple_messages_attachment_enrichment_max_error_attempts_env(monkeypatch) -> None:
    monkeypatch.delenv("APPLE_MESSAGES_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS", raising=False)
    assert apple_messages_attachment_enrichment_max_error_attempts() == 3

    monkeypatch.setenv("APPLE_MESSAGES_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS", "9")
    assert apple_messages_attachment_enrichment_max_error_attempts() == 9


def test_apple_messages_attachment_enrichment_backlog_sensor_skips_when_backlog_is_empty(monkeypatch) -> None:
    calls = []
    warehouse = FakeWarehouse()
    monkeypatch.setattr(apple_messages_attachment_enrichment_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(apple_messages_attachment_enrichment_defs, "warehouse_from_settings", lambda _settings: warehouse)
    monkeypatch.setattr(
        apple_messages_attachment_enrichment_defs,
        "has_file_enrichment_candidate",
        lambda *args, **kwargs: calls.append((args, kwargs)) or False,
    )

    with DagsterInstance.ephemeral() as instance:
        result = apple_messages_attachment_enrichment_defs.apple_messages_attachment_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "No Apple Messages attachments" in result.skip_message
    assert calls[0][1]["source"] is APPLE_MESSAGES_SOURCE
    assert calls[0][1]["prompt_version"] == APPLE_MESSAGES_SOURCE.prompt_version
    assert warehouse.closed


def test_apple_messages_attachment_enrichment_backlog_sensor_launches_when_backlog_exists(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(apple_messages_attachment_enrichment_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(apple_messages_attachment_enrichment_defs, "warehouse_from_settings", lambda _settings: warehouse)
    monkeypatch.setattr(
        apple_messages_attachment_enrichment_defs,
        "has_file_enrichment_candidate",
        lambda *args, **kwargs: True,
    )

    with DagsterInstance.ephemeral() as instance:
        result = apple_messages_attachment_enrichment_defs.apple_messages_attachment_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, RunRequest)
    assert result.tags == {"apple_messages_attachment_trigger": "enrichment_backlog"}
    assert warehouse.closed


def test_apple_messages_attachment_enrichment_backlog_sensor_skips_when_tables_missing(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(apple_messages_attachment_enrichment_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(apple_messages_attachment_enrichment_defs, "warehouse_from_settings", lambda _settings: warehouse)

    def boom(*_args, **_kwargs):
        raise RuntimeError('relation "apple_message_attachments" does not exist')

    monkeypatch.setattr(apple_messages_attachment_enrichment_defs, "has_file_enrichment_candidate", boom)

    with DagsterInstance.ephemeral() as instance:
        result = apple_messages_attachment_enrichment_defs.apple_messages_attachment_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "not ready yet" in result.skip_message
    assert warehouse.closed


class FakeSettings:
    apple_messages = object()
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
