"""Dagster wiring for photo vision enrichment (mirrors the WhatsApp pipeline)."""

from __future__ import annotations

from dagster import AssetKey, DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs import photo_enrichment as photo_enrichment_defs
from personal_data_warehouse.defs.photo_enrichment import (
    DEFAULT_PHOTO_ENRICHMENT_BATCH_SIZE,
    photo_enrichment,
    photo_enrichment_batch_size,
    photo_enrichment_hourly,
    photo_enrichment_job,
    photo_enrichment_max_error_attempts,
)
from personal_data_warehouse.file_attachment_enrichment import PHOTOS_SOURCE


def test_photo_enrichment_job_selects_asset() -> None:
    assert photo_enrichment_job.name == "photo_enrichment_job"


def test_photo_enrichment_depends_on_identity() -> None:
    assert photo_enrichment.asset_deps[AssetKey("photo_enrichment")] == {
        AssetKey("photo_identity"),
    }


def test_photo_enrichment_schedule_runs_hourly() -> None:
    assert photo_enrichment_hourly.cron_schedule == "23 * * * *"
    assert photo_enrichment_hourly.default_status.value == "RUNNING"


def test_photo_enrichment_backlog_sensor_runs_every_two_minutes() -> None:
    sensor = photo_enrichment_defs.photo_enrichment_backlog_sensor
    assert sensor.minimum_interval_seconds == 120
    assert sensor.default_status.value == "RUNNING"


def test_photo_enrichment_source_targets_canonical_renditions() -> None:
    # One enrichment per LOGICAL photo: the source scans the marts view whose
    # rows are the identity layer's per-asset thumbnails, with its own
    # task_type + prompt_version and a photo-tuned instructions block.
    assert PHOTOS_SOURCE.table == "photo_canonical_renditions"
    assert PHOTOS_SOURCE.task_type == "photo_enrichment"
    assert PHOTOS_SOURCE.prompt_version == "photo-agent-v1"
    assert PHOTOS_SOURCE.order_column == "capture_ts"
    assert not PHOTOS_SOURCE.pdf_requires_prior_extraction
    assert "personal photo" in PHOTOS_SOURCE.vision_instructions


def test_photo_enrichment_batch_size_defaults(monkeypatch) -> None:
    monkeypatch.delenv("PHOTO_ENRICHMENT_BATCH_SIZE", raising=False)
    assert photo_enrichment_batch_size() == DEFAULT_PHOTO_ENRICHMENT_BATCH_SIZE

    monkeypatch.setenv("PHOTO_ENRICHMENT_BATCH_SIZE", "7")
    assert photo_enrichment_batch_size() == 7


def test_photo_enrichment_max_error_attempts_env(monkeypatch) -> None:
    monkeypatch.delenv("PHOTO_ENRICHMENT_MAX_ERROR_ATTEMPTS", raising=False)
    assert photo_enrichment_max_error_attempts() == 3

    monkeypatch.setenv("PHOTO_ENRICHMENT_MAX_ERROR_ATTEMPTS", "9")
    assert photo_enrichment_max_error_attempts() == 9


def test_photo_enrichment_backlog_sensor_skips_when_backlog_is_empty(monkeypatch) -> None:
    calls = []
    warehouse = FakeWarehouse()
    monkeypatch.setattr(photo_enrichment_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(photo_enrichment_defs, "warehouse_from_settings", lambda _settings: warehouse)
    monkeypatch.setattr(
        photo_enrichment_defs,
        "has_file_enrichment_candidate",
        lambda *args, **kwargs: calls.append((args, kwargs)) or False,
    )

    with DagsterInstance.ephemeral() as instance:
        result = photo_enrichment_defs.photo_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "No photos" in result.skip_message
    assert calls[0][1]["source"] is PHOTOS_SOURCE
    assert calls[0][1]["prompt_version"] == PHOTOS_SOURCE.prompt_version
    assert warehouse.closed


def test_photo_enrichment_backlog_sensor_launches_when_backlog_exists(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(photo_enrichment_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(photo_enrichment_defs, "warehouse_from_settings", lambda _settings: warehouse)
    monkeypatch.setattr(photo_enrichment_defs, "has_file_enrichment_candidate", lambda *a, **k: True)

    with DagsterInstance.ephemeral() as instance:
        result = photo_enrichment_defs.photo_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, RunRequest)
    assert result.tags == {"photos_trigger": "enrichment_backlog"}
    assert warehouse.closed


def test_photo_enrichment_backlog_sensor_skips_when_tables_missing(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(photo_enrichment_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(photo_enrichment_defs, "warehouse_from_settings", lambda _settings: warehouse)

    def boom(*_args, **_kwargs):
        raise RuntimeError('relation "photo_canonical_renditions" does not exist')

    monkeypatch.setattr(photo_enrichment_defs, "has_file_enrichment_candidate", boom)

    with DagsterInstance.ephemeral() as instance:
        result = photo_enrichment_defs.photo_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "not ready yet" in result.skip_message
    assert warehouse.closed


def test_photo_enrichment_registered_in_repository() -> None:
    from personal_data_warehouse.definitions import defs

    repository = defs().get_repository_def()
    assert "photo_enrichment_backlog_sensor" in {sensor.name for sensor in repository.sensor_defs}
    assert "photo_enrichment_hourly" in {schedule.name for schedule in repository.schedule_defs}
    assert repository.has_job("photo_enrichment_job")


class FakeSettings:
    photos = object()
    agent = type("FakeAgentConfig", (), {"provider": "codex", "model": "gpt-5.6"})()


class FakeWarehouse:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True
