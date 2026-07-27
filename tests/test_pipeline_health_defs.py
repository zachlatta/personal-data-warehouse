from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from dagster import build_asset_context, build_schedule_context

import personal_data_warehouse.defs.pipeline_health as pipeline_health_defs
from personal_data_warehouse.pipeline_health import (
    PROBE_OK,
    PROBE_SKIPPED_UNINDEXED,
    PROBE_TIMEOUT,
    PipelineSnapshot,
    TableSnapshot,
)


class _FakeWarehouse:
    def __init__(self) -> None:
        self.ensured = False
        self.closed = False

    def ensure_pipeline_health_tables(self) -> None:
        self.ensured = True

    def close(self) -> None:
        self.closed = True


class _FakeCollector:
    instances: list["_FakeCollector"] = []

    def __init__(self, warehouse) -> None:
        self.warehouse = warehouse
        self.ran = False
        _FakeCollector.instances.append(self)

    def run(self):
        self.ran = True
        return _snapshots()


def _pipeline(name: str, **kwargs) -> PipelineSnapshot:
    defaults = dict(
        pipeline=name,
        label=name,
        kind="source",
        cadence="every 5 min",
        transport="test",
        note="",
        expected_data_interval_seconds=3600,
        expected_run_interval_seconds=0,
        last_write_at=None,
        newest_event_at=None,
        last_run_at=None,
        row_estimate=0,
        byte_size=0,
        table_count=1,
        tables_probed=1,
        tables_skipped=0,
        state_table="",
        state_rows=0,
        state_error_rows=0,
        state_attention_rows=0,
        last_error="",
        last_error_at=None,
    )
    defaults.update(kwargs)
    return PipelineSnapshot(**defaults)


def _table(table_id: str, probe_status: str) -> TableSnapshot:
    return TableSnapshot(
        table_id=table_id,
        pipeline="gmail",
        role="data",
        layer="base",
        table_schema="base_gmail",
        table_name="messages",
        written_at_column="synced_at",
        event_at_column="internal_date",
        last_write_at=None,
        newest_event_at=None,
        row_estimate=0,
        byte_size=0,
        probe_status=probe_status,
        probe_detail="",
        probe_ms=1,
        note="",
    )


def _snapshots():
    return (
        [
            _pipeline("gmail"),
            _pipeline(
                "slack",
                state_table="slack_sync_state",
                state_error_rows=2,
                last_error="ratelimited",
            ),
            _pipeline("plaid", state_table="plaid_sync_state", state_attention_rows=1),
        ],
        [
            _table("gmail_messages", PROBE_OK),
            _table("timeline_events", PROBE_SKIPPED_UNINDEXED),
            _table("slack_message_reactions", PROBE_TIMEOUT),
        ],
    )


@contextmanager
def _acquired_lock(**_kwargs):
    yield True


@contextmanager
def _busy_lock(**_kwargs):
    yield False


def _patch_common(monkeypatch) -> _FakeWarehouse:
    warehouse = _FakeWarehouse()
    _FakeCollector.instances = []
    monkeypatch.setattr(
        pipeline_health_defs,
        "load_settings",
        lambda **_: SimpleNamespace(postgres_database_url="postgresql://example/warehouse"),
    )
    monkeypatch.setattr(pipeline_health_defs, "warehouse_from_settings", lambda _settings: warehouse)
    monkeypatch.setattr(pipeline_health_defs, "PipelineHealthCollector", _FakeCollector)
    return warehouse


def test_asset_collects_and_reports_probe_outcomes(monkeypatch):
    warehouse = _patch_common(monkeypatch)
    monkeypatch.setattr(pipeline_health_defs, "exclusive_sync_lock", _acquired_lock)

    result = pipeline_health_defs.pipeline_health(build_asset_context())

    assert warehouse.ensured
    assert warehouse.closed
    assert _FakeCollector.instances[0].ran
    assert result.metadata["pipelines"].value == 3
    assert result.metadata["tables"].value == 3
    # A cost-guard skip is expected; a timeout is the one worth noticing.
    assert result.metadata["probes_skipped"].value == ["timeline_events"]
    assert result.metadata["probes_failed"].value == ["slack_message_reactions"]
    assert result.metadata["pipelines_with_errors"].value == ["slack"]
    assert result.metadata["pipelines_needing_attention"].value == ["plaid"]


def test_asset_skips_when_another_collection_is_active(monkeypatch):
    warehouse = _patch_common(monkeypatch)
    monkeypatch.setattr(pipeline_health_defs, "exclusive_sync_lock", _busy_lock)

    result = pipeline_health_defs.pipeline_health(build_asset_context())

    assert _FakeCollector.instances == []
    assert not warehouse.ensured
    assert warehouse.closed
    assert result.metadata["pipelines"].value == 0


def test_schedule_runs_every_ten_minutes_and_skips_overlap(monkeypatch):
    assert pipeline_health_defs.PIPELINE_HEALTH_CRON == "*/10 * * * *"
    calls = {}

    def _fake_guard(context, *, job_name):
        calls["job_name"] = job_name
        return {"skipped": True}

    monkeypatch.setattr(pipeline_health_defs, "skip_if_job_active", _fake_guard)
    result = pipeline_health_defs.pipeline_health_every_ten_minutes._execution_fn.decorated_fn(
        build_schedule_context()
    )
    assert result == {"skipped": True}
    assert calls["job_name"] == "pipeline_health_job"


def test_defs_expose_the_asset_job_and_schedule():
    defs = pipeline_health_defs.defs()
    assert [spec.key.to_user_string() for spec in defs.resolve_all_asset_specs()] == [
        "pipeline_health"
    ]
    assert [job.name for job in defs.jobs] == ["pipeline_health_job"]
    assert [schedule.name for schedule in defs.schedules] == ["pipeline_health_every_ten_minutes"]
