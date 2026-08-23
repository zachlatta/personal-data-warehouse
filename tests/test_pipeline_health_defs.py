from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from dagster import build_asset_context, build_schedule_context

import personal_data_warehouse.defs.pipeline_health as pipeline_health_defs
from personal_data_warehouse.pipeline_health import (
    PROBE_EMPTY,
    PROBE_ERROR,
    PROBE_OK,
    PROBE_SKIPPED_EXPENSIVE,
    PROBE_SKIPPED_UNINDEXED,
    PROBE_TIMEOUT,
    MartViewSnapshot,
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

    def run_all(self):
        pipelines, tables = self.run()
        return pipelines, tables, _mart_snapshots()


def _pipeline(name: str, **kwargs) -> PipelineSnapshot:
    defaults = dict(
        pipeline=name,
        label=name,
        kind="source",
        cadence="every 5 min",
        transport="test",
        note="",
        data_basis="",
        expected_data_interval_seconds=3600,
        expected_run_interval_seconds=0,
        expected_event_interval_seconds=3600,
        last_write_at=None,
        newest_event_at=None,
        last_run_at=None,
        event_tables_probed=0,
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


def _mart(view_id: str, probe_status: str) -> MartViewSnapshot:
    return MartViewSnapshot(
        view_id=view_id,
        domain="ops",
        view_schema="marts_ops",
        view_name=view_id,
        input_tables=["gmail_messages"],
        input_pipelines=["gmail"],
        input_count=1,
        stalest_pipeline="gmail",
        stalest_pipeline_at=None,
        stalest_pipeline_expected_seconds=3600,
        inputs_unmeasured=0,
        has_rows=1,
        definition_sha256="sha",
        first_seen_at=None,
        probe_status=probe_status,
        probe_detail="",
        probe_ms=2,
        note="",
    )


def _mart_snapshots():
    return [
        _mart("marts_ok", PROBE_OK),
        _mart("marts_empty", PROBE_EMPTY),
        _mart("marts_broken", PROBE_ERROR),
        _mart("marts_costly", PROBE_SKIPPED_EXPENSIVE),
    ]


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
    # Level 2: a mart that cannot answer "is there a row?" is a broken read
    # interface, and an empty one is a different claim worth seeing separately.
    assert result.metadata["marts"].value == 4
    assert result.metadata["marts_failing_probe"].value == ["marts_broken"]
    assert result.metadata["marts_empty"].value == ["marts_empty"]


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
