from contextlib import contextmanager
from unittest.mock import Mock

import pytest
from dagster import DefaultScheduleStatus, build_asset_context, build_schedule_context

from personal_data_warehouse.defs import priority_benchmark as asset_module
from personal_data_warehouse.pipeline_health import DAY, TABLE_PIPELINES, pipeline
from personal_data_warehouse.priority_benchmark import LabelsUnpublishedError


def test_repository_registers_daily_priority_benchmark():
    from personal_data_warehouse.definitions import defs

    repository = defs().get_repository_def()
    assert repository.has_job("priority_benchmark_job")
    schedule = repository.get_schedule_def("priority_benchmark_daily")
    assert schedule.job_name == "priority_benchmark_job"
    assert schedule.cron_schedule == "37 5 * * *"
    assert schedule.execution_timezone == "UTC"
    assert schedule.default_status == DefaultScheduleStatus.RUNNING
    assert int(asset_module.priority_benchmark_job.tags["dagster/max_runtime"]) == 900
    assert pipeline("priority_benchmark").expected_data_interval == 2 * DAY
    assert (
        TABLE_PIPELINES["priority_benchmark_results"].pipeline == "priority_benchmark"
    )


def test_schedule_skips_an_active_job(monkeypatch):
    guard = Mock(return_value={})
    monkeypatch.setattr(asset_module, "skip_if_job_active", guard)
    context = build_schedule_context()
    asset_module.priority_benchmark_daily(context)
    guard.assert_called_once_with(context, job_name="priority_benchmark_job")


@pytest.mark.parametrize("outcome", ["locked", "unpublished", "measured", "error"])
def test_asset_measures_or_skips_and_always_closes(monkeypatch, outcome):
    warehouse = Mock()
    monkeypatch.setattr(asset_module, "load_settings", Mock())
    monkeypatch.setattr(
        asset_module, "warehouse_from_settings", Mock(return_value=warehouse)
    )
    lock_calls = []

    @contextmanager
    def lock(**kwargs):
        lock_calls.append(kwargs)
        yield outcome != "locked"

    monkeypatch.setattr(asset_module, "exclusive_sync_lock", lock)
    error = (
        LabelsUnpublishedError("not published")
        if outcome == "unpublished"
        else RuntimeError("test failure") if outcome == "error" else None
    )
    run = Mock(
        return_value=[dict(adapter="example", labeled=10, correct=9, missing=0)],
        side_effect=error,
    )
    monkeypatch.setattr(asset_module, "measure", run)
    with build_asset_context() as context:
        if outcome == "error":
            with pytest.raises(RuntimeError, match="test failure"):
                asset_module.priority_benchmark(context)
        else:
            result = asset_module.priority_benchmark(context)
            if outcome == "unpublished":
                assert result.metadata == {"skipped": "labels unpublished"}
            elif outcome == "measured":
                assert result.metadata == dict(
                    adapters=1, labeled=10, correct=9, missing=0
                )
    warehouse.close.assert_called_once()
    assert lock_calls == [
        dict(
            name="priority_benchmark",
            postgres_lock_id=asset_module.PRIORITY_BENCHMARK_POSTGRES_LOCK_ID,
        )
    ]
    if outcome == "locked":
        warehouse.ensure_priority_benchmark_tables.assert_not_called()
        run.assert_not_called()
    else:
        warehouse.ensure_priority_benchmark_tables.assert_called_once()
        run.assert_called_once_with(warehouse)
    warehouse.ensure_timeline_tables.assert_not_called()
