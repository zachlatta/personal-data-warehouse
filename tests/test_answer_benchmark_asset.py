from contextlib import contextmanager
from unittest.mock import Mock

import pytest
from dagster import DefaultScheduleStatus, build_asset_context, build_schedule_context

from personal_data_warehouse.defs import answer_benchmark as asset_module
from personal_data_warehouse.pipeline_health import DAY, pipeline


def test_weekly_schedule_and_health():
    schedule = asset_module.answer_benchmark_weekly
    assert schedule.cron_schedule == '17 6 * * 2'
    assert schedule.execution_timezone == 'UTC'
    assert schedule.default_status == DefaultScheduleStatus.RUNNING
    assert int(asset_module.answer_benchmark_job.tags['dagster/max_runtime']) == 21600
    assert pipeline('answer_benchmark').expected_data_interval == 8 * DAY
    assert pipeline('answer_benchmark').data_basis


def test_schedule_checks_for_active_job(monkeypatch):
    guard = Mock(return_value={})
    monkeypatch.setattr(asset_module, 'skip_if_job_active', guard)
    context = build_schedule_context()
    asset_module.answer_benchmark_weekly(context)
    guard.assert_called_once_with(context, job_name='answer_benchmark_job')


@pytest.mark.parametrize('acquired,failed', [(False, False), (True, False), (True, True)])
def test_asset_lock_runner_and_cleanup(monkeypatch, acquired, failed):
    warehouse = Mock()
    monkeypatch.setattr(asset_module, 'load_settings', Mock())
    monkeypatch.setattr(asset_module, 'warehouse_from_settings', Mock(return_value=warehouse))
    lock_calls = []

    @contextmanager
    def lock(**kwargs):
        lock_calls.append(kwargs)
        yield acquired

    monkeypatch.setattr(asset_module, 'exclusive_sync_lock', lock)
    summary = Mock(questions=50, correct=48, elapsed_p50_ms=22000, pdw_calls_p50=5, errors=0)
    run = Mock(return_value=(summary, []), side_effect=RuntimeError('test failure') if failed else None)
    monkeypatch.setattr(asset_module, 'run_recorded_benchmark', run)
    resource = Mock()
    resource.runner.return_value = Mock()
    monkeypatch.setattr(asset_module, 'benchmark_agent_resource', Mock(return_value=resource))
    with build_asset_context() as context:
        if failed:
            with pytest.raises(RuntimeError, match='test failure'):
                asset_module.answer_benchmark(context)
        else:
            asset_module.answer_benchmark(context)
    warehouse.close.assert_called_once()
    assert lock_calls == [dict(name='answer_benchmark', postgres_lock_id=asset_module.ANSWER_BENCHMARK_POSTGRES_LOCK_ID)]
    if acquired:
        assert run.call_args.kwargs['warehouse'] is warehouse
        assert run.call_args.kwargs['provider'] == 'codex'
        assert run.call_args.kwargs['model'] == 'gpt-6.1-sol'
    else:
        run.assert_not_called()
        resource.runner.assert_not_called()


def test_benchmark_model_overrides(monkeypatch):
    monkeypatch.setenv('ANSWER_BENCHMARK_AGENT_PROVIDER', 'claude')
    monkeypatch.setenv('ANSWER_BENCHMARK_AGENT_MODEL', 'test-model')
    assert asset_module.benchmark_model() == ('claude', 'test-model')


def test_resource_keeps_isolation_and_sets_question_timeout(monkeypatch):
    from personal_data_warehouse.agent_resource import AgentResource
    from personal_data_warehouse.answer_benchmark import DEFAULT_QUESTION_TIMEOUT_SECONDS

    resource = AgentResource(docker_image="test-image", auth_volume="test-auth", timeout_seconds=1800)
    monkeypatch.setattr(AgentResource, 'from_config', Mock(return_value=resource))
    for name in ('ANSWER_BENCHMARK_AGENT_PROVIDER', 'ANSWER_BENCHMARK_AGENT_MODEL'):
        monkeypatch.delenv(name, raising=False)
    configured = asset_module.benchmark_agent_resource(Mock())
    assert configured.provider == 'codex'
    assert configured.model == 'gpt-6.1-sol'
    assert configured.timeout_seconds == DEFAULT_QUESTION_TIMEOUT_SECONDS
    assert configured.auth_volume == 'test-auth'
    assert configured.docker_image == 'test-image'


def test_repository_discovers_answer_benchmark():
    from personal_data_warehouse.definitions import defs

    repository = defs().get_repository_def()
    assert repository.has_job('answer_benchmark_job')
    assert repository.get_schedule_def('answer_benchmark_weekly').job_name == 'answer_benchmark_job'
