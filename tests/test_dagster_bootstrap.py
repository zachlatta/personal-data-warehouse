from __future__ import annotations

from dagster import DagsterInstance, DagsterRunStatus, job, op

from personal_data_warehouse.dagster_bootstrap import clear_in_progress_runs_at_boot


@op
def noop_op() -> None:
    pass


@job
def noop_job() -> None:
    noop_op()


def test_clear_in_progress_runs_at_boot_marks_active_runs_terminal() -> None:
    with DagsterInstance.ephemeral() as instance:
        started = instance.create_run_for_job(noop_job, status=DagsterRunStatus.STARTED)
        starting = instance.create_run_for_job(noop_job, status=DagsterRunStatus.STARTING)
        canceling = instance.create_run_for_job(noop_job, status=DagsterRunStatus.CANCELING)
        successful = instance.create_run_for_job(noop_job, status=DagsterRunStatus.SUCCESS)

        summary = clear_in_progress_runs_at_boot(instance)

        assert set(summary.failed_run_ids) == {started.run_id, starting.run_id}
        assert summary.canceled_run_ids == [canceling.run_id]
        assert instance.get_run_by_id(started.run_id).status == DagsterRunStatus.FAILURE
        assert instance.get_run_by_id(starting.run_id).status == DagsterRunStatus.FAILURE
        assert instance.get_run_by_id(canceling.run_id).status == DagsterRunStatus.CANCELED
        assert instance.get_run_by_id(successful.run_id).status == DagsterRunStatus.SUCCESS


def test_clear_in_progress_runs_at_boot_is_idempotent() -> None:
    with DagsterInstance.ephemeral() as instance:
        started = instance.create_run_for_job(noop_job, status=DagsterRunStatus.STARTED)

        first_summary = clear_in_progress_runs_at_boot(instance)
        second_summary = clear_in_progress_runs_at_boot(instance)

        assert first_summary.failed_run_ids == [started.run_id]
        assert second_summary.cleared_count == 0
