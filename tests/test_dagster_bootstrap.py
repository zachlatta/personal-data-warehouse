from __future__ import annotations

from dagster import DagsterInstance, DagsterRunStatus, job, op

from personal_data_warehouse.dagster_bootstrap import clear_in_progress_runs_at_boot


@op
def noop_op() -> None:
    pass


@job
def noop_job() -> None:
    noop_op()


def test_clear_in_progress_runs_at_boot_marks_active_runs_canceled() -> None:
    with DagsterInstance.ephemeral() as instance:
        started = instance.create_run_for_job(noop_job, status=DagsterRunStatus.STARTED)
        starting = instance.create_run_for_job(noop_job, status=DagsterRunStatus.STARTING)
        canceling = instance.create_run_for_job(noop_job, status=DagsterRunStatus.CANCELING)
        successful = instance.create_run_for_job(noop_job, status=DagsterRunStatus.SUCCESS)

        summary = clear_in_progress_runs_at_boot(instance)

        assert set(summary.canceled_run_ids) == {started.run_id, starting.run_id, canceling.run_id}
        assert instance.get_run_by_id(started.run_id).status == DagsterRunStatus.CANCELED
        assert instance.get_run_by_id(starting.run_id).status == DagsterRunStatus.CANCELED
        assert instance.get_run_by_id(canceling.run_id).status == DagsterRunStatus.CANCELED
        assert instance.get_run_by_id(successful.run_id).status == DagsterRunStatus.SUCCESS


def test_clear_in_progress_runs_at_boot_is_idempotent() -> None:
    with DagsterInstance.ephemeral() as instance:
        started = instance.create_run_for_job(noop_job, status=DagsterRunStatus.STARTED)

        first_summary = clear_in_progress_runs_at_boot(instance)
        second_summary = clear_in_progress_runs_at_boot(instance)

        assert first_summary.canceled_run_ids == [started.run_id]
        assert second_summary.cleared_count == 0


def test_dagster_bootstrap_main_sweeps_credentials_volumes_even_when_run_cleanup_disabled(monkeypatch) -> None:
    from personal_data_warehouse import dagster_bootstrap

    swept_timeouts: list[int] = []
    monkeypatch.setenv("DAGSTER_BOOT_CLEAR_IN_PROGRESS_RUNS", "0")
    monkeypatch.setenv("AGENT_TIMEOUT_SECONDS", "2100")
    monkeypatch.setattr(
        dagster_bootstrap,
        "sweep_stale_credentials_volumes",
        lambda *, timeout_seconds, **kwargs: swept_timeouts.append(timeout_seconds) or ["pdw-agent-credentials-old"],
    )

    dagster_bootstrap.main()

    assert swept_timeouts == [2100]


def test_dagster_bootstrap_main_continues_when_credentials_volume_sweep_fails(monkeypatch) -> None:
    from personal_data_warehouse import dagster_bootstrap

    monkeypatch.setenv("DAGSTER_BOOT_CLEAR_IN_PROGRESS_RUNS", "0")

    def broken_sweep(**kwargs):
        raise RuntimeError("docker unavailable at boot")

    monkeypatch.setattr(dagster_bootstrap, "sweep_stale_credentials_volumes", broken_sweep)

    # Must not raise even if the sweep throws an unexpected error.
    dagster_bootstrap.main()

