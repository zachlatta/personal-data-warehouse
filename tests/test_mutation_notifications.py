from __future__ import annotations

from pathlib import Path

from personal_data_warehouse.mutation_notifications import (
    MUTATION_NOTIFICATION_CHANNEL,
    run_notification_loop,
)


class _FakeListener:
    def __init__(self) -> None:
        self.wait_calls = 0
        self.closed = False

    def wait(self, timeout_seconds: float) -> bool:
        assert timeout_seconds == 30
        self.wait_calls += 1
        return True

    def close(self) -> None:
        self.closed = True


def test_notification_channel_is_stable_across_the_go_writer_and_python_workers() -> None:
    assert MUTATION_NOTIFICATION_CHANNEL == "pdw_upstream_mutations"


def test_resident_loop_drains_on_startup_and_again_after_a_notification() -> None:
    listener = _FakeListener()
    results = iter([True, False, True, False])
    process_calls = []

    def process_pending() -> bool:
        process_calls.append(True)
        return next(results)

    run_notification_loop(
        listener_factory=lambda: listener,
        process_pending=process_pending,
        stop_requested=lambda: len(process_calls) >= 4,
        poll_interval_seconds=30,
    )

    assert len(process_calls) == 4
    assert listener.wait_calls == 1
    assert listener.closed is True


def test_resident_loop_periodically_checks_the_durable_queue_without_a_notification() -> None:
    listener = _FakeListener()
    listener.wait = lambda _timeout: False  # type: ignore[method-assign]
    process_calls = []

    def process_pending() -> bool:
        process_calls.append(True)
        return False

    run_notification_loop(
        listener_factory=lambda: listener,
        process_pending=process_pending,
        stop_requested=lambda: len(process_calls) >= 2,
        poll_interval_seconds=30,
    )

    assert len(process_calls) == 2
    assert listener.closed is True


def test_dagster_container_supervises_the_resident_cloud_worker() -> None:
    start_script = (Path(__file__).resolve().parents[1] / "docker/start-dagster.sh").read_text()

    assert "python -m personal_data_warehouse.upstream_mutation_worker" in start_script
    assert 'mutation_worker_pid=""' in start_script
