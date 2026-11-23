from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

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


def test_the_resident_worker_holds_an_executor_for_every_cloud_provider(monkeypatch) -> None:
    # The resident worker and the Dagster fallback both take their executors
    # from build_cloud_mutation_executors; this builds the resident one with
    # the real constructors, so an executor that cannot be built fails here.
    from personal_data_warehouse import upstream_mutation_worker
    from personal_data_warehouse.defs.upstream_mutations import CLOUD_MUTATION_PROVIDERS

    class _Warehouse:
        def ensure_upstream_mutation_tables(self) -> None:
            pass

    monkeypatch.setattr(upstream_mutation_worker, "load_settings", lambda **kwargs: SimpleNamespace(resideo=None))
    monkeypatch.setattr(upstream_mutation_worker, "warehouse_from_settings", lambda settings: _Warehouse())

    executors = upstream_mutation_worker.CloudMutationProcessor().executors.by_provider()

    assert tuple(executors) == CLOUD_MUTATION_PROVIDERS
    assert all(executor is not None for executor in executors.values())


def test_the_built_executors_resync_gmail_settings_after_a_settings_mutation(monkeypatch) -> None:
    # 24b156c: only one worker passed the re-sync hook, so a label created by
    # the other stayed unknown to base_gmail.labels for half an hour.
    from personal_data_warehouse.defs import upstream_mutations

    resynced: list[set[str]] = []
    monkeypatch.setattr(
        upstream_mutations,
        "resync_gmail_settings",
        lambda *, settings, warehouse, accounts, logger: resynced.append(set(accounts)),
    )
    executors = upstream_mutations.build_cloud_mutation_executors(
        settings=SimpleNamespace(resideo=None), warehouse=object(), logger=None
    )

    executors.on_gmail_settings_changed({"zach@example.test"})

    assert resynced == [{"zach@example.test"}]


def test_every_provider_the_go_proposal_api_writes_has_a_python_worker_route() -> None:
    from personal_data_warehouse.defs.upstream_mutations import (
        APPLIED_AT_APPROVAL_MUTATION_PROVIDERS,
        CLOUD_MUTATION_PROVIDERS,
        LOCAL_ONLY_MUTATION_PROVIDERS,
    )

    routes = {
        "CloudMutationExecutors": set(CLOUD_MUTATION_PROVIDERS),
        "LOCAL_ONLY_MUTATION_PROVIDERS": set(LOCAL_ONLY_MUTATION_PROVIDERS),
        "APPLIED_AT_APPROVAL_MUTATION_PROVIDERS": set(APPLIED_AT_APPROVAL_MUTATION_PROVIDERS),
    }
    go_providers = go_mutation_providers()
    assert {"gmail", "google_people", "slack", "resideo", "apple_notes", "pdw"} <= go_providers
    unrouted = sorted(go_providers - set().union(*routes.values()))
    assert not unrouted, (
        f"the Go proposal API writes provider(s) {unrouted} that no worker claims, so approved rows wait forever. "
        "Add a field for each to CloudMutationExecutors (src/personal_data_warehouse/defs/upstream_mutations.py) "
        "and build it in build_cloud_mutation_executors, or list it in LOCAL_ONLY_MUTATION_PROVIDERS with its Go "
        "resident worker (app/internal/mutationworkers)."
    )
    routed_twice = sorted(
        provider
        for provider in set().union(*routes.values())
        if sum(provider in members for members in routes.values()) > 1
    )
    assert not routed_twice, f"provider(s) {routed_twice} are routed to more than one worker: {routes}"


def go_mutation_providers() -> set[str]:
    import re

    root = Path(__file__).resolve().parents[1] / "app/internal/mutations"
    pattern = re.compile(r'(?:\b\w+Provider\s*=\s*|\bProvider:\s*)"([a-z_]+)"')
    return {
        match
        for path in root.glob("*.go")
        if not path.name.endswith("_test.go")
        for match in pattern.findall(path.read_text())
    }
