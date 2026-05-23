from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs import upstream_mutations as upstream_mutation_defs
from personal_data_warehouse.contact_mutations import ContactMutationResult
from personal_data_warehouse.gmail_mutations import GmailMutationResult


class FakeWarehouse:
    def __init__(self, *, approved_count: int = 0, claimed=None, observed: int = 0) -> None:
        self.approved_count = approved_count
        self.claimed = list(claimed or [])
        self.observed = observed
        self.closed = False
        self.completed = []
        self.failed = []
        self.claim_limit = None
        self.claimed_by = None
        self.ensure_called = False

    def ensure_upstream_mutation_tables(self) -> None:
        self.ensure_called = True

    def approved_upstream_mutation_count(self) -> int:
        return self.approved_count

    def observe_succeeded_gmail_archive_mutations(self) -> int:
        return self.observed

    def observe_succeeded_gmail_unarchive_mutations(self) -> int:
        return 0

    def observe_succeeded_gmail_email_mutations(self) -> int:
        return 0

    def observe_succeeded_contact_mutations(self) -> int:
        return 0

    def claim_approved_upstream_mutations(self, *, limit: int, claimed_by: str):
        self.claim_limit = limit
        self.claimed_by = claimed_by
        return self.claimed

    def complete_upstream_mutation(self, mutation_id: str, *, result_json: dict, actor_id: str) -> None:
        self.completed.append((mutation_id, result_json, actor_id))

    def fail_upstream_mutation(
        self,
        mutation_id: str,
        *,
        status: str,
        error: str,
        result_json: dict,
        actor_id: str,
    ) -> None:
        self.failed.append((mutation_id, status, error, result_json, actor_id))

    def close(self) -> None:
        self.closed = True


class FakeExecutor:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.seen = []

    def execute(self, mutation):
        self.seen.append(mutation)
        return self.results.pop(0)


def test_upstream_mutation_sensor_runs_every_ten_seconds() -> None:
    sensor = upstream_mutation_defs.upstream_mutation_sensor

    assert sensor.minimum_interval_seconds == 10
    assert sensor.default_status.value == "RUNNING"


def test_upstream_mutation_sensor_skips_when_job_is_in_progress(monkeypatch) -> None:
    monkeypatch.setattr(
        upstream_mutation_defs,
        "skip_if_job_in_progress",
        lambda context, job_name: SkipReason("already active"),
    )
    monkeypatch.setattr(
        upstream_mutation_defs,
        "warehouse_from_settings",
        lambda settings: (_ for _ in ()).throw(AssertionError("warehouse should not be opened")),
    )

    with DagsterInstance.ephemeral() as instance:
        result = upstream_mutation_defs.upstream_mutation_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert result.skip_message == "already active"


def test_upstream_mutation_sensor_emits_run_when_approved_work_exists(monkeypatch) -> None:
    warehouse = FakeWarehouse(approved_count=2)
    monkeypatch.setattr(upstream_mutation_defs, "load_settings", lambda **_kwargs: object())
    monkeypatch.setattr(upstream_mutation_defs, "warehouse_from_settings", lambda _settings: warehouse)

    with DagsterInstance.ephemeral() as instance:
        result = upstream_mutation_defs.upstream_mutation_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"upstream_mutation_trigger": "approved_backlog", "approved_mutation_count": "2"}
    assert warehouse.closed is True


def test_process_upstream_mutation_batch_completes_and_fails_claimed_rows() -> None:
    mutation_a = {"id": "mut-a"}
    mutation_b = {"id": "mut-b"}
    warehouse = FakeWarehouse(
        claimed=[mutation_a, mutation_b],
        observed=1,
    )
    executor = FakeExecutor(
        [
            GmailMutationResult(status="succeeded", result_json={"archived_thread_ids": ["thread-1"]}),
            GmailMutationResult(status="failed_retryable", result_json={}, error="try again"),
        ]
    )
    contact_executor = FakeExecutor([])

    summary = upstream_mutation_defs.process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=executor,
        contact_executor=contact_executor,
        limit=5,
        claimed_by="worker-1",
    )

    assert warehouse.ensure_called is True
    assert warehouse.claim_limit == 5
    assert warehouse.claimed_by == "worker-1"
    assert warehouse.completed == [("mut-a", {"archived_thread_ids": ["thread-1"]}, "worker-1")]
    assert warehouse.failed == [("mut-b", "failed_retryable", "try again", {}, "worker-1")]
    assert summary.claimed == 2
    assert summary.succeeded == 1
    assert summary.failed_retryable == 1
    assert summary.observed == 1


def test_process_upstream_mutation_batch_routes_contact_mutations_to_contact_executor() -> None:
    mutation = {"id": "mut-contact", "provider": "google_people", "operation": "contacts.batch_mutation"}
    warehouse = FakeWarehouse(claimed=[mutation])
    gmail_executor = FakeExecutor([])
    contact_executor = FakeExecutor(
        [ContactMutationResult(status="succeeded", result_json={"operation_results": []})]
    )

    summary = upstream_mutation_defs.process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=gmail_executor,
        contact_executor=contact_executor,
        limit=5,
        claimed_by="worker-1",
    )

    assert contact_executor.seen == [mutation]
    assert gmail_executor.seen == []
    assert warehouse.completed == [("mut-contact", {"operation_results": []}, "worker-1")]
    assert summary.succeeded == 1
