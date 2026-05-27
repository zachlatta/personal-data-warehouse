from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs import upstream_mutations as upstream_mutation_defs
from personal_data_warehouse.calendar_mutations import CalendarMutationResult
from personal_data_warehouse.contact_mutations import ContactMutationResult
from personal_data_warehouse.gmail_mutations import (
    GMAIL_ARCHIVE_OPERATION,
    GMAIL_UNARCHIVE_OPERATION,
    GmailMutationResult,
)


class FakeWarehouse:
    def __init__(
        self,
        *,
        approved_count: int = 0,
        claimed=None,
        observed: int = 0,
        message_ids_by_thread_id=None,
    ) -> None:
        self.approved_count = approved_count
        self.claimed = list(claimed or [])
        self.observed = observed
        self.message_ids_by_thread_id = dict(message_ids_by_thread_id or {})
        self.closed = False
        self.completed = []
        self.failed = []
        self.claim_limit = None
        self.claimed_by = None
        self.ensure_called = False
        self.message_id_lookups = []

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

    def observe_succeeded_calendar_event_mutations(self) -> int:
        return 0

    def claim_approved_upstream_mutations(self, *, limit: int, claimed_by: str):
        self.claim_limit = limit
        self.claimed_by = claimed_by
        return self.claimed

    def gmail_message_ids_for_thread_label_mutation(self, *, account: str, thread_ids: list[str], archive: bool):
        self.message_id_lookups.append((account, thread_ids, archive))
        return {thread_id: list(self.message_ids_by_thread_id.get(thread_id, [])) for thread_id in thread_ids}

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


class FakeGmailExecutor(FakeExecutor):
    def __init__(self, results, *, batch_results=None) -> None:
        super().__init__(results)
        self.batch_results = list(batch_results or [])
        self.batch_calls = []

    def execute_message_batch_modify(self, *, account: str, operation: str, message_ids: list[str]):
        self.batch_calls.append((account, operation, message_ids))
        return self.batch_results.pop(0)


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
    mutation_a = {"id": "mut-a", "provider": "gmail"}
    mutation_b = {"id": "mut-b", "provider": "gmail"}
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
        calendar_executor=FakeExecutor([]),
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


def test_process_upstream_mutation_batch_batches_gmail_archive_rows() -> None:
    mutation_a = {
        "id": "mut-a",
        "provider": "gmail",
        "operation": GMAIL_ARCHIVE_OPERATION,
        "account": "zach@example.test",
        "payload_json": {"thread_ids": ["thread-1"]},
    }
    mutation_b = {
        "id": "mut-b",
        "provider": "gmail",
        "operation": GMAIL_ARCHIVE_OPERATION,
        "account": "zach@example.test",
        "payload_json": {"thread_ids": ["thread-2"]},
    }
    warehouse = FakeWarehouse(
        claimed=[mutation_a, mutation_b],
        message_ids_by_thread_id={"thread-1": ["message-1"], "thread-2": ["message-2"]},
    )
    gmail_executor = FakeGmailExecutor(
        [],
        batch_results=[
            GmailMutationResult(
                status="succeeded",
                result_json={
                    "archived_message_ids": ["message-1", "message-2"],
                    "batch_modified_message_ids": ["message-1", "message-2"],
                },
            )
        ],
    )

    summary = upstream_mutation_defs.process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=gmail_executor,
        contact_executor=FakeExecutor([]),
        calendar_executor=FakeExecutor([]),
        limit=100,
        claimed_by="worker-1",
    )

    assert warehouse.message_id_lookups == [("zach@example.test", ["thread-1", "thread-2"], True)]
    assert gmail_executor.batch_calls == [
        ("zach@example.test", GMAIL_ARCHIVE_OPERATION, ["message-1", "message-2"])
    ]
    assert gmail_executor.seen == []
    assert warehouse.completed == [
        ("mut-a", {"archived_thread_ids": ["thread-1"], "batch_modified_message_ids": ["message-1"]}, "worker-1"),
        ("mut-b", {"archived_thread_ids": ["thread-2"], "batch_modified_message_ids": ["message-2"]}, "worker-1"),
    ]
    assert warehouse.failed == []
    assert summary.claimed == 2
    assert summary.succeeded == 2


def test_process_upstream_mutation_batch_falls_back_when_batch_modify_has_no_messages() -> None:
    mutation = {
        "id": "mut-a",
        "provider": "gmail",
        "operation": GMAIL_UNARCHIVE_OPERATION,
        "account": "zach@example.test",
        "payload_json": {"thread_ids": ["thread-1"]},
    }
    warehouse = FakeWarehouse(claimed=[mutation], message_ids_by_thread_id={})
    gmail_executor = FakeGmailExecutor(
        [GmailMutationResult(status="succeeded", result_json={"unarchived_thread_ids": ["thread-1"]})],
    )

    summary = upstream_mutation_defs.process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=gmail_executor,
        contact_executor=FakeExecutor([]),
        calendar_executor=FakeExecutor([]),
        limit=100,
        claimed_by="worker-1",
    )

    assert gmail_executor.batch_calls == []
    assert gmail_executor.seen == [mutation]
    assert warehouse.completed == [("mut-a", {"unarchived_thread_ids": ["thread-1"]}, "worker-1")]
    assert summary.succeeded == 1


def test_process_upstream_mutation_batch_records_retryable_batch_failure_progress() -> None:
    mutation_a = {
        "id": "mut-a",
        "provider": "gmail",
        "operation": GMAIL_ARCHIVE_OPERATION,
        "account": "zach@example.test",
        "payload_json": {"thread_ids": ["thread-1"]},
    }
    mutation_b = {
        "id": "mut-b",
        "provider": "gmail",
        "operation": GMAIL_ARCHIVE_OPERATION,
        "account": "zach@example.test",
        "payload_json": {"thread_ids": ["thread-2"]},
    }
    warehouse = FakeWarehouse(
        claimed=[mutation_a, mutation_b],
        message_ids_by_thread_id={"thread-1": ["message-1"], "thread-2": ["message-2"]},
    )
    gmail_executor = FakeGmailExecutor(
        [],
        batch_results=[
            GmailMutationResult(
                status="failed_retryable",
                result_json={
                    "archived_message_ids": ["message-1"],
                    "batch_modified_message_ids": ["message-1"],
                },
                error="network down",
            )
        ],
    )

    summary = upstream_mutation_defs.process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=gmail_executor,
        contact_executor=FakeExecutor([]),
        calendar_executor=FakeExecutor([]),
        limit=100,
        claimed_by="worker-1",
    )

    assert gmail_executor.seen == []
    assert warehouse.completed == []
    assert warehouse.failed == [
        (
            "mut-a",
            "failed_retryable",
            "network down",
            {"archived_thread_ids": ["thread-1"], "batch_modified_message_ids": ["message-1"]},
            "worker-1",
        ),
        (
            "mut-b",
            "failed_retryable",
            "network down",
            {"archived_thread_ids": ["thread-2"], "batch_modified_message_ids": []},
            "worker-1",
        ),
    ]
    assert summary.failed_retryable == 2


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
        calendar_executor=FakeExecutor([]),
        limit=5,
        claimed_by="worker-1",
    )

    assert contact_executor.seen == [mutation]
    assert gmail_executor.seen == []
    assert warehouse.completed == [("mut-contact", {"operation_results": []}, "worker-1")]
    assert summary.succeeded == 1


def test_process_upstream_mutation_batch_defers_unknown_provider() -> None:
    mutation = {"id": "mut-unknown", "provider": "future_provider", "operation": "future.op"}
    warehouse = FakeWarehouse(claimed=[mutation])

    summary = upstream_mutation_defs.process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=FakeExecutor([]),
        contact_executor=FakeExecutor([]),
        calendar_executor=FakeExecutor([]),
        limit=5,
        claimed_by="worker-1",
    )

    assert warehouse.completed == []
    assert len(warehouse.failed) == 1
    failed_id, status, error, _, _ = warehouse.failed[0]
    assert failed_id == "mut-unknown"
    assert status == "failed_retryable"
    assert "unsupported provider" in error
    assert summary.failed_retryable == 1


def test_process_upstream_mutation_batch_routes_calendar_mutations_to_calendar_executor() -> None:
    mutation = {
        "id": "mut-calendar",
        "provider": "google_calendar",
        "operation": "calendar.create_event",
    }
    warehouse = FakeWarehouse(claimed=[mutation])
    calendar_executor = FakeExecutor(
        [CalendarMutationResult(status="succeeded", result_json={"event_id": "evt-1"})]
    )

    summary = upstream_mutation_defs.process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=FakeExecutor([]),
        contact_executor=FakeExecutor([]),
        calendar_executor=calendar_executor,
        limit=5,
        claimed_by="worker-1",
    )

    assert calendar_executor.seen == [mutation]
    assert warehouse.completed == [("mut-calendar", {"event_id": "evt-1"}, "worker-1")]
    assert summary.succeeded == 1
