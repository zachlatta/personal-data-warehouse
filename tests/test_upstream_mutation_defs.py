from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json

import pytest
from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs import upstream_mutations as upstream_mutation_defs
from personal_data_warehouse.calendar_mutations import CalendarMutationResult
from personal_data_warehouse.contact_mutations import ContactMutationResult
from personal_data_warehouse.gmail_mutations import (
    GMAIL_ARCHIVE_OPERATION,
    GMAIL_UNARCHIVE_OPERATION,
    GmailMutationResult,
)
from personal_data_warehouse.slack_mutations import (
    SLACK_MARK_CONVERSATION_READ_OPERATION,
    SlackMutationResult,
)


class FakeWarehouse:
    def __init__(
        self,
        *,
        approved_count: int = 0,
        claimed=None,
        observed: int = 0,
        message_ids_by_thread_id=None,
        reclaimed: int = 0,
        stale_reclaimable_count: int = 0,
        succeeded_count: int = 0,
        succeeded_updated_at: datetime | None = None,
    ) -> None:
        self.approved_count = approved_count
        self.claimed = list(claimed or [])
        self.observed = observed
        self.message_ids_by_thread_id = dict(message_ids_by_thread_id or {})
        self.closed = False
        self.completed = []
        self.bulk_completed = []
        self.failed = []
        self.claim_limit = None
        self.claimed_by = None
        self.ensure_called = False
        self.ensure_call_count = 0
        self.message_id_lookups = []
        self.reclaimed_count = reclaimed
        self.reclaim_calls = []
        self.stale_reclaimable_count = stale_reclaimable_count
        self.stale_reclaimable_calls = []
        self.approved_count_ensure_flags = []
        self.stale_reclaimable_ensure_flags = []
        self.calls = []
        self.succeeded_count = succeeded_count
        self.succeeded_updated_at = succeeded_updated_at or datetime(2026, 8, 29, tzinfo=UTC)

    def ensure_upstream_mutation_tables(self) -> None:
        self.ensure_called = True
        self.ensure_call_count += 1

    def approved_upstream_mutation_count(
        self,
        *,
        ensure_tables: bool = True,
        providers=None,
        exclude_providers=None,
    ) -> int:
        self.approved_count_ensure_flags.append(ensure_tables)
        return self.approved_count

    def stale_reclaimable_upstream_mutation_count(
        self, *, stale_after, idempotent_operations, ensure_tables: bool = True
    ) -> int:
        self.stale_reclaimable_calls.append((stale_after, tuple(idempotent_operations)))
        self.stale_reclaimable_ensure_flags.append(ensure_tables)
        return self.stale_reclaimable_count

    def observe_succeeded_gmail_archive_mutations(self, **_kwargs) -> int:
        self.calls.append("observe_gmail_archive")
        return self.observed

    def observe_succeeded_gmail_unarchive_mutations(self, **_kwargs) -> int:
        self.calls.append("observe_gmail_unarchive")
        return 0

    def observe_succeeded_gmail_email_mutations(self, **_kwargs) -> int:
        self.calls.append("observe_gmail_email")
        return 0

    def observe_succeeded_contact_mutations(self, **_kwargs) -> int:
        self.calls.append("observe_contacts")
        return 0

    def observe_succeeded_calendar_event_mutations(self, **_kwargs) -> int:
        self.calls.append("observe_calendar")
        return 0

    def observe_succeeded_slack_mark_conversation_read_mutations(self, **_kwargs) -> int:
        self.calls.append("observe_slack")
        return 0

    def succeeded_upstream_mutation_count(self, *, ensure_tables=True, exclude_providers=None) -> int:
        self.calls.append("count_succeeded")
        return self.succeeded_count

    def succeeded_upstream_mutation_observation_state(
        self, *, ensure_tables=True, exclude_providers=None
    ) -> tuple[int, datetime | None]:
        self.calls.append("observation_state")
        newest = self.succeeded_updated_at if self.succeeded_count else None
        return self.succeeded_count, newest

    def claim_approved_upstream_mutations(
        self,
        *,
        limit: int,
        claimed_by: str,
        providers=None,
        exclude_providers=None,
        ensure_tables=True,
    ):
        self.calls.append("claim")
        self.claim_limit = limit
        self.claimed_by = claimed_by
        return self.claimed

    def reclaim_stale_executing_mutations(
        self, *, stale_after, idempotent_operations, actor_id, ensure_tables=True
    ):
        self.reclaim_calls.append((stale_after, tuple(idempotent_operations), actor_id))
        return self.reclaimed_count

    def gmail_message_ids_for_thread_label_mutation(self, *, account: str, thread_ids: list[str], archive: bool):
        self.message_id_lookups.append((account, thread_ids, archive))
        return {thread_id: list(self.message_ids_by_thread_id.get(thread_id, [])) for thread_id in thread_ids}

    def complete_upstream_mutation(self, mutation_id: str, *, result_json: dict, actor_id: str) -> None:
        self.completed.append((mutation_id, result_json, actor_id))

    def complete_upstream_mutations(self, *, completions, actor_id: str, ensure_tables=True) -> int:
        self.bulk_completed.append((list(completions), actor_id))
        for mutation_id, result_json in completions:
            self.completed.append((mutation_id, result_json, actor_id))
        return len(completions)

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


@pytest.fixture(autouse=True)
def _reset_sensor_table_bootstrap():
    # The sensor bootstraps the upstream-mutation tables once per process via a
    # module-level guard; reset it so each test starts from a clean slate.
    upstream_mutation_defs._upstream_mutation_tables_ensured = False
    yield
    upstream_mutation_defs._upstream_mutation_tables_ensured = False


def test_upstream_mutation_sensor_runs_every_ten_seconds() -> None:
    sensor = upstream_mutation_defs.upstream_mutation_sensor

    assert sensor.minimum_interval_seconds == 10
    assert sensor.default_status.value == "RUNNING"


def test_upstream_mutation_observation_sensor_runs_separately_every_thirty_seconds() -> None:
    sensor = upstream_mutation_defs.upstream_mutation_observation_sensor

    assert sensor.minimum_interval_seconds == 30
    assert sensor.default_status.value == "RUNNING"


def test_upstream_mutation_observation_sensor_emits_run_for_succeeded_backlog(monkeypatch) -> None:
    warehouse = FakeWarehouse(succeeded_count=12)
    monkeypatch.setattr(upstream_mutation_defs, "load_settings", lambda **_kwargs: object())
    monkeypatch.setattr(upstream_mutation_defs, "warehouse_from_settings", lambda _settings: warehouse)

    with DagsterInstance.ephemeral() as instance:
        result = upstream_mutation_defs.upstream_mutation_observation_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, RunRequest)
    assert result.tags == {
        "upstream_mutation_trigger": "observation_backlog",
        "succeeded_mutation_count": "12",
    }


def test_upstream_mutation_observation_sensor_backs_off_an_unchanged_backlog(monkeypatch) -> None:
    updated_at = datetime(2026, 8, 29, tzinfo=UTC)
    warehouse = FakeWarehouse(succeeded_count=1, succeeded_updated_at=updated_at)
    cursor = json.dumps(
        {
            "watermark": updated_at.isoformat(),
            "attempts": 1,
            "next_retry_at": 4_102_444_800,
        }
    )
    monkeypatch.setattr(upstream_mutation_defs, "load_settings", lambda **_kwargs: object())
    monkeypatch.setattr(upstream_mutation_defs, "warehouse_from_settings", lambda _settings: warehouse)

    with DagsterInstance.ephemeral() as instance:
        result = upstream_mutation_defs.upstream_mutation_observation_sensor(
            build_sensor_context(instance=instance, cursor=cursor)
        )

    assert isinstance(result, SkipReason)
    assert "next observation retry" in result.skip_message


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


def test_upstream_mutation_sensor_emits_run_when_stale_reclaimable_work_exists(monkeypatch) -> None:
    warehouse = FakeWarehouse(stale_reclaimable_count=2)
    monkeypatch.setattr(upstream_mutation_defs, "load_settings", lambda **_kwargs: object())
    monkeypatch.setattr(upstream_mutation_defs, "warehouse_from_settings", lambda _settings: warehouse)

    with DagsterInstance.ephemeral() as instance:
        result = upstream_mutation_defs.upstream_mutation_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {
        "upstream_mutation_trigger": "stale_executing_reclaim",
        "approved_mutation_count": "0",
        "reclaimable_mutation_count": "2",
    }
    assert len(warehouse.stale_reclaimable_calls) == 1
    stale_after, idempotent_operations = warehouse.stale_reclaimable_calls[0]
    assert stale_after == timedelta(seconds=upstream_mutation_defs.DEFAULT_UPSTREAM_MUTATION_RECLAIM_AFTER_SECONDS)
    assert ("gmail", GMAIL_ARCHIVE_OPERATION) in idempotent_operations
    assert ("gmail", GMAIL_UNARCHIVE_OPERATION) in idempotent_operations
    assert ("slack", SLACK_MARK_CONVERSATION_READ_OPERATION) in idempotent_operations
    assert warehouse.closed is True


def test_upstream_mutation_sensor_skips_when_no_approved_or_reclaimable_work(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(upstream_mutation_defs, "load_settings", lambda **_kwargs: object())
    monkeypatch.setattr(upstream_mutation_defs, "warehouse_from_settings", lambda _settings: warehouse)

    with DagsterInstance.ephemeral() as instance:
        result = upstream_mutation_defs.upstream_mutation_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert result.skip_message == "No approved or stale reclaimable upstream mutations found."
    assert len(warehouse.stale_reclaimable_calls) == 1
    assert warehouse.closed is True


def test_upstream_mutation_sensor_does_not_run_ddl_on_the_count_hot_path(monkeypatch) -> None:
    warehouse = FakeWarehouse()
    monkeypatch.setattr(upstream_mutation_defs, "load_settings", lambda **_kwargs: object())
    monkeypatch.setattr(upstream_mutation_defs, "warehouse_from_settings", lambda _settings: warehouse)

    with DagsterInstance.ephemeral() as instance:
        upstream_mutation_defs.upstream_mutation_sensor(build_sensor_context(instance=instance))

    # The count queries must not trigger the expensive table/view DDL.
    assert warehouse.approved_count_ensure_flags == [False]
    assert warehouse.stale_reclaimable_ensure_flags == [False]


def test_upstream_mutation_sensor_bootstraps_tables_only_once_per_process(monkeypatch) -> None:
    warehouses: list[FakeWarehouse] = []

    def build_warehouse(_settings):
        warehouse = FakeWarehouse()
        warehouses.append(warehouse)
        return warehouse

    monkeypatch.setattr(upstream_mutation_defs, "load_settings", lambda **_kwargs: object())
    monkeypatch.setattr(upstream_mutation_defs, "warehouse_from_settings", build_warehouse)

    with DagsterInstance.ephemeral() as instance:
        upstream_mutation_defs.upstream_mutation_sensor(build_sensor_context(instance=instance))
        upstream_mutation_defs.upstream_mutation_sensor(build_sensor_context(instance=instance))

    # First tick bootstraps the tables; subsequent ticks reuse them and never
    # re-run the per-tick DDL that was timing out in production.
    assert warehouses[0].ensure_call_count == 1
    assert warehouses[1].ensure_call_count == 0
    assert all(wh.closed for wh in warehouses)


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
    assert summary.observed == 0


def test_process_upstream_mutation_batch_claims_before_any_observation_work() -> None:
    warehouse = FakeWarehouse(claimed=[])

    upstream_mutation_defs.process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=FakeGmailExecutor([]),
        contact_executor=FakeExecutor([]),
        calendar_executor=FakeExecutor([]),
        limit=5,
        claimed_by="worker-1",
    )

    assert warehouse.calls == ["claim"]


def test_observation_batch_is_separate_from_execution() -> None:
    warehouse = FakeWarehouse(observed=2)

    summary = upstream_mutation_defs.observe_upstream_mutation_batch(warehouse=warehouse)

    assert summary.observed == 2
    assert warehouse.calls == [
        "observe_gmail_archive",
        "observe_gmail_unarchive",
        "observe_gmail_email",
        "observe_contacts",
        "observe_calendar",
        "observe_slack",
    ]


def test_process_upstream_mutation_batch_reclaims_stale_executing_rows_before_claiming() -> None:
    warehouse = FakeWarehouse(reclaimed=3)

    summary = upstream_mutation_defs.process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=FakeGmailExecutor([]),
        contact_executor=FakeExecutor([]),
        calendar_executor=FakeExecutor([]),
        limit=10,
        claimed_by="worker-1",
        reclaim_after=timedelta(minutes=30),
    )

    assert summary.reclaimed == 3
    assert len(warehouse.reclaim_calls) == 1
    stale_after, idempotent_operations, actor_id = warehouse.reclaim_calls[0]
    assert stale_after == timedelta(minutes=30)
    assert actor_id == "worker-1"
    assert ("gmail", GMAIL_ARCHIVE_OPERATION) in idempotent_operations
    assert ("gmail", GMAIL_UNARCHIVE_OPERATION) in idempotent_operations


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
    assert warehouse.bulk_completed == [
        (
            [
                ("mut-a", {"archived_thread_ids": ["thread-1"], "batch_modified_message_ids": ["message-1"]}),
                ("mut-b", {"archived_thread_ids": ["thread-2"], "batch_modified_message_ids": ["message-2"]}),
            ],
            "worker-1",
        )
    ]
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


def test_process_upstream_mutation_batch_routes_slack_mark_read_to_slack_executor() -> None:
    mutation = {
        "id": "mut-slack",
        "provider": "slack",
        "operation": SLACK_MARK_CONVERSATION_READ_OPERATION,
    }
    warehouse = FakeWarehouse(claimed=[mutation])
    slack_executor = FakeExecutor(
        [
            SlackMutationResult(
                status="succeeded",
                result_json={"conversation_id": "D1", "message_ts": "1.000001", "team_id": "T1"},
            )
        ]
    )

    summary = upstream_mutation_defs.process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=FakeExecutor([]),
        contact_executor=FakeExecutor([]),
        calendar_executor=FakeExecutor([]),
        slack_executor=slack_executor,
        limit=5,
        claimed_by="worker-1",
    )

    assert slack_executor.seen == [mutation]
    assert warehouse.completed == [
        (
            "mut-slack",
            {"conversation_id": "D1", "message_ts": "1.000001", "team_id": "T1"},
            "worker-1",
        )
    ]
    assert summary.succeeded == 1
