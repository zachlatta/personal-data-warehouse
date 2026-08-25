from __future__ import annotations

from datetime import timedelta

import pytest

from personal_data_warehouse.apple_notes_mutations import (
    APPLE_NOTES_PROVIDER,
    AppleNotesMutationResult,
)
from personal_data_warehouse_apple_notes import mutation_worker
from personal_data_warehouse_apple_notes.mutation_worker import (
    AppleNotesMutationSummary,
    process_apple_notes_mutations,
)


class _FakeWarehouse:
    def __init__(self, claimable):
        self._claimable = list(claimable)
        self.claim_calls = []
        self.reclaim_calls = []
        self.completed = []
        self.failed = []
        self.ensured = 0

    def ensure_upstream_mutation_tables(self):
        self.ensured += 1

    def reclaim_stale_executing_mutations(self, *, stale_after, idempotent_operations, actor_id):
        self.reclaim_calls.append(tuple(idempotent_operations))
        return 0

    def claim_approved_upstream_mutations(self, *, limit, claimed_by, providers=None, exclude_providers=None):
        self.claim_calls.append({"limit": limit, "providers": providers, "exclude": exclude_providers})
        return self._claimable

    def complete_upstream_mutation(self, mutation_id, *, result_json, actor_id):
        self.completed.append((mutation_id, result_json))

    def fail_upstream_mutation(self, mutation_id, *, status, error, result_json, actor_id):
        self.failed.append((mutation_id, status, error))


class _FakeExecutor:
    def __init__(self, results):
        self.results = list(results)
        self.seen = []

    def execute(self, mutation):
        self.seen.append(mutation)
        return self.results.pop(0)


@pytest.fixture(autouse=True)
def _always_acquire_the_lock(monkeypatch):
    from contextlib import contextmanager

    @contextmanager
    def _lock(**_kwargs):
        yield True

    monkeypatch.setattr(mutation_worker, "exclusive_sync_lock", _lock)


def test_the_worker_claims_only_apple_notes_rows():
    warehouse = _FakeWarehouse([])
    process_apple_notes_mutations(warehouse=warehouse, executor=_FakeExecutor([]))
    assert warehouse.claim_calls[0]["providers"] == (APPLE_NOTES_PROVIDER,)


def test_a_create_is_never_reclaimed_because_replaying_it_duplicates_the_note():
    warehouse = _FakeWarehouse([])
    process_apple_notes_mutations(warehouse=warehouse, executor=_FakeExecutor([]))
    reclaimable = warehouse.reclaim_calls[0]
    assert (APPLE_NOTES_PROVIDER, "apple_notes.update_note") in reclaimable
    assert all(operation != "apple_notes.create_note" for _provider, operation in reclaimable)


def test_a_successful_mutation_is_completed_with_its_result():
    warehouse = _FakeWarehouse([{"id": "mut-1", "provider": APPLE_NOTES_PROVIDER}])
    executor = _FakeExecutor(
        [AppleNotesMutationResult(status="succeeded", result_json={"note_id": "x-coredata://A/ICNote/p1"})]
    )

    summary = process_apple_notes_mutations(warehouse=warehouse, executor=executor)

    assert summary.succeeded == 1
    assert warehouse.completed == [("mut-1", {"note_id": "x-coredata://A/ICNote/p1"})]
    assert warehouse.failed == []


def test_a_blocked_automation_grant_is_recorded_as_blocked_not_failed():
    warehouse = _FakeWarehouse([{"id": "mut-2", "provider": APPLE_NOTES_PROVIDER}])
    executor = _FakeExecutor(
        [AppleNotesMutationResult(status="blocked_missing_credentials", error="Automation permission")]
    )

    summary = process_apple_notes_mutations(warehouse=warehouse, executor=executor)

    assert summary.blocked_missing_credentials == 1
    assert warehouse.failed[0][1] == "blocked_missing_credentials"


def test_a_lost_lock_reports_a_skip_rather_than_an_empty_success(monkeypatch):
    from contextlib import contextmanager

    @contextmanager
    def _busy(**_kwargs):
        yield False

    monkeypatch.setattr(mutation_worker, "exclusive_sync_lock", _busy)
    warehouse = _FakeWarehouse([])

    summary = process_apple_notes_mutations(warehouse=warehouse, executor=_FakeExecutor([]))

    assert summary == AppleNotesMutationSummary(skipped_due_to_lock=True)
    assert warehouse.claim_calls == []


def test_the_cloud_worker_excludes_the_local_only_providers():
    # The cloud worker cannot reach Notes.app. If it claimed these rows it would fail
    # them as unknown-provider, bumping attempt_count and hiding them from the Mac.
    from personal_data_warehouse.defs.upstream_mutations import LOCAL_ONLY_MUTATION_PROVIDERS

    assert APPLE_NOTES_PROVIDER in LOCAL_ONLY_MUTATION_PROVIDERS
