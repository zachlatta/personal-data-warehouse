from __future__ import annotations

from datetime import timedelta

import plistlib
from pathlib import Path

import pytest

from personal_data_warehouse.apple_contacts_mutations import (
    APPLE_CONTACTS_PROVIDER,
    AppleContactsMutationResult,
)
from personal_data_warehouse_apple_contacts import mutation_worker
from personal_data_warehouse_apple_contacts.mutation_worker import (
    AppleContactsMutationSummary,
    process_apple_contacts_mutations,
)

ROOT = Path(__file__).resolve().parents[1]


class _FakeWarehouse:
    def __init__(self, claimable):
        self._claimable = list(claimable)
        self.claim_calls = []
        self.reclaim_calls = []
        self.completed = []
        self.failed = []

    def ensure_upstream_mutation_tables(self):
        pass

    def reclaim_stale_executing_mutations(self, *, stale_after, idempotent_operations, actor_id, ensure_tables=True):
        self.reclaim_calls.append(tuple(idempotent_operations))
        return 0

    def claim_approved_upstream_mutations(self, *, limit, claimed_by, providers=None, exclude_providers=None, ensure_tables=True):
        self.claim_calls.append({"limit": limit, "providers": providers})
        return self._claimable

    def complete_upstream_mutation(self, mutation_id, *, result_json, actor_id):
        self.completed.append((mutation_id, result_json))

    def fail_upstream_mutation(self, mutation_id, *, status, error, result_json, actor_id):
        self.failed.append((mutation_id, status, error))


class _FakeExecutor:
    def __init__(self, results):
        self.results = list(results)

    def execute(self, mutation):
        return self.results.pop(0)


@pytest.fixture(autouse=True)
def _always_acquire_the_lock(monkeypatch):
    from contextlib import contextmanager

    @contextmanager
    def fake_lock(**kwargs):
        yield True

    monkeypatch.setattr(mutation_worker, "exclusive_sync_lock", fake_lock)


def test_worker_claims_only_the_apple_contacts_provider_and_reclaims_only_updates():
    warehouse = _FakeWarehouse([{"id": "m1"}, {"id": "m2"}])
    executor = _FakeExecutor([
        AppleContactsMutationResult(status="succeeded", result_json={"card_id": "x"}),
        AppleContactsMutationResult(status="failed_terminal", error="gone"),
    ])

    summary = process_apple_contacts_mutations(warehouse=warehouse, executor=executor, reclaim_after=timedelta(minutes=1))

    assert summary == AppleContactsMutationSummary(claimed=2, succeeded=1, failed_terminal=1)
    assert warehouse.claim_calls[0]["providers"] == (APPLE_CONTACTS_PROVIDER,)
    assert warehouse.reclaim_calls == [((APPLE_CONTACTS_PROVIDER, "apple_contacts.update_contact"),)]
    assert warehouse.completed == [("m1", {"card_id": "x"})]
    assert warehouse.failed == [("m2", "failed_terminal", "gone")]


def test_the_cloud_worker_excludes_apple_contacts():
    from personal_data_warehouse.defs.upstream_mutations import LOCAL_ONLY_MUTATION_PROVIDERS

    assert APPLE_CONTACTS_PROVIDER in LOCAL_ONLY_MUTATION_PROVIDERS


def test_apple_contacts_mutation_worker_is_a_resident_launch_agent():
    plist_path = ROOT / "ops/launchd/com.zachlatta.personal-data-warehouse.apple-contacts-mutation-worker.plist"
    with plist_path.open("rb") as handle:
        plist = plistlib.load(handle)
    assert plist["Label"] == "com.zachlatta.personal-data-warehouse.apple-contacts-mutation-worker"
    assert plist["RunAtLoad"] is True
    assert plist["KeepAlive"] is True
    assert plist["ProgramArguments"] == [
        "/Users/zrl/dev/zachlatta/personal-data-warehouse/bin/apple-contacts-mutation-worker-launchd"
    ]
    wrapper = (ROOT / "bin/apple-contacts-mutation-worker-launchd").read_text()
    # Keep pdw out of the chain: TCC attributes the Contacts Automation grant to the
    # binaries in it, and pdw replaces its own binary on every release.
    assert "pdw " not in wrapper.replace("pdw_export_app_credentials", "").replace("_pdw-upload-lib", "")
    assert "personal_data_warehouse_apple_contacts.mutation_worker" in wrapper


def test_the_default_executor_resolves_merged_cards_through_the_warehouse(monkeypatch):
    warehouse = _FakeWarehouse([])
    warehouse.apple_contacts_merged_card_target = lambda card_id: "kept"
    built = {}

    class Spy:
        def __init__(self, **kwargs):
            built.update(kwargs)

    monkeypatch.setattr(mutation_worker, "AppleContactsMutationExecutor", Spy)
    process_apple_contacts_mutations(warehouse=warehouse)

    assert built["merged_into"]("anything") == "kept"
