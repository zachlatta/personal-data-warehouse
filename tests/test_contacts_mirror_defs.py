from __future__ import annotations

from contextlib import contextmanager

from dagster import build_asset_context, build_schedule_context

from personal_data_warehouse.contacts_mirror import MirrorPlan
from personal_data_warehouse.defs import contacts_mirror as defs_module


class _Warehouse:
    closed = False

    def close(self):
        self.closed = True


class _Proposer:
    def __init__(self):
        self.payloads = []

    def propose(self, payload):
        self.payloads.append(payload)
        return f"req_{len(self.payloads)}"


@contextmanager
def _acquired(**kwargs):
    yield True


def _wire(monkeypatch, *, pending=0, plan=None):
    warehouse = _Warehouse()
    proposer = _Proposer()
    monkeypatch.setenv("PDW_API_URL", "https://app.example.test")
    monkeypatch.setenv("PDW_SECRET_TOKEN", "secret")
    monkeypatch.setattr(defs_module, "load_settings", lambda **_: object())
    monkeypatch.setattr(defs_module, "warehouse_from_settings", lambda _s: warehouse)
    monkeypatch.setattr(defs_module, "exclusive_sync_lock", _acquired)
    monkeypatch.setattr(defs_module, "pending_mirror_requests", lambda _w: pending)
    monkeypatch.setattr(defs_module, "load_cards", lambda _w, google_account_email: ([], []))
    monkeypatch.setattr(defs_module, "plan_mirror", lambda g, a, **kw: plan or MirrorPlan("acct", "acct"))
    monkeypatch.setattr(defs_module, "_proposer", lambda *_: proposer)
    return warehouse, proposer


def test_skips_without_app_credentials(monkeypatch):
    monkeypatch.delenv("PDW_API_URL", raising=False)
    monkeypatch.delenv("MCP_BASE_URL", raising=False)
    result = defs_module.contacts_mirror(build_asset_context())
    assert result.metadata["skipped"].value == "no app credentials"


def test_stays_quiet_while_a_mirror_request_is_pending_review(monkeypatch):
    warehouse, proposer = _wire(monkeypatch, pending=2)
    result = defs_module.contacts_mirror(build_asset_context())
    assert result.metadata["skipped"].value == "pending review"
    assert proposer.payloads == [] and warehouse.closed


def test_proposes_nothing_when_the_books_already_match(monkeypatch):
    _, proposer = _wire(monkeypatch)
    result = defs_module.contacts_mirror(build_asset_context())
    assert proposer.payloads == []
    assert result.metadata["request_ids"].value == []


def test_files_one_request_per_book_when_there_is_a_delta(monkeypatch):
    plan = MirrorPlan("acct", "acct", apple_mutations=[{"type": "apple_contacts.create_contact", "account": "acct", "contact": {"given_name": "A"}}], google_operations=[{"op": "create_contact", "person": {}}], counts={"apple_created": 1, "google_created": 1})
    _, proposer = _wire(monkeypatch, plan=plan)
    result = defs_module.contacts_mirror(build_asset_context())
    assert [p["title"].startswith("Contacts mirror") for p in proposer.payloads] == [True, True]
    assert result.metadata["request_ids"].value == ["req_1", "req_2"]
    assert result.metadata["apple_created"].value == 1


def test_schedule_is_daily_and_skips_overlap(monkeypatch):
    assert defs_module.CONTACTS_MIRROR_CRON == "23 5 * * *"
    calls = {}
    monkeypatch.setattr(defs_module, "skip_if_job_active", lambda context, *, job_name: calls.setdefault("job", job_name))
    defs_module.contacts_mirror_daily._execution_fn.decorated_fn(build_schedule_context())
    assert calls["job"] == "contacts_mirror_job"


def test_defs_expose_the_asset_job_and_schedule():
    defs = defs_module.defs()
    assert [s.key.to_user_string() for s in defs.resolve_all_asset_specs()] == ["contacts_mirror"]
    assert [j.name for j in defs.jobs] == ["contacts_mirror_job"]
    assert [s.name for s in defs.schedules] == ["contacts_mirror_daily"]
