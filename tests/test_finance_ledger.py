"""Behavior of the finance ledger runner (plaid → accounts/links/observations).

The runner is the phase-1 analog of photo identity: raw plaid rows never learn
about ledger identity; the runner resolves them into finance.accounts via
finance.account_links and appends one balance observation per account per day.
Deleting the finance.* rows and re-running replays every decision.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.finance_ledger import (
    FinanceLedgerRunner,
    has_pending_finance_observations,
    plaid_account_kind_side,
    stable_finance_account_id,
)
from personal_data_warehouse.postgres import PostgresWarehouse


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = make_test_schema()
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


_TS = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


def _plaid_account_row(**overrides) -> dict:
    row = {
        "account": "z@x.test",
        "item_id": "item-1",
        "account_id": "acc-1",
        "name": "Checking",
        "official_name": "Acme Cash Management",
        "mask": "0001",
        "type": "depository",
        "subtype": "checking",
        "available_balance": 100.0,
        "current_balance": 123.45,
        "limit_balance": 0.0,
        "iso_currency_code": "USD",
        "unofficial_currency_code": "",
        "is_removed": 0,
        "raw_json": {},
        "synced_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _plaid_item_row(**overrides) -> dict:
    row = {
        "account": "z@x.test",
        "item_id": "item-1",
        "institution_id": "ins_1",
        "institution_name": "Acme Bank",
        "available_products": [],
        "billed_products": [],
        "webhook": "",
        "consent_expiration_time": _TS,
        "error_json": {},
        "raw_json": {},
        "linked_at": _TS,
        "synced_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _seed_plaid(warehouse, accounts) -> None:
    warehouse.ensure_plaid_tables()
    warehouse.insert_plaid_items([_plaid_item_row()])
    warehouse.insert_plaid_accounts(accounts)


# --- pure ------------------------------------------------------------------------


def test_stable_finance_account_id_is_deterministic():
    a = stable_finance_account_id("plaid", "z@x.test", "acc-1")
    b = stable_finance_account_id("plaid", "z@x.test", "acc-1")
    assert a == b
    assert a.startswith("fa_")
    assert len(a) == 3 + 24
    assert a != stable_finance_account_id("plaid", "z@x.test", "acc-2")
    assert a != stable_finance_account_id("manual_finance", "z@x.test", "acc-1")


@pytest.mark.parametrize(
    ("type_", "subtype", "expected"),
    [
        ("depository", "checking", ("checking", "asset")),
        ("depository", "savings", ("savings", "asset")),
        ("credit", "credit card", ("credit", "liability")),
        ("loan", "mortgage", ("mortgage", "liability")),
        ("loan", "student", ("other", "liability")),
        ("investment", "brokerage", ("brokerage", "asset")),
        ("brokerage", "brokerage", ("brokerage", "asset")),
        ("brokerage", "crypto exchange", ("brokerage", "asset")),
        ("brokerage", "ira", ("ira", "asset")),
        ("brokerage", "roth", ("ira", "asset")),
        ("", "", ("other", "asset")),
    ],
)
def test_plaid_account_kind_side_mapping(type_, subtype, expected):
    assert plaid_account_kind_side(type_, subtype) == expected


# --- live warehouse ---------------------------------------------------------------


def test_sync_registers_accounts_links_and_observations(warehouse):
    _seed_plaid(
        warehouse,
        [
            _plaid_account_row(),
            _plaid_account_row(
                account_id="acc-2",
                name="Rewards Card",
                mask="0002",
                type="credit",
                subtype="credit card",
                current_balance=42.5,
            ),
        ],
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.accounts_seen == 2
    assert summary.accounts_created == 2
    assert summary.links_created == 2
    assert summary.observations_upserted == 2

    accounts = warehouse._query(
        "SELECT account_id, kind, side, institution, mask FROM finance_accounts ORDER BY mask"
    )
    fa_checking = stable_finance_account_id("plaid", "z@x.test", "acc-1")
    fa_credit = stable_finance_account_id("plaid", "z@x.test", "acc-2")
    assert accounts == [
        (fa_checking, "checking", "asset", "Acme Bank", "0001"),
        (fa_credit, "credit", "liability", "Acme Bank", "0002"),
    ]

    links = warehouse._query(
        "SELECT source, source_account_key, account_id, match_method FROM finance_account_links ORDER BY source_account_key"
    )
    assert links == [
        ("plaid", "acc-1", fa_checking, "source_id"),
        ("plaid", "acc-2", fa_credit, "source_id"),
    ]

    observations = warehouse._query(
        "SELECT account_id, as_of, kind, value, currency, source FROM finance_observations ORDER BY account_id"
    )
    assert sorted(observations) == sorted(
        [
            (fa_checking, date(2026, 7, 13), "balance", Decimal("123.45"), "USD", "plaid"),
            (fa_credit, date(2026, 7, 13), "balance", Decimal("42.5"), "USD", "plaid"),
        ]
    )


def test_sync_is_idempotent_and_appends_across_days(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row()])
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    # Re-run the same day: no duplicate accounts/links, observation updated in place.
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(hour=18)).sync()
    assert summary.accounts_created == 0
    assert summary.links_created == 0
    assert warehouse._query("SELECT count(*) FROM finance_accounts") == [(1,)]
    assert warehouse._query("SELECT count(*) FROM finance_account_links") == [(1,)]
    assert warehouse._query("SELECT count(*) FROM finance_observations") == [(1,)]
    # The next day appends a new observation: history accrues.
    FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(day=14)).sync()
    assert warehouse._query("SELECT count(*) FROM finance_observations") == [(2,)]


def test_sync_refreshes_account_fields_but_preserves_identity(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row()])
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    created = warehouse._query("SELECT account_id, created_at FROM finance_accounts")
    # The institution renames the account; identity and created_at are stable.
    warehouse.insert_plaid_accounts(
        [_plaid_account_row(name="Premium Checking", sync_version=2)]
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(day=14)).sync()
    rows = warehouse._query("SELECT account_id, name, created_at FROM finance_accounts")
    assert rows == [(created[0][0], "Premium Checking", created[0][1])]


def test_sync_skips_removed_plaid_accounts(warehouse):
    _seed_plaid(
        warehouse,
        [
            _plaid_account_row(),
            _plaid_account_row(account_id="acc-gone", is_removed=1),
        ],
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.accounts_seen == 1
    assert warehouse._query("SELECT count(*) FROM finance_accounts") == [(1,)]


def test_has_pending_finance_observations(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row()])
    warehouse.ensure_finance_tables()
    assert has_pending_finance_observations(warehouse) is True
    FinanceLedgerRunner(warehouse=warehouse).sync()
    assert has_pending_finance_observations(warehouse) is False


def test_replay_rebuilds_identically(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row()])
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    before = warehouse._query(
        "SELECT account_id, kind, side FROM finance_accounts ORDER BY account_id"
    )
    warehouse._command("DELETE FROM finance_observations")
    warehouse._command("DELETE FROM finance_account_links")
    warehouse._command("DELETE FROM finance_accounts")
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    after = warehouse._query(
        "SELECT account_id, kind, side FROM finance_accounts ORDER BY account_id"
    )
    assert before == after
