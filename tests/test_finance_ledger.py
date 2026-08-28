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
    description_similarity,
    document_account_key,
    document_kind_side,
    has_pending_finance_observations,
    plaid_account_kind_side,
    stable_finance_account_id,
    stable_finance_transaction_id,
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


def _plaid_transaction_row(**overrides) -> dict:
    row = {
        "account": "z@x.test",
        "item_id": "item-1",
        "account_id": "acc-1",
        "transaction_id": "tx-1",
        "posted_at": _TS,
        "authorized_at": _TS,
        "name": "COFFEE SHOP",
        "merchant_name": "Coffee Shop",
        "amount": 4.5,  # plaid: positive = money out
        "iso_currency_code": "USD",
        "unofficial_currency_code": "",
        "category_json": [],
        "payment_channel": "in store",
        "pending": 0,
        "pending_transaction_id": "",
        "is_removed": 0,
        "raw_json": {},
        "synced_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _plaid_investment_transaction_row(**overrides) -> dict:
    row = {
        "account": "z@x.test",
        "item_id": "item-1",
        "account_id": "acc-b",
        "investment_transaction_id": "itx-1",
        "security_id": "",
        "transaction_at": _TS,
        "name": "TRANSFERRED FROM VS (Cash)",
        "quantity": 0.0,
        "amount": -100.0,  # plaid investments: positive = cash out (same as transactions)
        "price": 0.0,
        "fees": 0.0,
        "type": "cash",
        "subtype": "deposit",
        "iso_currency_code": "USD",
        "unofficial_currency_code": "",
        "raw_json": {},
        "synced_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _plaid_brokerage_account_row(**overrides) -> dict:
    return _plaid_account_row(
        account_id="acc-b",
        name="Wheel Fund",
        official_name="Acme Cash Management Brokerage",
        mask="0002",
        type="brokerage",
        subtype="brokerage",
        **overrides,
    )


def _extraction_row(**overrides) -> dict:
    from decimal import Decimal as D

    row = {
        "content_sha256": "sha-doc-1",
        "ai_provider": "agent_codex",
        "ai_model": "m",
        "ai_prompt_version": "manual-finance-agent-v1",
        "status": "ok",
        "error": "",
        "document_type": "bank_statement",
        "institution": "Acme Bank",
        "account_name_hint": "Checking",
        "account_mask": "0001",
        "period_start": date(2026, 6, 1),
        "period_end": date(2026, 6, 30),
        "currency": "USD",
        "closing_balance": D("1234.56"),
        "transactions_json": [],
        "balances_json": [],
        "valuations_json": [],
        "positions_json": [],
        "summary": "",
        "uncertainties_json": [],
        "raw_result_json": {},
        "ai_elapsed_ms": 0,
        "ai_processed_at": _TS,
        "created_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _document_row(**overrides) -> dict:
    row = {
        "source": "manual",
        "account": "z@x.test",
        "source_native_id": "sha-doc-1",
        "filename": "statement.pdf",
        "original_path": "acme-checking-0001/statement.pdf",
        "mime_type": "application/pdf",
        "size_bytes": 1,
        "content_sha256": "sha-doc-1",
        "file_modified_at": _TS,
        "raw_metadata_json": {},
        "storage_backend": "google_drive",
        "storage_key": "manual-finance/library/x.pdf",
        "storage_file_id": "drive-1",
        "storage_url": "",
        "metadata_storage_key": "",
        "metadata_storage_file_id": "",
        "metadata_storage_url": "",
        "metadata_content_sha256": "",
        "is_deleted": 0,
        "ingested_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _seed_document(warehouse, *, document=None, extraction=None) -> None:
    warehouse.ensure_manual_finance_tables()
    warehouse.insert_manual_finance_documents([document or _document_row()])
    warehouse.insert_manual_finance_extractions([extraction or _extraction_row()])


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
        "SELECT account_id, kind, side, institution, mask FROM @finance_accounts ORDER BY mask"
    )
    fa_checking = stable_finance_account_id("plaid", "z@x.test", "acc-1")
    fa_credit = stable_finance_account_id("plaid", "z@x.test", "acc-2")
    assert accounts == [
        (fa_checking, "checking", "asset", "Acme Bank", "0001"),
        (fa_credit, "credit", "liability", "Acme Bank", "0002"),
    ]

    links = warehouse._query(
        "SELECT source, source_account_key, account_id, match_method FROM @finance_account_links ORDER BY source_account_key"
    )
    assert links == [
        ("plaid", "acc-1", fa_checking, "source_id"),
        ("plaid", "acc-2", fa_credit, "source_id"),
    ]

    observations = warehouse._query(
        "SELECT account_id, as_of, kind, value, currency, source FROM @finance_observations ORDER BY account_id"
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
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(1,)]
    assert warehouse._query("SELECT count(*) FROM @finance_account_links") == [(1,)]
    assert warehouse._query("SELECT count(*) FROM @finance_observations") == [(1,)]
    # The next day appends a new observation: history accrues.
    FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(day=14)).sync()
    assert warehouse._query("SELECT count(*) FROM @finance_observations") == [(2,)]


def test_sync_refreshes_account_fields_but_preserves_identity(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row()])
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    created = warehouse._query("SELECT account_id, created_at FROM @finance_accounts")
    # The institution renames the account; identity and created_at are stable.
    warehouse.insert_plaid_accounts(
        [_plaid_account_row(name="Premium Checking", sync_version=2)]
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(day=14)).sync()
    rows = warehouse._query("SELECT account_id, name, created_at FROM @finance_accounts")
    assert rows == [(created[0][0], "Premium Checking", created[0][1])]


def test_sync_skips_daily_balances_for_action_required_items(warehouse):
    # Plaid keeps serving the LAST-KNOWN balance for an Item whose login died
    # (Capital One returned NO_ACCOUNTS for 13 days while base_plaid.accounts
    # still carried its final pre-death balances). Re-stamping those frozen
    # numbers as fresh daily observations made net worth look current when it
    # was not — a dead item's accounts must simply stop accruing observations
    # until the re-link, so the last honest as_of shows through.
    warehouse.ensure_plaid_tables()
    warehouse.insert_plaid_items(
        [
            _plaid_item_row(),
            _plaid_item_row(item_id="item-dead", institution_id="ins_2", institution_name="Dead Bank"),
        ]
    )
    warehouse.insert_plaid_accounts(
        [
            _plaid_account_row(),
            _plaid_account_row(
                item_id="item-dead",
                account_id="acc-dead",
                name="Dead Card",
                mask="9999",
                type="credit",
                subtype="credit card",
            ),
        ]
    )
    warehouse.insert_plaid_sync_state(
        account="z@x.test",
        item_id="item-dead",
        product="accounts",
        status="action_required",
        error="NO_ACCOUNTS: no valid accounts were found for this item",
        last_synced_at=_TS,
        updated_at=_TS,
    )

    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    # Both ledger accounts exist (identity keeps working for the re-link)...
    assert summary.accounts_seen == 2
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(2,)]
    # ...but only the healthy item's account got today's balance observation.
    assert summary.observations_upserted == 1
    healthy = stable_finance_account_id("plaid", "z@x.test", "acc-1")
    observations = warehouse._query("SELECT DISTINCT account_id FROM @finance_observations")
    assert observations == [(healthy,)]


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
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(1,)]


def test_has_pending_finance_observations(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row()])
    warehouse.ensure_finance_tables()
    assert has_pending_finance_observations(warehouse) is True
    FinanceLedgerRunner(warehouse=warehouse).sync()
    assert has_pending_finance_observations(warehouse) is False


def test_action_required_item_does_not_keep_backlog_sensor_running(warehouse):
    """A frozen Item is deliberately ineligible for a new daily observation.

    The backlog predicate must use the same eligibility rule as the builder;
    otherwise it launches a successful full-ledger rebuild every five minutes
    forever, while that rebuild correctly refuses to write the stale balance.
    """
    _seed_plaid(warehouse, [_plaid_account_row()])
    warehouse.ensure_finance_tables()
    warehouse.insert_plaid_sync_state(
        account="z@x.test",
        item_id="item-1",
        product="accounts",
        status="action_required",
        error="NO_ACCOUNTS",
        last_synced_at=_TS,
        updated_at=_TS,
    )

    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert warehouse._query("SELECT count(*) FROM @finance_observations") == [(0,)]
    assert has_pending_finance_observations(warehouse) is False


def test_rebuild_does_not_retimestamp_unchanged_manual_facts(warehouse):
    """A replay must not turn old document facts into fresh pipeline writes."""
    warehouse.ensure_plaid_tables()
    _seed_document(
        warehouse,
        extraction=_extraction_row(
            balances_json=[{"date": "2026-06-30", "balance": "1234.56"}],
            transactions_json=[
                {
                    "date": "2026-06-20",
                    "description": "MONTHLY PAYMENT",
                    "amount": "25.00",
                    "direction": "out",
                }
            ],
        ),
    )
    first = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    first_observed_at = warehouse._query(
        "SELECT observed_at FROM @finance_observations"
    )[0][0]
    first_transaction_at = warehouse._query(
        "SELECT created_at FROM @finance_transactions"
    )[0][0]
    first_link_at = warehouse._query(
        "SELECT created_at FROM @finance_transaction_links"
    )[0][0]
    assert first.observations_upserted == 1
    assert first.transactions_upserted == 1

    replay = FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(hour=18)).sync()

    assert replay.observations_upserted == 0
    assert replay.transactions_upserted == 0
    assert warehouse._query("SELECT observed_at FROM @finance_observations") == [
        (first_observed_at,)
    ]
    assert warehouse._query("SELECT created_at FROM @finance_transactions") == [
        (first_transaction_at,)
    ]
    assert warehouse._query("SELECT created_at FROM @finance_transaction_links") == [
        (first_link_at,)
    ]


def test_changed_transaction_advances_its_write_timestamp(warehouse):
    """Suppress identical rebuilds without hiding a real source-row change."""
    _seed_plaid(warehouse, [_plaid_account_row()])
    warehouse.insert_plaid_transactions([_plaid_transaction_row()])
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    changed_at = _TS.replace(hour=18)
    warehouse.insert_plaid_transactions(
        [_plaid_transaction_row(name="RENAMED COFFEE SHOP", sync_version=2)]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=changed_at).sync()

    assert summary.transactions_upserted == 1
    assert warehouse._query(
        "SELECT description, created_at FROM @finance_transactions"
    ) == [("RENAMED COFFEE SHOP", changed_at)]


def test_replay_rebuilds_identically(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row(), _plaid_brokerage_account_row()])
    warehouse.insert_plaid_transactions([_plaid_transaction_row()])
    warehouse.insert_plaid_investment_transactions([_plaid_investment_transaction_row()])
    _seed_document(
        warehouse,
        extraction=_extraction_row(
            transactions_json=[
                {"date": "2026-05-01", "description": "OLD DEPOSIT", "amount": "10.00", "direction": "in"}
            ]
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    snapshot_sql = (
        "SELECT account_id, kind, side FROM @finance_accounts ORDER BY account_id",
        "SELECT transaction_id, account_id, amount, source FROM @finance_transactions ORDER BY transaction_id",
        "SELECT source, source_row_key, transaction_id, match_method FROM @finance_transaction_links ORDER BY source, source_row_key",
    )
    before = [warehouse._query(sql) for sql in snapshot_sql]
    for table in (
        "finance_transaction_links",
        "finance_transactions",
        "finance_observations",
        "finance_account_links",
        "finance_accounts",
    ):
        warehouse._command(f"DELETE FROM @{table}")
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    after = [warehouse._query(sql) for sql in snapshot_sql]
    assert before == after


# --- phase 3: unified transactions -------------------------------------------------


def test_stable_transaction_id_is_deterministic():
    a = stable_finance_transaction_id("plaid", "z@x.test|tx-1")
    assert a == stable_finance_transaction_id("plaid", "z@x.test|tx-1")
    assert a.startswith("ft_")
    assert a != stable_finance_transaction_id("manual_finance", "z@x.test|tx-1")


@pytest.mark.parametrize(
    ("document_type", "hint", "folder", "expected"),
    [
        ("mortgage_statement", "", "", ("mortgage", "liability")),
        ("property_valuation", "", "real-estate-main-st", ("property", "asset")),
        ("fund_positions", "", "examplefund-i-lp", ("private_fund", "asset")),
        ("credit_card_statement", "", "", ("credit", "liability")),
        ("brokerage_statement", "", "", ("brokerage", "asset")),
        ("bank_statement", "Savings", "", ("savings", "asset")),
        ("bank_statement", "Checking", "", ("checking", "asset")),
        ("other", "", "vehicle-2020-truck", ("vehicle", "asset")),
        ("receipt", "", "", ("other", "asset")),
        ("other", "Personal receivable owed to Zach Latta", "", ("receivable", "asset")),
        ("receivable_record", "", "", ("receivable", "asset")),
        ("other", "", "personal-loan-receivable", ("receivable", "asset")),
    ],
)
def test_document_kind_side(document_type, hint, folder, expected):
    assert document_kind_side(document_type, name_hint=hint, account_folder=folder) == expected


def test_document_account_key_prefers_folder_over_extraction():
    assert (
        document_account_key(
            original_path="acme-checking-0001/statement.pdf",
            institution="Acme Bank of America",
            mask="9999",
            filename="statement.pdf",
        )
        == "acme-checking-0001"
    )
    assert (
        document_account_key(
            original_path="statement.pdf", institution="Acme Bank", mask="0001", filename="statement.pdf"
        )
        == "acme-bank|0001"
    )


def test_description_similarity():
    assert description_similarity("COFFEE SHOP #42", "Coffee Shop") > 0.5
    assert description_similarity("COFFEE SHOP", "AIRLINE TICKET") == 0.0


def test_plaid_transactions_become_signed_ledger_rows(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row()])
    warehouse.insert_plaid_transactions(
        [
            _plaid_transaction_row(),  # 4.50 out
            _plaid_transaction_row(transaction_id="tx-2", name="PAYCHECK", amount=-1000.0),
        ]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.transactions_upserted == 2
    rows = warehouse._query(
        "SELECT description, amount, source, pending FROM @finance_transactions ORDER BY amount"
    )
    # Plaid positive-out flips to ledger negative (outflow); inflow is positive.
    assert rows == [
        ("COFFEE SHOP", Decimal("-4.5"), "plaid", 0),
        ("PAYCHECK", Decimal("1000"), "plaid", 0),
    ]


def test_pending_row_merges_into_posted_successor_and_reconciles(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row()])
    warehouse.insert_plaid_transactions([_plaid_transaction_row(transaction_id="tx-p", pending=1)])
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    pending_ledger_id = stable_finance_transaction_id("plaid", "z@x.test|tx-p")
    assert warehouse._query("SELECT transaction_id FROM @finance_transactions") == [(pending_ledger_id,)]

    # The posted row arrives and the pending row tombstones (plaid behavior).
    warehouse.insert_plaid_transactions(
        [
            _plaid_transaction_row(transaction_id="tx-post", pending_transaction_id="tx-p", sync_version=2),
            _plaid_transaction_row(transaction_id="tx-p", pending=1, is_removed=1, sync_version=2),
        ]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(hour=13)).sync()
    posted_ledger_id = stable_finance_transaction_id("plaid", "z@x.test|tx-post")
    rows = warehouse._query("SELECT transaction_id, pending FROM @finance_transactions")
    # The pending row's ledger transaction is gone; only the posted row remains.
    assert rows == [(posted_ledger_id, 0)]
    assert summary.transactions_removed > 0

    # A LIVE pending row whose posted successor coexists links via pending_id
    # instead of founding its own ledger row.
    warehouse.insert_plaid_transactions(
        [
            _plaid_transaction_row(transaction_id="tx-p2", pending=1, sync_version=3),
            _plaid_transaction_row(transaction_id="tx-post2", pending_transaction_id="tx-p2", sync_version=3),
        ]
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(hour=14)).sync()
    links = dict(
        warehouse._query(
            "SELECT source_row_key, match_method FROM @finance_transaction_links WHERE source_row_key LIKE '%%tx-p2'"
        )
    )
    assert links == {"z@x.test|tx-p2": "pending_id"}


def test_plaid_investment_transactions_become_signed_ledger_rows(warehouse):
    # Brokerage accounts (cash-management brokerages et al.) report ALL their
    # activity through the investments product; those flows are ledger facts
    # exactly like transactions-product flows.
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    warehouse.insert_plaid_investment_transactions(
        [
            _plaid_investment_transaction_row(),  # deposit: -100 = cash in
            _plaid_investment_transaction_row(
                investment_transaction_id="itx-2",
                name="FDIC PURCHASE INTO CORE ACCOUNT",
                type="buy",
                subtype="buy",
                amount=100.0,
                quantity=100.0,
                price=1.0,
            ),
            _plaid_investment_transaction_row(
                investment_transaction_id="itx-3",
                name="FDIC INTEREST EARNED",
                subtype="interest",
                amount=-1.67,
            ),
        ]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.transactions_upserted == 3
    rows = warehouse._query(
        "SELECT description, amount, source, pending FROM @finance_transactions ORDER BY amount"
    )
    # Plaid positive-out flips to ledger negative (outflow); inflow is positive.
    assert rows == [
        ("FDIC PURCHASE INTO CORE ACCOUNT", Decimal("-100"), "plaid", 0),
        ("FDIC INTEREST EARNED", Decimal("1.67"), "plaid", 0),
        ("TRANSFERRED FROM VS (Cash)", Decimal("100"), "plaid", 0),
    ]
    links = dict(
        warehouse._query(
            "SELECT source_row_key, match_method FROM @finance_transaction_links ORDER BY source_row_key"
        )
    )
    # Investment rows live in their own source_row_key namespace.
    assert links == {
        "z@x.test|investment|itx-1": "source_id",
        "z@x.test|investment|itx-2": "source_id",
        "z@x.test|investment|itx-3": "source_id",
    }


def test_investment_and_transaction_feed_ids_never_collide(warehouse):
    # The two Plaid feeds have independent id spaces; the same raw id in both
    # must found two ledger rows, not overwrite one.
    _seed_plaid(warehouse, [_plaid_account_row(), _plaid_brokerage_account_row()])
    warehouse.insert_plaid_transactions([_plaid_transaction_row(transaction_id="tx-1")])
    warehouse.insert_plaid_investment_transactions(
        [_plaid_investment_transaction_row(investment_transaction_id="tx-1")]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.transactions_upserted == 2
    assert warehouse._query("SELECT count(*) FROM @finance_transactions") == [(2,)]


def test_document_transactions_dedup_against_investment_overlap(warehouse):
    # A brokerage statement's rows merge into investment-founded ledger rows
    # at the statement/plaid overlap seam, same as depository statements do
    # against transactions-product rows.
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    warehouse.insert_plaid_investment_transactions([_plaid_investment_transaction_row()])
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-brokerage",
            source_native_id="sha-brokerage",
            original_path="acme-wheel-fund-0002/statement.csv",
        ),
        extraction=_extraction_row(
            content_sha256="sha-brokerage",
            document_type="brokerage_statement",
            institution="Acme Bank",
            account_name_hint="Wheel Fund",
            account_mask="0002",
            transactions_json=[
                # Same account (mask 0002), same amount, one day off: merges.
                {"date": "2026-07-12", "description": "Transferred From", "amount": "100.00", "direction": "in"},
                # Outside plaid's window: founds a new ledger row.
                {"date": "2024-01-05", "description": "OLD TRANSFER", "amount": "50.00", "direction": "in"},
            ],
        ),
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.transactions_merged == 1
    rows = warehouse._query("SELECT description, amount, source FROM @finance_transactions ORDER BY posted_at")
    assert rows == [
        ("OLD TRANSFER", Decimal("50.00"), "manual_finance"),
        ("TRANSFERRED FROM VS (Cash)", Decimal("100"), "plaid"),
    ]
    # The doc's account resolved to the EXISTING plaid account by mask: no new account.
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(1,)]
    links = dict(
        warehouse._query(
            "SELECT source_row_key, match_method FROM @finance_transaction_links WHERE source = 'manual_finance'"
        )
    )
    assert links == {"sha-brokerage|0": "fuzzy_amount_date", "sha-brokerage|1": "source_id"}


def test_document_transactions_dedup_against_plaid_overlap(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row()])
    warehouse.insert_plaid_transactions([_plaid_transaction_row()])  # COFFEE 4.50 out on 7/13
    _seed_document(
        warehouse,
        extraction=_extraction_row(
            transactions_json=[
                # Same account (mask 0001), same amount, one day off: merges.
                {"date": "2026-07-12", "description": "COFFEE SHOP", "amount": "4.50", "direction": "out"},
                # Outside plaid's window: founds a new ledger row.
                {"date": "2024-01-05", "description": "OLD RENT", "amount": "900.00", "direction": "out"},
            ]
        ),
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.transactions_merged == 1
    rows = warehouse._query("SELECT description, amount, source FROM @finance_transactions ORDER BY posted_at")
    assert rows == [
        ("OLD RENT", Decimal("-900.00"), "manual_finance"),
        ("COFFEE SHOP", Decimal("-4.5"), "plaid"),
    ]
    # The doc's account resolved to the EXISTING plaid account by mask: no new account.
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(1,)]
    links = dict(
        warehouse._query(
            "SELECT source_row_key, match_method FROM @finance_transaction_links WHERE source = 'manual_finance'"
        )
    )
    assert links == {"sha-doc-1|0": "fuzzy_amount_date", "sha-doc-1|1": "source_id"}


def test_mortgage_statement_founds_account_and_principal_observations(warehouse):
    _seed_plaid(warehouse, [_plaid_account_row()])
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-mortgage",
            source_native_id="sha-mortgage",
            original_path="acme-mortgage-servicing-0009/statement-2026-06.pdf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-mortgage",
            document_type="mortgage_statement",
            institution="Acme Mortgage Servicing",
            account_name_hint="Mortgage",
            account_mask="0009",
            balances_json=[{"date": "2026-06-30", "balance": "412345.67"}],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    accounts = warehouse._query(
        "SELECT kind, side, institution FROM @finance_accounts WHERE mask = '0009'"
    )
    assert accounts == [("mortgage", "liability", "Acme Mortgage Servicing")]
    observations = warehouse._query(
        "SELECT kind, value, source FROM @finance_observations WHERE as_of = %s",
        (date(2026, 6, 30),),
    )
    assert observations == [("principal", Decimal("412345.67"), "manual_finance")]
    # Net worth now subtracts the mortgage principal.
    total = warehouse._query("SELECT SUM(signed_value) FROM @marts_finance_net_worth")
    assert total == [(Decimal("123.45") - Decimal("412345.67"),)]


def test_multi_entity_valuation_doc_prefers_total_else_first(warehouse):
    warehouse.ensure_plaid_tables()
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-fund",
            source_native_id="sha-fund",
            filename="positions.rtf",
            original_path="examplefund-i-lp/positions.rtf",
            mime_type="text/rtf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-fund",
            document_type="fund_positions",
            institution="Carta",
            account_name_hint="Fund I LP",
            account_mask="",
            valuations_json=[
                {"date": "2026-04-11", "value": "5241.59", "description": "SPV A — NAV"},
                {"date": "2026-04-11", "value": "7312.44", "description": "Fund I — NAV"},
                {"date": "2026-04-11", "value": "4993.98", "description": "SPV B — NAV"},
                {"date": "2026-04-11", "value": "24680.15", "description": "Totals — NAV"},
                # A same-day set WITHOUT a totals row restates one asset
                # (estimate + low/high bounds + rental): the primary figure is
                # listed first and alternatives must never sum.
                {"date": "2026-05-11", "value": "525000", "description": "Estimate"},
                {"date": "2026-05-11", "value": "445000", "description": "Estimated sale price — low"},
                {"date": "2026-05-11", "value": "539000", "description": "Estimated sale price — high"},
                {"date": "2026-05-11", "value": "1905", "description": "Rental estimate per month"},
                # Negative entries are deltas (depreciation), never values —
                # even when their description matches the totals heuristic.
                {"date": "2026-06-11", "value": "-11330", "description": "Total depreciation over five years"},
                {"date": "2026-06-11", "value": "16835", "description": "Trade-in range — low"},
            ],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    rows = warehouse._query(
        "SELECT as_of, value FROM @finance_observations WHERE kind = 'valuation' ORDER BY as_of"
    )
    assert rows == [
        (date(2026, 4, 11), Decimal("24680.15")),
        (date(2026, 5, 11), Decimal("525000")),
        (date(2026, 6, 11), Decimal("16835")),
    ]


def test_folder_spanning_account_number_change_resolves_by_any_mask(warehouse):
    # One folder holds statements across a clearing migration: the OLDEST
    # document carries a retired mask, later ones carry the mask plaid knows.
    # Resolution must consider every mask in the group, not the founding
    # document's — otherwise the folder founds a duplicate account.
    _seed_plaid(warehouse, [_plaid_account_row(type="brokerage", subtype="brokerage", mask="4420")])
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-apex-era",
            source_native_id="sha-apex-era",
            original_path="broker-individual-4417/statement-2018-11.pdf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-apex-era",
            document_type="brokerage_statement",
            institution="Broker / Old Clearing Corp",
            account_mask="4417",
            balances_json=[{"date": "2018-11-30", "balance": "100.00"}],
        ),
    )
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-modern",
            source_native_id="sha-modern",
            original_path="broker-individual-4417/statement-2026-03.pdf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-modern",
            document_type="brokerage_statement",
            institution="Broker",
            account_mask="4420",
            balances_json=[{"date": "2026-03-31", "balance": "200.00"}],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    # No duplicate account: the folder resolved to the existing plaid account.
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(1,)]
    links = dict(
        warehouse._query(
            "SELECT source_account_key, match_method FROM @finance_account_links WHERE source = 'manual_finance'"
        )
    )
    assert links == {"broker-individual-4417": "mask"}
    # Both eras' balances land on the one account.
    assert warehouse._query("SELECT count(*) FROM @finance_observations WHERE source='manual_finance'") == [(2,)]


def test_a_link_made_from_thinner_evidence_is_re_resolved_next_run(warehouse):
    """A document group's link is a derived decision, not a fact, so it has to
    move when the evidence moves.

    Robinhood's crypto folder founded its link from the one statement that had
    been extracted at the time, and that statement printed the linked
    BROKERAGE account number instead of the crypto one. Every later statement
    reported the crypto mask, but the link was consulted before resolution and
    never revisited, so the crypto trades were booked against the brokerage
    account for six weeks — where nothing could dedupe them against the plaid
    crypto rows describing the same trades.
    """
    _seed_plaid(
        warehouse,
        [
            _plaid_account_row(
                account_id="acc-individual",
                name="Broker individual",
                mask="4420",
                type="brokerage",
                subtype="brokerage",
            ),
            _plaid_account_row(
                account_id="acc-crypto",
                name="Crypto",
                mask="9910",
                type="brokerage",
                subtype="crypto exchange",
            ),
        ],
    )
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-crypto-2024-12",
            source_native_id="sha-crypto-2024-12",
            original_path="broker-crypto-9910/crypto-2024-12.pdf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-crypto-2024-12",
            document_type="brokerage_statement",
            institution="Broker",
            account_name_hint="Crypto",
            account_mask="4420",  # the statement header carries the brokerage number
            balances_json=[{"date": "2024-12-31", "balance": "100.00"}],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    crypto_id = warehouse._query(
        "SELECT account_id FROM @finance_accounts WHERE mask = '9910'"
    )[0][0]
    individual_id = warehouse._query(
        "SELECT account_id FROM @finance_accounts WHERE mask = '4420'"
    )[0][0]
    assert warehouse._query(
        "SELECT account_id FROM @finance_account_links WHERE source_account_key = 'broker-crypto-9910'"
    ) == [(individual_id,)]

    # The rest of the folder arrives, and every one of those statements names
    # the crypto account.
    for month in ("2025-01", "2025-02"):
        _seed_document(
            warehouse,
            document=_document_row(
                content_sha256=f"sha-crypto-{month}",
                source_native_id=f"sha-crypto-{month}",
                original_path=f"broker-crypto-9910/crypto-{month}.pdf",
            ),
            extraction=_extraction_row(
                content_sha256=f"sha-crypto-{month}",
                document_type="brokerage_statement",
                institution="Broker",
                account_name_hint="Crypto",
                account_mask="9910",
                balances_json=[{"date": f"{month}-28", "balance": "100.00"}],
            ),
        )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert warehouse._query(
        "SELECT account_id FROM @finance_account_links WHERE source_account_key = 'broker-crypto-9910'"
    ) == [(crypto_id,)]
    # The folder's observations follow the link rather than staying behind.
    assert warehouse._query(
        "SELECT count(*) FROM @finance_observations WHERE source = 'manual_finance' AND account_id = %s",
        (individual_id,),
    ) == [(0,)]


def test_re_resolution_keeps_an_account_the_documents_themselves_founded(warehouse):
    """Re-resolving must not orphan a founded account. A private-fund folder
    matches no plaid mask on any run, so its link has to stay put."""
    _seed_plaid(warehouse, [_plaid_account_row()])
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-fund",
            source_native_id="sha-fund",
            original_path="acme-fund-i-lp/2026-q1.pdf",
            filename="2026-q1.pdf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-fund",
            document_type="fund_statement",
            institution="Acme Fund I LP",
            account_name_hint="Acme Fund I LP",
            account_mask="",
            valuations_json=[{"date": "2026-03-31", "value": "50000.00"}],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    before = warehouse._query(
        "SELECT account_id FROM @finance_account_links WHERE source_account_key = 'acme-fund-i-lp'"
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    after = warehouse._query(
        "SELECT account_id FROM @finance_account_links WHERE source_account_key = 'acme-fund-i-lp'"
    )
    assert before == after and before
    assert warehouse._query(
        "SELECT count(*) FROM @finance_accounts WHERE account_id = %s", (before[0][0],)
    ) == [(1,)]


def test_valuation_documents_found_asset_accounts(warehouse):
    warehouse.ensure_plaid_tables()
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-zillow",
            source_native_id="sha-zillow",
            filename="zestimate.png",
            original_path="real-estate-main-st/zestimate.png",
            mime_type="image/png",
        ),
        extraction=_extraction_row(
            content_sha256="sha-zillow",
            document_type="property_valuation",
            institution="",
            account_name_hint="Main St house",
            account_mask="",
            valuations_json=[{"date": "2026-07-01", "value": "650000", "description": "estimate"}],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    accounts = warehouse._query("SELECT kind, side, name FROM @finance_accounts")
    assert accounts == [("property", "asset", "Main St house")]
    observations = warehouse._query("SELECT kind, value FROM @finance_observations")
    assert observations == [("valuation", Decimal("650000"))]


# --- re-linked institutions (Plaid mints a new item id, not a repaired one) --------


def _credit_card_row(**overrides) -> dict:
    """A credit card as Plaid reports it — the shape a re-link duplicates."""
    defaults = {
        "account_id": "acc-card",
        "name": "Rewards Card",
        "official_name": "Rewards Card",
        "mask": "4242",
        "type": "credit",
        "subtype": "credit card",
        "current_balance": 100.0,
    }
    return _plaid_account_row(**{**defaults, **overrides})


def test_relinked_plaid_account_adopts_the_existing_ledger_account(warehouse):
    """Re-linking an institution is the same card under a new plaid id.

    Plaid mints a fresh item_id AND fresh account_ids when Link runs again, so
    keying ledger identity on the plaid account id alone forks every account
    and double-counts net worth. The new account resolves onto the existing
    logical account by institution+mask instead.
    """
    _seed_plaid(warehouse, [_credit_card_row()])
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    original = stable_finance_account_id("plaid", "z@x.test", "acc-card")

    # The dead item is retired (`pdw ingest plaid unlink`) and the institution
    # comes back under a new item with new plaid account ids.
    warehouse._command("DELETE FROM @plaid_accounts WHERE item_id = 'item-1'")
    warehouse.insert_plaid_items([_plaid_item_row(item_id="item-2")])
    warehouse.insert_plaid_accounts(
        [_credit_card_row(item_id="item-2", account_id="acc-card-2", current_balance=250.0)]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(day=14)).sync()

    assert summary.accounts_created == 0
    assert warehouse._query("SELECT account_id FROM @finance_accounts") == [(original,)]
    assert warehouse._query(
        "SELECT source_account_key, account_id, match_method FROM @finance_account_links ORDER BY source_account_key"
    ) == [
        ("acc-card", original, "source_id"),
        ("acc-card-2", original, "institution_mask"),
    ]
    # One continuous balance history across the re-link.
    assert warehouse._query(
        "SELECT as_of, value FROM @finance_observations ORDER BY as_of"
    ) == [
        (date(2026, 7, 13), Decimal("100.00")),
        (date(2026, 7, 14), Decimal("250.00")),
    ]


def test_concurrent_duplicate_plaid_items_keep_separate_ledger_accounts(warehouse):
    """While both items are live we cannot tell which one is authoritative.

    Merging them would make the day's balance observation a race between two
    live sources, so each keeps its own account until the operator retires one.
    """
    _seed_plaid(warehouse, [_credit_card_row()])
    warehouse.insert_plaid_items([_plaid_item_row(item_id="item-2")])
    warehouse.insert_plaid_accounts(
        [_credit_card_row(item_id="item-2", account_id="acc-card-2", current_balance=250.0)]
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(2,)]
    assert warehouse._query("SELECT count(*) FROM @finance_observations") == [(2,)]


def test_retiring_a_forked_plaid_item_merges_and_prunes_the_duplicate(warehouse):
    """The production incident, end to end.

    A re-link forked a card before the dead item was retired:
    marts_finance.net_worth carried two live rows per card and the transaction
    overlap was double-counted. Unlinking the old item must leave exactly one
    logical account per card, carrying the whole history (including the
    statement documents linked to it), and no residue.
    """
    _seed_plaid(warehouse, [_credit_card_row()])
    warehouse.insert_plaid_transactions(
        [_plaid_transaction_row(account_id="acc-card", transaction_id="tx-old", amount=12.34)]
    )
    _seed_document(
        warehouse,
        extraction=_extraction_row(
            document_type="credit_card_statement",
            account_mask="4242",
            balances_json=[{"date": "2026-06-30", "balance": "500.00"}],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    original = stable_finance_account_id("plaid", "z@x.test", "acc-card")

    # A later re-link forks the card: a second item, a second ledger account.
    warehouse.insert_plaid_items([_plaid_item_row(item_id="item-2")])
    warehouse.insert_plaid_accounts(
        [_credit_card_row(item_id="item-2", account_id="acc-card-2", current_balance=250.0)]
    )
    warehouse.insert_plaid_transactions(
        [
            _plaid_transaction_row(
                item_id="item-2", account_id="acc-card-2", transaction_id="tx-new", amount=12.34
            )
        ]
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(day=14)).sync()
    forked = stable_finance_account_id("plaid", "z@x.test", "acc-card-2")
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(2,)]
    assert warehouse._query("SELECT count(*) FROM @finance_transactions") == [(2,)]

    # `pdw ingest plaid unlink item-1` deletes the dead item's raw rows.
    warehouse._command("DELETE FROM @plaid_accounts WHERE item_id = 'item-1'")
    warehouse._command("DELETE FROM @plaid_transactions WHERE item_id = 'item-1'")
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(day=15)).sync()

    assert summary.accounts_merged == 1
    assert summary.accounts_pruned == 1
    assert warehouse._query("SELECT account_id FROM @finance_accounts") == [(original,)]
    assert warehouse._query(
        "SELECT count(*) FROM @finance_observations WHERE account_id = %s", (forked,)
    ) == [(0,)]
    # The statement document keeps pointing at the surviving account, and the
    # overlap collapses back to one transaction.
    assert warehouse._query(
        "SELECT account_id FROM @finance_account_links WHERE source = 'manual_finance'"
    ) == [(original,)]
    assert warehouse._query("SELECT account_id, count(*) FROM @finance_transactions GROUP BY 1") == [
        (original, 1)
    ]


def test_plaid_accounts_without_a_mask_never_adopt_another_account(warehouse):
    """Mask+institution is the identity evidence; without it, never merge."""
    _seed_plaid(warehouse, [_plaid_account_row(mask="")])
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    warehouse.insert_plaid_items([_plaid_item_row(item_id="item-2")])
    warehouse.insert_plaid_accounts(
        [_plaid_account_row(item_id="item-2", account_id="acc-2", mask="")]
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(2,)]


def test_ledger_never_prunes_an_account_a_source_still_links_to(warehouse):
    """Pruning is only for merge residue — an unlinked institution keeps its
    account and its history until something else claims it."""
    _seed_plaid(warehouse, [_plaid_account_row()])
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    warehouse._command("DELETE FROM @plaid_accounts")
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(day=14)).sync()

    assert summary.accounts_pruned == 0
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(1,)]
    assert warehouse._query("SELECT count(*) FROM @finance_observations") == [(1,)]


# --- security trades + tax lots ------------------------------------------------


def _security_row(**overrides) -> dict:
    row = {
        "account": "z@x.test",
        "security_id": "sec-net",
        "name": "Acme Networks Inc - Ordinary Shares - Class A",
        "ticker_symbol": "ACME",
        "type": "equity",
        "close_price": 80.0,
        "close_price_as_of": _TS,
        "iso_currency_code": "USD",
        "unofficial_currency_code": "",
        "raw_json": {},
        "synced_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _holding_row(**overrides) -> dict:
    row = {
        "account": "z@x.test",
        "item_id": "item-1",
        "account_id": "acc-b",
        "security_id": "sec-net",
        "quantity": 10.0,
        "institution_value": 800.0,
        "institution_price": 80.0,
        "institution_price_as_of": _TS,
        "cost_basis": 400.0,
        "iso_currency_code": "USD",
        "unofficial_currency_code": "",
        "raw_json": {},
        "synced_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _doc_trade(**overrides) -> dict:
    entry = {
        "date": "2020-08-10",
        "description": "Acme Networks",
        "amount": "400.00",
        "direction": "out",
        "security_name": "Acme Networks",
        "ticker": "ACME",
        "cusip": "111111AA1",
        "quantity": "10",
        "price_per_share": "40.00",
        "trade_side": "buy",
        "fees": "",
    }
    entry.update(overrides)
    return entry


def test_statement_trades_build_lots_plaid_cannot_reach(warehouse):
    """The whole point: a 2020 buy is outside Plaid's 730-day window, so the
    statement is the only witness to that lot's date and cost."""
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-b",
            source_native_id="sha-b",
            original_path="acme-wheel-fund-0002/2020-08.csv",
        ),
        extraction=_extraction_row(
            content_sha256="sha-b",
            document_type="brokerage_statement",
            account_mask="0002",
            transactions_json=[
                _doc_trade(),
                # A plain cash row must NOT become a trade.
                {
                    "date": "2020-08-03",
                    "description": "ACH Deposit",
                    "amount": "1000.00",
                    "direction": "in",
                    "security_name": "",
                    "ticker": "",
                    "cusip": "",
                    "quantity": "",
                    "price_per_share": "",
                    "trade_side": "",
                    "fees": "",
                },
            ],
        ),
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.security_trades_upserted == 1
    assert summary.tax_lots_built == 1
    rows = warehouse._query(
        "SELECT ticker, side, quantity, price, asset_class, source FROM @finance_security_transactions"
    )
    assert rows == [("ACME", "buy", Decimal("10"), Decimal("40.00"), "spot", "manual_finance")]
    lots = warehouse._query(
        """
        SELECT ticker, acquired_on, quantity, quantity_remaining, cost_basis, term, status
        FROM @marts_finance_tax_lots
        """
    )
    assert lots == [("ACME", date(2020, 8, 10), Decimal("10"), Decimal("10"), Decimal("400.00"), "long", "open")]


def test_security_trades_dedup_against_the_plaid_overlap(warehouse):
    """153 real statements overlap Plaid's window. A trade both sources
    describe must be ONE fact — a doubled trade makes a confidently wrong lot."""
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    warehouse.insert_plaid_investment_securities([_security_row()])
    warehouse.insert_plaid_investment_transactions(
        [
            _plaid_investment_transaction_row(
                investment_transaction_id="itx-buy",
                security_id="sec-net",
                name="ACME buy",
                type="buy",
                subtype="buy",
                quantity=10.0,
                price=40.0,
                amount=400.0,
                transaction_at=_TS,
            )
        ]
    )
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-b",
            source_native_id="sha-b",
            original_path="acme-wheel-fund-0002/statement.csv",
        ),
        extraction=_extraction_row(
            content_sha256="sha-b",
            document_type="brokerage_statement",
            account_mask="0002",
            transactions_json=[
                # Same security, side, quantity, one day off: merges into Plaid.
                _doc_trade(date=_TS.date().replace(day=_TS.day - 1).isoformat()),
                # A genuinely older buy: founds its own trade.
                _doc_trade(date="2020-08-10", quantity="3", amount="120.00"),
            ],
        ),
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.security_trades_merged == 1
    assert summary.security_trades_upserted == 2
    sources = warehouse._query(
        "SELECT source, count(*) FROM @finance_security_transactions GROUP BY 1 ORDER BY 1"
    )
    assert sources == [("manual_finance", 1), ("plaid", 1)]
    # Both source rows resolve to a trade, and the merged one records why.
    methods = dict(
        warehouse._query(
            "SELECT source_row_key, match_method FROM @finance_security_transaction_links "
            "WHERE source = 'manual_finance' ORDER BY source_row_key"
        )
    )
    assert methods == {"sha-b|0": "security_quantity_date", "sha-b|1": "source_id"}


def test_crypto_relink_converges_to_the_same_state_as_a_full_replay(warehouse):
    """The two defects that produced ~$60k of phantom open crypto lots, end to
    end, including the full-replay check that originally lived in a manual
    verification script.

    Robinhood reports crypto as its own plaid account while its crypto
    statements live in their own folder. The folder's link was made from the
    single statement extracted at the time, which printed the BROKERAGE
    account number, and was then never revisited — so the statement's trades
    sat in the brokerage account and could not be deduped against the plaid
    rows describing the same trades. Even in one account they would not have
    merged: plaid prints crypto quantities to six decimals, and on a 0.003 BTC
    buy that rounding is wider than the relative quantity tolerance.
    """
    _seed_plaid(
        warehouse,
        [
            _plaid_account_row(
                account_id="acc-individual",
                name="Broker individual",
                mask="4420",
                type="brokerage",
                subtype="brokerage",
            ),
            _plaid_account_row(
                account_id="acc-crypto",
                name="Crypto",
                mask="9910",
                type="brokerage",
                subtype="crypto exchange",
            ),
        ],
    )
    warehouse.insert_plaid_investment_securities(
        [_security_row(security_id="sec-btc", name="Bitcoin", ticker_symbol="BTC", type="cryptocurrency")]
    )
    warehouse.insert_plaid_investment_transactions(
        [
            _plaid_investment_transaction_row(
                account_id="acc-crypto",
                investment_transaction_id="itx-btc",
                security_id="sec-btc",
                name="BTC buy",
                type="buy",
                subtype="buy",
                quantity=0.003183,  # six decimals is all plaid prints
                price=94225.0,
                amount=299.922,
                transaction_at=_TS,
            )
        ]
    )
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-crypto-a",
            source_native_id="sha-crypto-a",
            original_path="broker-crypto-9910/crypto-2026-07.pdf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-crypto-a",
            document_type="brokerage_statement",
            institution="Broker",
            account_name_hint="Crypto",
            account_mask="4420",  # the statement header carries the brokerage number
            transactions_json=[
                _doc_trade(
                    date=_TS.date().isoformat(),
                    description="Bitcoin",
                    security_name="Bitcoin",
                    ticker="BTC",
                    cusip="",
                    quantity="0.00318255",
                    price_per_share="",
                    amount="299.92",
                )
            ],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    # The bug's shape: one buy, booked twice, in two different accounts.
    assert warehouse._query(
        "SELECT count(*) FROM @marts_finance_tax_lots WHERE status = 'open'"
    ) == [(2,)]

    # The rest of the folder arrives, and those statements name the crypto
    # account — so the group's own evidence now points somewhere else.
    for month in ("2026-08", "2026-09"):
        _seed_document(
            warehouse,
            document=_document_row(
                content_sha256=f"sha-crypto-{month}",
                source_native_id=f"sha-crypto-{month}",
                original_path=f"broker-crypto-9910/crypto-{month}.pdf",
            ),
            extraction=_extraction_row(
                content_sha256=f"sha-crypto-{month}",
                document_type="brokerage_statement",
                institution="Broker",
                account_name_hint="Crypto",
                account_mask="9910",
            ),
        )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert summary.links_relinked == 1
    assert summary.security_trades_merged == 1
    lots = warehouse._query(
        "SELECT account_name, ticker, quantity_remaining, status FROM @marts_finance_tax_lots"
    )
    assert lots == [("Crypto", "BTC", Decimal("0.003183"), "open")]

    # A correction applied incrementally must converge to exactly the state a
    # fresh replay derives from the complete source corpus. This is the useful
    # contract from the former live-corpus verification script, expressed as a
    # deterministic regression test instead of an operator-run comparison.
    snapshot_queries = (
        "SELECT source, source_account_key, account_id, match_method "
        "FROM @finance_account_links ORDER BY source, source_account_key",
        "SELECT transaction_id, account_id, security_key, ticker, side, quantity, amount, source "
        "FROM @finance_security_transactions ORDER BY transaction_id",
        "SELECT source, source_row_key, transaction_id, match_method "
        "FROM @finance_security_transaction_links ORDER BY source, source_row_key",
        "SELECT lot_id, account_id, security_key, quantity_remaining, cost_basis_remaining, status "
        "FROM @finance_tax_lots ORDER BY lot_id",
    )
    incrementally_corrected = [warehouse._query(query) for query in snapshot_queries]

    for table in (
        "finance_tax_lots",
        "finance_security_transaction_links",
        "finance_security_transactions",
        "finance_transaction_links",
        "finance_transactions",
        "finance_observations",
        "finance_account_links",
        "finance_accounts",
    ):
        warehouse._command(f"DELETE FROM @{table}")

    replay = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert replay.links_relinked == 0
    assert replay.security_trades_merged == 1
    assert [warehouse._query(query) for query in snapshot_queries] == incrementally_corrected


def test_option_contracts_do_not_pollute_the_underlying_position(warehouse):
    """Real row: 'ACME 09/18/2020 Call $60.00' qty 1 @ $0.30. One contract is
    100 shares — counting it as a share of ACME corrupts the position."""
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-b",
            source_native_id="sha-b",
            original_path="acme-wheel-fund-0002/statement.csv",
        ),
        extraction=_extraction_row(
            content_sha256="sha-b",
            document_type="brokerage_statement",
            account_mask="0002",
            transactions_json=[
                _doc_trade(),
                _doc_trade(
                    description="ACME 09/18/2020 Call $60.00",
                    security_name="ACME 09/18/2020 Call $60.00",
                    cusip="",
                    quantity="1",
                    price_per_share="0.30",
                    amount="30.00",
                ),
            ],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    keys = warehouse._query(
        "SELECT asset_class, security_key, quantity FROM @finance_security_transactions ORDER BY asset_class"
    )
    assert [row[0] for row in keys] == ["option", "spot"]
    assert keys[0][1] != keys[1][1]
    # The spot lot holds 10 shares, not 11.
    assert warehouse._query(
        "SELECT sum(quantity_remaining) FROM @finance_tax_lots WHERE security_key = %s", (keys[1][1],)
    ) == [(Decimal("10"),)]
    # The option statement quotes a $0.30 per-share premium, but one contract
    # represents 100 shares and the printed transaction total is $30. The lot
    # basis must follow that grounded total, not price * contract count.
    assert warehouse._query(
        "SELECT cost_basis FROM @finance_tax_lots WHERE security_key = %s", (keys[0][1],)
    ) == [(Decimal("30.00"),)]
    assert warehouse._query(
        "SELECT price FROM @finance_security_transactions WHERE security_key = %s", (keys[0][1],)
    ) == [(Decimal("30.00"),)]


def test_transferred_in_shares_never_invent_a_cost_basis(warehouse):
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    warehouse.insert_plaid_investment_securities([_security_row()])
    warehouse.insert_plaid_investment_transactions(
        [
            _plaid_investment_transaction_row(
                investment_transaction_id="itx-xfer",
                security_id="sec-net",
                name="ACME transfer",
                type="transfer",
                subtype="transfer",
                quantity=25.0,
                price=0.0,
                amount=0.0,
            )
        ]
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    rows = warehouse._query(
        "SELECT basis_known, quantity_remaining, cost_basis FROM @finance_tax_lots"
    )
    assert rows == [(0, Decimal("25"), Decimal("0"))]
    # The read surface shows an ABSENT basis, never a free acquisition.
    assert warehouse._query(
        "SELECT cost_basis, cost_per_unit, realized_gain FROM @marts_finance_tax_lots"
    ) == [(None, None, None)]


def test_position_coverage_surfaces_independent_basis_disagreement(warehouse):
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    warehouse.insert_plaid_investment_securities([_security_row()])
    warehouse.insert_plaid_investment_holdings([_holding_row(cost_basis=450.0)])
    warehouse.insert_plaid_investment_transactions(
        [
            _plaid_investment_transaction_row(
                investment_transaction_id="itx-buy",
                security_id="sec-net",
                name="ACME buy",
                type="buy",
                subtype="buy",
                quantity=10.0,
                price=40.0,
                amount=400.0,
            )
        ]
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert warehouse._query(
        "SELECT quantity_held, quantity_with_known_basis, basis_difference, coverage_status "
        "FROM @marts_finance_position_coverage"
    ) == [(Decimal("10"), Decimal("10"), Decimal("-50"), "basis_mismatch")]


def test_position_coverage_reports_open_lots_the_account_does_not_hold(warehouse):
    """The detector the crypto double-booking got past.

    Coverage used to start from held positions, so open lots for a security an
    account does NOT hold produced no row at all — which is exactly the shape
    of a trade booked into the wrong account. The account has a holdings feed
    disagreeing with the lots, so the disagreement is reportable; a
    statement-only account, which has no feed to disagree with, still is not.
    """
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    warehouse.insert_plaid_investment_securities([_security_row()])
    warehouse.insert_plaid_investment_holdings([_holding_row()])
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-b",
            source_native_id="sha-b",
            original_path="acme-wheel-fund-0002/statement.csv",
        ),
        extraction=_extraction_row(
            content_sha256="sha-b",
            document_type="brokerage_statement",
            account_mask="0002",
            transactions_json=[
                _doc_trade(),
                # A security this account has never held: a buy that belongs to
                # some other account, or one whose disposal never reached us.
                _doc_trade(
                    description="Zenith Systems",
                    security_name="Zenith Systems",
                    ticker="ZNTH",
                    cusip="222222BB2",
                    quantity="5",
                    price_per_share="20.00",
                    amount="100.00",
                ),
            ],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    coverage = dict(
        warehouse._query(
            "SELECT ticker, coverage_status FROM @marts_finance_position_coverage ORDER BY ticker"
        )
    )
    assert coverage == {"ACME": "complete", "ZNTH": "no_holding"}
    assert warehouse._query(
        "SELECT quantity_held, quantity_with_lots FROM @marts_finance_position_coverage "
        "WHERE ticker = 'ZNTH'"
    ) == [(Decimal("0"), Decimal("5"))]


def test_position_coverage_excludes_plaid_cash_securities(warehouse):
    """A money-market sweep vehicle (Plaid type='cash', e.g. SPAXX) has no trade
    history to reconstruct — its basis IS its value — so it must not sit in
    position_coverage as a permanent basis_mismatch. Found on the real corpus
    2026-08-21: two SPAXX rows whose basis_difference exactly equalled the
    position value, pure structural noise."""
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    warehouse.insert_plaid_investment_securities(
        [
            _security_row(),
            _security_row(
                security_id="sec-mm",
                name="Fidelity Government Money Market Fund",
                ticker_symbol="SPAXX",
                type="cash",
                close_price=1.0,
            ),
        ]
    )
    warehouse.insert_plaid_investment_holdings(
        [
            _holding_row(cost_basis=400.0),
            _holding_row(
                security_id="sec-mm",
                quantity=4868.02,
                institution_value=4868.02,
                institution_price=1.0,
                cost_basis=0.0,
            ),
        ]
    )
    warehouse.insert_plaid_investment_transactions(
        [
            _plaid_investment_transaction_row(
                investment_transaction_id="itx-buy",
                security_id="sec-net",
                name="ACME buy",
                type="buy",
                subtype="buy",
                quantity=10.0,
                price=40.0,
                amount=400.0,
            )
        ]
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert warehouse._query(
        "SELECT ticker FROM @marts_finance_position_coverage ORDER BY ticker"
    ) == [("ACME",)]


def test_security_ledger_replays_identically(warehouse):
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    warehouse.insert_plaid_investment_securities([_security_row()])
    warehouse.insert_plaid_investment_transactions(
        [
            _plaid_investment_transaction_row(
                investment_transaction_id="itx-buy",
                security_id="sec-net",
                name="ACME buy",
                type="buy",
                subtype="buy",
                quantity=10.0,
                price=40.0,
                amount=400.0,
            )
        ]
    )
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-b",
            source_native_id="sha-b",
            original_path="acme-wheel-fund-0002/statement.csv",
        ),
        extraction=_extraction_row(
            content_sha256="sha-b",
            document_type="brokerage_statement",
            account_mask="0002",
            transactions_json=[_doc_trade()],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    sql = (
        "SELECT transaction_id, security_key, ticker, side, quantity FROM @finance_security_transactions ORDER BY transaction_id",
        "SELECT source, source_row_key, transaction_id, match_method FROM @finance_security_transaction_links ORDER BY source, source_row_key",
        "SELECT lot_id, account_id, security_key, acquired_on, quantity_remaining, status FROM @finance_tax_lots ORDER BY lot_id",
    )
    before = [warehouse._query(q) for q in sql]
    warehouse._command("DELETE FROM @finance_security_transaction_links")
    warehouse._command("DELETE FROM @finance_security_transactions")
    warehouse._command("DELETE FROM @finance_tax_lots")
    FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(day=14)).sync()
    assert [warehouse._query(q) for q in sql] == before


def test_lots_shrink_when_a_source_trade_disappears(warehouse):
    """Lots are a reduction, not accumulated state: a corrected extraction that
    removes a trade must not leave its lot behind as a confident fiction."""
    _seed_plaid(warehouse, [_plaid_brokerage_account_row()])
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-b",
            source_native_id="sha-b",
            original_path="acme-wheel-fund-0002/statement.csv",
        ),
        extraction=_extraction_row(
            content_sha256="sha-b",
            document_type="brokerage_statement",
            account_mask="0002",
            transactions_json=[_doc_trade(), _doc_trade(date="2020-09-10", quantity="5", amount="200.00")],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert warehouse._query("SELECT count(*) FROM @finance_tax_lots") == [(2,)]

    warehouse._command("DELETE FROM @manual_finance_extractions")
    warehouse.insert_manual_finance_extractions(
        [
            _extraction_row(
                content_sha256="sha-b",
                document_type="brokerage_statement",
                account_mask="0002",
                transactions_json=[_doc_trade()],
            )
        ]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(day=14)).sync()
    assert summary.security_trades_removed == 1
    assert warehouse._query("SELECT count(*) FROM @finance_tax_lots") == [(1,)]
    assert warehouse._query("SELECT count(*) FROM @finance_security_transaction_links") == [(1,)]
