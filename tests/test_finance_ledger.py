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
    warehouse.insert_plaid_transactions([_plaid_transaction_row()])
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
        "SELECT account_id, kind, side FROM finance_accounts ORDER BY account_id",
        "SELECT transaction_id, account_id, amount, source FROM finance_transactions ORDER BY transaction_id",
        "SELECT source, source_row_key, transaction_id, match_method FROM finance_transaction_links ORDER BY source, source_row_key",
    )
    before = [warehouse._query(sql) for sql in snapshot_sql]
    for table in (
        "finance_transaction_links",
        "finance_transactions",
        "finance_observations",
        "finance_account_links",
        "finance_accounts",
    ):
        warehouse._command(f"DELETE FROM {table}")
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
        "SELECT description, amount, source, pending FROM finance_transactions ORDER BY amount"
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
    assert warehouse._query("SELECT transaction_id FROM finance_transactions") == [(pending_ledger_id,)]

    # The posted row arrives and the pending row tombstones (plaid behavior).
    warehouse.insert_plaid_transactions(
        [
            _plaid_transaction_row(transaction_id="tx-post", pending_transaction_id="tx-p", sync_version=2),
            _plaid_transaction_row(transaction_id="tx-p", pending=1, is_removed=1, sync_version=2),
        ]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS.replace(hour=13)).sync()
    posted_ledger_id = stable_finance_transaction_id("plaid", "z@x.test|tx-post")
    rows = warehouse._query("SELECT transaction_id, pending FROM finance_transactions")
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
            "SELECT source_row_key, match_method FROM finance_transaction_links WHERE source_row_key LIKE '%%tx-p2'"
        )
    )
    assert links == {"z@x.test|tx-p2": "pending_id"}


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
    rows = warehouse._query("SELECT description, amount, source FROM finance_transactions ORDER BY posted_at")
    assert rows == [
        ("OLD RENT", Decimal("-900.00"), "manual_finance"),
        ("COFFEE SHOP", Decimal("-4.5"), "plaid"),
    ]
    # The doc's account resolved to the EXISTING plaid account by mask: no new account.
    assert warehouse._query("SELECT count(*) FROM finance_accounts") == [(1,)]
    links = dict(
        warehouse._query(
            "SELECT source_row_key, match_method FROM finance_transaction_links WHERE source = 'manual_finance'"
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
        "SELECT kind, side, institution FROM finance_accounts WHERE mask = '0009'"
    )
    assert accounts == [("mortgage", "liability", "Acme Mortgage Servicing")]
    observations = warehouse._query(
        "SELECT kind, value, source FROM finance_observations WHERE as_of = %s",
        (date(2026, 6, 30),),
    )
    assert observations == [("principal", Decimal("412345.67"), "manual_finance")]
    # Net worth now subtracts the mortgage principal.
    total = warehouse._query("SELECT SUM(signed_value) FROM finance_net_worth")
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
                {"date": "2026-04-11", "value": "6561.81", "description": "Fund I — NAV"},
                {"date": "2026-04-11", "value": "4993.98", "description": "SPV B — NAV"},
                {"date": "2026-04-11", "value": "16797.38", "description": "Totals — NAV"},
                # A same-day set WITHOUT a totals row restates one asset
                # (estimate + low/high bounds + rental): the primary figure is
                # listed first and alternatives must never sum.
                {"date": "2026-05-11", "value": "468000", "description": "Estimate"},
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
        "SELECT as_of, value FROM finance_observations WHERE kind = 'valuation' ORDER BY as_of"
    )
    assert rows == [
        (date(2026, 4, 11), Decimal("16797.38")),
        (date(2026, 5, 11), Decimal("468000")),
        (date(2026, 6, 11), Decimal("16835")),
    ]


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
    accounts = warehouse._query("SELECT kind, side, name FROM finance_accounts")
    assert accounts == [("property", "asset", "Main St house")]
    observations = warehouse._query("SELECT kind, value FROM finance_observations")
    assert observations == [("valuation", Decimal("650000"))]
