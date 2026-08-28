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
    NON_VALUE_OBSERVATION_KINDS,
    _daily_valuations,
    REPORTING_SCOPE_ENTITY,
    UNIDENTIFIED_ACCOUNT_KEY,
    VALUE_BASIS_TAX,
    FinanceLedgerRunner,
    description_similarity,
    document_account_key,
    document_kind_side,
    document_reports_a_tax_basis,
    document_reports_an_entity,
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
        "reporting_scope": "account_holder",
        "account_holder": "Zach Lata",
        "value_basis": "market",
        "period_start": date(2026, 6, 1),
        "period_end": date(2026, 6, 30),
        "currency": "USD",
        "closing_balance": D("1234.56"),
        "transactions_json": [],
        "balances_json": [],
        "valuations_json": [],
        "positions_json": [],
        "commitments_json": [],
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


def test_an_institution_with_no_account_number_is_not_an_account_key():
    """An ``<institution>|`` key was a catch-all, and it cost seven figures.

    An institution name identifies a COUNTERPARTY. Keyed on it alone, every
    document that party ever sent lands in one ledger account: on 2026-08-27 a
    partnership's own financial statements, two unrelated investment vehicles,
    a tax notice and the owner's real capital account statements all collapsed
    into one, and the FUND's total members' equity became his largest asset.
    Every other branch of this function identifies exactly one account -- a
    folder by the uploader's contract, a mask by account number, a filename by
    document.
    """
    assert (
        document_account_key(
            original_path="1Q26 Unaudited Financials.pdf",
            institution="Carta",
            mask="",
            filename="1Q26 Unaudited Financials.pdf",
        )
        == UNIDENTIFIED_ACCOUNT_KEY
    )
    # A document with no institution either still keys on its own filename,
    # which is per-document and therefore never a bucket.
    assert (
        document_account_key(
            original_path="Debt_Record.pdf", institution="", mask="", filename="Debt_Record.pdf"
        )
        == "debt-record"
    )
    # And a folder still wins over everything, mask or no mask.
    assert (
        document_account_key(
            original_path="fundadmin-example-fund-i-lp/1Q26.pdf",
            institution="Carta",
            mask="",
            filename="1Q26.pdf",
        )
        == "fundadmin-example-fund-i-lp"
    )


def test_reporting_scope_and_value_basis_are_read_as_declared_never_inferred():
    assert document_reports_an_entity({"reporting_scope": "entity"}) is True
    assert document_reports_an_entity({"reporting_scope": "account_holder"}) is False
    # A pre-v3 extraction has no scope at all. Absence is NOT entity (that
    # would silently drop the whole existing corpus) and NOT account_holder
    # (that is the bug); the key guard is what protects an un-re-extracted
    # corpus.
    assert document_reports_an_entity({"reporting_scope": ""}) is False
    assert document_reports_an_entity({}) is False
    assert document_reports_a_tax_basis({"value_basis": "tax"}) is True
    assert document_reports_a_tax_basis({"value_basis": "market"}) is False
    assert document_reports_a_tax_basis({}) is False


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
        "SELECT as_of, value FROM @finance_observations WHERE kind = 'valuation' ORDER BY as_of"
    )
    assert rows == [
        (date(2026, 4, 11), Decimal("16797.38")),
        (date(2026, 5, 11), Decimal("468000")),
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
            original_path="broker-individual-5270/statement-2018-11.pdf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-apex-era",
            document_type="brokerage_statement",
            institution="Broker / Old Clearing Corp",
            account_mask="5270",
            balances_json=[{"date": "2018-11-30", "balance": "100.00"}],
        ),
    )
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-modern",
            source_native_id="sha-modern",
            original_path="broker-individual-5270/statement-2026-03.pdf",
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
    assert links == {"broker-individual-5270": "mask"}
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


# --- entity documents are not the owner's money -----------------------------------


def _fund_level_financials(**overrides) -> dict:
    """A private fund's own Q1 2026 unaudited financial statements.

    Every field except ``reporting_scope`` is indistinguishable from an
    investor's capital account statement for the same fund on the same day:
    the fund's name, a closing balance, a period, transactions.
    """
    defaults = dict(
        content_sha256="sha-fund-financials",
        document_type="entity_financial_statement",
        institution="Carta",
        account_name_hint="Example Fund I LP",
        account_mask="",
        reporting_scope="entity",
        account_holder="",
        value_basis="market",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        closing_balance=Decimal("4820000"),
        balances_json=[
            {"date": "2026-01-01", "balance": "3100000"},
            {"date": "2026-03-31", "balance": "4820000"},
        ],
        valuations_json=[
            {"date": "2026-03-31", "value": "6350000", "description": "Total assets"},
            {"date": "2026-03-31", "value": "4820000", "description": "Total members' equity"},
        ],
        transactions_json=[
            {
                "date": "2025-07-10",
                "description": "Portfolio Co SAFE",
                "amount": "1500000",
                "direction": "out",
                "security_name": "Portfolio Co SAFE",
                "ticker": "",
                "cusip": "",
                "quantity": "",
                "price_per_share": "",
                "trade_side": "buy",
                "fees": "",
            }
        ],
    )
    defaults.update(overrides)
    return _extraction_row(**defaults)


def test_a_fund_level_document_never_becomes_the_owners_balance(warehouse):
    """The seven-figure 2026-08-27 bug, in one test.

    A private fund sends its LPs two documents that look alike in every
    extracted field: its own financial statements and their capital account
    statement. Booking the first as a personal account reported the whole
    partnership's members' equity as the investor's asset and put the fund's
    entire portfolio purchase history into his transaction ledger.
    """
    warehouse.ensure_plaid_tables()
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-fund-financials",
            source_native_id="sha-fund-financials",
            filename="1Q26 Unaudited Financials.pdf",
            # In an account folder, so the key guard cannot be what saves it:
            # only the declared scope can.
            original_path="fundadmin-example-fund-i-lp/1Q26 Unaudited Financials.pdf",
        ),
        extraction=_fund_level_financials(),
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert summary.documents_withheld_entity == 1
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(0,)]
    assert warehouse._query("SELECT count(*) FROM @finance_observations") == [(0,)]
    assert warehouse._query("SELECT count(*) FROM @finance_transactions") == [(0,)]
    # The extraction itself is untouched: the document remains a fact of
    # record, it is only never the owner's stock or flow.
    assert warehouse._query(
        "SELECT count(*) FROM @manual_finance_extractions WHERE status = 'ok'"
    ) == [(1,)]


def test_a_document_naming_an_institution_but_no_account_is_withheld(warehouse):
    """The corpus-root case, which needs no re-extraction to be safe.

    Nineteen private-fund files were uploaded to the root of the
    manual-finance corpus, so they had no account folder, and a fund statement
    prints no account mask. ``institution|mask`` therefore degenerated to
    ``<institution>|``, a bucket. Refusing it understates rather than fabricates -- and it is the
    only guard that works on a corpus extracted before v3.
    """
    warehouse.ensure_plaid_tables()
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-root",
            source_native_id="sha-root",
            filename="1Q26 Unaudited Financials.pdf",
            original_path="1Q26 Unaudited Financials.pdf",
        ),
        extraction=_fund_level_financials(
            content_sha256="sha-root",
            # Pre-v3: no scope was ever asked for, so the ledger cannot know.
            reporting_scope="",
            value_basis="",
        ),
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert summary.documents_withheld_unidentified == 1
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(0,)]
    assert warehouse._query("SELECT count(*) FROM @finance_observations") == [(0,)]


def test_net_worth_stays_the_owners_when_an_entity_statement_arrives(warehouse):
    """The plausibility assertion: one document must not move net worth 13x.

    Zach's take-home is about $118k a year, so a single new PDF adding
    $6.48M to net worth is not a market move, it is a modelling error. This
    seeds the real shape -- a personal balance beside a fund's own balance
    sheet -- and pins that net worth reports the personal one alone.
    """
    _seed_plaid(warehouse, [_plaid_account_row(current_balance=12636.04)])
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-fund-financials",
            source_native_id="sha-fund-financials",
            filename="1Q26 Unaudited Financials.pdf",
            original_path="fundadmin-example-fund-i-lp/1Q26 Unaudited Financials.pdf",
        ),
        extraction=_fund_level_financials(),
    )
    warehouse.insert_manual_finance_documents(
        [
            _document_row(
                content_sha256="sha-capital-account",
                source_native_id="sha-capital-account",
                filename="Capital Account Statements.pdf",
                original_path="fundadmin-example-fund-i-lp/Capital Account Statements.pdf",
            )
        ]
    )
    warehouse.insert_manual_finance_extractions(
        [
            _extraction_row(
                content_sha256="sha-capital-account",
                document_type="capital_account_statement",
                institution="Carta",
                account_name_hint="Example Fund I LP",
                account_mask="",
                reporting_scope="account_holder",
                account_holder="Zach Lata",
                value_basis="market",
                period_end=date(2026, 3, 31),
                closing_balance=Decimal("8140"),
                balances_json=[{"date": "2026-03-31", "balance": "8140"}],
            )
        ]
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    rows = warehouse._query(
        "SELECT name, value FROM @marts_finance_net_worth ORDER BY value DESC"
    )
    assert rows == [("Checking", Decimal("12636.04")), ("Example Fund I LP", Decimal("8140"))]
    (net_worth,) = warehouse._query("SELECT sum(signed_value) FROM @marts_finance_net_worth")[0]
    assert net_worth == Decimal("20776.04")


def test_tax_basis_capital_is_stored_but_never_summed_into_net_worth(warehouse):
    """A Schedule K-1 states TAX basis, which is not what the position is worth.

    K-1 tax-basis capital sat in net worth beside the same fund's NAV: the
    position was counted twice, and the second count was on an incompatible
    measure. The fact is still stored -- it is real, and it is what the IRS was
    told -- under its own observation kind.
    """
    warehouse.ensure_plaid_tables()
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-k1",
            source_native_id="sha-k1",
            filename="Schedule K-1.pdf",
            original_path="fundadmin-example-fund-i-lp/Schedule K-1.pdf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-k1",
            document_type="tax_form",
            institution="Example Fund I LP",
            account_name_hint="Example Fund I LP partnership interest",
            account_mask="",
            reporting_scope="account_holder",
            account_holder="Zach Lata",
            value_basis="tax",
            period_end=date(2025, 12, 31),
            closing_balance=Decimal("3275"),
            balances_json=[{"date": "2025-12-31", "balance": "3275"}],
            transactions_json=[
                {
                    "date": "2025-12-31",
                    "description": "Ordinary business income (loss)",
                    "amount": "220",
                    "direction": "out",
                    "security_name": "",
                    "ticker": "",
                    "cusip": "",
                    "quantity": "",
                    "price_per_share": "",
                    "trade_side": "",
                    "fees": "",
                }
            ],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert warehouse._query("SELECT kind, value FROM @finance_observations") == [
        ("tax_basis", Decimal("3275"))
    ]
    # No market value for the account, so it contributes no net-worth line at
    # all rather than a wrong one.
    assert warehouse._query("SELECT count(*) FROM @marts_finance_net_worth") == [(0,)]
    # A K-1's line items are allocated shares of partnership income, not money
    # that moved through the partner's account.
    assert warehouse._query("SELECT count(*) FROM @finance_transactions") == [(0,)]


def test_unfunded_commitment_is_recorded_and_kept_out_of_net_worth(warehouse):
    """A future capital call is a real obligation with nowhere to live before v3.

    It is deliberately not a liability: a commitment is contingent on the fund
    calling it, and booking it as debt would make net worth disagree with every
    statement. marts_finance.commitments is where it is answerable.
    """
    warehouse.ensure_plaid_tables()
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-capital-call",
            source_native_id="sha-capital-call",
            filename="capital-call.pdf",
            original_path="fundadmin-example-fund-i-lp/capital-call.pdf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-capital-call",
            document_type="capital_call_notice",
            institution="Carta",
            account_name_hint="Example Fund I LP",
            account_mask="",
            reporting_scope="account_holder",
            account_holder="Zach Lata",
            value_basis="market",
            period_end=date(2026, 4, 22),
            closing_balance=Decimal("0"),
            balances_json=[{"date": "2026-04-22", "balance": "12000"}],
            commitments_json=[
                {
                    "date": "2026-04-22",
                    "committed": "40000",
                    "called": "12000",
                    "unfunded": "28000",
                    "description": "Example Fund I LP",
                }
            ],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert warehouse._query(
        "SELECT kind, value FROM @finance_observations ORDER BY kind"
    ) == [
        ("balance", Decimal("12000")),
        ("called_capital", Decimal("12000")),
        ("commitment", Decimal("40000")),
        ("unfunded_commitment", Decimal("28000")),
    ]
    assert warehouse._query(
        "SELECT committed, called, unfunded FROM @marts_finance_commitments"
    ) == [(Decimal("40000"), Decimal("12000"), Decimal("28000"))]
    # Net worth reports what is owned today: the called capital, not the
    # commitment and not the unfunded balance.
    assert warehouse._query("SELECT sum(signed_value) FROM @marts_finance_net_worth") == [
        (Decimal("12000"),)
    ]


def test_a_link_whose_documents_are_gone_is_reconciled_away(warehouse):
    """Re-resolution cannot reach a group that no longer exists.

    7adf12e made document links re-resolve every run so a frozen decision
    cannot survive. That only walks groups that still exist -- so when a
    group's documents are withheld, moved into a folder, or deleted, its link
    stayed behind, kept its ledger account above ``prune_unlinked_finance_accounts``
    (which only reaches accounts with zero links), and the catch-all account
    lived on with no document claiming it.
    """
    warehouse.ensure_plaid_tables()
    _seed_document(
        warehouse,
        document=_document_row(
            content_sha256="sha-root",
            source_native_id="sha-root",
            filename="fund-report.pdf",
            original_path="fund-report.pdf",
        ),
        extraction=_extraction_row(
            content_sha256="sha-root",
            document_type="fund_positions",
            institution="",  # no institution: keys on the filename stem
            account_name_hint="Example Fund",
            account_mask="",
            balances_json=[{"date": "2026-03-31", "balance": "1000"}],
        ),
    )
    FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert warehouse._query("SELECT count(*) FROM @finance_account_links") == [(1,)]
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(1,)]

    # The same document, now filed under an institution with no mask: its key
    # becomes unidentifiable and the group disappears.
    warehouse.insert_manual_finance_extractions(
        [
            _extraction_row(
                content_sha256="sha-root",
                ai_prompt_version="manual-finance-agent-v3",
                document_type="fund_positions",
                institution="Carta",
                account_name_hint="Example Fund",
                account_mask="",
                balances_json=[{"date": "2026-03-31", "balance": "1000"}],
                created_at=datetime(2026, 7, 13, 13, 0, tzinfo=UTC),
            )
        ]
    )
    summary = FinanceLedgerRunner(
        warehouse=warehouse, now=datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    ).sync()

    assert summary.links_removed == 1
    assert summary.accounts_pruned == 1
    assert warehouse._query("SELECT count(*) FROM @finance_accounts") == [(0,)]
    assert warehouse._query("SELECT count(*) FROM @finance_observations") == [(0,)]


def test_ledger_and_extraction_contract_agree_on_scope_tokens():
    """The ledger repeats the extraction contract's enums rather than importing
    them, to stay free of the extraction runner's agent/Docker dependencies.
    A silent divergence would make every entity document book again."""
    from personal_data_warehouse import finance_ledger as ledger
    from personal_data_warehouse import manual_finance_extraction as contract

    assert REPORTING_SCOPE_ENTITY == contract.REPORTING_SCOPE_ENTITY
    assert VALUE_BASIS_TAX == contract.VALUE_BASIS_TAX
    for name in (
        "VALUATION_MEASURE_POSITION",
        "VALUATION_MEASURE_COST_BASIS",
        "VALUATION_MEASURE_REFERENCE",
        "VALUATION_MEASURE_UNKNOWN",
    ):
        assert getattr(ledger, name) == getattr(contract, name), name


def test_every_non_value_observation_kind_is_excluded_from_net_worth(warehouse):
    """The ledger stores facts and derives status at read time, so a kind that
    is not a current value must be filtered by every reader of a VALUE. The
    SQL list lives in postgres.py and cannot import the Python one; this is
    what keeps them the same list."""
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts(
        [
            {
                "account_id": "fa_test",
                "account": "z@x.test",
                "name": "Fund",
                "kind": "private_fund",
                "side": "asset",
                "currency": "USD",
                "institution": "Carta",
                "mask": "",
                "created_at": _TS,
                "updated_at": _TS,
                "sync_version": 1,
            }
        ]
    )
    warehouse.insert_finance_observations(
        [
            {
                "account_id": "fa_test",
                "as_of": date(2026, 6, 1),
                "kind": "valuation",
                "value": Decimal("100"),
                "currency": "USD",
                "source": "manual_finance",
                "observed_at": _TS,
                "sync_version": 1,
            }
        ]
        + [
            {
                "account_id": "fa_test",
                # Newer than the valuation on purpose: net worth takes the
                # LATEST observation, so a non-value kind that is not filtered
                # wins outright rather than merely being added.
                "as_of": date(2026, 7, 1),
                "kind": kind,
                "value": Decimal("999999"),
                "currency": "USD",
                "source": "manual_finance",
                "observed_at": _TS,
                "sync_version": 1,
            }
            for kind in NON_VALUE_OBSERVATION_KINDS
        ]
    )
    assert warehouse._query(
        "SELECT observation_kind, value FROM @marts_finance_net_worth"
    ) == [("valuation", Decimal("100"))]
    assert warehouse._query(
        "SELECT net_worth FROM @marts_finance_net_worth_history ORDER BY day DESC LIMIT 1"
    ) == [(Decimal("100"),)]
    assert warehouse._query(
        "SELECT latest_observation_kind, latest_value FROM @marts_finance_accounts"
    ) == [("valuation", Decimal("100"))]


# --- a contractual figure is not the holder's position value ----------------------


def test_a_valuation_cap_is_never_the_holders_position_value():
    """A SAFE's post-money valuation cap is a ceiling on the ISSUER.

    It is the biggest and most prominent number on the page, so every
    "primary figure first" heuristic picks it — and the document is
    unambiguously the investor's own, so `reporting_scope` cannot help. Only
    the entry's own `measure` can.
    """
    assert _daily_valuations(
        [{"date": "2026-08-27", "value": "25000000", "description": "Post-Money Valuation Cap",
          "measure": "reference"}]
    ) == []
    # A carrying value beats a cost basis on the same day...
    assert _daily_valuations(
        [
            {"date": "2026-08-27", "value": "2000", "description": "Cost basis", "measure": "cost_basis"},
            {"date": "2026-08-27", "value": "2400", "description": "Carrying value",
             "measure": "position_value"},
            {"date": "2026-08-27", "value": "25000000", "description": "Valuation cap",
             "measure": "reference"},
        ]
    ) == [(date(2026, 8, 27), Decimal("2400"))]
    # ...but a document stating ONLY a cost basis still produces a value: an
    # angel SAFE really is carried at cost.
    assert _daily_valuations(
        [{"date": "2026-08-27", "value": "2000", "description": "Cost basis", "measure": "cost_basis"}]
    ) == [(date(2026, 8, 27), Decimal("2000"))]
    # Pre-v3 entries carry no measure and behave exactly as they always did,
    # so re-extraction is what improves the corpus, never a silent regression.
    assert _daily_valuations(
        [{"date": "2026-08-27", "value": "468000", "description": "Estimate"}]
    ) == [(date(2026, 8, 27), Decimal("468000"))]


def test_a_reference_figure_in_one_document_never_outvotes_the_position_in_another(warehouse):
    """The live 2026-08-28 shape: one folder, three documents, one account.

    An angel investment folder holds a position record (cost basis $2,000), the
    executed SAFE (whose only valuation is the $25,000,000 cap) and the
    company's wire instructions. All three share the folder, so all three map to
    one account key, and the SAFE sorts LAST by content sha — which under plain
    last-write-wins made the cap the account's value and would have moved net
    worth by 12,500x.
    """
    warehouse.ensure_plaid_tables()
    warehouse.ensure_manual_finance_tables()
    warehouse.insert_manual_finance_documents(
        [
            _document_row(
                content_sha256=sha,
                source_native_id=sha,
                filename=name,
                original_path=f"example-angel-inc/{name}",
            )
            for sha, name in (
                ("0036aaaa", "2026-08-27-investment-record.rtf"),
                ("d752bbbb", "2026-08-27-wire-instructions.pdf"),
                ("ec72cccc", "2026-08-27-safe-executed.pdf"),
            )
        ]
    )
    warehouse.insert_manual_finance_extractions(
        [
            _extraction_row(
                content_sha256="0036aaaa",
                document_type="other",
                institution="Example Angel Inc.",
                account_name_hint="Private investment position in Example Angel Inc.",
                account_mask="",
                closing_balance=Decimal("0"),
                valuations_json=[
                    {"date": "2026-08-27", "value": "2000.00", "description": "Cost basis",
                     "measure": "cost_basis"},
                ],
            ),
            _extraction_row(
                content_sha256="d752bbbb",
                document_type="other",
                institution="Example Bank",
                account_name_hint="Example Angel Inc. — Checking",
                # The COMPANY's bank account number, on a document filed in the
                # investor's folder. It must not become the investor's identity.
                account_mask="1482",
                closing_balance=Decimal("0"),
                valuations_json=[],
            ),
            _extraction_row(
                content_sha256="ec72cccc",
                document_type="other",
                institution="Example Angel Inc.",
                account_name_hint="Example Angel Inc. SAFE",
                account_mask="",
                closing_balance=Decimal("0"),
                valuations_json=[
                    {"date": "2026-08-27", "value": "25000000",
                     "description": "Post-Money Valuation Cap", "measure": "reference"},
                ],
            ),
        ]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    # The cap is dropped, so there is no conflict left to refuse and the
    # account carries the real position.
    assert summary.observation_conflicts == 0
    assert warehouse._query(
        "SELECT kind, value FROM @finance_observations ORDER BY kind"
    ) == [("valuation", Decimal("2000.00"))]
    assert warehouse._query("SELECT sum(signed_value) FROM @marts_finance_net_worth") == [
        (Decimal("2000.00"),)
    ]
    assert warehouse._query(
        "SELECT count(*) FROM @finance_observations WHERE value > 1000000"
    ) == [(0,)]


def test_two_documents_disagreeing_about_one_account_day_book_neither(warehouse):
    """The deterministic backstop, which needs no re-extraction.

    This is the live 2026-08-28 corpus exactly as it stood: v2 extractions with
    no `measure`, so the cap looks like an ordinary valuation. Silent
    last-write-wins is the mechanism behind BOTH incidents — it replaced a real
    capital balance with a fund's members' equity, and it was about to replace
    an angel position with that SAFE's valuation cap. Refusing understates by
    one day; guessing is how a wrong number gets quoted.
    """
    warehouse.ensure_plaid_tables()
    warehouse.ensure_manual_finance_tables()
    warehouse.insert_manual_finance_documents(
        [
            _document_row(content_sha256=sha, source_native_id=sha, filename=f"{sha}.pdf",
                          original_path=f"example-angel-inc/{sha}.pdf")
            for sha in ("0036aaaa", "ec72cccc")
        ]
    )
    warehouse.insert_manual_finance_extractions(
        [
            _extraction_row(
                content_sha256="0036aaaa",
                institution="Example Angel Inc.",
                account_name_hint="Private investment position",
                account_mask="",
                closing_balance=Decimal("0"),
                # No `measure` anywhere: this is what a v2 row looks like.
                valuations_json=[{"date": "2026-08-27", "value": "2000.00",
                                  "description": "Cost basis"}],
            ),
            _extraction_row(
                content_sha256="ec72cccc",
                institution="Example Angel Inc.",
                account_name_hint="Example Angel Inc. SAFE",
                account_mask="",
                closing_balance=Decimal("0"),
                valuations_json=[{"date": "2026-08-27", "value": "25000000",
                                  "description": "Post-Money Valuation Cap"}],
            ),
        ]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()

    assert summary.observation_conflicts == 1
    # NEITHER claim is booked — not the later one, and not the earlier one,
    # because keeping the earlier is the same coin-flip by a different name.
    assert warehouse._query("SELECT count(*) FROM @finance_observations") == [(0,)]
    assert warehouse._query("SELECT count(*) FROM @marts_finance_net_worth") == [(0,)]


def test_one_document_restating_a_running_balance_is_not_a_conflict(warehouse):
    """A credit-card statement prints several running balances for one day.

    Measured on the production corpus 2026-08-28, one statement claimed five
    different balances for a single date. That is the document restating
    itself, not two sources disagreeing, so the refusal must be scoped to
    CROSS-document — otherwise the guard deletes real balance history.
    """
    warehouse.ensure_plaid_tables()
    _seed_document(
        warehouse,
        document=_document_row(content_sha256="sha-card", source_native_id="sha-card",
                               original_path="example-card-4242/statement.pdf"),
        extraction=_extraction_row(
            content_sha256="sha-card",
            document_type="credit_card_statement",
            institution="Example Card",
            account_mask="4242",
            balances_json=[
                {"date": "2026-02-01", "balance": "6753.81"},
                {"date": "2026-02-01", "balance": "3571.31"},
                {"date": "2026-02-01", "balance": "668.99"},
            ],
        ),
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.observation_conflicts == 0
    assert warehouse._query("SELECT value FROM @finance_observations") == [(Decimal("668.99"),)]


def test_a_rounding_difference_between_two_documents_is_not_a_conflict(warehouse):
    """The tolerance is measured, not guessed.

    Over the whole production corpus the only genuine cross-document
    disagreement other than the two incidents is a $0.00-vs-$0.51 rounding
    difference on a 2018 brokerage statement. Refusing that would cost real
    history for nothing.
    """
    warehouse.ensure_plaid_tables()
    warehouse.ensure_manual_finance_tables()
    warehouse.insert_manual_finance_documents(
        [
            _document_row(content_sha256=sha, source_native_id=sha, filename=f"{sha}.pdf",
                          original_path=f"example-brokerage-5270/{sha}.pdf")
            for sha in ("c716f1ee", "de2f9358")
        ]
    )
    warehouse.insert_manual_finance_extractions(
        [
            _extraction_row(content_sha256="c716f1ee", account_mask="5270",
                            institution="Example Brokerage",
                            balances_json=[{"date": "2018-11-30", "balance": "0.00"}]),
            _extraction_row(content_sha256="de2f9358", account_mask="5270",
                            institution="Example Brokerage",
                            balances_json=[{"date": "2018-11-30", "balance": "0.51"}]),
        ]
    )
    summary = FinanceLedgerRunner(warehouse=warehouse, now=_TS).sync()
    assert summary.observation_conflicts == 0
    assert warehouse._query("SELECT value FROM @finance_observations") == [(Decimal("0.51"),)]
