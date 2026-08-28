from __future__ import annotations

import os
from datetime import UTC, datetime

import psycopg2
import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.postgres_readonly import PostgresReadOnlyRunner
from personal_data_warehouse.relations import relation


def test_plaid_relations_use_plaid_source_schema_and_private_tokens() -> None:
    assert relation("plaid_accounts").schema == "base_plaid"
    assert relation("plaid_accounts").name == "accounts"
    assert relation("plaid_transactions").schema == "base_plaid"
    assert relation("plaid_item_tokens").schema == "private"


def test_plaid_table_specs_define_idempotent_upsert_keys() -> None:
    assert POSTGRES_TABLES["plaid_items"].primary_key == ("account", "item_id")
    assert POSTGRES_TABLES["plaid_accounts"].primary_key == ("account", "account_id")
    assert POSTGRES_TABLES["plaid_transactions"].primary_key == ("account", "transaction_id")
    assert POSTGRES_TABLES["plaid_investment_holdings"].primary_key == ("account", "account_id", "security_id")
    assert POSTGRES_TABLES["plaid_sync_state"].primary_key == ("account", "item_id", "product")


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = make_test_schema("plaid")
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def _relation_exists(warehouse: PostgresWarehouse, logical_name: str) -> bool:
    rel = relation(logical_name).with_namespace(warehouse.schema_namespace)
    rows = warehouse._query(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (rel.schema, rel.name),
    )
    return bool(rows)


def _view_exists(warehouse: PostgresWarehouse, schema: str, name: str) -> bool:
    rows = warehouse._query(
        """
        SELECT 1
        FROM information_schema.views
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (warehouse.physical_schema_name(schema), name),
    )
    return bool(rows)


def test_ensure_plaid_tables_creates_raw_private_and_finance_mart_views(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_plaid_tables()

    for logical_name in (
        "plaid_items",
        "plaid_accounts",
        "plaid_transactions",
        "plaid_investment_securities",
        "plaid_investment_holdings",
        "plaid_investment_transactions",
        "plaid_liabilities",
        "plaid_sync_state",
        "plaid_item_tokens",
    ):
        assert _relation_exists(warehouse, logical_name), logical_name

    for view_name in ("investment_holdings", "investment_transactions", "liabilities"):
        assert _view_exists(warehouse, "marts_finance", view_name), view_name

    # marts_finance.accounts / marts_finance.transactions are ledger views owned
    # by ensure_finance_tables now (they read derived_finance.*, not base_plaid.*).
    warehouse.ensure_finance_tables()
    for view_name in ("accounts", "transactions"):
        assert _view_exists(warehouse, "marts_finance", view_name), view_name


def test_plaid_query_role_can_read_source_tables_but_not_private_tokens(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_plaid_tables()
    connection = psycopg2.connect(_postgres_url())
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'SET LOCAL ROLE "{warehouse.query_role}"')
            cursor.execute(f'SELECT count(*) FROM {warehouse.sql_relation("plaid_accounts")}')
            assert cursor.fetchone() == (0,)
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cursor.execute(f'SELECT access_token FROM {warehouse.sql_relation("plaid_item_tokens")}')
    finally:
        connection.rollback()
        connection.close()


def test_python_readonly_runner_cannot_read_private_plaid_tokens(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_plaid_tables()
    runner = PostgresReadOnlyRunner(warehouse)
    try:
        assert runner.query(
            f'SELECT count(*) AS count FROM {warehouse.sql_relation("plaid_accounts")}',
            max_rows=1,
        ).rows == [{"count": 0}]
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            runner.query(
                f'SELECT access_token FROM {warehouse.sql_relation("plaid_item_tokens")}',
                max_rows=1,
            )
    finally:
        runner.close()


def test_plaid_rows_and_unrelated_root_finance_relations_both_survive_ensure(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_plaid_tables()
    warehouse.upsert_plaid_item_token(
        account="zach@example.com",
        item_id="item-1",
        access_token="access-token",
        institution_id="ins_1",
        institution_name="Example Bank",
        linked_at=datetime(2026, 7, 9, 12, tzinfo=UTC),
    )
    warehouse.insert_plaid_accounts(
        [
            {
                "account": "zach@example.com",
                "item_id": "item-1",
                "account_id": "acc-1",
                "name": "Checking",
                "official_name": "Plaid Checking",
                "mask": "0000",
                "type": "depository",
                "subtype": "checking",
                "available_balance": 100.0,
                "current_balance": 110.0,
                "limit_balance": 0.0,
                "iso_currency_code": "USD",
                "unofficial_currency_code": "",
                "is_removed": 0,
                "raw_json": {"account_id": "acc-1"},
                "synced_at": datetime(2026, 7, 9, 12, tzinfo=UTC),
                "sync_version": 1,
            }
        ]
    )

    # A generic ensure_tables call must preserve both the canonical Plaid
    # source schema and unrelated root-level finance relations.
    legacy_table_sql = f'CREATE TABLE IF NOT EXISTS "{warehouse.schema_namespace}"."finance_accounts" (id text PRIMARY KEY)'
    warehouse._raw_command(legacy_table_sql)
    warehouse.ensure_tables()

    tokens = warehouse.load_plaid_item_tokens()
    assert len(tokens) == 1
    assert tokens[0].access_token == "access-token"
    assert _relation_exists(warehouse, "plaid_accounts")
    legacy_rows = warehouse._query(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = 'finance_accounts'
        LIMIT 1
        """,
        (warehouse.schema_namespace,),
    )
    assert legacy_rows == [(1,)]


def test_plaid_snapshot_reconciliation_tombstones_accounts_and_deletes_absent_rows(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_plaid_tables()
    now = datetime(2026, 7, 9, 12, tzinfo=UTC)

    def account_row(account_id: str) -> dict:
        return {
            "account": "zach@example.com",
            "item_id": "item-1",
            "account_id": account_id,
            "name": "Account",
            "official_name": "Account",
            "mask": "0000",
            "type": "brokerage",
            "subtype": "brokerage",
            "available_balance": 0.0,
            "current_balance": 0.0,
            "limit_balance": 0.0,
            "iso_currency_code": "USD",
            "unofficial_currency_code": "",
            "is_removed": 0,
            "raw_json": {"account_id": account_id, "balances": {}},
            "synced_at": now,
            "sync_version": 1,
        }

    def holding_row(account_id: str, security_id: str) -> dict:
        return {
            "account": "zach@example.com",
            "item_id": "item-1",
            "account_id": account_id,
            "security_id": security_id,
            "quantity": 1.0,
            "institution_value": 1.0,
            "institution_price": 1.0,
            "institution_price_as_of": now,
            "cost_basis": 1.0,
            "iso_currency_code": "USD",
            "unofficial_currency_code": "",
            "raw_json": {},
            "synced_at": now,
            "sync_version": 1,
        }

    def liability_row(account_id: str, liability_type: str) -> dict:
        return {
            "account": "zach@example.com",
            "item_id": "item-1",
            "account_id": account_id,
            "liability_type": liability_type,
            "last_payment_amount": 0.0,
            "last_statement_balance": 0.0,
            "minimum_payment_amount": 0.0,
            "next_payment_due_at": now,
            "origination_principal_amount": 0.0,
            "outstanding_interest_amount": 0.0,
            "is_overdue": 0,
            "iso_currency_code": "USD",
            "unofficial_currency_code": "",
            "raw_json": {},
            "synced_at": now,
            "sync_version": 1,
        }

    warehouse.insert_plaid_accounts([account_row("active"), account_row("closed")])
    warehouse.insert_plaid_investment_holdings(
        [holding_row("active", "held"), holding_row("active", "sold")]
    )
    warehouse.insert_plaid_liabilities(
        [liability_row("active", "credit"), liability_row("active", "student")]
    )

    assert warehouse.mark_missing_plaid_accounts_removed(
        account="zach@example.com",
        item_id="item-1",
        active_account_ids={"active"},
        synced_at=now,
    ) == 1
    assert warehouse.delete_missing_plaid_investment_holdings(
        account="zach@example.com",
        item_id="item-1",
        active_holding_keys={("active", "held")},
    ) == 1
    assert warehouse.delete_missing_plaid_liabilities(
        account="zach@example.com",
        item_id="item-1",
        active_liability_keys={("active", "credit")},
    ) == 1

    assert warehouse._query(
        f"SELECT account_id, is_removed FROM {warehouse.sql_relation('plaid_accounts')} ORDER BY account_id"
    ) == [("active", 0), ("closed", 1)]
    assert warehouse._query(
        f"SELECT account_id, security_id FROM {warehouse.sql_relation('plaid_investment_holdings')}"
    ) == [("active", "held")]
    assert warehouse._query(
        f"SELECT account_id, liability_type FROM {warehouse.sql_relation('plaid_liabilities')}"
    ) == [("active", "credit")]


def _seed_item(warehouse: PostgresWarehouse, item_id: str, *, account_id: str, transaction_id: str) -> None:
    now = datetime(2026, 7, 25, 12, tzinfo=UTC)
    warehouse.upsert_plaid_item_token(
        account="zach@example.com",
        item_id=item_id,
        access_token=f"access-{item_id}",
        institution_id="ins_1",
        institution_name="Example Bank",
        linked_at=now,
    )
    warehouse.insert_plaid_items(
        [
            {
                "account": "zach@example.com",
                "item_id": item_id,
                "institution_id": "ins_1",
                "institution_name": "Example Bank",
                "available_products": [],
                "billed_products": [],
                "webhook": "",
                "consent_expiration_time": now,
                "error_json": {},
                "raw_json": {},
                "linked_at": now,
                "synced_at": now,
                "sync_version": 1,
            }
        ]
    )
    warehouse.insert_plaid_accounts(
        [
            {
                "account": "zach@example.com",
                "item_id": item_id,
                "account_id": account_id,
                "name": "Rewards Card",
                "official_name": "Rewards Card",
                "mask": "4242",
                "type": "credit",
                "subtype": "credit card",
                "available_balance": 0.0,
                "current_balance": 10.0,
                "limit_balance": 0.0,
                "iso_currency_code": "USD",
                "unofficial_currency_code": "",
                "is_removed": 0,
                "raw_json": {},
                "synced_at": now,
                "sync_version": 1,
            }
        ]
    )
    warehouse.insert_plaid_transactions(
        [
            {
                "account": "zach@example.com",
                "item_id": item_id,
                "account_id": account_id,
                "transaction_id": transaction_id,
                "posted_at": now,
                "authorized_at": now,
                "name": "COFFEE",
                "merchant_name": "Coffee",
                "amount": 4.5,
                "iso_currency_code": "USD",
                "unofficial_currency_code": "",
                "category_json": [],
                "payment_channel": "in store",
                "pending": 0,
                "pending_transaction_id": "",
                "is_removed": 0,
                "raw_json": {},
                "synced_at": now,
                "sync_version": 1,
            }
        ]
    )
    warehouse.insert_plaid_sync_state(
        account="zach@example.com",
        item_id=item_id,
        product="transactions",
        cursor="cursor-1",
        status="ok",
        last_synced_at=now,
        updated_at=now,
    )


def test_delete_plaid_item_removes_every_item_scoped_row_and_spares_other_items(
    warehouse: PostgresWarehouse,
) -> None:
    """Retiring a duplicate Item must take its rows and nothing else.

    A re-link that mints a new item_id leaves the old Item's accounts and
    transactions behind, double-counting both net worth and the transaction
    overlap; `pdw ingest plaid unlink` deletes exactly that Item's rows.
    """
    warehouse.ensure_plaid_tables()
    _seed_item(warehouse, "item-old", account_id="acc-old", transaction_id="tx-old")
    _seed_item(warehouse, "item-new", account_id="acc-new", transaction_id="tx-new")

    counts = warehouse.count_plaid_item_rows(account="zach@example.com", item_id="item-old")
    assert counts == {
        "plaid_item_tokens": 1,
        "plaid_items": 1,
        "plaid_accounts": 1,
        "plaid_transactions": 1,
        "plaid_investment_holdings": 0,
        "plaid_investment_transactions": 0,
        "plaid_liabilities": 0,
        "plaid_sync_state": 1,
    }
    assert warehouse.delete_plaid_item(account="zach@example.com", item_id="item-old") == counts

    assert [item.item_id for item in warehouse.load_plaid_item_tokens()] == ["item-new"]
    for logical_name, column in (
        ("plaid_items", "item_id"),
        ("plaid_accounts", "account_id"),
        ("plaid_transactions", "transaction_id"),
    ):
        rows = warehouse._query(f"SELECT {column} FROM {warehouse.sql_relation(logical_name)}")
        assert [row[0] for row in rows] == [
            {"plaid_items": "item-new", "plaid_accounts": "acc-new", "plaid_transactions": "tx-new"}[
                logical_name
            ]
        ], logical_name
    assert warehouse._query(
        f"SELECT item_id FROM {warehouse.sql_relation('plaid_sync_state')}"
    ) == [("item-new",)]
    # Deleting an already-retired item is a no-op, not an error.
    assert warehouse.delete_plaid_item(account="zach@example.com", item_id="item-old") == {
        table: 0 for table in counts
    }


# --- marts_ops.plaid_item_health ----------------------------------------------


def _item_row(item_id: str, *, now: datetime, error: dict | None = None) -> dict:
    return {
        "account": "zach@example.com",
        "item_id": item_id,
        "institution_id": "ins_1",
        "institution_name": "Example Bank",
        "available_products": [],
        "billed_products": [],
        "webhook": "",
        "consent_expiration_time": now,
        "error_json": error or {},
        "raw_json": {},
        "linked_at": now,
        "synced_at": now,
        "sync_version": 1,
    }


def _plaid_account_row(item_id: str, account_id: str, name: str, *, now: datetime) -> dict:
    return {
        "account": "zach@example.com",
        "item_id": item_id,
        "account_id": account_id,
        "name": name,
        "official_name": name,
        "mask": "4242",
        "type": "credit",
        "subtype": "credit card",
        "available_balance": 0.0,
        "current_balance": 10.0,
        "limit_balance": 0.0,
        "iso_currency_code": "USD",
        "unofficial_currency_code": "",
        "is_removed": 0,
        "raw_json": {},
        "synced_at": now,
        "sync_version": 1,
    }


def test_plaid_item_health_names_the_broken_item(warehouse: PostgresWarehouse) -> None:
    """A broken Item must be identifiable, not just counted.

    ``base_plaid.items.error_json`` was written by every sync and read by
    nothing. The only visible trace of a dead Item was the 'action_required'
    tally on marts_ops.pipeline_health, which says how many need attention but
    never which — so a Capital One Item sat in ITEM_ERROR / NO_ACCOUNTS with
    its card frozen and no way to see it short of querying the raw column by
    hand. The run stays green on purpose (failing it once produced 262
    consecutive failed runs), so this row is the compensating signal.
    """
    warehouse.ensure_plaid_tables()
    now = datetime(2026, 8, 16, 5, 0, tzinfo=UTC)
    warehouse.insert_plaid_items(
        [
            _item_row("item-ok", now=now),
            _item_row(
                "item-broken",
                now=now,
                error={
                    "error_code": "NO_ACCOUNTS",
                    "error_type": "ITEM_ERROR",
                    "error_message": "no valid accounts were found for this item",
                },
            ),
        ]
    )
    checking = _plaid_account_row("item-ok", "acc-ok", "Checking", now=now)
    checking["mask"] = "1111"
    checking["type"] = "depository"
    warehouse.insert_plaid_accounts(
        [
            checking,
            _plaid_account_row("item-broken", "acc-broken", "Venture X", now=now),
        ]
    )
    rows = warehouse._query(
        """
        SELECT item_id, status, error_code, error_type, account_names, account_count
        FROM @marts_ops_plaid_item_health
        ORDER BY item_id
        """
    )
    assert rows == [
        ("item-broken", "action_required", "NO_ACCOUNTS", "ITEM_ERROR", "Venture X", 1),
        ("item-ok", "ok", "", "", "Checking", 1),
    ]


def test_plaid_item_health_reports_the_frozen_accounts_last_transaction(
    warehouse: PostgresWarehouse,
) -> None:
    """The Item's error and its data going quiet belong on the same row.

    Knowing an Item is broken is only half the answer; the operational question
    is how much data has been missed since, which is the age of the newest
    transaction across the accounts that Item feeds.
    """
    warehouse.ensure_plaid_tables()
    now = datetime(2026, 8, 16, 5, 0, tzinfo=UTC)
    frozen_at = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    warehouse.insert_plaid_items([_item_row("item-1", now=now)])
    warehouse.insert_plaid_accounts(
        [_plaid_account_row("item-1", "acc-1", "Venture X", now=now)]
    )
    warehouse.insert_plaid_transactions(
        [
            {
                "account": "zach@example.com",
                "item_id": "item-1",
                "account_id": "acc-1",
                "transaction_id": "txn-1",
                "posted_at": frozen_at,
                "authorized_at": frozen_at,
                "name": "COFFEE",
                "merchant_name": "Coffee",
                "amount": 4.5,
                "iso_currency_code": "USD",
                "unofficial_currency_code": "",
                "category_json": [],
                "payment_channel": "in store",
                "pending": 0,
                "pending_transaction_id": "",
                "is_removed": 0,
                "raw_json": {},
                "synced_at": now,
                "sync_version": 1,
            }
        ]
    )
    rows = warehouse._query(
        "SELECT newest_transaction_at FROM @marts_ops_plaid_item_health WHERE item_id = %s",
        ("item-1",),
    )
    assert rows == [(frozen_at,)]


def test_plaid_item_health_covers_an_item_with_no_accounts(
    warehouse: PostgresWarehouse,
) -> None:
    """NO_ACCOUNTS means exactly that, so the join must not drop the row.

    An inner join here would hide the single most important case: the Item
    whose accounts Plaid can no longer see.
    """
    warehouse.ensure_plaid_tables()
    now = datetime(2026, 8, 16, 5, 0, tzinfo=UTC)
    warehouse.insert_plaid_items(
        [_item_row("item-empty", now=now, error={"error_code": "NO_ACCOUNTS"})]
    )
    rows = warehouse._query(
        """
        SELECT item_id, status, account_count, account_names, newest_transaction_at
        FROM @marts_ops_plaid_item_health
        """
    )
    assert rows == [("item-empty", "action_required", 0, "", None)]


def test_plaid_item_health_flags_two_live_items_over_the_same_accounts(
    warehouse: PostgresWarehouse,
) -> None:
    """A re-link that minted a second Item double-counts every balance.

    Measured 2026-08-28: two Capital One Items, both ``ok``, both syncing,
    both carrying the same two cards by mask, and marts_finance.net_worth
    held two rows per mask -- while every health surface read green. An
    Item is only healthy if it is also the ONLY live Item for its accounts;
    the older one (by newest transaction) is the one to retire.
    """
    warehouse.ensure_plaid_tables()
    now = datetime(2026, 8, 28, 2, 0, tzinfo=UTC)
    warehouse.insert_plaid_items(
        [
            _item_row("item-old", now=now),
            _item_row("item-new", now=now),
            _item_row("item-other", now=now),
        ]
    )
    other = _plaid_account_row("item-other", "acc-other", "Checking", now=now)
    other["mask"] = "1111"
    other["type"] = "depository"
    warehouse.insert_plaid_accounts(
        [
            _plaid_account_row("item-old", "acc-old", "Venture X", now=now),
            _plaid_account_row("item-new", "acc-new", "Venture X", now=now),
            other,
        ]
    )
    rows = warehouse._query(
        """
        SELECT item_id, status, duplicate_item_ids
        FROM @marts_ops_plaid_item_health
        ORDER BY item_id
        """
    )
    assert rows == [
        ("item-new", "duplicate", ["item-old"]),
        ("item-old", "duplicate", ["item-new"]),
        ("item-other", "ok", []),
    ]


def test_plaid_item_health_ignores_removed_accounts_when_looking_for_duplicates(
    warehouse: PostgresWarehouse,
) -> None:
    """A retired Item's tombstoned accounts must not haunt the survivor."""
    warehouse.ensure_plaid_tables()
    now = datetime(2026, 8, 28, 2, 0, tzinfo=UTC)
    warehouse.insert_plaid_items([_item_row("item-a", now=now), _item_row("item-b", now=now)])
    removed = _plaid_account_row("item-b", "acc-b", "Venture X", now=now)
    removed["is_removed"] = 1
    warehouse.insert_plaid_accounts(
        [_plaid_account_row("item-a", "acc-a", "Venture X", now=now), removed]
    )
    rows = warehouse._query(
        "SELECT item_id, status FROM @marts_ops_plaid_item_health ORDER BY item_id"
    )
    assert rows == [("item-a", "ok"), ("item-b", "ok")]
