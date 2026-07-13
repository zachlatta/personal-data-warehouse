from __future__ import annotations

import os
from datetime import UTC, datetime

import psycopg2
import pytest
from dotenv import load_dotenv

from tests.conftest import make_test_schema

from personal_data_warehouse.postgres import POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.postgres_readonly import PostgresReadOnlyRunner
from personal_data_warehouse.relations import query_relation


def test_plaid_relations_use_plaid_source_schema_and_private_tokens() -> None:
    assert query_relation("plaid_accounts").schema == "plaid"
    assert query_relation("plaid_accounts").name == "accounts"
    assert query_relation("plaid_transactions").schema == "plaid"
    assert query_relation("plaid_item_tokens").schema == "private"


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
        for schema_name in wh.physical_schema_names(include_private=True) + [schema]:
            wh._raw_command(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        wh.close()


def _relation_exists(warehouse: PostgresWarehouse, logical_name: str) -> bool:
    rel = query_relation(logical_name).with_namespace(warehouse.schema_namespace)
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

    for view_name in (
        "finance_accounts",
        "finance_transactions",
        "finance_investment_holdings",
        "finance_investment_transactions",
        "finance_liabilities",
    ):
        assert _view_exists(warehouse, "marts", view_name), view_name


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
