"""Warehouse schema + marts contract for the finance ledger.

The ledger is the derived stocks-and-flows layer: logical accounts
(finance.accounts + finance.account_links, photos-identity pattern) and
append-only point-in-time observations (finance.observations). Net worth is
read through marts.finance_net_worth / marts.finance_net_worth_history.
Money columns are NUMERIC and observation days are DATE — never floats or
timestamps.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.relations import DERIVED_SCHEMAS, relation
from personal_data_warehouse.schema import (
    FINANCE_ACCOUNT_COLUMNS,
    FINANCE_ACCOUNT_LINK_COLUMNS,
    FINANCE_OBSERVATION_COLUMNS,
)


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


_TS = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


def _account_row(**overrides) -> dict:
    row = {
        "account_id": "fa_1",
        "account": "z@x.test",
        "name": "Checking ...0001",
        "kind": "checking",
        "side": "asset",
        "currency": "USD",
        "institution": "Acme Bank",
        "mask": "0001",
        "created_at": _TS,
        "updated_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _link_row(**overrides) -> dict:
    row = {
        "source": "plaid",
        "account": "z@x.test",
        "source_account_key": "acc-1",
        "account_id": "fa_1",
        "match_method": "source_id",
        "match_score": 1.0,
        "created_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _observation_row(**overrides) -> dict:
    row = {
        "account_id": "fa_1",
        "as_of": date(2026, 7, 1),
        "kind": "balance",
        "value": Decimal("123.45"),
        "currency": "USD",
        "source": "plaid",
        "observed_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


# --- pure registry contracts ---------------------------------------------------


def test_finance_relations_are_registered():
    assert "finance" in DERIVED_SCHEMAS
    assert (relation("finance_accounts").schema, relation("finance_accounts").name) == ("finance", "accounts")
    assert (relation("finance_account_links").schema, relation("finance_account_links").name) == (
        "finance",
        "account_links",
    )
    assert (relation("finance_observations").schema, relation("finance_observations").name) == (
        "finance",
        "observations",
    )


def test_finance_table_specs():
    assert POSTGRES_TABLES["finance_accounts"].columns == FINANCE_ACCOUNT_COLUMNS
    assert POSTGRES_TABLES["finance_accounts"].primary_key == ("account_id",)
    assert POSTGRES_TABLES["finance_account_links"].columns == FINANCE_ACCOUNT_LINK_COLUMNS
    assert POSTGRES_TABLES["finance_account_links"].primary_key == (
        "source",
        "account",
        "source_account_key",
    )
    assert POSTGRES_TABLES["finance_observations"].columns == FINANCE_OBSERVATION_COLUMNS
    # One row per account per day per kind per source; re-syncs upsert in place.
    assert POSTGRES_TABLES["finance_observations"].primary_key == (
        "account_id",
        "as_of",
        "kind",
        "source",
    )


# --- live schema (Postgres) -----------------------------------------------------


def test_ensure_finance_tables_is_idempotent_and_creates_marts_views(warehouse):
    warehouse.ensure_finance_tables()
    warehouse.ensure_finance_tables()
    rows = warehouse._query(
        """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = ANY(%s)
        """,
        (warehouse.physical_schema_names(include_private=True),),
    )
    relations = {(schema, table): type_ for schema, table, type_ in rows}

    def phys(schema: str) -> str:
        return warehouse.physical_schema_name(schema)

    assert relations[(phys("finance"), "accounts")] == "BASE TABLE"
    assert relations[(phys("finance"), "account_links")] == "BASE TABLE"
    assert relations[(phys("finance"), "observations")] == "BASE TABLE"
    assert relations[(phys("marts"), "finance_net_worth")] == "VIEW"
    assert relations[(phys("marts"), "finance_net_worth_history")] == "VIEW"


def test_finance_money_is_numeric_and_days_are_dates(warehouse):
    warehouse.ensure_finance_tables()
    rows = warehouse._query(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = 'observations'
        """,
        (warehouse.physical_schema_name("finance"),),
    )
    types = dict(rows)
    assert types["value"] == "numeric"
    assert types["as_of"] == "date"
    assert types["observed_at"] == "timestamp with time zone"
    rows = warehouse._query(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = 'transactions'
        """,
        (warehouse.physical_schema_name("finance"),),
    )
    assert dict(rows)["amount"] == "numeric"


def test_finance_transactions_mart_reads_the_ledger(warehouse):
    from datetime import datetime as dt

    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts([_account_row()])
    warehouse.insert_finance_transactions(
        [
            {
                "transaction_id": "ft_1",
                "account_id": "fa_1",
                "posted_at": _TS,
                "amount": Decimal("-4.50"),
                "currency": "USD",
                "description": "COFFEE SHOP",
                "merchant": "Coffee Shop",
                "pending": 0,
                "source": "plaid",
                "created_at": _TS,
                "sync_version": 1,
            }
        ]
    )
    marts = warehouse.physical_schema_name("marts")
    rows = warehouse._query(
        f'SELECT transaction_id, account_name, institution, amount, source FROM "{marts}".finance_transactions'
    )
    assert rows == [("ft_1", "Checking ...0001", "Acme Bank", Decimal("-4.50"), "plaid")]


def test_finance_accounts_mart_carries_latest_observation(warehouse):
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts([_account_row()])
    marts = warehouse.physical_schema_name("marts")
    # Accounts without observations still appear (latest_value NULL).
    rows = warehouse._query(f'SELECT account_id, latest_value FROM "{marts}".finance_accounts')
    assert rows == [("fa_1", None)]
    warehouse.insert_finance_observations(
        [
            _observation_row(value=Decimal("100.00")),
            _observation_row(as_of=date(2026, 6, 1), value=Decimal("999.99")),
        ]
    )
    rows = warehouse._query(f'SELECT latest_value, latest_as_of FROM "{marts}".finance_accounts')
    assert rows == [(Decimal("100.00"), date(2026, 7, 1))]


def test_observation_upsert_is_idempotent_per_account_day(warehouse):
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_observations([_observation_row()])
    # Later sync the same day updates the row in place.
    warehouse.insert_finance_observations(
        [_observation_row(value=Decimal("150.00"), sync_version=2)]
    )
    rows = warehouse._query("SELECT value, sync_version FROM finance_observations")
    assert rows == [(Decimal("150.00"), 2)]
    # Stale writes are ignored.
    warehouse.insert_finance_observations(
        [_observation_row(value=Decimal("1.00"), sync_version=1)]
    )
    rows = warehouse._query("SELECT value, sync_version FROM finance_observations")
    assert rows == [(Decimal("150.00"), 2)]
    # A different day appends instead of updating: history is preserved.
    warehouse.insert_finance_observations(
        [_observation_row(as_of=date(2026, 7, 2), value=Decimal("160.00"), sync_version=3)]
    )
    rows = warehouse._query("SELECT count(*) FROM finance_observations")
    assert rows == [(2,)]


def test_net_worth_signs_liabilities_and_uses_latest_observation(warehouse):
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts(
        [
            _account_row(),
            _account_row(account_id="fa_2", name="Credit card", kind="credit", side="liability", mask="0002"),
        ]
    )
    warehouse.insert_finance_observations(
        [
            _observation_row(value=Decimal("100.00")),
            # Older observation must lose to the newer one.
            _observation_row(as_of=date(2026, 6, 1), value=Decimal("999.99")),
            _observation_row(account_id="fa_2", value=Decimal("40.00")),
        ]
    )
    rows = warehouse._query(
        "SELECT account_id, value, signed_value FROM finance_net_worth ORDER BY account_id"
    )
    assert rows == [
        ("fa_1", Decimal("100.00"), Decimal("100.00")),
        ("fa_2", Decimal("40.00"), Decimal("-40.00")),
    ]
    total = warehouse._query("SELECT SUM(signed_value) FROM finance_net_worth")
    assert total == [(Decimal("60.00"),)]


def test_net_worth_prefers_balance_over_valuation_on_the_same_day(warehouse):
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts([_account_row()])
    warehouse.insert_finance_observations(
        [
            _observation_row(value=Decimal("100.00")),
            _observation_row(kind="valuation", value=Decimal("500.00")),
        ]
    )
    rows = warehouse._query("SELECT observation_kind, value FROM finance_net_worth")
    assert rows == [("balance", Decimal("100.00"))]


def test_net_worth_history_forward_fills_gap_days(warehouse):
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts(
        [
            _account_row(),
            _account_row(account_id="fa_2", kind="credit", side="liability"),
        ]
    )
    warehouse.insert_finance_observations(
        [
            _observation_row(as_of=date(2026, 7, 1), value=Decimal("100.00")),
            _observation_row(as_of=date(2026, 7, 3), value=Decimal("120.00")),
            _observation_row(account_id="fa_2", as_of=date(2026, 7, 1), value=Decimal("30.00")),
        ]
    )
    rows = warehouse._query(
        """
        SELECT day, assets, liabilities, net_worth
        FROM finance_net_worth_history
        WHERE day BETWEEN %s AND %s
        ORDER BY day
        """,
        (date(2026, 7, 1), date(2026, 7, 3)),
    )
    assert rows == [
        (date(2026, 7, 1), Decimal("100.00"), Decimal("30.00"), Decimal("70.00")),
        # No observations on the 2nd: both accounts carry their last-known value.
        (date(2026, 7, 2), Decimal("100.00"), Decimal("30.00"), Decimal("70.00")),
        (date(2026, 7, 3), Decimal("120.00"), Decimal("30.00"), Decimal("90.00")),
    ]


def test_net_worth_history_excludes_accounts_before_first_observation(warehouse):
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts(
        [
            _account_row(),
            _account_row(account_id="fa_late", kind="brokerage"),
        ]
    )
    warehouse.insert_finance_observations(
        [
            _observation_row(as_of=date(2026, 7, 1), value=Decimal("100.00")),
            _observation_row(account_id="fa_late", as_of=date(2026, 7, 3), value=Decimal("50.00")),
        ]
    )
    rows = warehouse._query(
        """
        SELECT day, assets FROM finance_net_worth_history
        WHERE day BETWEEN %s AND %s ORDER BY day
        """,
        (date(2026, 7, 1), date(2026, 7, 3)),
    )
    assert rows == [
        (date(2026, 7, 1), Decimal("100.00")),
        (date(2026, 7, 2), Decimal("100.00")),
        (date(2026, 7, 3), Decimal("150.00")),
    ]
