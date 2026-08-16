"""Warehouse schema + marts contract for the finance ledger.

The ledger is the derived stocks-and-flows layer: logical accounts
(finance.accounts + finance.account_links, photos-identity pattern) and
append-only point-in-time observations (finance.observations). Net worth is
read through marts_finance.net_worth / marts_finance.net_worth_history.
Money columns are NUMERIC and observation days are DATE — never floats or
timestamps.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
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
    assert "derived_finance" in DERIVED_SCHEMAS
    assert (relation("finance_accounts").schema, relation("finance_accounts").name) == ("derived_finance", "accounts")
    assert (relation("finance_account_links").schema, relation("finance_account_links").name) == (
        "derived_finance",
        "account_links",
    )
    assert (relation("finance_observations").schema, relation("finance_observations").name) == (
        "derived_finance",
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
        (warehouse.physical_schema_names(include_hidden=True),),
    )
    relations = {(schema, table): type_ for schema, table, type_ in rows}

    def phys(schema: str) -> str:
        return warehouse.physical_schema_name(schema)

    assert relations[(phys("derived_finance"), "accounts")] == "BASE TABLE"
    assert relations[(phys("derived_finance"), "account_links")] == "BASE TABLE"
    assert relations[(phys("derived_finance"), "observations")] == "BASE TABLE"
    assert relations[(phys("marts_finance"), "net_worth")] == "VIEW"
    assert relations[(phys("marts_finance"), "net_worth_history")] == "VIEW"


def test_finance_money_is_numeric_and_days_are_dates(warehouse):
    warehouse.ensure_finance_tables()
    rows = warehouse._query(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = 'observations'
        """,
        (warehouse.physical_schema_name("derived_finance"),),
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
        (warehouse.physical_schema_name("derived_finance"),),
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
    rows = warehouse._query(
        "SELECT transaction_id, account_name, institution, amount, source FROM @marts_finance_transactions"
    )
    assert rows == [("ft_1", "Checking ...0001", "Acme Bank", Decimal("-4.50"), "plaid")]


def test_finance_accounts_mart_carries_latest_observation(warehouse):
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts([_account_row()])
    # Accounts without observations still appear (latest_value NULL).
    rows = warehouse._query("SELECT account_id, latest_value FROM @marts_finance_accounts")
    assert rows == [("fa_1", None)]
    warehouse.insert_finance_observations(
        [
            _observation_row(value=Decimal("100.00")),
            _observation_row(as_of=date(2026, 6, 1), value=Decimal("999.99")),
        ]
    )
    rows = warehouse._query("SELECT latest_value, latest_as_of FROM @marts_finance_accounts")
    assert rows == [(Decimal("100.00"), date(2026, 7, 1))]


def test_observation_upsert_is_idempotent_per_account_day(warehouse):
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_observations([_observation_row()])
    # Later sync the same day updates the row in place.
    warehouse.insert_finance_observations(
        [_observation_row(value=Decimal("150.00"), sync_version=2)]
    )
    rows = warehouse._query("SELECT value, sync_version FROM @finance_observations")
    assert rows == [(Decimal("150.00"), 2)]
    # Stale writes are ignored.
    warehouse.insert_finance_observations(
        [_observation_row(value=Decimal("1.00"), sync_version=1)]
    )
    rows = warehouse._query("SELECT value, sync_version FROM @finance_observations")
    assert rows == [(Decimal("150.00"), 2)]
    # A different day appends instead of updating: history is preserved.
    warehouse.insert_finance_observations(
        [_observation_row(as_of=date(2026, 7, 2), value=Decimal("160.00"), sync_version=3)]
    )
    rows = warehouse._query("SELECT count(*) FROM @finance_observations")
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
        "SELECT account_id, value, signed_value FROM @marts_finance_net_worth ORDER BY account_id"
    )
    assert rows == [
        ("fa_1", Decimal("100.00"), Decimal("100.00")),
        ("fa_2", Decimal("40.00"), Decimal("-40.00")),
    ]
    total = warehouse._query("SELECT SUM(signed_value) FROM @marts_finance_net_worth")
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
    rows = warehouse._query("SELECT observation_kind, value FROM @marts_finance_net_worth")
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
        FROM @marts_finance_net_worth_history
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
        SELECT day, assets FROM @marts_finance_net_worth_history
        WHERE day BETWEEN %s AND %s ORDER BY day
        """,
        (date(2026, 7, 1), date(2026, 7, 3)),
    )
    assert rows == [
        (date(2026, 7, 1), Decimal("100.00")),
        (date(2026, 7, 2), Decimal("100.00")),
        (date(2026, 7, 3), Decimal("150.00")),
    ]


# --- per-account freshness (marts_finance.account_freshness) -------------------
#
# Regression cover for the Venture X outage: one credit card stopped producing
# transactions on 2026-03-21 and nothing noticed until 2026-08-16, because
# table-level freshness on base_plaid.transactions stayed green the whole time
# (three other institutions kept writing to it) and the statement pipeline that
# had been covering the card is declared manual, hence never stale.


def _txn_row(**overrides) -> dict:
    row = {
        "transaction_id": "ft_1",
        "account_id": "fa_1",
        "posted_at": _TS,
        "amount": Decimal("-12.34"),
        "currency": "USD",
        "description": "Coffee",
        "merchant": "Cafe",
        "pending": 0,
        "source": "plaid",
        "created_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _cadence_rows(account_id: str, *, count: int, every_days: float, quiet_days: float) -> list[dict]:
    """``count`` transactions spaced ``every_days`` apart, ending ``quiet_days`` ago."""
    now = datetime.now(tz=UTC)
    last = now - timedelta(days=quiet_days)
    return [
        _txn_row(
            transaction_id=f"ft_{account_id}_{index}",
            account_id=account_id,
            posted_at=last - timedelta(days=every_days * index),
        )
        for index in range(count)
    ]


def _freshness(warehouse) -> dict[str, tuple]:
    rows = warehouse._query(
        """
        SELECT account_id, status, baseline_gaps, quiet_ratio
        FROM @marts_finance_account_freshness
        """
    )
    return {row[0]: (row[1], row[2], row[3]) for row in rows}


def test_account_freshness_flags_a_dense_account_that_went_quiet(warehouse):
    """A daily-cadence account silent for two months is stale, not ok.

    This is the shape of the missed outage: heavy, regular use and then
    nothing.
    """
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts([_account_row(account_id="fa_card", kind="credit")])
    warehouse.insert_finance_transactions(
        _cadence_rows("fa_card", count=40, every_days=1, quiet_days=60)
    )
    status, gaps, ratio = _freshness(warehouse)["fa_card"]
    assert status == "stale"
    assert gaps == 20, "cadence uses the most recent N intervals"
    # Silent for 60x its own typical one-day gap.
    assert ratio == pytest.approx(Decimal("60"), abs=Decimal("1"))


def test_account_freshness_leaves_a_naturally_slow_account_alone(warehouse):
    """The same 60 days of silence is unremarkable at a monthly cadence.

    A single global threshold cannot express this, which is why the expectation
    is measured per account instead of declared.
    """
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts([_account_row(account_id="fa_ira", kind="ira")])
    warehouse.insert_finance_transactions(
        _cadence_rows("fa_ira", count=12, every_days=30, quiet_days=60)
    )
    status, _, ratio = _freshness(warehouse)["fa_ira"]
    assert status == "ok"
    assert ratio == pytest.approx(Decimal("2"), abs=Decimal("0.2"))


def test_account_freshness_baseline_ignores_the_silence_it_is_measuring(warehouse):
    """The cadence window ends at the last transaction, not at now().

    Measured over a trailing window from now(), an account quiet for longer
    than the window contributes zero intervals and disappears into 'sparse' —
    the longer it stays broken, the more normal broken looks. Anchoring to the
    last transaction is what keeps a long outage loud.
    """
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts([_account_row(account_id="fa_old", kind="credit")])
    warehouse.insert_finance_transactions(
        _cadence_rows("fa_old", count=30, every_days=1, quiet_days=200)
    )
    status, gaps, _ = _freshness(warehouse)["fa_old"]
    assert gaps == 20, "gaps must come from before the outage, not after it"
    assert status == "stale"


def test_account_freshness_reports_rather_than_judges_thin_history(warehouse):
    """Too few intervals for a percentile, and no transactions at all.

    A verdict built on three data points is how a monitor teaches you to ignore
    it; valuation-only accounts (a house, a car) are silent by nature.
    """
    warehouse.ensure_finance_tables()
    warehouse.insert_finance_accounts(
        [
            _account_row(account_id="fa_thin", kind="credit"),
            _account_row(account_id="fa_house", kind="property"),
        ]
    )
    warehouse.insert_finance_transactions(
        _cadence_rows("fa_thin", count=3, every_days=1, quiet_days=400)
    )
    freshness = _freshness(warehouse)
    assert freshness["fa_thin"][0] == "sparse"
    assert freshness["fa_house"] == ("no_transactions", 0, None)
