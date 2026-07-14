from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

import pytest

from personal_data_warehouse.config import PlaidConfig
from personal_data_warehouse.plaid_sync import PlaidSyncRunner, PlaidLinkedItem


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakePlaidClient:
    def __init__(self) -> None:
        self.transactions_cursor_seen = None

    def item_get(self, access_token: str):
        assert access_token == "access-token"
        return {
            "item": {
                "item_id": "item-1",
                "institution_id": "ins_1",
                "available_products": ["transactions", "investments", "liabilities"],
                "billed_products": ["transactions"],
            }
        }

    def accounts_get(self, access_token: str):
        assert access_token == "access-token"
        return {
            "accounts": [
                {
                    "account_id": "acc-1",
                    "name": "Checking",
                    "official_name": "Plaid Checking",
                    "mask": "0000",
                    "type": "depository",
                    "subtype": "checking",
                    "balances": {
                        "available": 100.0,
                        "current": 110.0,
                        "limit": None,
                        "iso_currency_code": "USD",
                    },
                }
            ]
        }

    def transactions_sync(self, access_token: str, *, cursor: str | None, count: int):
        assert access_token == "access-token"
        assert count == 500
        self.transactions_cursor_seen = cursor
        return {
            "accounts": [],
            "added": [
                {
                    "transaction_id": "txn-1",
                    "account_id": "acc-1",
                    "date": "2026-07-01",
                    "authorized_date": "2026-06-30",
                    "name": "Coffee",
                    "merchant_name": "Coffee Shop",
                    "amount": 4.25,
                    "iso_currency_code": "USD",
                    "category": ["Food and Drink", "Restaurants", "Coffee Shop"],
                    "payment_channel": "in store",
                    "pending": False,
                }
            ],
            "modified": [],
            "removed": [{"transaction_id": "txn-old"}],
            "next_cursor": "cursor-2",
            "has_more": False,
        }

    def investments_holdings_get(self, access_token: str):
        assert access_token == "access-token"
        return {
            "accounts": [],
            "securities": [
                {
                    "security_id": "sec-1",
                    "name": "Example ETF",
                    "ticker_symbol": "EXM",
                    "type": "etf",
                    "close_price": 12.34,
                    "close_price_as_of": "2026-07-01",
                    "iso_currency_code": "USD",
                }
            ],
            "holdings": [
                {
                    "account_id": "acc-1",
                    "security_id": "sec-1",
                    "quantity": 2,
                    "institution_value": 24.68,
                    "institution_price": 12.34,
                    "institution_price_as_of": "2026-07-01",
                    "cost_basis": 20.00,
                    "iso_currency_code": "USD",
                }
            ],
        }

    def investments_transactions_get(self, access_token: str, *, start_date: date, end_date: date, count: int, offset: int):
        assert access_token == "access-token"
        assert count == 500
        assert offset == 0
        return {
            "investment_transactions": [
                {
                    "investment_transaction_id": "itxn-1",
                    "account_id": "acc-1",
                    "security_id": "sec-historical",
                    "date": "2026-07-01",
                    "name": "Buy Example ETF",
                    "quantity": 1,
                    "amount": 12.34,
                    "price": 12.34,
                    "fees": 0,
                    "type": "buy",
                    "subtype": "buy",
                    "iso_currency_code": "USD",
                }
            ],
            "securities": [
                {
                    "security_id": "sec-historical",
                    "name": "Historical Fund",
                    "ticker_symbol": "HST",
                    "type": "mutual fund",
                    "close_price": 9.87,
                    "close_price_as_of": "2026-06-30",
                    "iso_currency_code": "USD",
                }
            ],
            "total_investment_transactions": 1,
        }

    def liabilities_get(self, access_token: str):
        assert access_token == "access-token"
        return {
            "liabilities": {
                "credit": [
                    {
                        "account_id": "acc-1",
                        "last_payment_amount": 50.0,
                        "last_statement_balance": 100.0,
                        "minimum_payment_amount": 25.0,
                        "is_overdue": False,
                    }
                ]
            }
        }


@dataclass
class FakePlaidWarehouse:
    ensure_called: bool = False
    items: list[dict] | None = None
    accounts: list[dict] | None = None
    transactions: list[dict] | None = None
    removed_transactions: list[str] | None = None
    securities: list[dict] | None = None
    holdings: list[dict] | None = None
    investment_transactions: list[dict] | None = None
    liabilities: list[dict] | None = None
    sync_state_rows: list[dict] | None = None
    active_account_ids: set[str] | None = None
    active_holding_keys: set[tuple[str, str]] | None = None
    active_liability_keys: set[tuple[str, str]] | None = None

    def ensure_plaid_tables(self) -> None:
        self.ensure_called = True

    def load_plaid_item_tokens(self):
        return [
            PlaidLinkedItem(
                account="zach@example.com",
                item_id="item-1",
                access_token="access-token",
                institution_id="ins_1",
                institution_name="Example Bank",
            )
        ]

    def load_plaid_sync_state(self):
        return {("zach@example.com", "item-1", "transactions"): {"cursor": "cursor-1"}}

    def insert_plaid_items(self, rows):
        self.items = rows

    def insert_plaid_accounts(self, rows):
        self.accounts = rows

    def mark_missing_plaid_accounts_removed(self, **kwargs):
        self.active_account_ids = kwargs["active_account_ids"]
        return 0

    def insert_plaid_transactions(self, rows):
        self.transactions = rows

    def mark_plaid_transactions_removed(self, **kwargs):
        self.removed_transactions = kwargs["transaction_ids"]

    def insert_plaid_investment_securities(self, rows):
        self.securities = rows

    def insert_plaid_investment_holdings(self, rows):
        self.holdings = rows

    def delete_missing_plaid_investment_holdings(self, **kwargs):
        self.active_holding_keys = kwargs["active_holding_keys"]
        return 0

    def insert_plaid_investment_transactions(self, rows):
        self.investment_transactions = rows

    def insert_plaid_liabilities(self, rows):
        self.liabilities = rows

    def delete_missing_plaid_liabilities(self, **kwargs):
        self.active_liability_keys = kwargs["active_liability_keys"]
        return 0

    def insert_plaid_sync_state(self, **row):
        if self.sync_state_rows is None:
            self.sync_state_rows = []
        self.sync_state_rows.append(row)


def test_plaid_sync_runner_pulls_all_supported_plaid_products() -> None:
    config = PlaidConfig(
        account="zach@example.com",
        client_id="client-id",
        secret="secret",
        environment="sandbox",
        products=("transactions", "investments", "liabilities"),
        country_codes=("US",),
        client_name="Personal Data Warehouse",
        transactions_lookback_days=730,
    )
    warehouse = FakePlaidWarehouse()

    summary = PlaidSyncRunner(
        config=config,
        warehouse=warehouse,
        plaid_client=FakePlaidClient(),
        logger=FakeLogger(),
        now=lambda: datetime(2026, 7, 9, 12, tzinfo=UTC),
    ).sync_all()

    assert warehouse.ensure_called is True
    assert warehouse.items and warehouse.items[0]["item_id"] == "item-1"
    assert warehouse.accounts and warehouse.accounts[0]["account_id"] == "acc-1"
    assert warehouse.transactions and warehouse.transactions[0]["transaction_id"] == "txn-1"
    assert warehouse.transactions[0]["posted_at"] == datetime(2026, 7, 1, tzinfo=UTC)
    assert warehouse.removed_transactions == ["txn-old"]
    assert warehouse.securities
    assert {row["security_id"] for row in warehouse.securities} == {"sec-1", "sec-historical"}
    assert warehouse.holdings and warehouse.holdings[0]["security_id"] == "sec-1"
    assert warehouse.investment_transactions and warehouse.investment_transactions[0]["investment_transaction_id"] == "itxn-1"
    assert warehouse.liabilities and warehouse.liabilities[0]["liability_type"] == "credit"
    assert warehouse.active_account_ids == {"acc-1"}
    assert warehouse.active_holding_keys == {("acc-1", "sec-1")}
    assert warehouse.active_liability_keys == {("acc-1", "credit")}
    assert {row["product"]: row["status"] for row in warehouse.sync_state_rows or []} == {
        "accounts": "ok",
        "transactions": "ok",
        "investments": "ok",
        "liabilities": "ok",
    }
    assert summary.items == 1
    assert summary.accounts == 1
    assert summary.transactions == 1
    assert summary.investment_holdings == 1
    assert summary.liabilities == 1


def test_plaid_sync_skips_products_not_available_for_item() -> None:
    class TransactionsAndLiabilitiesClient(FakePlaidClient):
        def item_get(self, access_token: str):
            response = super().item_get(access_token)
            response["item"]["available_products"] = ["liabilities"]
            response["item"]["billed_products"] = ["transactions"]
            response["item"]["consented_products"] = ["transactions", "investments", "liabilities"]
            return response

        def investments_holdings_get(self, access_token: str):
            raise AssertionError("unsupported Investments product must not be called")

    config = PlaidConfig(
        account="user@example.com",
        client_id="client-id",
        secret="secret",
        environment="sandbox",
        products=("transactions", "investments", "liabilities"),
        country_codes=("US",),
        client_name="Personal Data Warehouse",
    )
    warehouse = FakePlaidWarehouse()

    summary = PlaidSyncRunner(
        config=config,
        warehouse=warehouse,
        plaid_client=TransactionsAndLiabilitiesClient(),
        logger=FakeLogger(),
        now=lambda: datetime(2026, 7, 9, 12, tzinfo=UTC),
    ).sync_all()

    states = {row["product"]: row["status"] for row in warehouse.sync_state_rows or []}
    assert states == {
        "accounts": "ok",
        "transactions": "ok",
        "investments": "unsupported",
        "liabilities": "ok",
    }
    assert summary.transactions == 1
    assert summary.investment_transactions == 0
    assert summary.liabilities == 1


def test_plaid_sync_records_product_failure_without_token_leak_and_continues_other_products() -> None:
    class FailingTransactionsClient(FakePlaidClient):
        def transactions_sync(self, access_token: str, *, cursor: str | None, count: int):
            raise RuntimeError(f"ITEM_LOGIN_REQUIRED for token {access_token}")

    config = PlaidConfig(
        account="zach@example.com",
        client_id="client-id",
        secret="secret",
        environment="sandbox",
        products=("transactions", "investments", "liabilities"),
        country_codes=("US",),
        client_name="Personal Data Warehouse",
    )
    warehouse = FakePlaidWarehouse()

    with pytest.raises(RuntimeError, match="transactions"):
        PlaidSyncRunner(
            config=config,
            warehouse=warehouse,
            plaid_client=FailingTransactionsClient(),
            logger=FakeLogger(),
            now=lambda: datetime(2026, 7, 9, 12, tzinfo=UTC),
        ).sync_all()

    states = {row["product"]: row for row in warehouse.sync_state_rows or []}
    assert states["transactions"]["status"] == "failed"
    assert "ITEM_LOGIN_REQUIRED" in states["transactions"]["error"]
    assert "access-token" not in states["transactions"]["error"]
    assert states["investments"]["status"] == "ok"
    assert states["liabilities"]["status"] == "ok"
    # The failed product retains its prior successful timestamp instead of
    # pretending the failed attempt completed successfully.
    assert states["transactions"]["last_synced_at"] == datetime.fromtimestamp(0, tz=UTC)
