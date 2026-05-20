from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
import json

from personal_data_warehouse.config import PlaidConfig, PlaidItem, load_settings
from personal_data_warehouse.finance_sync import (
    FinanceSyncRunner,
    PlaidApiError,
    account_to_row,
    liability_rows,
    transaction_to_row,
)


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakePlaidClient:
    def __init__(self) -> None:
        self.transaction_cursor_seen = []

    def accounts_get(self, access_token: str):
        return {
            "item": {
                "item_id": "item-1",
                "institution_id": "ins_capital_one",
                "institution_name": "Capital One",
                "available_products": ["investments"],
                "billed_products": ["transactions"],
                "consented_products": ["transactions", "investments", "liabilities"],
                "webhook": "https://example.test/webhook",
            },
            "accounts": [
                {
                    "account_id": "account-1",
                    "name": "Checking",
                    "official_name": "360 Checking",
                    "mask": "0000",
                    "type": "depository",
                    "subtype": "checking",
                    "balances": {
                        "current": 123.45,
                        "available": 120.0,
                        "limit": None,
                        "iso_currency_code": "USD",
                    },
                }
            ],
        }

    def transactions_sync(self, access_token: str, cursor: str | None):
        self.transaction_cursor_seen.append(cursor)
        return {
            "added": [
                {
                    "account_id": "account-1",
                    "transaction_id": "tx-1",
                    "date": "2026-05-18",
                    "name": "Coffee",
                    "merchant_name": "Cafe",
                    "amount": 5.25,
                    "iso_currency_code": "USD",
                    "personal_finance_category": {"primary": "FOOD_AND_DRINK", "detailed": "COFFEE"},
                    "payment_channel": "in store",
                    "pending": False,
                }
            ],
            "modified": [],
            "removed": [{"account_id": "account-1", "transaction_id": "old-tx"}],
            "next_cursor": "cursor-2",
            "has_more": False,
        }

    def investments_holdings_get(self, access_token: str):
        return {
            "holdings": [
                {
                    "account_id": "account-1",
                    "security_id": "sec-1",
                    "quantity": 2,
                    "institution_price": 50,
                    "institution_value": 100,
                    "iso_currency_code": "USD",
                }
            ],
            "securities": [
                {
                    "security_id": "sec-1",
                    "name": "Example ETF",
                    "ticker_symbol": "ETF",
                    "type": "etf",
                    "close_price": 50,
                    "iso_currency_code": "USD",
                }
            ],
        }

    def investments_transactions_get(self, **kwargs):
        return {
            "investment_transactions": [
                {
                    "investment_transaction_id": "itx-1",
                    "account_id": "account-1",
                    "security_id": "sec-1",
                    "date": "2026-05-01",
                    "name": "Buy ETF",
                    "quantity": 1,
                    "amount": 50,
                    "price": 50,
                    "fees": 0,
                    "type": "buy",
                    "subtype": "buy",
                    "iso_currency_code": "USD",
                }
            ],
            "securities": [],
            "total_investment_transactions": 1,
        }

    def liabilities_get(self, access_token: str):
        return {
            "liabilities": {
                "mortgage": [
                    {
                        "account_id": "mortgage-1",
                        "account_number": "1234",
                        "origination_date": "2024-01-01",
                        "maturity_date": "2054-01-01",
                        "interest_rate": {"percentage": 6.25, "type": "fixed"},
                        "next_payment_due_date": "2026-06-01",
                        "next_monthly_payment": 3000,
                        "origination_principal_amount": 500000,
                        "outstanding_principal_balance": 490000,
                    }
                ]
            }
        }


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensure_finance_tables_called = False
        self.states = {("plaid", "capital_one", "item-1", "transactions"): {"cursor": "cursor-1"}}
        self.items = []
        self.accounts = []
        self.transactions = []
        self.holdings = []
        self.securities = []
        self.investment_transactions = []
        self.liabilities = []
        self.state_rows = []

    def ensure_finance_tables(self) -> None:
        self.ensure_finance_tables_called = True

    def load_finance_sync_state(self):
        return self.states

    def insert_finance_items(self, rows):
        self.items.extend(rows)

    def insert_finance_accounts(self, rows):
        self.accounts.extend(rows)

    def insert_finance_transactions(self, rows):
        self.transactions.extend(rows)

    def insert_finance_investment_holdings(self, rows):
        self.holdings.extend(rows)

    def insert_finance_investment_securities(self, rows):
        self.securities.extend(rows)

    def insert_finance_investment_transactions(self, rows):
        self.investment_transactions.extend(rows)

    def insert_finance_liabilities(self, rows):
        self.liabilities.extend(rows)

    def insert_finance_sync_state(self, **row):
        self.state_rows.append(row)


def test_load_settings_parses_plaid_items(monkeypatch) -> None:
    monkeypatch.setenv("PLAID_CLIENT_ID", "client-id")
    monkeypatch.setenv("PLAID_SECRET", "secret")
    monkeypatch.setenv("PLAID_ENV", "development")
    monkeypatch.setenv("PLAID_ITEMS", "capital_one,robinhood")
    monkeypatch.setenv("PLAID_CAPITAL_ONE_ACCESS_TOKEN", "access-capital-one")
    monkeypatch.setenv("PLAID_CAPITAL_ONE_ITEM_ID", "item-capital-one")
    monkeypatch.setenv("PLAID_ROBINHOOD_ACCESS_TOKEN", "access-robinhood")
    monkeypatch.setenv("PLAID_PRODUCTS", "transactions")
    monkeypatch.setenv("PLAID_ADDITIONAL_CONSENTED_PRODUCTS", "investments,liabilities")

    settings = load_settings(require_gmail=False, require_postgres=False, require_finance=True)

    assert settings.plaid is not None
    assert settings.plaid.environment == "development"
    assert settings.plaid.products == ("transactions",)
    assert settings.plaid.additional_consented_products == ("investments", "liabilities")
    assert [item.name for item in settings.plaid.items] == ["capital_one", "robinhood"]
    assert settings.plaid.items[0].access_token_env == "PLAID_CAPITAL_ONE_ACCESS_TOKEN"


def test_finance_row_mappers_keep_queryable_fields_and_raw_json() -> None:
    synced_at = datetime(2026, 5, 19, 12, tzinfo=UTC)

    account_row = account_to_row(
        item_name="capital_one",
        item_id="item-1",
        account={
            "account_id": "account-1",
            "name": "Checking",
            "type": "depository",
            "subtype": "checking",
            "balances": {"current": 10, "available": 8, "iso_currency_code": "USD"},
        },
        synced_at=synced_at,
    )
    transaction_row = transaction_to_row(
        item_name="capital_one",
        item_id="item-1",
        transaction={
            "account_id": "account-1",
            "transaction_id": "tx-1",
            "date": "2026-05-18",
            "datetime": "2026-05-18T10:30:00Z",
            "name": "Coffee",
            "amount": 4.5,
            "personal_finance_category": {"primary": "FOOD_AND_DRINK", "detailed": "COFFEE"},
        },
        synced_at=synced_at,
    )
    mortgage_rows = liability_rows(
        item_name="mortgage",
        item_id="item-2",
        liabilities={
            "mortgage": [
                {
                    "account_id": "mortgage-1",
                    "interest_rate": {"percentage": 6.25, "type": "fixed"},
                    "next_monthly_payment": 3000,
                }
            ]
        },
        synced_at=synced_at,
    )

    assert account_row["current_balance"] == 10.0
    assert transaction_row["datetime"] == datetime(2026, 5, 18, 10, 30, tzinfo=UTC)
    assert transaction_row["personal_finance_category_primary"] == "FOOD_AND_DRINK"
    assert json.loads(transaction_row["raw_json"])["name"] == "Coffee"
    assert mortgage_rows[0]["liability_type"] == "mortgage"
    assert mortgage_rows[0]["interest_rate_percentage"] == 6.25


def test_finance_sync_runner_writes_all_supported_product_rows() -> None:
    config = PlaidConfig(
        client_id="client-id",
        secret="secret",
        environment="sandbox",
        api_version="2020-09-14",
        client_name="Personal Data Warehouse",
        products=("transactions",),
        additional_consented_products=("investments", "liabilities"),
        optional_products=(),
        required_if_supported_products=(),
        country_codes=("US",),
        redirect_uri=None,
        webhook=None,
        items=(
            PlaidItem(
                name="capital_one",
                access_token="access-token",
                access_token_env="PLAID_CAPITAL_ONE_ACCESS_TOKEN",
                item_id="item-1",
                institution_name="Capital One",
            ),
        ),
    )
    warehouse = FakeWarehouse()
    client = FakePlaidClient()
    now = lambda: datetime(2026, 5, 19, 12, tzinfo=UTC)

    summaries = FinanceSyncRunner(
        settings=SimpleNamespace(plaid=config),
        warehouse=warehouse,
        logger=FakeLogger(),
        client=client,
        now=now,
    ).sync_all()

    assert warehouse.ensure_finance_tables_called
    assert client.transaction_cursor_seen == ["cursor-1"]
    assert len(warehouse.items) == 1
    assert len(warehouse.accounts) == 1
    assert {row["transaction_id"] for row in warehouse.transactions} == {"tx-1", "old-tx"}
    assert len(warehouse.holdings) == 1
    assert len(warehouse.securities) == 1
    assert len(warehouse.investment_transactions) == 1
    assert len(warehouse.liabilities) == 1
    assert summaries[0].transactions_added == 1
    assert summaries[0].transactions_removed == 1
    assert summaries[0].investment_holdings_written == 1
    assert summaries[0].liabilities_written == 1


def test_finance_sync_runner_records_product_errors_without_failing_item() -> None:
    class ProductErrorClient(FakePlaidClient):
        def investments_holdings_get(self, access_token: str):
            raise PlaidApiError("/investments/holdings/get", 400, {"error_code": "PRODUCT_NOT_READY"})

    config = PlaidConfig(
        client_id="client-id",
        secret="secret",
        environment="sandbox",
        api_version="2020-09-14",
        client_name="Personal Data Warehouse",
        products=("transactions",),
        additional_consented_products=("investments",),
        optional_products=(),
        required_if_supported_products=(),
        country_codes=("US",),
        redirect_uri=None,
        webhook=None,
        items=(
            PlaidItem(
                name="robinhood",
                access_token="access-token",
                access_token_env="PLAID_ROBINHOOD_ACCESS_TOKEN",
                item_id="item-1",
                institution_name="Robinhood",
            ),
        ),
    )
    warehouse = FakeWarehouse()
    now = lambda: datetime(2026, 5, 19, 12, tzinfo=UTC)

    summaries = FinanceSyncRunner(
        settings=SimpleNamespace(plaid=config),
        warehouse=warehouse,
        logger=FakeLogger(),
        client=ProductErrorClient(),
        now=now,
    ).sync_all()

    assert summaries[0].item_name == "robinhood"
    assert any(row["object_type"] == "investment_holdings" and row["status"] == "error" for row in warehouse.state_rows)
