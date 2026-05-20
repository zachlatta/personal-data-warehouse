from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import json
import logging
import os
from pathlib import Path
import threading
import time
from collections.abc import Callable
from typing import Any, Mapping
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

from personal_data_warehouse.config import PlaidConfig, PlaidItem, env_slug, load_settings
from personal_data_warehouse.gmail_auth import update_env_file
from personal_data_warehouse.warehouse import warehouse_from_settings

PLAID_SOURCE = "plaid"
PLAID_BASE_URLS = {
    "sandbox": "https://sandbox.plaid.com",
    "development": "https://development.plaid.com",
    "production": "https://production.plaid.com",
}
EPOCH_UTC = datetime.fromtimestamp(0, tz=UTC)


@dataclass(frozen=True)
class FinanceSyncSummary:
    item_name: str
    item_id: str
    institution_name: str
    accounts_written: int
    transactions_added: int
    transactions_modified: int
    transactions_removed: int
    investment_holdings_written: int
    investment_securities_written: int
    investment_transactions_written: int
    liabilities_written: int


class PlaidApiError(RuntimeError):
    def __init__(self, endpoint: str, status_code: int, payload: Mapping[str, Any]) -> None:
        self.endpoint = endpoint
        self.status_code = status_code
        self.payload = dict(payload)
        error_code = payload.get("error_code") or payload.get("error_type") or status_code
        error_message = payload.get("error_message") or payload.get("display_message") or "Plaid API request failed"
        super().__init__(f"{endpoint} failed ({error_code}): {error_message}")


class PlaidClient:
    def __init__(self, config: PlaidConfig) -> None:
        self._config = config
        self._base_url = PLAID_BASE_URLS[config.environment]

    def create_link_token(
        self,
        *,
        item_name: str,
        products: tuple[str, ...],
        additional_consented_products: tuple[str, ...],
        optional_products: tuple[str, ...],
        required_if_supported_products: tuple[str, ...],
        redirect_uri: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "user": {"client_user_id": f"pdw-{env_slug(item_name).lower()}"},
            "client_name": self._config.client_name,
            "products": list(products),
            "country_codes": list(self._config.country_codes),
            "language": "en",
        }
        if additional_consented_products:
            payload["additional_consented_products"] = list(additional_consented_products)
        if optional_products:
            payload["optional_products"] = list(optional_products)
        if required_if_supported_products:
            payload["required_if_supported_products"] = list(required_if_supported_products)
        if redirect_uri:
            payload["redirect_uri"] = redirect_uri
        elif self._config.redirect_uri:
            payload["redirect_uri"] = self._config.redirect_uri
        if self._config.webhook:
            payload["webhook"] = self._config.webhook
        return self._post("/link/token/create", payload)

    def exchange_public_token(self, public_token: str) -> dict[str, Any]:
        return self._post("/item/public_token/exchange", {"public_token": public_token})

    def item_get(self, access_token: str) -> dict[str, Any]:
        return self._post("/item/get", {"access_token": access_token})

    def accounts_get(self, access_token: str) -> dict[str, Any]:
        return self._post("/accounts/get", {"access_token": access_token})

    def transactions_sync(self, access_token: str, cursor: str | None) -> dict[str, Any]:
        payload: dict[str, Any] = {"access_token": access_token}
        if cursor:
            payload["cursor"] = cursor
        return self._post("/transactions/sync", payload)

    def investments_holdings_get(self, access_token: str) -> dict[str, Any]:
        return self._post("/investments/holdings/get", {"access_token": access_token})

    def investments_transactions_get(
        self,
        *,
        access_token: str,
        start_date: date,
        end_date: date,
        count: int,
        offset: int,
    ) -> dict[str, Any]:
        return self._post(
            "/investments/transactions/get",
            {
                "access_token": access_token,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "options": {"count": count, "offset": offset},
            },
        )

    def liabilities_get(self, access_token: str) -> dict[str, Any]:
        return self._post("/liabilities/get", {"access_token": access_token})

    def _post(self, endpoint: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        response = requests.post(
            self._base_url + endpoint,
            headers={
                "Content-Type": "application/json",
                "Plaid-Version": self._config.api_version,
            },
            json={
                "client_id": self._config.client_id,
                "secret": self._config.secret,
                **dict(payload),
            },
            timeout=self._config.request_timeout_seconds,
        )
        try:
            data = response.json()
        except ValueError:
            data = {"error_message": response.text}
        if response.status_code >= 400:
            raise PlaidApiError(endpoint, response.status_code, data)
        return data


class FinanceSyncRunner:
    def __init__(
        self,
        *,
        settings,
        warehouse,
        logger,
        client: PlaidClient | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if settings.plaid is None:
            raise ValueError("Plaid finance sync is not configured")
        self._settings = settings
        self._config: PlaidConfig = settings.plaid
        self._warehouse = warehouse
        self._logger = logger
        self._client = client or PlaidClient(self._config)
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync_all(self) -> list[FinanceSyncSummary]:
        self._warehouse.ensure_finance_tables()
        state_by_key = self._warehouse.load_finance_sync_state()
        summaries: list[FinanceSyncSummary] = []
        failures: list[str] = []

        for item in self._config.items:
            try:
                summaries.append(self._sync_item(item=item, state_by_key=state_by_key))
            except Exception as exc:
                item_id = item.item_id or ""
                self._warehouse.insert_finance_sync_state(
                    source=PLAID_SOURCE,
                    item_name=item.name,
                    item_id=item_id,
                    object_type="item",
                    cursor="",
                    start_date="",
                    end_date="",
                    status="failed",
                    error=str(exc),
                    updated_at=self._now(),
                )
                failures.append(f"{item.name}: {exc}")

        if failures:
            raise RuntimeError("Finance sync failed for: " + "; ".join(failures))
        return summaries

    def _sync_item(
        self,
        *,
        item: PlaidItem,
        state_by_key: Mapping[tuple[str, str, str, str], Mapping[str, Any]],
    ) -> FinanceSyncSummary:
        synced_at = self._now()
        account_response = self._client.accounts_get(item.access_token)
        plaid_item = _mapping(account_response.get("item"))
        item_id = _first_text(plaid_item.get("item_id"), item.item_id, item.name)
        institution_name = _first_text(plaid_item.get("institution_name"), item.institution_name)

        self._logger.info("Syncing Plaid finance item %s (%s)", item.name, institution_name or item_id)
        self._warehouse.insert_finance_items(
            [
                item_to_row(
                    item_name=item.name,
                    item_id=item_id,
                    item_payload=plaid_item,
                    access_token_env=item.access_token_env,
                    synced_at=synced_at,
                )
            ]
        )
        accounts = [_mapping(account) for account in account_response.get("accounts", [])]
        self._warehouse.insert_finance_accounts(
            [
                account_to_row(
                    item_name=item.name,
                    item_id=item_id,
                    account=account,
                    synced_at=synced_at,
                )
                for account in accounts
            ]
        )

        transactions_added, transactions_modified, transactions_removed = self._sync_transactions(
            item=item,
            item_id=item_id,
            state=state_by_key.get((PLAID_SOURCE, item.name, item_id, "transactions")),
        )
        holdings_written, holdings_securities_written = self._sync_investment_holdings(
            item=item,
            item_id=item_id,
        )
        investment_transactions_written, transaction_securities_written = self._sync_investment_transactions(
            item=item,
            item_id=item_id,
        )
        liabilities_written = self._sync_liabilities(item=item, item_id=item_id)

        self._warehouse.insert_finance_sync_state(
            source=PLAID_SOURCE,
            item_name=item.name,
            item_id=item_id,
            object_type="item",
            cursor="",
            start_date="",
            end_date="",
            status="ok",
            error="",
            updated_at=self._now(),
        )
        return FinanceSyncSummary(
            item_name=item.name,
            item_id=item_id,
            institution_name=institution_name,
            accounts_written=len(accounts),
            transactions_added=transactions_added,
            transactions_modified=transactions_modified,
            transactions_removed=transactions_removed,
            investment_holdings_written=holdings_written,
            investment_securities_written=holdings_securities_written + transaction_securities_written,
            investment_transactions_written=investment_transactions_written,
            liabilities_written=liabilities_written,
        )

    def _sync_transactions(
        self,
        *,
        item: PlaidItem,
        item_id: str,
        state: Mapping[str, Any] | None,
    ) -> tuple[int, int, int]:
        cursor = str((state or {}).get("cursor", ""))
        added_rows: list[dict[str, Any]] = []
        modified_rows: list[dict[str, Any]] = []
        removed_rows: list[dict[str, Any]] = []
        next_cursor = cursor
        try:
            while True:
                response = self._client.transactions_sync(item.access_token, next_cursor or None)
                synced_at = self._now()
                added_rows.extend(
                    transaction_to_row(
                        item_name=item.name,
                        item_id=item_id,
                        transaction=_mapping(transaction),
                        synced_at=synced_at,
                    )
                    for transaction in response.get("added", [])
                )
                modified_rows.extend(
                    transaction_to_row(
                        item_name=item.name,
                        item_id=item_id,
                        transaction=_mapping(transaction),
                        synced_at=synced_at,
                    )
                    for transaction in response.get("modified", [])
                )
                removed_rows.extend(
                    removed_transaction_to_row(
                        item_name=item.name,
                        item_id=item_id,
                        transaction=_mapping(transaction),
                        synced_at=synced_at,
                    )
                    for transaction in response.get("removed", [])
                )
                next_cursor = _first_text(response.get("next_cursor"), next_cursor)
                if not response.get("has_more"):
                    break
        except PlaidApiError as exc:
            self._record_product_error(item_name=item.name, item_id=item_id, object_type="transactions", error=exc)
            return (0, 0, 0)

        rows = [*added_rows, *modified_rows, *removed_rows]
        self._warehouse.insert_finance_transactions(rows)
        self._warehouse.insert_finance_sync_state(
            source=PLAID_SOURCE,
            item_name=item.name,
            item_id=item_id,
            object_type="transactions",
            cursor=next_cursor,
            start_date="",
            end_date="",
            status="ok",
            error="",
            updated_at=self._now(),
        )
        return (len(added_rows), len(modified_rows), len(removed_rows))

    def _sync_investment_holdings(self, *, item: PlaidItem, item_id: str) -> tuple[int, int]:
        try:
            response = self._client.investments_holdings_get(item.access_token)
        except PlaidApiError as exc:
            self._record_product_error(item_name=item.name, item_id=item_id, object_type="investment_holdings", error=exc)
            return (0, 0)

        synced_at = self._now()
        holding_rows = [
            investment_holding_to_row(
                item_name=item.name,
                item_id=item_id,
                holding=_mapping(holding),
                synced_at=synced_at,
            )
            for holding in response.get("holdings", [])
        ]
        security_rows = [
            investment_security_to_row(
                item_name=item.name,
                item_id=item_id,
                security=_mapping(security),
                synced_at=synced_at,
            )
            for security in response.get("securities", [])
        ]
        self._warehouse.insert_finance_investment_holdings(holding_rows)
        self._warehouse.insert_finance_investment_securities(security_rows)
        self._warehouse.insert_finance_sync_state(
            source=PLAID_SOURCE,
            item_name=item.name,
            item_id=item_id,
            object_type="investment_holdings",
            cursor="",
            start_date="",
            end_date="",
            status="ok",
            error="",
            updated_at=self._now(),
        )
        return (len(holding_rows), len(security_rows))

    def _sync_investment_transactions(self, *, item: PlaidItem, item_id: str) -> tuple[int, int]:
        end = self._now().date()
        start = end - timedelta(days=self._config.investments_lookback_days)
        offset = 0
        count = 500
        transaction_rows: list[dict[str, Any]] = []
        security_rows: list[dict[str, Any]] = []
        try:
            while True:
                response = self._client.investments_transactions_get(
                    access_token=item.access_token,
                    start_date=start,
                    end_date=end,
                    count=count,
                    offset=offset,
                )
                synced_at = self._now()
                page_transactions = [
                    investment_transaction_to_row(
                        item_name=item.name,
                        item_id=item_id,
                        transaction=_mapping(transaction),
                        synced_at=synced_at,
                    )
                    for transaction in response.get("investment_transactions", [])
                ]
                transaction_rows.extend(page_transactions)
                security_rows.extend(
                    investment_security_to_row(
                        item_name=item.name,
                        item_id=item_id,
                        security=_mapping(security),
                        synced_at=synced_at,
                    )
                    for security in response.get("securities", [])
                )
                total = int(response.get("total_investment_transactions") or len(page_transactions))
                offset += len(page_transactions)
                if not page_transactions or offset >= total:
                    break
        except PlaidApiError as exc:
            self._record_product_error(
                item_name=item.name,
                item_id=item_id,
                object_type="investment_transactions",
                error=exc,
                start_date=start.isoformat(),
                end_date=end.isoformat(),
            )
            return (0, 0)

        self._warehouse.insert_finance_investment_transactions(transaction_rows)
        self._warehouse.insert_finance_investment_securities(security_rows)
        self._warehouse.insert_finance_sync_state(
            source=PLAID_SOURCE,
            item_name=item.name,
            item_id=item_id,
            object_type="investment_transactions",
            cursor="",
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            status="ok",
            error="",
            updated_at=self._now(),
        )
        return (len(transaction_rows), len(security_rows))

    def _sync_liabilities(self, *, item: PlaidItem, item_id: str) -> int:
        try:
            response = self._client.liabilities_get(item.access_token)
        except PlaidApiError as exc:
            self._record_product_error(item_name=item.name, item_id=item_id, object_type="liabilities", error=exc)
            return 0

        synced_at = self._now()
        rows = liability_rows(
            item_name=item.name,
            item_id=item_id,
            liabilities=_mapping(response.get("liabilities")),
            synced_at=synced_at,
        )
        self._warehouse.insert_finance_liabilities(rows)
        self._warehouse.insert_finance_sync_state(
            source=PLAID_SOURCE,
            item_name=item.name,
            item_id=item_id,
            object_type="liabilities",
            cursor="",
            start_date="",
            end_date="",
            status="ok",
            error="",
            updated_at=self._now(),
        )
        return len(rows)

    def _record_product_error(
        self,
        *,
        item_name: str,
        item_id: str,
        object_type: str,
        error: Exception,
        start_date: str = "",
        end_date: str = "",
    ) -> None:
        self._logger.warning("Skipping Plaid %s sync for %s: %s", object_type, item_name, error)
        self._warehouse.insert_finance_sync_state(
            source=PLAID_SOURCE,
            item_name=item_name,
            item_id=item_id,
            object_type=object_type,
            cursor="",
            start_date=start_date,
            end_date=end_date,
            status="error",
            error=str(error),
            updated_at=self._now(),
        )


def item_to_row(
    *,
    item_name: str,
    item_id: str,
    item_payload: Mapping[str, Any],
    access_token_env: str,
    synced_at: datetime,
) -> dict[str, Any]:
    return {
        "source": PLAID_SOURCE,
        "item_name": item_name,
        "item_id": item_id,
        "institution_id": _text(item_payload.get("institution_id")),
        "institution_name": _text(item_payload.get("institution_name")),
        "available_products_json": _json(item_payload.get("available_products", [])),
        "billed_products_json": _json(item_payload.get("billed_products", [])),
        "consented_products_json": _json(item_payload.get("consented_products", [])),
        "error_json": _json(item_payload.get("error") or {}),
        "webhook": _text(item_payload.get("webhook")),
        "raw_json": _json(item_payload),
        "access_token_env": access_token_env,
        "synced_at": _utc(synced_at),
        "sync_version": _sync_version(synced_at),
    }


def account_to_row(
    *,
    item_name: str,
    item_id: str,
    account: Mapping[str, Any],
    synced_at: datetime,
    is_deleted: int = 0,
) -> dict[str, Any]:
    balances = _mapping(account.get("balances"))
    return {
        "source": PLAID_SOURCE,
        "item_name": item_name,
        "item_id": item_id,
        "account_id": _text(account.get("account_id")),
        "persistent_account_id": _text(account.get("persistent_account_id")),
        "name": _text(account.get("name")),
        "official_name": _text(account.get("official_name")),
        "mask": _text(account.get("mask")),
        "type": _text(account.get("type")),
        "subtype": _text(account.get("subtype")),
        "verification_status": _text(account.get("verification_status")),
        "current_balance": _number(balances.get("current")),
        "available_balance": _number(balances.get("available")),
        "limit_balance": _number(balances.get("limit")),
        "iso_currency_code": _text(balances.get("iso_currency_code")),
        "unofficial_currency_code": _text(balances.get("unofficial_currency_code")),
        "is_deleted": int(is_deleted),
        "raw_json": _json(account),
        "synced_at": _utc(synced_at),
        "sync_version": _sync_version(synced_at),
    }


def transaction_to_row(
    *,
    item_name: str,
    item_id: str,
    transaction: Mapping[str, Any],
    synced_at: datetime,
) -> dict[str, Any]:
    personal_category = _mapping(transaction.get("personal_finance_category"))
    return {
        "source": PLAID_SOURCE,
        "item_name": item_name,
        "item_id": item_id,
        "account_id": _text(transaction.get("account_id")),
        "transaction_id": _text(transaction.get("transaction_id")),
        "pending_transaction_id": _text(transaction.get("pending_transaction_id")),
        "date": _text(transaction.get("date")),
        "authorized_date": _text(transaction.get("authorized_date")),
        "datetime": _parse_datetime(transaction.get("datetime")),
        "authorized_datetime": _parse_datetime(transaction.get("authorized_datetime")),
        "name": _text(transaction.get("name")),
        "merchant_name": _text(transaction.get("merchant_name")),
        "payment_channel": _text(transaction.get("payment_channel")),
        "amount": _number(transaction.get("amount")),
        "iso_currency_code": _text(transaction.get("iso_currency_code")),
        "unofficial_currency_code": _text(transaction.get("unofficial_currency_code")),
        "category_id": _text(transaction.get("category_id")),
        "category_json": _json(transaction.get("category", [])),
        "personal_finance_category_primary": _text(personal_category.get("primary")),
        "personal_finance_category_detailed": _text(personal_category.get("detailed")),
        "counterparties_json": _json(transaction.get("counterparties", [])),
        "location_json": _json(transaction.get("location", {})),
        "pending": _bool_int(transaction.get("pending")),
        "is_removed": 0,
        "raw_json": _json(transaction),
        "synced_at": _utc(synced_at),
        "sync_version": _sync_version(synced_at),
    }


def removed_transaction_to_row(
    *,
    item_name: str,
    item_id: str,
    transaction: Mapping[str, Any],
    synced_at: datetime,
) -> dict[str, Any]:
    row = transaction_to_row(
        item_name=item_name,
        item_id=item_id,
        transaction={
            "account_id": transaction.get("account_id"),
            "transaction_id": transaction.get("transaction_id"),
        },
        synced_at=synced_at,
    )
    row["is_removed"] = 1
    row["raw_json"] = _json(transaction)
    return row


def investment_holding_to_row(
    *,
    item_name: str,
    item_id: str,
    holding: Mapping[str, Any],
    synced_at: datetime,
) -> dict[str, Any]:
    return {
        "source": PLAID_SOURCE,
        "item_name": item_name,
        "item_id": item_id,
        "account_id": _text(holding.get("account_id")),
        "security_id": _text(holding.get("security_id")),
        "quantity": _number(holding.get("quantity")),
        "cost_basis": _number(holding.get("cost_basis")),
        "institution_price": _number(holding.get("institution_price")),
        "institution_price_as_of": _text(holding.get("institution_price_as_of")),
        "institution_value": _number(holding.get("institution_value")),
        "iso_currency_code": _text(holding.get("iso_currency_code")),
        "unofficial_currency_code": _text(holding.get("unofficial_currency_code")),
        "vested_quantity": _number(holding.get("vested_quantity")),
        "vested_value": _number(holding.get("vested_value")),
        "is_deleted": 0,
        "raw_json": _json(holding),
        "synced_at": _utc(synced_at),
        "sync_version": _sync_version(synced_at),
    }


def investment_security_to_row(
    *,
    item_name: str,
    item_id: str,
    security: Mapping[str, Any],
    synced_at: datetime,
) -> dict[str, Any]:
    return {
        "source": PLAID_SOURCE,
        "item_name": item_name,
        "item_id": item_id,
        "security_id": _first_text(security.get("security_id"), security.get("proxy_security_id")),
        "institution_security_id": _text(security.get("institution_security_id")),
        "proxy_security_id": _text(security.get("proxy_security_id")),
        "name": _text(security.get("name")),
        "ticker_symbol": _text(security.get("ticker_symbol")),
        "isin": _text(security.get("isin")),
        "cusip": _text(security.get("cusip")),
        "sedol": _text(security.get("sedol")),
        "type": _text(security.get("type")),
        "close_price": _number(security.get("close_price")),
        "close_price_as_of": _text(security.get("close_price_as_of")),
        "iso_currency_code": _text(security.get("iso_currency_code")),
        "unofficial_currency_code": _text(security.get("unofficial_currency_code")),
        "market_identifier_code": _text(security.get("market_identifier_code")),
        "raw_json": _json(security),
        "synced_at": _utc(synced_at),
        "sync_version": _sync_version(synced_at),
    }


def investment_transaction_to_row(
    *,
    item_name: str,
    item_id: str,
    transaction: Mapping[str, Any],
    synced_at: datetime,
) -> dict[str, Any]:
    return {
        "source": PLAID_SOURCE,
        "item_name": item_name,
        "item_id": item_id,
        "account_id": _text(transaction.get("account_id")),
        "investment_transaction_id": _text(transaction.get("investment_transaction_id")),
        "security_id": _text(transaction.get("security_id")),
        "date": _text(transaction.get("date")),
        "name": _text(transaction.get("name")),
        "quantity": _number(transaction.get("quantity")),
        "amount": _number(transaction.get("amount")),
        "price": _number(transaction.get("price")),
        "fees": _number(transaction.get("fees")),
        "type": _text(transaction.get("type")),
        "subtype": _text(transaction.get("subtype")),
        "iso_currency_code": _text(transaction.get("iso_currency_code")),
        "unofficial_currency_code": _text(transaction.get("unofficial_currency_code")),
        "cancel_transaction_id": _text(transaction.get("cancel_transaction_id")),
        "is_deleted": 0,
        "raw_json": _json(transaction),
        "synced_at": _utc(synced_at),
        "sync_version": _sync_version(synced_at),
    }


def liability_rows(
    *,
    item_name: str,
    item_id: str,
    liabilities: Mapping[str, Any],
    synced_at: datetime,
) -> list[dict[str, Any]]:
    rows = []
    for liability_type in ("credit", "mortgage", "student"):
        for liability in liabilities.get(liability_type, []) or []:
            rows.append(
                liability_to_row(
                    item_name=item_name,
                    item_id=item_id,
                    liability_type=liability_type,
                    liability=_mapping(liability),
                    synced_at=synced_at,
                )
            )
    return rows


def liability_to_row(
    *,
    item_name: str,
    item_id: str,
    liability_type: str,
    liability: Mapping[str, Any],
    synced_at: datetime,
) -> dict[str, Any]:
    interest_rate = _mapping(liability.get("interest_rate"))
    return {
        "source": PLAID_SOURCE,
        "item_name": item_name,
        "item_id": item_id,
        "account_id": _text(liability.get("account_id")),
        "liability_type": liability_type,
        "account_number": _text(liability.get("account_number")),
        "origination_date": _text(liability.get("origination_date")),
        "maturity_date": _text(liability.get("maturity_date")),
        "interest_rate_percentage": _number(
            _first_value(interest_rate.get("percentage"), liability.get("interest_rate_percentage"))
        ),
        "interest_rate_type": _first_text(interest_rate.get("type"), liability.get("interest_rate_type")),
        "last_payment_amount": _number(liability.get("last_payment_amount")),
        "last_payment_date": _text(liability.get("last_payment_date")),
        "next_payment_due_date": _text(liability.get("next_payment_due_date")),
        "next_monthly_payment": _number(liability.get("next_monthly_payment")),
        "original_principal_balance": _number(liability.get("origination_principal_amount")),
        "outstanding_principal_balance": _number(
            _first_value(liability.get("outstanding_principal_balance"), liability.get("principal"))
        ),
        "ytd_interest_paid": _number(liability.get("ytd_interest_paid")),
        "ytd_principal_paid": _number(liability.get("ytd_principal_paid")),
        "minimum_payment_amount": _number(liability.get("minimum_payment_amount")),
        "last_statement_issue_date": _text(liability.get("last_statement_issue_date")),
        "last_statement_balance": _number(liability.get("last_statement_balance")),
        "is_overdue": _bool_int(liability.get("is_overdue")),
        "is_deleted": 0,
        "raw_json": _json(liability),
        "synced_at": _utc(synced_at),
        "sync_version": _sync_version(synced_at),
    }


def run_auth_flow(
    *,
    config: PlaidConfig,
    item_name: str,
    products: tuple[str, ...],
    additional_consented_products: tuple[str, ...],
    optional_products: tuple[str, ...],
    required_if_supported_products: tuple[str, ...],
    host: str,
    port: int,
    redirect_uri: str | None,
    write_env: bool,
    env_path: Path,
    open_browser: bool = True,
) -> dict[str, Any]:
    client = PlaidClient(config)
    link_token_response = client.create_link_token(
        item_name=item_name,
        products=products,
        additional_consented_products=additional_consented_products,
        optional_products=optional_products,
        required_if_supported_products=required_if_supported_products,
        redirect_uri=redirect_uri,
    )
    link_token = str(link_token_response["link_token"])
    result: dict[str, Any] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path.startswith("/") or self.path.startswith("/oauth-return"):
                body = _link_page(link_token=link_token, item_name=item_name).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404)

        def do_POST(self) -> None:
            if self.path != "/exchange":
                self.send_error(404)
                return
            content_length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(content_length).decode("utf-8"))
            token_response = client.exchange_public_token(str(payload["public_token"]))
            access_token = str(token_response["access_token"])
            item_id = str(token_response["item_id"])
            item_payload = _mapping(client.item_get(access_token).get("item"))
            institution_name = _first_text(
                _mapping(payload.get("metadata")).get("institution", {}).get("name")
                if isinstance(_mapping(payload.get("metadata")).get("institution"), Mapping)
                else "",
                item_payload.get("institution_name"),
            )
            env_values = _plaid_env_values(
                item_name=item_name,
                access_token=access_token,
                item_id=item_id,
                institution_name=institution_name,
            )
            if write_env:
                update_env_file(env_path, env_values)
            result.update(
                {
                    "item_name": item_name,
                    "item_id": item_id,
                    "institution_name": institution_name,
                    "env_values": env_values,
                    "write_env": write_env,
                }
            )
            response_body = json.dumps({"ok": True, "write_env": write_env}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)
            threading.Thread(target=self.server.shutdown, daemon=True).start()

        def log_message(self, format: str, *args: Any) -> None:
            return

    server = ThreadingHTTPServer((host, port), Handler)
    url = f"http://{host}:{server.server_port}/"
    if open_browser:
        webbrowser.open(url)
    print(f"Open {url} to link {item_name} with Plaid.")
    server.serve_forever()
    server.server_close()
    if not result:
        raise RuntimeError("Plaid Link flow exited without exchanging a public token")
    return result


def _link_page(*, link_token: str, item_name: str) -> str:
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Link {item_name}</title>
  <script src="https://cdn.plaid.com/link/v2/stable/link-initialize.js"></script>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 40px; }}
    button {{ font: inherit; padding: 10px 14px; }}
    #status {{ margin-top: 16px; }}
  </style>
</head>
<body>
  <button id="link">Link {item_name}</button>
  <div id="status">Plaid Link is loading.</div>
  <script>
    const token = {json.dumps(link_token)};
    localStorage.setItem("pdw_plaid_link_token", token);
    const status = document.getElementById("status");
    const handler = Plaid.create({{
      token,
      receivedRedirectUri: window.location.search.includes("oauth_state_id") ? window.location.href : null,
      onSuccess: async function(public_token, metadata) {{
        status.textContent = "Exchanging token...";
        const response = await fetch("/exchange", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ public_token, metadata }}),
        }});
        if (!response.ok) {{
          status.textContent = "Token exchange failed.";
          return;
        }}
        status.textContent = "Linked. You can close this tab.";
      }},
      onExit: function(err) {{
        status.textContent = err ? (err.error_code + ": " + err.error_message) : "Exited Plaid Link.";
      }},
    }});
    document.getElementById("link").addEventListener("click", function() {{ handler.open(); }});
    handler.open();
  </script>
</body>
</html>"""


def _plaid_env_values(
    *,
    item_name: str,
    access_token: str,
    item_id: str,
    institution_name: str,
) -> dict[str, str]:
    slug = env_slug(item_name)
    existing_items = list(_parse_items_env(os.getenv("PLAID_ITEMS")))
    if item_name not in existing_items:
        existing_items.append(item_name)
    values = {
        "PLAID_ITEMS": ",".join(existing_items),
        f"PLAID_{slug}_ACCESS_TOKEN": access_token,
        f"PLAID_{slug}_ITEM_ID": item_id,
    }
    if institution_name:
        values[f"PLAID_{slug}_INSTITUTION_NAME"] = institution_name
    return values


def _parse_items_env(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    settings = load_settings(require_gmail=False, require_finance=True)
    warehouse = warehouse_from_settings(settings)
    summaries = FinanceSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logging.getLogger("finance_sync"),
    ).sync_all()
    print(
        json.dumps(
            [
                {
                    "item_name": summary.item_name,
                    "item_id": summary.item_id,
                    "institution_name": summary.institution_name,
                    "accounts_written": summary.accounts_written,
                    "transactions_added": summary.transactions_added,
                    "transactions_modified": summary.transactions_modified,
                    "transactions_removed": summary.transactions_removed,
                    "investment_holdings_written": summary.investment_holdings_written,
                    "investment_securities_written": summary.investment_securities_written,
                    "investment_transactions_written": summary.investment_transactions_written,
                    "liabilities_written": summary.liabilities_written,
                }
                for summary in summaries
            ],
            indent=2,
            sort_keys=True,
        )
    )


def auth_main() -> None:
    parser = argparse.ArgumentParser(description="Authorize a Plaid item and print or write env vars.")
    parser.add_argument("--item", required=True, help="Local item name, e.g. capital_one, robinhood, mortgage")
    parser.add_argument("--products", default=None, help="Comma-separated Plaid products for Link")
    parser.add_argument(
        "--additional-consented-products",
        default=None,
        help="Comma-separated Plaid products to consent for later endpoint calls",
    )
    parser.add_argument("--optional-products", default=None, help="Comma-separated optional Plaid products")
    parser.add_argument(
        "--required-if-supported-products",
        default=None,
        help="Comma-separated required-if-supported Plaid products",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--redirect-uri", default=None)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--write-env", action="store_true")
    parser.add_argument("--no-open-browser", action="store_true")
    args = parser.parse_args()

    settings = load_settings(require_gmail=False, require_postgres=False)
    if settings.plaid is None:
        raise ValueError("PLAID_CLIENT_ID and PLAID_SECRET must be set before running Plaid auth")
    config = settings.plaid
    result = run_auth_flow(
        config=config,
        item_name=args.item,
        products=_parse_items_env(args.products) or config.products,
        additional_consented_products=_parse_items_env(args.additional_consented_products)
        if args.additional_consented_products is not None
        else config.additional_consented_products,
        optional_products=_parse_items_env(args.optional_products)
        if args.optional_products is not None
        else config.optional_products,
        required_if_supported_products=_parse_items_env(args.required_if_supported_products)
        if args.required_if_supported_products is not None
        else config.required_if_supported_products,
        host=args.host,
        port=args.port,
        redirect_uri=args.redirect_uri,
        write_env=args.write_env,
        env_path=Path(args.env_file),
        open_browser=not args.no_open_browser,
    )
    if result["write_env"]:
        print(f"Wrote Plaid token env vars for {result['item_name']} to {args.env_file}")
        return
    for key, value in result["env_values"].items():
        print(f"{key}={value}")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _first_text(*values: Any) -> str:
    for value in values:
        text = _text(value).strip()
        if text:
            return text
    return ""


def _first_value(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _number(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def _bool_int(value: Any) -> int:
    return 1 if bool(value) else 0


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True, separators=(",", ":"), default=str)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _sync_version(value: datetime) -> int:
    return int(_utc(value).timestamp() * 1_000_000)


def _parse_datetime(value: Any) -> datetime:
    if not value:
        return EPOCH_UTC
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return EPOCH_UTC
    return _utc(parsed)


if __name__ == "__main__":
    main()
