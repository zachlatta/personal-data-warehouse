from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import hashlib
from typing import Any

import requests

from personal_data_warehouse.config import PlaidConfig, Settings, load_settings
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.schema import PlaidLinkedItem
from personal_data_warehouse.warehouse import warehouse_from_settings

PLAID_ENV_BASE_URLS = {
    "sandbox": "https://sandbox.plaid.com",
    "development": "https://development.plaid.com",
    "production": "https://production.plaid.com",
}
PLAID_PAGE_SIZE = 500

# Terminal sync_state status for an Item that only a fresh Link run can repair.
PLAID_STATUS_ACTION_REQUIRED = "action_required"

# Plaid error codes that retrying can never clear: the Item's credentials,
# consent, or account selection have to be re-established through Link.
# Treating them as run failures turns one dead institution into a permanently
# red sync job: a single linked Item answering NO_ACCOUNTS produced 262
# consecutive failed runs over five days, burying every other schedule signal.
# They get their own terminal state instead: recorded in plaid.sync_state,
# warned about on every run, and surfaced in the asset's Dagster metadata,
# without pretending the pull succeeded and without failing the run.
PLAID_ACTION_REQUIRED_ERROR_CODES = frozenset(
    {
        "ACCESS_NOT_GRANTED",
        "INSUFFICIENT_CREDENTIALS",
        "INVALID_CREDENTIALS",
        "INVALID_MFA",
        "INVALID_SEND_METHOD",
        "INVALID_UPDATED_USERNAME",
        "ITEM_LOCKED",
        "ITEM_LOGIN_REQUIRED",
        "ITEM_NOT_SUPPORTED",
        "MFA_NOT_SUPPORTED",
        "NO_ACCOUNTS",
        "NO_AUTH_ACCOUNTS",
        "NO_INVESTMENT_ACCOUNTS",
        "NO_LIABILITY_ACCOUNTS",
        "PENDING_DISCONNECT",
        "PENDING_EXPIRATION",
        "USER_INPUT_TIMEOUT",
        "USER_PERMISSION_REVOKED",
    }
)


@dataclass(frozen=True)
class PlaidSyncSummary:
    items: int = 0
    accounts: int = 0
    transactions: int = 0
    removed_transactions: int = 0
    investment_securities: int = 0
    investment_holdings: int = 0
    investment_transactions: int = 0
    liabilities: int = 0
    action_required: int = 0


class PlaidAPIError(RuntimeError):
    """Raised when a Plaid HTTP request fails."""


class PlaidClient:
    def __init__(self, config: PlaidConfig, *, session: requests.Session | None = None) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._base_url = (config.base_url or PLAID_ENV_BASE_URLS[config.environment]).rstrip("/")

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def client_user_id(self) -> str:
        # Plaid explicitly forbids PII such as an email address in
        # client_user_id. Keep the local account label in Postgres and send
        # Plaid only a stable, opaque identifier derived from it.
        digest = hashlib.sha256(self._config.account.encode("utf-8")).hexdigest()
        return f"pdw-{digest}"

    def create_link_token(self, *, account: str | None = None) -> dict[str, Any]:
        account_label = account or self._config.account
        digest = hashlib.sha256(account_label.encode("utf-8")).hexdigest()
        configured_products = list(self._config.products)
        required_products = ["transactions"] if "transactions" in configured_products else configured_products
        payload: dict[str, Any] = {
            "client_name": self._config.client_name,
            "user": {"client_user_id": f"pdw-{digest}"},
            "products": required_products,
            "country_codes": list(self._config.country_codes),
            "language": self._config.language,
        }
        additional_products = [product for product in configured_products if product not in required_products]
        if additional_products:
            payload["additional_consented_products"] = additional_products
        if "transactions" in self._config.products:
            payload["transactions"] = {"days_requested": self._config.transactions_lookback_days}
        if self._config.redirect_uri:
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

    def transactions_sync(self, access_token: str, *, cursor: str | None, count: int = PLAID_PAGE_SIZE) -> dict[str, Any]:
        payload: dict[str, Any] = {"access_token": access_token, "count": count}
        if cursor:
            payload["cursor"] = cursor
        return self._post("/transactions/sync", payload)

    def investments_holdings_get(self, access_token: str) -> dict[str, Any]:
        return self._post("/investments/holdings/get", {"access_token": access_token})

    def investments_transactions_get(
        self,
        access_token: str,
        *,
        start_date: date,
        end_date: date,
        count: int = PLAID_PAGE_SIZE,
        offset: int = 0,
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
        body = {
            "client_id": self._config.client_id,
            "secret": self._config.secret,
            **dict(payload),
        }
        response = self._session.post(
            self._base_url + endpoint,
            json=body,
            timeout=self._config.request_timeout_seconds,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise PlaidAPIError(_plaid_error_message(response)) from exc
        try:
            data = response.json()
        except ValueError as exc:
            raise PlaidAPIError(f"Plaid {endpoint} returned non-JSON response") from exc
        if not isinstance(data, dict):
            raise PlaidAPIError(f"Plaid {endpoint} returned unexpected response shape")
        if data.get("error_code"):
            raise PlaidAPIError(_plaid_error_from_json(data))
        return data


class PlaidSyncRunner:
    def __init__(
        self,
        *,
        config: PlaidConfig,
        warehouse: PostgresWarehouse,
        logger,
        plaid_client: PlaidClient | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._warehouse = warehouse
        self._logger = logger
        self._client = plaid_client or PlaidClient(config)
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync_all(self) -> PlaidSyncSummary:
        self._warehouse.ensure_plaid_tables()
        linked_items = self._warehouse.load_plaid_item_tokens()
        state = self._warehouse.load_plaid_sync_state()
        summary = PlaidSyncSummary(items=len(linked_items))
        failures: list[str] = []
        for item in linked_items:
            item_summary, item_failures = self._sync_item(item, state)
            summary = _merge_summary(summary, item_summary)
            failures.extend(item_failures)
        if failures:
            raise RuntimeError("Plaid sync failed: " + "; ".join(failures))
        return summary

    def _sync_item(
        self,
        item: PlaidLinkedItem,
        state: Mapping[tuple[str, str, str], Mapping[str, Any]],
    ) -> tuple[PlaidSyncSummary, list[str]]:
        synced_at = _ensure_utc(self._now())
        sync_version = _sync_version(synced_at)
        item_id = item.item_id
        failures: list[str] = []
        institution = item.institution_name or item.institution_id or "linked institution"
        try:
            item_response = self._client.item_get(item.access_token)
        except Exception as exc:
            error = self._safe_error(exc, item)
            products = ("accounts", *self._config.products)
            fatal = False
            for product in products:
                if self._record_failure(state, item.account, item_id, product, error, synced_at, institution):
                    fatal = True
            summary = PlaidSyncSummary(action_required=0 if fatal else len(products))
            return summary, [f"{institution} item: {error}"] if fatal else []

        plaid_item = dict(item_response.get("item") or {})
        item_id = str(plaid_item.get("item_id") or item.item_id)
        institution_id = str(plaid_item.get("institution_id") or item.institution_id)
        product_fields = ("available_products", "billed_products", "products")
        has_product_metadata = any(field in plaid_item for field in product_fields)
        item_products = {
            product
            for field in product_fields
            for product in _string_list(plaid_item.get(field))
        }

        def supports(product: str) -> bool:
            return not has_product_metadata or product in item_products

        self._warehouse.insert_plaid_items(
            [
                {
                    "account": item.account,
                    "item_id": item_id,
                    "institution_id": institution_id,
                    "institution_name": item.institution_name,
                    "available_products": _string_list(plaid_item.get("available_products")),
                    "billed_products": _string_list(plaid_item.get("billed_products")),
                    "webhook": str(plaid_item.get("webhook") or ""),
                    "consent_expiration_time": _parse_optional_datetime(plaid_item.get("consent_expiration_time")),
                    "error_json": plaid_item.get("error") or {},
                    "raw_json": plaid_item,
                    "linked_at": synced_at,
                    "synced_at": synced_at,
                    "sync_version": sync_version,
                }
            ]
        )
        summary = PlaidSyncSummary()

        try:
            account_count = self._sync_accounts(
                item=item,
                item_id=item_id,
                synced_at=synced_at,
                sync_version=sync_version,
            )
        except Exception as exc:
            error = self._safe_error(exc, item)
            if self._record_failure(state, item.account, item_id, "accounts", error, synced_at, institution):
                failures.append(f"{institution} accounts: {error}")
            else:
                summary = _merge_summary(summary, PlaidSyncSummary(action_required=1))
        else:
            summary = _merge_summary(summary, PlaidSyncSummary(accounts=account_count))
            self._record_state(item.account, item_id, "accounts", "", "ok", "", synced_at)

        if "transactions" in self._config.products and not supports("transactions"):
            cursor = str(state.get((item.account, item_id, "transactions"), {}).get("cursor") or "")
            self._record_state(item.account, item_id, "transactions", cursor, "unsupported", "", synced_at)
        elif "transactions" in self._config.products:
            try:
                transaction_count, removed_count, next_cursor = self._sync_transactions(
                    item=item,
                    item_id=item_id,
                    cursor=str(state.get((item.account, item_id, "transactions"), {}).get("cursor") or ""),
                    synced_at=synced_at,
                    sync_version=sync_version,
                )
            except Exception as exc:
                error = self._safe_error(exc, item)
                if self._record_failure(state, item.account, item_id, "transactions", error, synced_at, institution):
                    failures.append(f"{institution} transactions: {error}")
                else:
                    summary = _merge_summary(summary, PlaidSyncSummary(action_required=1))
            else:
                self._record_state(item.account, item_id, "transactions", next_cursor, "ok", "", synced_at)
                summary = _merge_summary(
                    summary,
                    PlaidSyncSummary(transactions=transaction_count, removed_transactions=removed_count),
                )

        if "investments" in self._config.products and not supports("investments"):
            self._record_state(item.account, item_id, "investments", "", "unsupported", "", synced_at)
        elif "investments" in self._config.products:
            try:
                investments_summary = self._sync_investments(
                    item=item,
                    item_id=item_id,
                    synced_at=synced_at,
                    sync_version=sync_version,
                )
            except Exception as exc:
                error = self._safe_error(exc, item)
                if self._record_failure(state, item.account, item_id, "investments", error, synced_at, institution):
                    failures.append(f"{institution} investments: {error}")
                else:
                    summary = _merge_summary(summary, PlaidSyncSummary(action_required=1))
            else:
                self._record_state(item.account, item_id, "investments", "", "ok", "", synced_at)
                summary = _merge_summary(summary, investments_summary)

        if "liabilities" in self._config.products and not supports("liabilities"):
            self._record_state(item.account, item_id, "liabilities", "", "unsupported", "", synced_at)
        elif "liabilities" in self._config.products:
            try:
                liability_count = self._sync_liabilities(
                    item=item,
                    item_id=item_id,
                    synced_at=synced_at,
                    sync_version=sync_version,
                )
            except Exception as exc:
                error = self._safe_error(exc, item)
                if self._record_failure(state, item.account, item_id, "liabilities", error, synced_at, institution):
                    failures.append(f"{institution} liabilities: {error}")
                else:
                    summary = _merge_summary(summary, PlaidSyncSummary(action_required=1))
            else:
                self._record_state(item.account, item_id, "liabilities", "", "ok", "", synced_at)
                summary = _merge_summary(summary, PlaidSyncSummary(liabilities=liability_count))

        return summary, failures

    def _sync_accounts(
        self,
        *,
        item: PlaidLinkedItem,
        item_id: str,
        synced_at: datetime,
        sync_version: int,
    ) -> int:
        response = self._client.accounts_get(item.access_token)
        accounts = [
            _account_row(account=item.account, item_id=item_id, row=dict(raw), synced_at=synced_at, sync_version=sync_version)
            for raw in response.get("accounts") or []
            if isinstance(raw, Mapping)
        ]
        self._warehouse.insert_plaid_accounts(accounts)
        self._warehouse.mark_missing_plaid_accounts_removed(
            account=item.account,
            item_id=item_id,
            active_account_ids={row["account_id"] for row in accounts},
            synced_at=synced_at,
        )
        return len(accounts)

    def _sync_transactions(
        self,
        *,
        item: PlaidLinkedItem,
        item_id: str,
        cursor: str,
        synced_at: datetime,
        sync_version: int,
    ) -> tuple[int, int, str]:
        added_or_modified: list[dict[str, Any]] = []
        removed_ids: list[str] = []
        next_cursor = cursor
        while True:
            response = self._client.transactions_sync(item.access_token, cursor=next_cursor or None, count=PLAID_PAGE_SIZE)
            for raw in [*(response.get("added") or []), *(response.get("modified") or [])]:
                if isinstance(raw, Mapping):
                    added_or_modified.append(
                        _transaction_row(
                            account=item.account,
                            item_id=item_id,
                            row=dict(raw),
                            synced_at=synced_at,
                            sync_version=sync_version,
                        )
                    )
            for raw in response.get("removed") or []:
                if isinstance(raw, Mapping) and raw.get("transaction_id"):
                    removed_ids.append(str(raw["transaction_id"]))
            next_cursor = str(response.get("next_cursor") or next_cursor or "")
            if not response.get("has_more"):
                break
        self._warehouse.insert_plaid_transactions(added_or_modified)
        removed_count = self._warehouse.mark_plaid_transactions_removed(
            account=item.account,
            item_id=item_id,
            transaction_ids=removed_ids,
            synced_at=synced_at,
        ) or 0
        return len(added_or_modified), removed_count, next_cursor

    def _sync_investments(
        self,
        *,
        item: PlaidLinkedItem,
        item_id: str,
        synced_at: datetime,
        sync_version: int,
    ) -> PlaidSyncSummary:
        holdings_response = self._client.investments_holdings_get(item.access_token)
        security_by_id: dict[str, dict[str, Any]] = {}
        for raw in holdings_response.get("securities") or []:
            if not isinstance(raw, Mapping):
                continue
            security = _security_row(
                account=item.account,
                row=dict(raw),
                synced_at=synced_at,
                sync_version=sync_version,
            )
            if security["security_id"]:
                security_by_id[security["security_id"]] = security
        holdings = [
            _holding_row(
                account=item.account,
                item_id=item_id,
                row=dict(raw),
                synced_at=synced_at,
                sync_version=sync_version,
            )
            for raw in holdings_response.get("holdings") or []
            if isinstance(raw, Mapping)
        ]
        self._warehouse.insert_plaid_investment_holdings(holdings)
        self._warehouse.delete_missing_plaid_investment_holdings(
            account=item.account,
            item_id=item_id,
            active_holding_keys={(row["account_id"], row["security_id"]) for row in holdings},
        )

        investment_transactions: list[dict[str, Any]] = []
        end_date = synced_at.date()
        start_date = end_date - timedelta(days=self._config.transactions_lookback_days)
        offset = 0
        while True:
            response = self._client.investments_transactions_get(
                item.access_token,
                start_date=start_date,
                end_date=end_date,
                count=PLAID_PAGE_SIZE,
                offset=offset,
            )
            page = [raw for raw in response.get("investment_transactions") or [] if isinstance(raw, Mapping)]
            for raw in response.get("securities") or []:
                if not isinstance(raw, Mapping):
                    continue
                security = _security_row(
                    account=item.account,
                    row=dict(raw),
                    synced_at=synced_at,
                    sync_version=sync_version,
                )
                if security["security_id"]:
                    security_by_id[security["security_id"]] = security
            investment_transactions.extend(
                _investment_transaction_row(
                    account=item.account,
                    item_id=item_id,
                    row=dict(raw),
                    synced_at=synced_at,
                    sync_version=sync_version,
                )
                for raw in page
            )
            total = int(response.get("total_investment_transactions") or len(investment_transactions))
            offset += len(page)
            if offset >= total or not page:
                break
        securities = list(security_by_id.values())
        self._warehouse.insert_plaid_investment_securities(securities)
        self._warehouse.insert_plaid_investment_transactions(investment_transactions)
        return PlaidSyncSummary(
            investment_securities=len(securities),
            investment_holdings=len(holdings),
            investment_transactions=len(investment_transactions),
        )

    def _sync_liabilities(
        self,
        *,
        item: PlaidLinkedItem,
        item_id: str,
        synced_at: datetime,
        sync_version: int,
    ) -> int:
        response = self._client.liabilities_get(item.access_token)
        liabilities = response.get("liabilities") or {}
        rows: list[dict[str, Any]] = []
        if isinstance(liabilities, Mapping):
            for liability_type, values in liabilities.items():
                if not isinstance(values, list):
                    continue
                for raw in values:
                    if isinstance(raw, Mapping):
                        rows.append(
                            _liability_row(
                                account=item.account,
                                item_id=item_id,
                                liability_type=str(liability_type),
                                row=dict(raw),
                                synced_at=synced_at,
                                sync_version=sync_version,
                            )
                        )
        self._warehouse.insert_plaid_liabilities(rows)
        self._warehouse.delete_missing_plaid_liabilities(
            account=item.account,
            item_id=item_id,
            active_liability_keys={(row["account_id"], row["liability_type"]) for row in rows},
        )
        return len(rows)

    def _record_failure(
        self,
        state: Mapping[tuple[str, str, str], Mapping[str, Any]],
        account: str,
        item_id: str,
        product: str,
        error: str,
        attempted_at: datetime,
        institution: str,
    ) -> bool:
        """Persist a failed product pull; return True when it should fail the run.

        Permanent Item errors (see ``PLAID_ACTION_REQUIRED_ERROR_CODES``) are
        recorded under their own status and reported as non-fatal: only a fresh
        Link run repairs them, so retrying every 30 minutes forever just keeps
        the schedule red. The prior cursor and last successful timestamp are
        preserved either way, so re-linking resumes instead of replaying.
        """

        action_required = _plaid_error_code(error) in PLAID_ACTION_REQUIRED_ERROR_CODES
        previous = state.get((account, item_id, product), {})
        previous_success = previous.get("last_synced_at")
        if not isinstance(previous_success, datetime):
            previous_success = datetime.fromtimestamp(0, tz=UTC)
        self._warehouse.insert_plaid_sync_state(
            account=account,
            item_id=item_id,
            product=product,
            cursor=str(previous.get("cursor") or ""),
            status=PLAID_STATUS_ACTION_REQUIRED if action_required else "failed",
            error=error,
            last_synced_at=_ensure_utc(previous_success),
            updated_at=attempted_at,
        )
        if action_required:
            self._logger.warning(
                "Plaid %s %s needs re-linking (run `pdw ingest plaid link`): %s",
                institution,
                product,
                error,
            )
        return not action_required

    def _safe_error(self, exc: Exception, item: PlaidLinkedItem) -> str:
        message = str(exc) or type(exc).__name__
        for secret in (item.access_token, self._config.secret, self._config.client_id):
            if secret:
                message = message.replace(secret, "[redacted]")
        return message[:2000]

    def _record_state(
        self,
        account: str,
        item_id: str,
        product: str,
        cursor: str,
        status: str,
        error: str,
        synced_at: datetime,
    ) -> None:
        self._warehouse.insert_plaid_sync_state(
            account=account,
            item_id=item_id,
            product=product,
            cursor=cursor,
            status=status,
            error=error,
            last_synced_at=synced_at,
            updated_at=synced_at,
        )


def sync_from_settings(*, settings: Settings | None = None, logger=None) -> PlaidSyncSummary:
    settings = settings or load_settings(require_gmail=False, require_plaid=True)
    if settings.plaid is None:
        raise ValueError("Plaid is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        return PlaidSyncRunner(config=settings.plaid, warehouse=warehouse, logger=logger or _NullLogger()).sync_all()
    finally:
        warehouse.close()


def _merge_summary(left: PlaidSyncSummary, right: PlaidSyncSummary) -> PlaidSyncSummary:
    return PlaidSyncSummary(
        items=left.items + right.items,
        accounts=left.accounts + right.accounts,
        transactions=left.transactions + right.transactions,
        removed_transactions=left.removed_transactions + right.removed_transactions,
        investment_securities=left.investment_securities + right.investment_securities,
        investment_holdings=left.investment_holdings + right.investment_holdings,
        investment_transactions=left.investment_transactions + right.investment_transactions,
        liabilities=left.liabilities + right.liabilities,
        action_required=left.action_required + right.action_required,
    )


def _plaid_error_code(message: str) -> str:
    """Extract the leading Plaid error code from a `"<CODE>: <message>"` string.

    Only messages Plaid itself produced (via ``_plaid_error_from_json``) carry a
    code, so anything else — a transport error, a bug in our own row mapping —
    stays fatal by default.
    """

    code, separator, _ = message.partition(":")
    if not separator:
        return ""
    code = code.strip()
    return code if code and code.isupper() else ""


def _account_row(*, account: str, item_id: str, row: dict[str, Any], synced_at: datetime, sync_version: int) -> dict[str, Any]:
    balances = row.get("balances") if isinstance(row.get("balances"), Mapping) else {}
    return {
        "account": account,
        "item_id": item_id,
        "account_id": str(row.get("account_id") or ""),
        "name": str(row.get("name") or ""),
        "official_name": str(row.get("official_name") or ""),
        "mask": str(row.get("mask") or ""),
        "type": str(row.get("type") or ""),
        "subtype": str(row.get("subtype") or ""),
        "available_balance": _float(balances.get("available")),
        "current_balance": _float(balances.get("current")),
        "limit_balance": _float(balances.get("limit")),
        "iso_currency_code": str(balances.get("iso_currency_code") or row.get("iso_currency_code") or ""),
        "unofficial_currency_code": str(balances.get("unofficial_currency_code") or row.get("unofficial_currency_code") or ""),
        "is_removed": 0,
        "raw_json": row,
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def _transaction_row(*, account: str, item_id: str, row: dict[str, Any], synced_at: datetime, sync_version: int) -> dict[str, Any]:
    return {
        "account": account,
        "item_id": item_id,
        "account_id": str(row.get("account_id") or ""),
        "transaction_id": str(row.get("transaction_id") or ""),
        "posted_at": _parse_date(row.get("date")),
        "authorized_at": _parse_date(row.get("authorized_date") or row.get("authorized_datetime")),
        "name": str(row.get("name") or ""),
        "merchant_name": str(row.get("merchant_name") or ""),
        "amount": _float(row.get("amount")),
        "iso_currency_code": str(row.get("iso_currency_code") or ""),
        "unofficial_currency_code": str(row.get("unofficial_currency_code") or ""),
        "category_json": row.get("category") or [],
        "payment_channel": str(row.get("payment_channel") or ""),
        "pending": _int_bool(row.get("pending")),
        "pending_transaction_id": str(row.get("pending_transaction_id") or ""),
        "is_removed": 0,
        "raw_json": row,
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def _security_row(*, account: str, row: dict[str, Any], synced_at: datetime, sync_version: int) -> dict[str, Any]:
    return {
        "account": account,
        "security_id": str(row.get("security_id") or ""),
        "name": str(row.get("name") or ""),
        "ticker_symbol": str(row.get("ticker_symbol") or ""),
        "type": str(row.get("type") or ""),
        "close_price": _float(row.get("close_price")),
        "close_price_as_of": _parse_date(row.get("close_price_as_of")),
        "iso_currency_code": str(row.get("iso_currency_code") or ""),
        "unofficial_currency_code": str(row.get("unofficial_currency_code") or ""),
        "raw_json": row,
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def _holding_row(*, account: str, item_id: str, row: dict[str, Any], synced_at: datetime, sync_version: int) -> dict[str, Any]:
    return {
        "account": account,
        "item_id": item_id,
        "account_id": str(row.get("account_id") or ""),
        "security_id": str(row.get("security_id") or ""),
        "quantity": _float(row.get("quantity")),
        "institution_value": _float(row.get("institution_value")),
        "institution_price": _float(row.get("institution_price")),
        "institution_price_as_of": _parse_date(row.get("institution_price_as_of")),
        "cost_basis": _float(row.get("cost_basis")),
        "iso_currency_code": str(row.get("iso_currency_code") or ""),
        "unofficial_currency_code": str(row.get("unofficial_currency_code") or ""),
        "raw_json": row,
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def _investment_transaction_row(*, account: str, item_id: str, row: dict[str, Any], synced_at: datetime, sync_version: int) -> dict[str, Any]:
    return {
        "account": account,
        "item_id": item_id,
        "account_id": str(row.get("account_id") or ""),
        "investment_transaction_id": str(row.get("investment_transaction_id") or ""),
        "security_id": str(row.get("security_id") or ""),
        "transaction_at": _parse_date(row.get("date")),
        "name": str(row.get("name") or ""),
        "quantity": _float(row.get("quantity")),
        "amount": _float(row.get("amount")),
        "price": _float(row.get("price")),
        "fees": _float(row.get("fees")),
        "type": str(row.get("type") or ""),
        "subtype": str(row.get("subtype") or ""),
        "iso_currency_code": str(row.get("iso_currency_code") or ""),
        "unofficial_currency_code": str(row.get("unofficial_currency_code") or ""),
        "raw_json": row,
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def _liability_row(
    *,
    account: str,
    item_id: str,
    liability_type: str,
    row: dict[str, Any],
    synced_at: datetime,
    sync_version: int,
) -> dict[str, Any]:
    return {
        "account": account,
        "item_id": item_id,
        "account_id": str(row.get("account_id") or ""),
        "liability_type": liability_type,
        "last_payment_amount": _float(row.get("last_payment_amount")),
        "last_statement_balance": _float(row.get("last_statement_balance")),
        "minimum_payment_amount": _float(row.get("minimum_payment_amount")),
        "next_payment_due_at": _parse_date(row.get("next_payment_due_date")),
        "origination_principal_amount": _float(row.get("origination_principal_amount")),
        "outstanding_interest_amount": _float(row.get("outstanding_interest_amount")),
        "is_overdue": _int_bool(row.get("is_overdue")),
        "iso_currency_code": str(row.get("iso_currency_code") or ""),
        "unofficial_currency_code": str(row.get("unofficial_currency_code") or ""),
        "raw_json": row,
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def _parse_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    if isinstance(value, str) and value:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return datetime.fromtimestamp(0, tz=UTC)


def _parse_optional_datetime(value: Any) -> datetime:
    return _parse_date(value)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _sync_version(value: datetime) -> int:
    return int(_ensure_utc(value).timestamp() * 1_000_000)


def _float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def _int_bool(value: Any) -> int:
    return 1 if bool(value) else 0


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _plaid_error_message(response) -> str:
    try:
        data = response.json()
    except ValueError:
        return f"Plaid HTTP {response.status_code}"
    if isinstance(data, dict):
        return _plaid_error_from_json(data)
    return f"Plaid HTTP {response.status_code}"


def _plaid_error_from_json(data: Mapping[str, Any]) -> str:
    code = str(data.get("error_code") or "PLAID_ERROR")
    message = str(data.get("error_message") or data.get("display_message") or "Plaid request failed")
    return f"{code}: {message}"


class _NullLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass
