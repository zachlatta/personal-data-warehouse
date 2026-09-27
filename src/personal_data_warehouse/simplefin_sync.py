"""SimpleFIN finance sync: the second provider feed beside Plaid.

SimpleFIN (https://www.simplefin.org/protocol.html) is a read-only protocol
served by the SimpleFIN Bridge: one claimed access URL covers every
institution the user connected there, and ``GET /accounts`` returns all of
them at once with their balance, the transactions in a date window, and (for
brokerages) holdings. There is no per-institution token, no cursor and no
webhook: the bridge refreshes each connection about once a day, and this
runner re-reads a bounded window each run and upserts what it finds.

What lands here is faithful provider data in ``base_simplefin.*``. Turning it
into the owner's stocks and flows — and reconciling it with the Plaid rows
that describe the SAME accounts — is the finance ledger's job
(``finance_ledger.py``): it resolves each SimpleFIN account onto the logical
account Plaid founded by institution + mask (a transaction-overlap match when
the bridge prints no mask) and dedups the flows against Plaid's by exact
amount and posting date. Neither provider learns about the other here.

Sign conventions, per the protocol: transaction ``amount`` is positive when
money enters the account (the ledger's convention already, so no negation),
and ``balance`` is signed from the customer's side — a credit card owing
money reads negative. Dates are UNIX epochs; a pending transaction may carry
``posted = 0``.

Credential: the setup token Zach copies out of the bridge is a base64 claim
URL that can be POSTed exactly once; the response body is the access URL
(``https://user:pass@host/simplefin``). ``python -m
personal_data_warehouse.simplefin_sync claim`` does that exchange and prints
the URL, which is what ``SIMPLEFIN_ACCESS_URL`` carries. The URL is a secret
and is redacted from every recorded error.
"""

from __future__ import annotations

import base64
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests

from personal_data_warehouse.config import SimpleFINConfig, Settings, load_settings
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.warehouse import warehouse_from_settings

#: The bridge's own limit on one request's date range.
SIMPLEFIN_MAX_WINDOW_DAYS = 90

#: ``ops.simplefin_sync_state.account_id`` of the row that carries the access
#: URL's own verdict rather than one account's.
SIMPLEFIN_CONNECTION_STATE_ID = ""

# Terminal statuses. `action_required` is the credential: the bridge answered
# 402/403 (the access URL was revoked, or the subscription lapsed) and no retry
# clears it -- a human has to issue a new setup token. `attention` is the
# bridge's own per-connection message ("Connection to X may need attention"),
# which usually means the institution wants a re-login inside the bridge; the
# data keeps flowing for every other connection, so it colours the row rather
# than failing the run.
SIMPLEFIN_STATUS_ACTION_REQUIRED = "action_required"
SIMPLEFIN_STATUS_ATTENTION = "attention"
SIMPLEFIN_STATUS_OK = "ok"
SIMPLEFIN_STATUS_FAILED = "failed"


class SimpleFINAPIError(RuntimeError):
    """Raised when a SimpleFIN request fails for a retryable reason."""


class SimpleFINAuthError(SimpleFINAPIError):
    """The access URL itself was refused: only a fresh setup token repairs it."""


@dataclass(frozen=True)
class SimpleFINSyncSummary:
    accounts: int = 0
    transactions: int = 0
    removed_transactions: int = 0
    holdings: int = 0
    requests: int = 0
    action_required: int = 0
    attention: int = 0


def claim_setup_token(setup_token: str, *, session: requests.Session | None = None, timeout: int = 60) -> str:
    """Exchange a one-time setup token for the access URL.

    The token is base64 of a claim URL; POSTing it once returns the access URL
    and invalidates the token. Never log either.
    """

    token = setup_token.strip()
    if not token:
        raise ValueError("a SimpleFIN setup token is required")
    try:
        claim_url = base64.b64decode(token, validate=True).decode("utf-8").strip()
    except Exception as exc:  # noqa: BLE001 - one message for every malformed token
        raise ValueError("the SimpleFIN setup token is not base64 of a claim URL") from exc
    if not claim_url.startswith("https://"):
        raise ValueError("the SimpleFIN setup token does not decode to an https claim URL")
    response = (session or requests.Session()).post(claim_url, headers={"Content-Length": "0"}, timeout=timeout)
    if response.status_code != 200:
        raise SimpleFINAPIError(
            f"SimpleFIN claim returned HTTP {response.status_code}: a setup token can be claimed only once"
        )
    access_url = response.text.strip()
    if not access_url.startswith("https://") or "@" not in access_url:
        raise SimpleFINAPIError("SimpleFIN claim did not return an access URL")
    return access_url


def redact_access_url(access_url: str, text: str) -> str:
    """Strip the access URL and its credential from ``text``."""

    if not access_url:
        return text
    parts = urlsplit(access_url)
    out = text.replace(access_url, "[redacted]")
    if parts.password:
        out = out.replace(parts.password, "[redacted]")
    if parts.username:
        out = out.replace(parts.username, "[redacted]")
    return out


class SimpleFINClient:
    def __init__(self, config: SimpleFINConfig, *, session: requests.Session | None = None) -> None:
        self._config = config
        self._session = session or requests.Session()
        parts = urlsplit(config.access_url)
        self._auth = (parts.username or "", parts.password or "")
        netloc = parts.hostname or ""
        if parts.port:
            netloc = f"{netloc}:{parts.port}"
        self._base_url = urlunsplit((parts.scheme, netloc, parts.path.rstrip("/"), "", ""))

    @property
    def base_url(self) -> str:
        return self._base_url

    def accounts(self, *, start: datetime, end: datetime, pending: bool = True) -> dict[str, Any]:
        """``GET /accounts`` for one window; ``end`` is exclusive per the protocol."""

        params: dict[str, Any] = {
            "start-date": int(_ensure_utc(start).timestamp()),
            "end-date": int(_ensure_utc(end).timestamp()),
        }
        if pending:
            params["pending"] = 1
        try:
            response = self._session.get(
                self._base_url + "/accounts",
                params=params,
                auth=self._auth,
                timeout=self._config.request_timeout_seconds,
            )
        except requests.RequestException as exc:
            raise SimpleFINAPIError(f"SimpleFIN request failed: {exc}") from exc
        if response.status_code in (401, 402, 403):
            raise SimpleFINAuthError(
                f"SimpleFIN refused the access URL (HTTP {response.status_code}): "
                "claim a new setup token and set SIMPLEFIN_ACCESS_URL"
            )
        if response.status_code != 200:
            raise SimpleFINAPIError(f"SimpleFIN /accounts returned HTTP {response.status_code}")
        try:
            data = response.json()
        except ValueError as exc:
            raise SimpleFINAPIError("SimpleFIN /accounts returned non-JSON") from exc
        if not isinstance(data, Mapping) or not isinstance(data.get("accounts"), list):
            raise SimpleFINAPIError("SimpleFIN /accounts returned an unexpected shape")
        return dict(data)


class SimpleFINSyncRunner:
    def __init__(
        self,
        *,
        config: SimpleFINConfig,
        warehouse: PostgresWarehouse,
        logger,
        client: SimpleFINClient | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._warehouse = warehouse
        self._logger = logger
        self._client = client or SimpleFINClient(config)
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync_all(self) -> SimpleFINSyncSummary:
        self._warehouse.ensure_simplefin_tables()
        owner = self._config.account
        synced_at = _ensure_utc(self._now())
        sync_version = _sync_version(synced_at)
        state = self._warehouse.load_simplefin_sync_state(account=owner)

        start = self._window_start(state, synced_at)
        windows = list(_windows(start, synced_at, timedelta(days=self._config.window_days)))
        accounts_by_id: dict[str, dict[str, Any]] = {}
        transactions_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        holdings: list[dict[str, Any]] = []
        errors: list[Any] = []
        try:
            for window_start, window_end in windows:
                response = self._client.accounts(start=window_start, end=window_end, pending=True)
                errors = list(response.get("errors") or [])
                for raw in response["accounts"]:
                    if not isinstance(raw, Mapping):
                        continue
                    account_row = _account_row(owner=owner, raw=dict(raw), synced_at=synced_at, sync_version=sync_version)
                    if not account_row["account_id"]:
                        continue
                    # The last window answers newest, and every window carries the
                    # same current balance, so the final upsert is the live one.
                    accounts_by_id[account_row["account_id"]] = account_row
                    for tx in raw.get("transactions") or []:
                        if not isinstance(tx, Mapping):
                            continue
                        row = _transaction_row(
                            owner=owner,
                            account_id=account_row["account_id"],
                            raw=dict(tx),
                            synced_at=synced_at,
                            sync_version=sync_version,
                        )
                        if row["transaction_id"]:
                            transactions_by_key[(row["account_id"], row["transaction_id"])] = row
                    if window_end == windows[-1][1]:
                        for holding in raw.get("holdings") or []:
                            if isinstance(holding, Mapping):
                                row = _holding_row(
                                    owner=owner,
                                    account_id=account_row["account_id"],
                                    raw=dict(holding),
                                    synced_at=synced_at,
                                    sync_version=sync_version,
                                )
                                if row["holding_id"]:
                                    holdings.append(row)
        except SimpleFINAuthError as exc:
            error = self._safe_error(exc)
            self._logger.warning("SimpleFIN access URL refused; claim a new setup token: %s", error)
            self._record_state(owner, SIMPLEFIN_CONNECTION_STATE_ID, state, SIMPLEFIN_STATUS_ACTION_REQUIRED, error, synced_at)
            return SimpleFINSyncSummary(requests=len(windows), action_required=1)
        except Exception as exc:
            error = self._safe_error(exc)
            self._record_state(owner, SIMPLEFIN_CONNECTION_STATE_ID, state, SIMPLEFIN_STATUS_FAILED, error, synced_at)
            raise SimpleFINAPIError(f"SimpleFIN sync failed: {error}") from exc

        account_rows = list(accounts_by_id.values())
        transaction_rows = list(transactions_by_key.values())
        self._warehouse.insert_simplefin_accounts(account_rows)
        self._warehouse.mark_missing_simplefin_accounts_removed(
            account=owner, active_account_ids=set(accounts_by_id), synced_at=synced_at
        )
        self._warehouse.insert_simplefin_transactions(transaction_rows)
        # A pending transaction the bridge no longer reports inside the window
        # either posted under a NEW id (the protocol allows that) or was
        # cancelled. Either way the pending row is no longer a fact about the
        # account; tombstone it so the ledger drops its provisional flow. Posted
        # rows are never tombstoned by absence: the bridge serves a bounded
        # window and an old row falling out of it is not a removal.
        removed = self._warehouse.mark_missing_pending_simplefin_transactions_removed(
            account=owner,
            active_keys=set(transactions_by_key),
            window_start=start,
            synced_at=synced_at,
        )
        holdings_by_account: dict[str, set[str]] = {}
        for row in holdings:
            holdings_by_account.setdefault(row["account_id"], set()).add(row["holding_id"])
        self._warehouse.insert_simplefin_holdings(holdings)
        for account_id in accounts_by_id:
            self._warehouse.delete_missing_simplefin_holdings(
                account=owner, account_id=account_id, active_holding_ids=holdings_by_account.get(account_id, set())
            )

        # The bridge's own per-connection/per-account messages. A message
        # naming an account colours that account; anything else colours the
        # connection row. Neither fails the run: the data that DID arrive is
        # real, and the repair is inside the bridge, not here.
        account_errors, connection_errors = _classify_errors(errors, set(accounts_by_id))
        attention = 0
        cursor = str(int(synced_at.timestamp()))
        for account_id in sorted(accounts_by_id):
            message = account_errors.get(account_id, "")
            status = SIMPLEFIN_STATUS_ATTENTION if message else SIMPLEFIN_STATUS_OK
            attention += 1 if message else 0
            self._warehouse.insert_simplefin_sync_state(
                account=owner,
                account_id=account_id,
                cursor=cursor,
                status=status,
                error=self._safe_error(message) if message else "",
                last_synced_at=synced_at,
                updated_at=synced_at,
            )
        connection_message = "; ".join(connection_errors)
        if connection_message:
            attention += 1
            self._logger.warning("SimpleFIN bridge reports: %s", connection_message)
        self._warehouse.insert_simplefin_sync_state(
            account=owner,
            account_id=SIMPLEFIN_CONNECTION_STATE_ID,
            cursor=cursor,
            status=SIMPLEFIN_STATUS_ATTENTION if connection_message else SIMPLEFIN_STATUS_OK,
            error=self._safe_error(connection_message)[:2000],
            last_synced_at=synced_at,
            updated_at=synced_at,
        )
        return SimpleFINSyncSummary(
            accounts=len(account_rows),
            transactions=len(transaction_rows),
            removed_transactions=removed,
            holdings=len(holdings),
            requests=len(windows),
            attention=attention,
        )

    def _window_start(self, state: Mapping[str, Mapping[str, Any]], now: datetime) -> datetime:
        """Where this run's read begins.

        The response is global, so one window serves every account: the oldest
        successful cursor among the known accounts, minus an overlap for rows
        the institution posts late, or the full lookback when any live account
        has never been read (the first run, or a newly connected institution).
        """

        floor = now - timedelta(days=self._config.lookback_days)
        cursors: list[datetime] = []
        for account_id, row in state.items():
            if account_id == SIMPLEFIN_CONNECTION_STATE_ID:
                continue
            cursor = str(row.get("cursor") or "").strip()
            if not cursor.isdigit():
                return floor
            cursors.append(datetime.fromtimestamp(int(cursor), tz=UTC))
        if not cursors:
            return floor
        if self._warehouse.simplefin_has_accounts_without_state(account=self._config.account):
            return floor
        return max(floor, min(cursors) - timedelta(days=self._config.overlap_days))

    def _record_state(
        self,
        owner: str,
        account_id: str,
        state: Mapping[str, Mapping[str, Any]],
        status: str,
        error: str,
        attempted_at: datetime,
    ) -> None:
        previous = state.get(account_id, {})
        previous_success = previous.get("last_synced_at")
        if not isinstance(previous_success, datetime):
            previous_success = datetime.fromtimestamp(0, tz=UTC)
        self._warehouse.insert_simplefin_sync_state(
            account=owner,
            account_id=account_id,
            cursor=str(previous.get("cursor") or ""),
            status=status,
            error=error[:2000],
            last_synced_at=_ensure_utc(previous_success),
            updated_at=attempted_at,
        )

    def _safe_error(self, exc: Exception | str) -> str:
        message = str(exc) or type(exc).__name__ if isinstance(exc, Exception) else str(exc)
        return redact_access_url(self._config.access_url, message)[:2000]


def sync_from_settings(*, settings: Settings | None = None, logger=None) -> SimpleFINSyncSummary:
    settings = settings or load_settings(require_gmail=False, require_simplefin=True)
    if settings.simplefin is None:
        raise ValueError("SimpleFIN is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        return SimpleFINSyncRunner(
            config=settings.simplefin, warehouse=warehouse, logger=logger or _NullLogger()
        ).sync_all()
    finally:
        warehouse.close()


# --- row mapping -------------------------------------------------------------


def _account_row(*, owner: str, raw: dict[str, Any], synced_at: datetime, sync_version: int) -> dict[str, Any]:
    org = raw.get("org") if isinstance(raw.get("org"), Mapping) else {}
    return {
        "account": owner,
        "account_id": str(raw.get("id") or ""),
        "org_id": str(org.get("id") or org.get("sfin-url") or org.get("domain") or ""),
        "org_name": str(org.get("name") or org.get("domain") or ""),
        "org_domain": str(org.get("domain") or ""),
        "org_url": str(org.get("url") or ""),
        "name": str(raw.get("name") or ""),
        "currency": str(raw.get("currency") or ""),
        "balance": _float(raw.get("balance")),
        "available_balance": _float(raw.get("available-balance")),
        "balance_at": _epoch(raw.get("balance-date")),
        "is_removed": 0,
        "extra_json": raw.get("extra") if isinstance(raw.get("extra"), Mapping) else {},
        "raw_json": {key: value for key, value in raw.items() if key not in ("transactions", "holdings")},
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def _transaction_row(
    *, owner: str, account_id: str, raw: dict[str, Any], synced_at: datetime, sync_version: int
) -> dict[str, Any]:
    posted = _epoch(raw.get("posted"))
    transacted = _epoch(raw.get("transacted_at"))
    pending = 1 if bool(raw.get("pending")) or int(raw.get("posted") or 0) == 0 else 0
    if pending and posted.timestamp() == 0:
        # A pending transaction may carry posted = 0; the ledger needs a real
        # day to match on, and transacted_at is the day it happened.
        posted = transacted
    return {
        "account": owner,
        "account_id": account_id,
        "transaction_id": str(raw.get("id") or ""),
        "posted_at": posted,
        "transacted_at": transacted,
        "amount": _float(raw.get("amount")),
        "description": str(raw.get("description") or ""),
        "payee": str(raw.get("payee") or ""),
        "memo": str(raw.get("memo") or ""),
        "mcc": "" if raw.get("mcc") is None else str(raw.get("mcc")),
        "pending": pending,
        "is_removed": 0,
        "extra_json": raw.get("extra") if isinstance(raw.get("extra"), Mapping) else {},
        "raw_json": raw,
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def _holding_row(
    *, owner: str, account_id: str, raw: dict[str, Any], synced_at: datetime, sync_version: int
) -> dict[str, Any]:
    return {
        "account": owner,
        "account_id": account_id,
        "holding_id": str(raw.get("id") or ""),
        "symbol": str(raw.get("symbol") or ""),
        "description": str(raw.get("description") or ""),
        "currency": str(raw.get("currency") or ""),
        "shares": _float(raw.get("shares")),
        "cost_basis": _float(raw.get("cost_basis")),
        "market_value": _float(raw.get("market_value")),
        "purchase_price": _float(raw.get("purchase_price")),
        "acquired_at": _epoch(raw.get("created")),
        "raw_json": raw,
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def _classify_errors(errors: list[Any], account_ids: set[str]) -> tuple[dict[str, str], list[str]]:
    """Split the bridge's ``errors`` into per-account and connection messages.

    Protocol 1.0 bridges answer strings; newer ones answer ``{code, msg,
    conn_id?, account_id?}`` objects. A message naming a known account is that
    account's; everything else belongs to the connection.
    """

    per_account: dict[str, str] = {}
    connection: list[str] = []
    for entry in errors:
        if isinstance(entry, Mapping):
            message = str(entry.get("msg") or entry.get("code") or "").strip()
            account_id = str(entry.get("account_id") or "").strip()
            if account_id and account_id in account_ids:
                per_account[account_id] = message
                continue
            conn_id = str(entry.get("conn_id") or "").strip()
            if conn_id:
                message = f"{message} (connection {conn_id})"
        else:
            message = str(entry).strip()
        if message:
            connection.append(message)
    return per_account, connection


def _windows(start: datetime, end: datetime, size: timedelta):
    cursor = start
    while cursor < end:
        nxt = min(cursor + size, end)
        yield cursor, nxt
        cursor = nxt
    if start >= end:
        yield start, end


def _epoch(value: Any) -> datetime:
    try:
        seconds = int(value or 0)
    except (TypeError, ValueError):
        return datetime.fromtimestamp(0, tz=UTC)
    if seconds <= 0:
        return datetime.fromtimestamp(0, tz=UTC)
    return datetime.fromtimestamp(seconds, tz=UTC)


def _float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    return float(str(value).replace(",", ""))


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _sync_version(value: datetime) -> int:
    return int(_ensure_utc(value).timestamp() * 1_000_000)


class _NullLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args[:1] == ["claim"]:
        token = args[1] if len(args) > 1 else sys.stdin.readline()
        print(claim_setup_token(token))
        return 0
    if args[:1] == ["sync"]:
        import logging

        logging.basicConfig(level=logging.INFO)
        summary = sync_from_settings(logger=logging.getLogger("simplefin"))
        print(summary)
        return 0
    print(
        "usage: python -m personal_data_warehouse.simplefin_sync claim [<setup-token> | < token-on-stdin]\n"
        "       python -m personal_data_warehouse.simplefin_sync sync",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
