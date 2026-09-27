"""SimpleFIN sync: one access URL, every institution, a bounded window per run."""

from __future__ import annotations

import base64
import os
from datetime import UTC, datetime, timedelta

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.config import SimpleFINConfig, load_settings
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.simplefin_sync import (
    SIMPLEFIN_CONNECTION_STATE_ID,
    SIMPLEFIN_STATUS_ACTION_REQUIRED,
    SIMPLEFIN_STATUS_ATTENTION,
    SimpleFINAuthError,
    SimpleFINClient,
    SimpleFINSyncRunner,
    _classify_errors,
    claim_setup_token,
    redact_access_url,
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


ACCESS_URL = "https://user1:secret2@bridge.example/simplefin"
NOW = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)
CONFIG = SimpleFINConfig(account="z@x.test", access_url=ACCESS_URL, lookback_days=100, window_days=60, overlap_days=7)


class FakeLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, message, *args, **kwargs) -> None:
        self.warnings.append(str(message) % args if args else str(message))


def _org():
    return {"domain": "www.bank.example", "name": "Example Bank", "url": "https://www.bank.example", "id": "www.bank.example"}


def _tx(id_, posted, amount, description, **extra):
    return {"id": id_, "posted": posted, "amount": amount, "description": description, "payee": description,
            "memo": description.upper(), "transacted_at": posted, "mcc": None, **extra}


class FakeClient:
    def __init__(self, *, errors=None, auth_error=False) -> None:
        self.windows: list[tuple[datetime, datetime]] = []
        self.errors = errors or []
        self.auth_error = auth_error
        self.pending_ids = {"pend-1"}

    def accounts(self, *, start, end, pending=True):
        self.windows.append((start, end))
        if self.auth_error:
            raise SimpleFINAuthError("SimpleFIN refused the access URL (HTTP 403)")
        posted = int((NOW - timedelta(days=2)).timestamp())
        txs = [_tx("tx-1", posted, "-12.50", "Coffee"), _tx("tx-2", posted - 86400, "1960.00", "Pay day")]
        if "pend-1" in self.pending_ids:
            txs.append(_tx("pend-1", 0, "-40.00", "Gas", pending=True, transacted_at=posted))
        return {
            "errors": self.errors,
            "accounts": [
                {
                    "id": "ACT-1", "name": "Venture X (5520)", "currency": "USD", "balance": "-32.55",
                    "available-balance": "0.00", "balance-date": int((NOW - timedelta(hours=3)).timestamp()),
                    "org": _org(), "transactions": txs,
                    "holdings": [{"id": "H-1", "symbol": "AAPL", "shares": "2.0", "cost_basis": "300.00",
                                  "market_value": "450.00", "purchase_price": "150.00", "currency": "USD",
                                  "description": "Apple", "created": posted}],
                },
                {
                    "id": "ACT-2", "name": "Checking", "currency": "USD", "balance": "1200.00",
                    "available-balance": "1200.00", "balance-date": int(NOW.timestamp()),
                    "org": _org(), "transactions": [],
                },
            ],
        }


def test_claim_setup_token_posts_the_decoded_claim_url_once():
    class Session:
        def __init__(self):
            self.posted = []

        def post(self, url, headers=None, timeout=None):
            self.posted.append(url)

            class R:
                status_code = 200
                text = "https://a:b@bridge.example/simplefin\n"

            return R()

    session = Session()
    token = base64.b64encode(b"https://bridge.example/simplefin/claim/ABC").decode()
    assert claim_setup_token(token, session=session) == "https://a:b@bridge.example/simplefin"
    assert session.posted == ["https://bridge.example/simplefin/claim/ABC"]
    with pytest.raises(ValueError):
        claim_setup_token("not base64!!")


def test_client_splits_the_credential_out_of_the_access_url():
    client = SimpleFINClient(CONFIG)
    assert client.base_url == "https://bridge.example/simplefin"
    assert client._auth == ("user1", "secret2")


def test_redaction_hides_the_url_and_both_credential_halves():
    text = f"GET {ACCESS_URL}/accounts failed for user1 with secret2"
    redacted = redact_access_url(ACCESS_URL, text)
    assert "secret2" not in redacted and "user1" not in redacted and "bridge.example/simplefin" not in redacted


def test_classify_errors_reads_strings_and_objects():
    per_account, connection = _classify_errors(
        ["Connection to Example Bank may need attention", {"code": "x", "msg": "stale", "account_id": "ACT-1"},
         {"code": "y", "msg": "relogin", "conn_id": "CON-9"}],
        {"ACT-1"},
    )
    assert per_account == {"ACT-1": "stale"}
    assert connection == ["Connection to Example Bank may need attention", "relogin (connection CON-9)"]


def test_settings_require_the_claimed_https_url(monkeypatch):
    monkeypatch.setenv("SIMPLEFIN_ACCESS_URL", ACCESS_URL)
    monkeypatch.setenv("PLAID_ACCOUNT", "z@x.test")
    monkeypatch.setenv("PLAID_CLIENT_ID", "id")
    monkeypatch.setenv("PLAID_SECRET", "s")
    monkeypatch.setenv("POSTGRES_DATABASE_URL", "postgresql://x")
    settings = load_settings(require_gmail=False)
    assert settings.simplefin is not None
    assert settings.simplefin.account == "z@x.test"  # inherits Plaid's owner label
    monkeypatch.setenv("SIMPLEFIN_ACCESS_URL", "http://insecure")
    with pytest.raises(ValueError):
        load_settings(require_gmail=False)


def test_first_run_walks_the_lookback_in_windows_and_lands_every_row(warehouse):
    client = FakeClient()
    logger = FakeLogger()
    runner = SimpleFINSyncRunner(config=CONFIG, warehouse=warehouse, logger=logger, client=client, now=lambda: NOW)
    summary = runner.sync_all()
    assert summary.accounts == 2 and summary.transactions == 3 and summary.holdings == 1
    # 100 days of lookback in 60-day windows = 2 requests, contiguous, ending now.
    assert summary.requests == 2 and client.windows[0][0] == NOW - timedelta(days=100)
    assert client.windows[-1][1] == NOW and client.windows[0][1] == client.windows[1][0]

    accounts = warehouse._query_dicts("SELECT account_id, org_name, name, balance, balance_at FROM @simplefin_accounts ORDER BY account_id")
    assert [a["account_id"] for a in accounts] == ["ACT-1", "ACT-2"]
    assert accounts[0]["org_name"] == "Example Bank" and accounts[0]["balance"] == -32.55
    assert accounts[0]["balance_at"] == NOW - timedelta(hours=3)
    txs = warehouse._query_dicts("SELECT transaction_id, amount, pending, posted_at, payee FROM @simplefin_transactions ORDER BY transaction_id")
    by_id = {t["transaction_id"]: t for t in txs}
    assert by_id["tx-1"]["amount"] == -12.5 and by_id["tx-1"]["pending"] == 0 and by_id["tx-1"]["payee"] == "Coffee"
    # A pending row with posted = 0 takes its transacted day, never 1970.
    assert by_id["pend-1"]["pending"] == 1 and by_id["pend-1"]["posted_at"] == NOW - timedelta(days=2)
    holdings = warehouse._query_dicts("SELECT symbol, shares, market_value FROM @simplefin_holdings")
    assert holdings == [{"symbol": "AAPL", "shares": 2.0, "market_value": 450.0}]
    state = warehouse.load_simplefin_sync_state(account="z@x.test")
    assert set(state) == {SIMPLEFIN_CONNECTION_STATE_ID, "ACT-1", "ACT-2"}
    assert all(row["status"] == "ok" for row in state.values())
    assert state["ACT-1"]["cursor"] == str(int(NOW.timestamp()))


def test_second_run_reads_only_the_overlap_and_tombstones_a_vanished_pending_row(warehouse):
    client = FakeClient()
    runner = SimpleFINSyncRunner(config=CONFIG, warehouse=warehouse, logger=FakeLogger(), client=client, now=lambda: NOW)
    runner.sync_all()
    client.windows.clear()
    client.pending_ids.clear()  # the pending purchase posted under a new id (or was cancelled)
    later = NOW + timedelta(hours=1)
    runner = SimpleFINSyncRunner(config=CONFIG, warehouse=warehouse, logger=FakeLogger(), client=client, now=lambda: later)
    summary = runner.sync_all()
    assert summary.requests == 1
    assert client.windows == [(NOW - timedelta(days=7), later)]
    assert summary.removed_transactions == 1
    rows = warehouse._query_dicts("SELECT transaction_id, is_removed FROM @simplefin_transactions ORDER BY transaction_id")
    assert {r["transaction_id"]: r["is_removed"] for r in rows} == {"pend-1": 1, "tx-1": 0, "tx-2": 0}


def test_an_account_the_bridge_stops_reporting_is_tombstoned_not_deleted(warehouse):
    client = FakeClient()
    SimpleFINSyncRunner(config=CONFIG, warehouse=warehouse, logger=FakeLogger(), client=client, now=lambda: NOW).sync_all()
    warehouse.mark_missing_simplefin_accounts_removed(account="z@x.test", active_account_ids={"ACT-1"}, synced_at=NOW)
    rows = warehouse._query_dicts("SELECT account_id, is_removed FROM @simplefin_accounts ORDER BY account_id")
    assert rows == [{"account_id": "ACT-1", "is_removed": 0}, {"account_id": "ACT-2", "is_removed": 1}]


def test_a_refused_access_url_is_action_required_and_keeps_the_run_green(warehouse):
    client = FakeClient(auth_error=True)
    logger = FakeLogger()
    summary = SimpleFINSyncRunner(config=CONFIG, warehouse=warehouse, logger=logger, client=client, now=lambda: NOW).sync_all()
    assert summary.action_required == 1 and summary.accounts == 0
    state = warehouse.load_simplefin_sync_state(account="z@x.test")
    assert state[SIMPLEFIN_CONNECTION_STATE_ID]["status"] == SIMPLEFIN_STATUS_ACTION_REQUIRED
    assert "403" in state[SIMPLEFIN_CONNECTION_STATE_ID]["error"]
    assert "secret2" not in state[SIMPLEFIN_CONNECTION_STATE_ID]["error"]
    assert logger.warnings and "setup token" in logger.warnings[0]


def test_bridge_attention_messages_colour_the_row_without_failing(warehouse):
    client = FakeClient(errors=["Connection to Example Bank may need attention", {"msg": "stale", "account_id": "ACT-2"}])
    summary = SimpleFINSyncRunner(config=CONFIG, warehouse=warehouse, logger=FakeLogger(), client=client, now=lambda: NOW).sync_all()
    assert summary.attention == 2 and summary.accounts == 2
    state = warehouse.load_simplefin_sync_state(account="z@x.test")
    assert state[SIMPLEFIN_CONNECTION_STATE_ID]["status"] == SIMPLEFIN_STATUS_ATTENTION
    assert state["ACT-2"]["status"] == SIMPLEFIN_STATUS_ATTENTION and state["ACT-2"]["error"] == "stale"
    assert state["ACT-1"]["status"] == "ok"
