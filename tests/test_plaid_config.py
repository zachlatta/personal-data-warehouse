from __future__ import annotations

from personal_data_warehouse.config import load_settings


def test_load_settings_reads_plaid_finance_config(monkeypatch) -> None:
    monkeypatch.setenv("PLAID_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("PLAID_CLIENT_ID", "client-id")
    monkeypatch.setenv("PLAID_SECRET", "secret")
    monkeypatch.setenv("PLAID_ENV", "sandbox")
    monkeypatch.setenv("PLAID_PRODUCTS", "transactions, investments, liabilities")
    monkeypatch.setenv("PLAID_COUNTRY_CODES", "US,CA")
    monkeypatch.setenv("PLAID_CLIENT_NAME", "Personal Data Warehouse")
    monkeypatch.setenv("PLAID_TRANSACTIONS_LOOKBACK_DAYS", "730")

    settings = load_settings(require_postgres=False, require_gmail=False, require_plaid=True)

    assert settings.plaid is not None
    assert settings.plaid.account == "zach@example.com"
    assert settings.plaid.client_id == "client-id"
    assert settings.plaid.secret == "secret"
    assert settings.plaid.environment == "sandbox"
    assert settings.plaid.products == ("transactions", "investments", "liabilities")
    assert settings.plaid.country_codes == ("US", "CA")
    assert settings.plaid.client_name == "Personal Data Warehouse"
    assert settings.plaid.transactions_lookback_days == 730


def test_load_settings_requires_plaid_credentials_for_finance(monkeypatch) -> None:
    monkeypatch.setenv("PLAID_ACCOUNT", "zach@example.com")
    monkeypatch.delenv("PLAID_CLIENT_ID", raising=False)
    monkeypatch.delenv("PLAID_SECRET", raising=False)

    try:
        load_settings(require_postgres=False, require_gmail=False, require_plaid=True)
    except ValueError as exc:
        assert "PLAID_CLIENT_ID" in str(exc)
        assert "PLAID_SECRET" in str(exc)
    else:
        raise AssertionError("missing Plaid credentials should fail fast")
