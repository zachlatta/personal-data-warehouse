from __future__ import annotations

from types import SimpleNamespace

from dagster import build_asset_context

from personal_data_warehouse.defs import finance_sync as finance_defs
from personal_data_warehouse.defs.finance_sync import finance_account_sync_hourly


def test_finance_sync_schedule_runs_hourly_by_default() -> None:
    assert finance_account_sync_hourly.cron_schedule == "0 * * * *"
    assert finance_account_sync_hourly.default_status.value == "RUNNING"


def test_finance_sync_skips_when_plaid_is_not_configured(monkeypatch) -> None:
    warehouse = FakeFinanceWarehouse()
    monkeypatch.setattr(finance_defs, "load_settings", lambda **_kwargs: SimpleNamespace(plaid=None))
    monkeypatch.setattr(finance_defs, "warehouse_from_settings", lambda _settings: warehouse)

    result = finance_defs.finance_account_sync(build_asset_context())

    assert warehouse.ensured is True
    assert result.metadata["skipped_due_to_config"] is True


class FakeFinanceWarehouse:
    def __init__(self) -> None:
        self.ensured = False

    def ensure_finance_tables(self) -> None:
        self.ensured = True
