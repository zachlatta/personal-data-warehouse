from __future__ import annotations

from personal_data_warehouse.defs.plaid_sync import plaid_finance_sync_every_thirty_minutes


def test_plaid_finance_sync_schedule_runs_every_thirty_minutes_by_default() -> None:
    assert plaid_finance_sync_every_thirty_minutes.cron_schedule == "*/30 * * * *"
    assert plaid_finance_sync_every_thirty_minutes.default_status.value == "RUNNING"
