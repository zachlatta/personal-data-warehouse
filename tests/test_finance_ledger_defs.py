from __future__ import annotations

from personal_data_warehouse.defs.finance_ledger import (
    finance_ledger,
    finance_ledger_backlog_sensor,
    finance_ledger_job,
    finance_ledger_schedule,
)


def test_finance_ledger_schedule_follows_each_plaid_sync() -> None:
    # Plaid syncs on */30; the ledger snapshots shortly after each sync window.
    assert finance_ledger_schedule.cron_schedule == "7,37 * * * *"
    assert finance_ledger_schedule.default_status.value == "RUNNING"


def test_finance_ledger_sensor_is_running_by_default() -> None:
    assert finance_ledger_backlog_sensor.default_status.value == "RUNNING"


def test_finance_ledger_job_selects_the_asset() -> None:
    assert finance_ledger_job.name == "finance_ledger_job"
    assert finance_ledger.key.path == ["finance_ledger"]
