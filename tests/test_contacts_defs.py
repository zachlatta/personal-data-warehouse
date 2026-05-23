from __future__ import annotations

from personal_data_warehouse.defs.contacts_sync import contacts_sync_hourly


def test_contacts_sync_schedule_runs_hourly_by_default() -> None:
    assert contacts_sync_hourly.cron_schedule == "0 * * * *"
    assert contacts_sync_hourly.default_status.value == "RUNNING"
