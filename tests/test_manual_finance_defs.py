from __future__ import annotations

from personal_data_warehouse.defs.manual_finance_drive_ingest import (
    manual_finance_drive_ingest,
    manual_finance_drive_ingest_job,
    manual_finance_drive_inbox_sensor,
)
from personal_data_warehouse.defs.manual_finance_extraction import (
    manual_finance_extraction,
    manual_finance_extraction_backlog_sensor,
    manual_finance_extraction_hourly,
    manual_finance_extraction_job,
)


def test_drive_ingest_sensor_is_running_by_default() -> None:
    assert manual_finance_drive_inbox_sensor.default_status.value == "RUNNING"
    assert manual_finance_drive_ingest_job.name == "manual_finance_drive_ingest_job"
    assert manual_finance_drive_ingest.key.path == ["manual_finance_drive_ingest"]


def test_extraction_schedule_and_sensor() -> None:
    assert manual_finance_extraction_hourly.cron_schedule == "53 * * * *"
    assert manual_finance_extraction_hourly.default_status.value == "RUNNING"
    assert manual_finance_extraction_backlog_sensor.default_status.value == "RUNNING"
    assert manual_finance_extraction_job.name == "manual_finance_extraction_job"
    assert manual_finance_extraction.key.path == ["manual_finance_extraction"]
