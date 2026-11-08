from copy import deepcopy
from datetime import UTC, datetime
import logging

import pytest

from personal_data_warehouse.android_location_drive_ingest import AndroidLocationDriveIngestRunner, record_to_row
from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import cleanup_test_warehouse, make_test_schema


def envelope():
    return dict(schema_version=1, source="android_location", record_type="location", account="test@example.test", device="testphone", exported_at="2026-11-01T12:30:00Z", received_at="2026-11-01T13:00:00Z", record=dict(sample_id="synthetic-1", latitude=0.0, longitude=0.0, accuracy_m=600.5, provider="network", measured_at_ms=1793534400000))


def test_receipt_does_not_rewrite_measurement_time():
    row = record_to_row(envelope())
    assert row["measured_at"] == datetime(2026, 11, 1, 12, tzinfo=UTC)
    assert row["received_at"] == datetime(2026, 11, 1, 13, tzinfo=UTC)
    assert row["accuracy_m"] == 600.5
    assert row["raw_json"] == envelope()["record"]


@pytest.mark.parametrize("field,value", [("latitude", 91), ("longitude", -181), ("accuracy_m", -1), ("latitude", float("nan")), ("accuracy_m", float("inf")), ("sample_id", "")])
def test_invalid_sample_rejected(field, value):
    e = envelope()
    e["record"][field] = value
    with pytest.raises(ValueError):
        record_to_row(e)


def test_server_receipt_required():
    e = envelope()
    del e["received_at"]
    with pytest.raises(ValueError):
        record_to_row(e)


def test_retry_is_idempotent_in_real_warehouse():
    import os
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"], schema=make_test_schema())
    try:
        def run(e):
            return AndroidLocationDriveIngestRunner(warehouse=wh, batch_source=lambda: [{"records": [e]}], logger=logging.getLogger(__name__)).sync()
        e = envelope()
        run(e)
        retry = deepcopy(e)
        retry["received_at"] = "2026-11-01T14:00:00Z"
        run(retry)
        rows = wh._query_dicts("SELECT * FROM @android_location_samples")
        assert len(rows) == 1
        assert rows[0]["accuracy_m"] == 600.5
        assert rows[0]["received_at"] == datetime(2026, 11, 1, 13, tzinfo=UTC)
        assert rows[0]["synced_at"] > rows[0]["received_at"]
    finally:
        cleanup_test_warehouse(wh)
