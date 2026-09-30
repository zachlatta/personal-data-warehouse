"""Exercise the Go uploader-heartbeat store against the Python-provisioned table."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import cleanup_test_warehouse, make_test_schema


@pytest.mark.local_integration
def test_heartbeat_failure_streak_and_python_schema_agree():
    url = os.environ["POSTGRES_DATABASE_URL"]
    warehouse = PostgresWarehouse(url, schema=make_test_schema("heartbeat_streak"))
    try:
        warehouse._ensure_table_group(["uploader_heartbeats"])
        result = subprocess.run(
            ["go", "test", "./internal/server", "-run", "^TestUploaderHeartbeatStoreStreakIntegration$", "-count=1", "-v"],
            cwd=Path(__file__).resolve().parents[1] / "app",
            env={
                **os.environ,
                "PDW_HEARTBEAT_TEST_DATABASE_URL": url,
                "PDW_HEARTBEAT_TEST_SCHEMA": warehouse.physical_schema_name("ops"),
            },
            text=True,
            capture_output=True,
            timeout=300,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        # The run must have happened, not skipped.
        assert "--- PASS: TestUploaderHeartbeatStoreStreakIntegration" in result.stdout, result.stdout
    finally:
        cleanup_test_warehouse(warehouse)
