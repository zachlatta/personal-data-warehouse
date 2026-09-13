"""Exercise the Go credential store against the Python-provisioned table."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import cleanup_test_warehouse, make_test_schema


@pytest.mark.local_integration
def test_mcp_proxy_persistence_and_python_schema_agree():
    url = os.environ["POSTGRES_DATABASE_URL"]
    warehouse = PostgresWarehouse(url, schema=make_test_schema("mcp_proxy"))
    try:
        warehouse._ensure_table_group(["mcp_connections"])
        result = subprocess.run(
            ["go", "test", "./internal/mcpproxy", "-run", "^TestPostgresStoreIntegration$", "-count=1"],
            cwd=Path(__file__).resolve().parents[1] / "app",
            env={
                **os.environ,
                "PDW_PROXY_TEST_DATABASE_URL": url,
                "PDW_PROXY_TEST_SCHEMA": warehouse.physical_schema_name("private"),
            },
            text=True,
            capture_output=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stdout + result.stderr
    finally:
        cleanup_test_warehouse(warehouse)
