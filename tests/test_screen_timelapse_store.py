"""The Go app writes the screen timelapse rows, and provisions their tables.

Run against a Python-provisioned schema, the Go row store upserts and reads
back both kinds; run against an empty schema it provisions the tables itself,
and what it creates must be exactly what the Python TableSpec/IndexSpec would
-- whichever side reaches a database first leaves the same tables.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import cleanup_test_warehouse, make_test_schema


def _shape(warehouse: PostgresWarehouse, schema: str) -> dict:
    columns = warehouse._query_dicts(
        """
        SELECT table_name, column_name, data_type, is_nullable, column_default, ordinal_position
        FROM information_schema.columns WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
        """,
        (schema,),
    )
    indexes = warehouse._query_dicts(
        "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = %s ORDER BY indexname",
        (schema,),
    )
    return {
        "columns": [
            (c["table_name"], c["column_name"], c["data_type"], c["is_nullable"], c["column_default"])
            for c in columns
        ],
        # Index names are per schema; their definitions name the schema, so
        # compare them with it taken out.
        "indexes": [
            (i["tablename"], i["indexname"], i["indexdef"].replace(schema, "<schema>")) for i in indexes
        ],
    }


@pytest.mark.local_integration
def test_go_row_store_writes_python_tables_and_provisions_identical_ones():
    url = os.environ["POSTGRES_DATABASE_URL"]
    python_side = PostgresWarehouse(url, schema=make_test_schema("screen_timelapse_py"))
    go_side = PostgresWarehouse(url, schema=make_test_schema("screen_timelapse_go"))
    try:
        python_side.ensure_screen_timelapse_tables()
        python_schema = python_side.physical_schema_name("base_screen_timelapse")
        go_schema = go_side.physical_schema_name("base_screen_timelapse")
        result = subprocess.run(
            ["go", "test", "./internal/server", "-run", "^TestScreenTimelapseRowStoreIntegration$", "-count=1", "-v"],
            cwd=Path(__file__).resolve().parents[1] / "app",
            env={
                **os.environ,
                "PDW_SCREEN_TIMELAPSE_TEST_DATABASE_URL": url,
                "PDW_SCREEN_TIMELAPSE_TEST_SCHEMA": python_schema,
                "PDW_SCREEN_TIMELAPSE_TEST_GO_SCHEMA": go_schema,
            },
            text=True,
            capture_output=True,
            timeout=300,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        # The run must have happened, not skipped.
        assert "--- PASS: TestScreenTimelapseRowStoreIntegration" in result.stdout, result.stdout
        python_shape = _shape(python_side, python_schema)
        assert python_shape["columns"], "the Python side provisioned nothing"
        assert _shape(python_side, go_schema) == python_shape
    finally:
        cleanup_test_warehouse(python_side)
        cleanup_test_warehouse(go_side)
