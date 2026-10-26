"""Exercise the Go search-timing store against the Python-provisioned tables.

The app deploys before the Python ensure path runs, so it creates both tables
itself with its own DDL. The two must agree column for column -- type, NOT
NULL and default -- or whichever side provisioned first decides what the other
reads. Go writes through the real store into the Python tables, and builds its
DDL in a sibling schema this test then compares.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import cleanup_test_warehouse, make_test_schema

_COLUMNS_SQL = """
SELECT c.table_name, c.column_name, c.data_type, c.is_nullable, c.column_default
FROM information_schema.columns c
WHERE c.table_schema = %s
  AND c.table_name IN ('search_request_timings', 'search_request_timing_state')
ORDER BY c.table_name, c.ordinal_position
"""

_PRIMARY_KEYS_SQL = """
SELECT tc.table_name, string_agg(k.column_name, ',' ORDER BY k.ordinal_position)
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage k
  ON k.constraint_schema = tc.constraint_schema AND k.constraint_name = tc.constraint_name
WHERE tc.table_schema = %s AND tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name IN ('search_request_timings', 'search_request_timing_state')
GROUP BY tc.table_name ORDER BY tc.table_name
"""


@pytest.mark.local_integration
def test_go_search_timing_store_writes_the_python_tables_and_its_ddl_matches():
    url = os.environ["POSTGRES_DATABASE_URL"]
    warehouse = PostgresWarehouse(url, schema=make_test_schema("search_timings"))
    ops_schema = warehouse.physical_schema_name("ops")
    go_schema = f"{ops_schema}_go"
    try:
        warehouse._ensure_table_group(["search_request_timings", "search_request_timing_state"])
        result = subprocess.run(
            ["go", "test", "./internal/server", "-run", "^TestSearchTimingStoreIntegration$", "-count=1", "-v"],
            cwd=Path(__file__).resolve().parents[1] / "app",
            env={
                **os.environ,
                "PDW_SEARCH_TIMING_TEST_DATABASE_URL": url,
                "PDW_SEARCH_TIMING_TEST_SCHEMA": ops_schema,
                "PDW_SEARCH_TIMING_TEST_GO_SCHEMA": go_schema,
            },
            text=True,
            capture_output=True,
            timeout=300,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        # The run must have happened, not skipped.
        assert "--- PASS: TestSearchTimingStoreIntegration" in result.stdout, result.stdout

        python_columns = warehouse._query(_COLUMNS_SQL, (ops_schema,))
        go_columns = warehouse._query(_COLUMNS_SQL, (go_schema,))
        assert python_columns, "Python provisioned no search timing tables"
        assert go_columns == python_columns
        assert warehouse._query(_PRIMARY_KEYS_SQL, (go_schema,)) == warehouse._query(
            _PRIMARY_KEYS_SQL, (ops_schema,)
        )
    finally:
        warehouse._command(f'DROP SCHEMA IF EXISTS "{go_schema}" CASCADE')
        cleanup_test_warehouse(warehouse)
