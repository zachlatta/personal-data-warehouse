from __future__ import annotations

import os

import psycopg2
import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.postgres_readonly import PostgresReadOnlyRunner


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


def test_postgres_readonly_runner_uses_dedicated_read_only_connection() -> None:
    schema = make_test_schema("readonly")
    warehouse = PostgresWarehouse(_postgres_url(), schema=schema)
    runner = PostgresReadOnlyRunner(warehouse)
    try:
        assert runner._connection is not warehouse._connection

        with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
            with runner._connection.cursor() as cursor:
                cursor.execute("CREATE TEMP TABLE pdw_ro_probe (x int)")
        runner._connection.rollback()

        with runner._connection.cursor() as cursor:
            cursor.execute("SHOW statement_timeout")
            assert cursor.fetchone()[0] in ("30s", "30000ms", "30000")
    finally:
        runner.close()
        cleanup_test_warehouse(warehouse)
