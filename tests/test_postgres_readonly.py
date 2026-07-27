from __future__ import annotations

from datetime import UTC, date, datetime
import os

import psycopg2
import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.postgres_readonly import PostgresReadOnlyRunner, tsv_value


def test_tsv_value_formats_postgres_values_stably() -> None:
    assert tsv_value(None) == ""
    assert tsv_value("plain") == "plain"
    assert tsv_value(b"hello\xff") == "hello\ufffd"
    assert tsv_value(date(2026, 7, 27)) == "2026-07-27"
    assert tsv_value(datetime(2026, 7, 27, 13, 30, tzinfo=UTC)) == "2026-07-27T13:30:00+00:00"
    assert tsv_value({"b": 2, "a": 1}) == '{"a":1,"b":2}'


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
