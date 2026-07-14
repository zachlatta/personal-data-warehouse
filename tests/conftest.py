from __future__ import annotations

import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from dotenv import load_dotenv

if TYPE_CHECKING:
    from personal_data_warehouse.postgres import PostgresWarehouse

# The warehouse tests run against a real (shared) Postgres, one throwaway
# schema per test. A run that dies before teardown — killed terminal, dropped
# tailnet connection — leaks its schemas into that shared database forever,
# polluting information_schema for every other client. Embedding a UTC
# timestamp in each schema name lets the next test run reap anything old
# enough that its run is certainly dead.
_TEST_SCHEMA_TIMESTAMP = re.compile(r"^pdw_test_(\d{14})_")
_TEST_SCHEMA_MAX_AGE = timedelta(hours=12)


def make_test_schema(label: str = "") -> str:
    stamp = datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S")
    suffix = f"{label}_" if label else ""
    return f"pdw_test_{stamp}_{suffix}{uuid.uuid4().hex}"


def cleanup_test_warehouse(warehouse: PostgresWarehouse) -> None:
    """Drop every schema owned by a test warehouse, then close it."""
    try:
        schema_names = [
            *warehouse.physical_schema_names(include_private=True),
            warehouse.schema_namespace,
        ]
        for schema_name in schema_names:
            warehouse._raw_command(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
    finally:
        warehouse.close()


def reap_stale_test_schemas(connection) -> list[str]:
    cutoff = datetime.now(tz=UTC) - _TEST_SCHEMA_MAX_AGE
    dropped: list[str] = []
    with connection.cursor() as cursor:
        cursor.execute("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'pdw\\_test\\_%'")
        for (schema,) in cursor.fetchall():
            match = _TEST_SCHEMA_TIMESTAMP.match(schema)
            if match is None:
                continue
            stamp = datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=UTC)
            if stamp >= cutoff:
                continue
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            dropped.append(schema)
    return dropped


@pytest.fixture(scope="session", autouse=True)
def _reap_leaked_test_schemas():
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if url:
        import psycopg2

        try:
            connection = psycopg2.connect(url)
        except psycopg2.Error as error:
            print(f"schema reaper: could not connect, skipping ({error})")
        else:
            connection.autocommit = True
            try:
                dropped = reap_stale_test_schemas(connection)
                if dropped:
                    print(f"schema reaper: dropped {len(dropped)} stale test schemas")
            finally:
                connection.close()
    yield
