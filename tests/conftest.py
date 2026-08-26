from __future__ import annotations

import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from dotenv import load_dotenv

from tests.local_test_runtime import (
    LocalTestStartupError,
    PostgresTestRuntime,
    callable_requires_postgres,
    subscription_tests_enabled,
    subscription_tests_skip_reason,
)

if TYPE_CHECKING:
    from personal_data_warehouse.postgres import PostgresWarehouse


_SUBSCRIPTION_TESTS_ENABLED = pytest.StashKey[bool]()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--unit-only",
        action="store_true",
        default=False,
        help="Skip disposable Postgres and all local Docker/subscription integration tests.",
    )


def pytest_configure(config: pytest.Config) -> None:
    load_dotenv()
    unit_only = bool(config.getoption("--unit-only"))
    try:
        enabled = subscription_tests_enabled(os.environ)
        runtime = PostgresTestRuntime(unit_only=unit_only)
        runtime.start()
    except LocalTestStartupError as error:
        raise pytest.UsageError(str(error)) from error

    config.stash[_SUBSCRIPTION_TESTS_ENABLED] = enabled
    config.add_cleanup(runtime.close)
    if unit_only and not os.environ.get("POSTGRES_DATABASE_URL"):
        unit_only_url = "postgresql://unit-only.invalid/pdw"
        os.environ["POSTGRES_DATABASE_URL"] = unit_only_url

        def remove_unit_only_url() -> None:
            if os.environ.get("POSTGRES_DATABASE_URL") == unit_only_url:
                os.environ.pop("POSTGRES_DATABASE_URL", None)

        config.add_cleanup(remove_unit_only_url)


def _item_requires_postgres(item: pytest.Item) -> bool:
    test_function = getattr(item, "function", None)
    if callable(test_function) and callable_requires_postgres(test_function, include_root=False):
        return True
    fixture_info = getattr(item, "_fixtureinfo", None)
    if fixture_info is None:
        return False
    return any(
        fixture_def.argname != "_reap_leaked_test_schemas"
        and callable_requires_postgres(fixture_def.func)
        for fixture_defs in fixture_info.name2fixturedefs.values()
        for fixture_def in fixture_defs
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    unit_only = bool(config.getoption("--unit-only"))
    subscription_enabled = config.stash[_SUBSCRIPTION_TESTS_ENABLED]
    unit_only_skip = pytest.mark.skip(reason="--unit-only explicitly disables local integration tests")
    subscription_skip = pytest.mark.skip(reason=subscription_tests_skip_reason(os.environ))

    for item in items:
        if _item_requires_postgres(item):
            item.add_marker(pytest.mark.local_integration)
        if unit_only and item.get_closest_marker("local_integration") is not None:
            item.add_marker(unit_only_skip)
        elif not subscription_enabled and item.get_closest_marker("subscription_agent") is not None:
            item.add_marker(subscription_skip)


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
            *warehouse.physical_schema_names(include_hidden=True),
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
def _reap_leaked_test_schemas(pytestconfig: pytest.Config):
    if pytestconfig.getoption("--unit-only"):
        yield
        return
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
