from __future__ import annotations

import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from dotenv import load_dotenv

from tests.local_test_runtime import (
    OWNED_POSTGRES_URL_ENV,
    ActiveDatabase,
    DisposableDatabases,
    LocalTestStartupError,
    PostgresTestRuntime,
    active_database,
    auto_worker_count,
    available_memory_bytes,
    busy_cpus_now,
    callable_requires_postgres,
    direct_postgres_url,
    schedule_key,
    set_active_database,
    subscription_tests_enabled,
    subscription_tests_skip_reason,
    template_provisioner,
    use_worker_database,
    warehouse_database_name,
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
    if runtime.owned_container is not None:
        os.environ["POSTGRES_DATABASE_URL"] = direct_postgres_url(
            os.environ["POSTGRES_DATABASE_URL"], runtime.owned_container
        )
        os.environ[OWNED_POSTGRES_URL_ENV] = os.environ["POSTGRES_DATABASE_URL"]
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    try:
        if use_worker_database(os.environ, worker):
            databases = DisposableDatabases.create(os.environ["POSTGRES_DATABASE_URL"], worker=worker)
            config.stash[_DISPOSABLE_DATABASES] = databases
            config.add_cleanup(databases.close)
    except LocalTestStartupError as error:
        raise pytest.UsageError(str(error)) from error
    if unit_only and not os.environ.get("POSTGRES_DATABASE_URL"):
        unit_only_url = "postgresql://unit-only.invalid/pdw"
        os.environ["POSTGRES_DATABASE_URL"] = unit_only_url

        def remove_unit_only_url() -> None:
            if os.environ.get("POSTGRES_DATABASE_URL") == unit_only_url:
                os.environ.pop("POSTGRES_DATABASE_URL", None)

        config.add_cleanup(remove_unit_only_url)


_DISPOSABLE_DATABASES = pytest.StashKey[DisposableDatabases]()


@pytest.fixture(autouse=True)
def _disposable_database(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    """Run each Postgres test in its own database cloned from a worker template.

    Only in the run's own cluster: a configured POSTGRES_DATABASE_URL keeps the
    shared database and per-schema cleanup. The test's warehouse fixtures read
    the URL from the environment, so they reach the clone unchanged.
    """
    databases = request.config.stash.get(_DISPOSABLE_DATABASES, None)
    if databases is None or request.node.get_closest_marker("local_integration") is None:
        yield
        return
    provisioner = _item_provisioner(request.node)
    url = databases.clone(databases.template_for(provisioner))
    monkeypatch.setenv("POSTGRES_DATABASE_URL", url)
    set_active_database(
        ActiveDatabase(name=url.rsplit("/", 1)[1], provisioner=provisioner, namespace=databases.namespace)
    )
    try:
        yield
    finally:
        set_active_database(None)
        databases.drop(url)


def _item_provisioner(item: pytest.Item):
    found = set()
    test_function = getattr(item, "function", None)
    if callable(test_function):
        found.add(template_provisioner(test_function))
    fixture_info = getattr(item, "_fixtureinfo", None)
    for fixture_defs in (fixture_info.name2fixturedefs.values() if fixture_info else ()):
        for fixture_def in fixture_defs:
            found.add(template_provisioner(fixture_def.func))
    found.discard(None)
    return next(iter(found)) if len(found) == 1 else None


def pytest_xdist_auto_num_workers(config: pytest.Config) -> int:
    load_dotenv()
    cpus = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count() or 1
    return auto_worker_count(
        os.environ,
        cpus=cpus,
        busy_cpus=busy_cpus_now(),
        available_bytes=available_memory_bytes(),
    )


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

    # Hand xdist the longest tests first, by the last run's durations, so the
    # run does not end with one worker finishing a 20 s test alone. Collection
    # order put the heaviest files (pipeline health, search index, timeline)
    # last: on 2026-10-24 workers went idle from 110s while a few finished
    # them at 137s. Every worker reads the same cache file and the sort is
    # stable, so each still collects the identical order xdist requires.
    cache = getattr(config, "cache", None)  # absent under -p no:cacheprovider
    durations = cache.get(_DURATIONS_CACHE_KEY, {}) if cache is not None else {}
    items.sort(
        key=lambda item: schedule_key(
            item.nodeid,
            requires_postgres=item.get_closest_marker("local_integration") is not None,
            durations=durations,
        )
    )


_DURATIONS_CACHE_KEY = "pdw/test-durations"
# This run's per-test seconds (setup + call + teardown), filled on the xdist
# controller or in a serial run; workers skip it so one process writes the cache.
_RUN_DURATIONS: dict[str, float] = {}


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if not os.environ.get("PYTEST_XDIST_WORKER"):
        _RUN_DURATIONS[report.nodeid] = _RUN_DURATIONS.get(report.nodeid, 0.0) + float(report.duration)


def pytest_sessionfinish(session: pytest.Session) -> None:
    cache = getattr(session.config, "cache", None)
    if os.environ.get("PYTEST_XDIST_WORKER") or cache is None or not _RUN_DURATIONS:
        return
    durations = dict(cache.get(_DURATIONS_CACHE_KEY, {}))
    durations.update({nodeid: round(seconds, 4) for nodeid, seconds in _RUN_DURATIONS.items()})
    cache.set(_DURATIONS_CACHE_KEY, durations)


# The warehouse tests run against a real (shared) Postgres, one throwaway
# schema per test. A run that dies before teardown — killed terminal, dropped
# tailnet connection — leaks its schemas into that shared database forever,
# polluting information_schema for every other client. Embedding a UTC
# timestamp in each schema name lets the next test run reap anything old
# enough that its run is certainly dead.
_TEST_SCHEMA_TIMESTAMP = re.compile(r"^pdw_test_(\d{14})_")
_TEST_SCHEMA_MAX_AGE = timedelta(hours=12)


def make_test_schema(label: str = "") -> str:
    active = active_database()
    if active is not None and active.provisioner is not None and not label and not active.namespace_taken:
        # The test's database was cloned from a template provisioned in this
        # namespace; its first warehouse is that warehouse.
        active.namespace_taken = True
        return active.namespace
    stamp = datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S")
    suffix = f"{label}_" if label else ""
    return f"pdw_test_{stamp}_{suffix}{uuid.uuid4().hex}"


def cleanup_test_warehouse(warehouse: PostgresWarehouse) -> None:
    """Drop every schema owned by a test warehouse, then close it.

    In a disposable per-test database the whole database is dropped after the
    test, so only the connection needs closing.
    """
    active = active_database()
    if active is not None and warehouse_database_name(warehouse) == active.name:
        warehouse.close()
        return
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
