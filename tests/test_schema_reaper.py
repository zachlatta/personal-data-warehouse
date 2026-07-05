from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests.conftest import make_test_schema, reap_stale_test_schemas
from tests.test_postgres_warehouse import _postgres_url


@pytest.fixture()
def connection():
    import psycopg2

    conn = psycopg2.connect(_postgres_url())
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


def test_make_test_schema_embeds_a_current_utc_timestamp() -> None:
    before = datetime.now(tz=UTC).replace(microsecond=0)
    schema = make_test_schema()

    assert schema.startswith("pdw_test_")
    stamp = datetime.strptime(schema.split("_")[2], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    assert before - timedelta(seconds=5) <= stamp <= before + timedelta(seconds=5)


def test_make_test_schema_supports_a_label() -> None:
    schema = make_test_schema("dest")
    parts = schema.split("_")
    assert parts[0] == "pdw"
    assert parts[1] == "test"
    assert parts[3] == "dest"


def test_reap_drops_only_stale_timestamped_schemas(connection) -> None:
    stale_stamp = (datetime.now(tz=UTC) - timedelta(hours=13)).strftime("%Y%m%d%H%M%S")
    stale = f"pdw_test_{stale_stamp}_deadbeef"
    fresh = make_test_schema()
    legacy = "pdw_test_0badc0ffee0badc0ffee0badc0ffee00"

    with connection.cursor() as cursor:
        for schema in (stale, fresh, legacy):
            cursor.execute(f'CREATE SCHEMA "{schema}"')

    try:
        dropped = reap_stale_test_schemas(connection)

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname IN (%s, %s, %s)",
                (stale, fresh, legacy),
            )
            remaining = {row[0] for row in cursor.fetchall()}

        assert stale in dropped
        assert remaining == {fresh, legacy}
    finally:
        with connection.cursor() as cursor:
            for schema in (stale, fresh, legacy):
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
