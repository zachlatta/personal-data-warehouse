"""Exercise the Go credential store against the Python-provisioned table."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import cleanup_test_warehouse, make_test_schema


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"], schema=make_test_schema("mcp_health"))
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


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


def test_mcp_connection_health_shows_a_dead_connection_with_its_cause(warehouse):
    """Until 2026-11-23 a connection's status lived only inside its sealed payload.

    hack_club_data_warehouse failed with 'authorization expired' for ~11 hours
    and no surface outside agent transcripts showed it. The app now writes the
    status, when it changed and a redacted cause beside the payload, and this
    view reads one row per connection.
    """
    warehouse.ensure_pipeline_health_tables()
    changed = "2026-11-22 17:12:00+00"
    warehouse._command(
        """
        INSERT INTO @mcp_connections (name, payload, status, status_changed_at, last_error)
        VALUES ('warehouse', 'sealed', 'authorization_failed', %s, 'token endpoint answered 400 invalid_grant'),
               ('tasks', 'sealed', 'connected', %s, ''),
               ('flaky', 'sealed', 'connected', %s, 'transient: token endpoint answered 503'),
               ('gone', 'sealed', 'deleted', %s, ''),
               ('legacy', 'sealed', '', '1970-01-01 00:00:00+00', '')
        """,
        (changed, changed, changed, changed),
    )
    rows = {
        row["connection"]: row
        for row in warehouse._query_dicts("SELECT * FROM @marts_ops_mcp_connection_health")
    }
    assert set(rows) == {"warehouse", "tasks", "flaky", "legacy"}
    assert rows["warehouse"]["status"] == "failing"
    assert rows["warehouse"]["connection_status"] == "authorization_failed"
    assert rows["warehouse"]["last_error"] == "token endpoint answered 400 invalid_grant"
    assert rows["warehouse"]["status_changed_at"] is not None
    assert rows["tasks"]["status"] == "ok"
    assert rows["tasks"]["last_error"] is None
    assert rows["flaky"]["status"] == "ok"
    assert rows["flaky"]["last_error"] == "transient: token endpoint answered 503"
    # Written before the app stored a plain status: unknown, not ok.
    assert rows["legacy"]["status"] == "unknown"
    assert rows["legacy"]["status_changed_at"] is None
