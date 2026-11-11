from __future__ import annotations

import os
from pathlib import Path
import subprocess
import secrets

import psycopg2
from psycopg2.extensions import make_dsn

from tests.test_postgres_warehouse import warehouse  # noqa: F401


def test_script_worker_reads_and_telemetry_ddl_match(warehouse, monkeypatch):
    warehouse.ensure_pipeline_health_tables()
    schema = warehouse.physical_schema_name("ops")
    login = "pdw_reader_" + secrets.token_hex(8)
    password = secrets.token_urlsafe(32)
    monkeypatch.setenv("PDW_QUERY_POSTGRES_LOGIN", login)
    monkeypatch.setenv("PDW_QUERY_POSTGRES_PASSWORD", password)
    warehouse._ensure_query_role()
    assert warehouse._query_role_setup_needed() is False
    # Repair password rotation once; a stable ensure is read-only afterwards.
    password = secrets.token_urlsafe(32)
    monkeypatch.setenv("PDW_QUERY_POSTGRES_PASSWORD", password)
    assert warehouse._query_role_setup_needed() is True
    warehouse._ensure_query_role()
    assert warehouse._query_role_setup_needed() is False
    query_url = make_dsn(os.environ["POSTGRES_DATABASE_URL"], user=login, password=password)
    with psycopg2.connect(query_url) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=session_user")
            assert cursor.fetchone() == (False,) * 5
            for statement in ["SET ROLE postgres", "SELECT pg_read_file('/etc/passwd')"]:
                try:
                    cursor.execute(statement)
                except psycopg2.errors.InsufficientPrivilege:
                    conn.rollback()
                else:
                    raise AssertionError(f"unprivileged login accepted {statement}")
    result = subprocess.run(
        ["go", "test", "./internal/scripts", "-run", "^TestScriptDatabaseIntegration$", "-count=1", "-v"],
        cwd=Path(__file__).resolve().parents[1] / "app",
        env={**os.environ, "PDW_SCRIPT_TEST_URL": os.environ["POSTGRES_DATABASE_URL"],
             "PDW_SCRIPT_TEST_QUERY_URL": query_url,
             "PDW_SCRIPT_TEST_SCHEMA": schema, "PDW_SCRIPT_TEST_ROLE": warehouse.query_role},
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--- PASS: TestScriptDatabaseIntegration" in result.stdout
    def columns(table):
        return warehouse._query("""SELECT column_name,data_type,is_nullable,column_default
            FROM information_schema.columns WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position""", (schema, table))
    assert columns("script_runs") == columns("script_runs_go")
    assert warehouse._query("SELECT count(*) FROM @script_runs")[0][0] == 1
    warehouse._raw_command(f'DROP ROLE "{login}"')


def test_script_summary_groups_failures_by_outcome(warehouse):
    warehouse.ensure_pipeline_health_tables()
    for i, outcome in enumerate(["ok", "cpu_limit", "script_error", "cpu_limit", "busy"]):
        warehouse._command("INSERT INTO @script_runs (run_id,occurred_at,outcome) VALUES (%s,now(),%s)", (str(i), outcome))
    assert dict(warehouse._query("SELECT outcome,runs FROM @marts_script_runs")) == {"ok": 1, "cpu_limit": 2, "script_error": 1, "busy": 1}
