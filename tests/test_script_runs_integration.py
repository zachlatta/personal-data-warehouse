from __future__ import annotations

import os
from pathlib import Path
import subprocess

from tests.test_postgres_warehouse import warehouse  # noqa: F401


def test_script_worker_reads_and_telemetry_ddl_match(warehouse):
    warehouse.ensure_pipeline_health_tables()
    schema = warehouse.physical_schema_name("ops")
    result = subprocess.run(
        ["go", "test", "./internal/scripts", "-run", "^TestScriptDatabaseIntegration$", "-count=1", "-v"],
        cwd=Path(__file__).resolve().parents[1] / "app",
        env={**os.environ, "PDW_SCRIPT_TEST_URL": os.environ["POSTGRES_DATABASE_URL"],
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
