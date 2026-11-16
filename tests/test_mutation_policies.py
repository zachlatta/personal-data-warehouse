"""Mutation auto-approval policies: the Go app owns them; these tests hold the
Python twin of their DDL to the Go one and run the Go store's integration tests
against this suite's disposable Postgres (CI's `go test` has no database)."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
GO_POLICY_STORE = ROOT / "app/internal/mutations/policy_store.go"


def _normalize(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def test_python_declares_the_same_policy_ddl_as_the_go_app() -> None:
    go = GO_POLICY_STORE.read_text()
    block = go.split("var mutationPolicySchemaStatements = []string{", 1)[1].split("\n}\n", 1)[0]
    statements = [_normalize(s) for s in re.findall(r"`(.*?)`", block, re.S)]
    assert len(statements) >= 4
    python = _normalize((ROOT / "src/personal_data_warehouse/postgres.py").read_text())
    for statement in statements:
        assert statement in python, f"postgres.py is missing the Go DDL: {statement[:80]}"


def test_the_claim_time_hand_change_check_is_the_go_query() -> None:
    """The pause after a hand change is judged at propose time in Go and again
    at claim time in Python; both must read the same setpoint history."""
    from personal_data_warehouse.upstream_mutation_policy import THERMOSTAT_MANUAL_CHANGE_SQL

    go = (ROOT / "app/internal/mutations/policy_store.go").read_text()
    query = go.split("const thermostatManualChangeSQL = `", 1)[1].split("`", 1)[0]
    for number, name in (("$1", "account"), ("$2", "device"), ("$3", "since"), ("$4", "pattern")):
        query = query.replace(number, f"%({name})s")
    assert _normalize(query) == _normalize(THERMOSTAT_MANUAL_CHANGE_SQL)


def test_go_declares_the_notification_outbox_exactly_as_python_owns_it() -> None:
    """The Go app creates the outbox table its policy outcome loop writes to,
    for a fresh database where it starts first; Python owns the DDL."""
    go = GO_POLICY_STORE.read_text()
    block = go.split("var notificationOutboxStatements = []string{", 1)[1].split("\n}\n", 1)[0]
    statements = [_normalize(s.replace('"', "'")) for s in re.findall(r'`(.*?)`|"(CREATE INDEX[^"]*)"', block, re.S) for s in s if s]
    assert len(statements) == 2
    python = _normalize((ROOT / "src/personal_data_warehouse/notifications.py").read_text().replace('"', "'"))
    for statement in statements:
        assert statement in python, f"notifications.py does not declare: {statement[:80]}"


# Every Postgres-backed Go test in these packages runs here, whole packages,
# never a hand-kept list: a new test is run (or this fails) without editing
# this file. They skip without PDW_TEST_POSTGRES_URL, which is how CI's plain
# `go test ./...` runs them, so a skip here means a test was not exercised.
GO_POSTGRES_PACKAGES = ("./internal/mutations", "./internal/mutationworkers/...")


@pytest.mark.local_integration
def test_every_go_postgres_test_in_the_mutation_packages_runs() -> None:
    result = subprocess.run(
        ["go", "test", *GO_POSTGRES_PACKAGES, "-count=1", "-p", "1", "-v"],
        cwd=ROOT / "app",
        env={**os.environ, "PDW_TEST_POSTGRES_URL": os.environ["POSTGRES_DATABASE_URL"]},
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stdout[-20000:] + result.stderr
    assert "PDW_TEST_POSTGRES_URL is not set" not in result.stdout, "a Postgres-backed Go test skipped"
    assert result.stdout.count("--- PASS") >= 10, result.stdout[-5000:]


@pytest.mark.local_integration
def test_go_policy_outcome_alerts_survive_restart_and_transport_failure() -> None:
    """The outcome is a row in the notification outbox; the Go outbox worker
    delivers it through a failing transport and after a dead worker's lease.
    Its own database at the default layout, which the Go store reads."""
    import psycopg2
    from psycopg2 import sql

    from psycopg2.extensions import make_dsn

    from tests.conftest import make_test_schema
    from personal_data_warehouse.notifications import ensure_notification_tables
    from personal_data_warehouse.postgres import PostgresWarehouse

    admin = psycopg2.connect(os.environ["POSTGRES_DATABASE_URL"])
    admin.autocommit = True
    database = make_test_schema("outbox")
    with admin.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    wh = None
    try:
        url = make_dsn(os.environ["POSTGRES_DATABASE_URL"], dbname=database)
        wh = PostgresWarehouse(url)
        wh.ensure_upstream_mutation_tables()
        wh.ensure_timeline_tables()
        ensure_notification_tables(wh)
        result = subprocess.run(
            ["go", "test", "./internal/server", "-run", "^TestPolicyOutcomeAlertsSurviveRestartAndTransportFailure$", "-count=1", "-v"],
            cwd=ROOT / "app",
            env={**os.environ, "PDW_POLICY_OUTBOX_TEST_URL": url},
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "--- PASS: TestPolicyOutcomeAlertsSurviveRestartAndTransportFailure" in result.stdout, result.stdout
    finally:
        if wh is not None:
            wh.close()
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        admin.close()
