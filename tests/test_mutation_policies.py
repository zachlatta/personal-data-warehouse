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
