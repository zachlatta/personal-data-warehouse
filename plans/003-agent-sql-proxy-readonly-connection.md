# Plan 003: Run the agent SQL proxy on a dedicated read-only connection with a real timeout

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat e118893..HEAD -- src/personal_data_warehouse/postgres.py src/personal_data_warehouse/postgres_readonly.py src/personal_data_warehouse/agent_runner.py`
> If any of these changed since this plan was written, compare the "Current
> state" excerpts against the live code before proceeding; on a mismatch,
> treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: LOW
- **Depends on**: none (integration test needs `POSTGRES_DATABASE_URL`, see plan 005)
- **Category**: security
- **Planned at**: commit `e118893`, 2026-07-06

## Why this matters

The warehouse exposes a "read-only SQL" tool to sandboxed LLM **agent
containers** (used by voice-memo / attachment enrichment). Those containers run
model-generated SQL over content that can be attacker-influenced (emails,
images → prompt injection). Two problems make the "read-only" guarantee weaker
than intended:

1. **The agent SQL runs on the primary read-write, autocommit connection** — the
   same connection/role that created and writes every warehouse table. The only
   thing stopping a write is an app-level keyword blocklist
   (`readonly_sql.py`), which is a denylist (it does not block `COPY`,
   `pg_read_file`, `lo_export`, `dblink`, etc.) and can desync from the server
   parser (it doesn't understand Postgres dollar-quoting). Any bypass commits
   immediately because the connection is autocommit — there is no read-only
   transaction to reject the write.
2. **`SET LOCAL statement_timeout = '30s'` is a no-op** on that autocommit
   connection: `SET LOCAL` only applies for the current transaction, and under
   autocommit the SET statement *is* its own transaction, so the subsequent
   query runs with no timeout. A pathological/expensive agent query can pin the
   single shared warehouse connection and stall the whole pipeline.

Notably the timeline read path already does the right thing
(`timeline.py:1450-1452` sets `default_transaction_read_only = on`); the agent
path does not — this is decision drift.

The fix: give the read-only runner its **own** dedicated connection, opened
read-only at the session level with a real `statement_timeout`, isolated from
the write connection. The keyword blocklist stays as a second layer. (A
least-privilege Postgres *role* is a stronger follow-up but needs a DBA `GRANT`
outside this executor's reach — see Maintenance notes.)

## Current state

The runner executes agent SQL on the shared connection —
`src/personal_data_warehouse/postgres_readonly.py:27-40`:

```python
class PostgresReadOnlyRunner:
    def __init__(self, warehouse) -> None:
        self._warehouse = warehouse

    def query(self, sql: str, *, max_rows: int) -> RawResult:
        with self._warehouse._connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '30s'")   # no-op under autocommit
            cursor.execute(sql)
            columns = [description.name for description in cursor.description or ()]
            if max_rows > 0:
                rows = cursor.fetchmany(max_rows)
            else:
                rows = cursor.fetchall()
        return rows_to_raw_result(columns, rows)
```

`self._warehouse._connection` is the primary read-write connection —
`src/personal_data_warehouse/postgres.py:1217-1231`:

```python
class PostgresWarehouse:
    def __init__(self, postgres_database_url: str, *, schema: str = "public") -> None:
        normalized = normalize_postgres_url(postgres_database_url)
        if not normalized:
            raise ValueError("POSTGRES_DATABASE_URL must be set")
        self._schema = _validate_identifier(schema)
        self._connection = psycopg2.connect(normalized)
        self._connection.autocommit = True
        ...
        self._command(f"CREATE SCHEMA IF NOT EXISTS {_identifier(self._schema)}")
        self._command(f"SET search_path TO {_identifier(self._schema)}")

    def close(self) -> None:
        self._connection.close()
```

Note the URL is **not** currently stored on the warehouse (only `_connection`
and `_schema`), so Step 1 adds it.

The runner is constructed per agent run and handed to the proxy —
`src/personal_data_warehouse/agent_runner.py:247-265` (`run_with_warehouse`):

```python
    def run_with_warehouse(self, request, *, warehouse, max_rows=50, max_field_chars=3000):
        query_service = PostgresReadOnlyService(
            PostgresReadOnlyRunner(warehouse),
            max_rows=max_rows,
            max_field_chars=max_field_chars,
        )
        with run_agent_tool_proxy(
            query_service=query_service,
            bind_host=self._config.tool_proxy_bind_host,
            public_host=self._config.tool_proxy_public_host,
        ) as tool_env:
            return self.run(request.with_extra_env(tool_env))
```

The warehouse passed in is the shared production warehouse
(`apple_voice_memos_enrichment.py:125` passes `warehouse=self._warehouse`).

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Read-only unit tests (no PG) | `uv run pytest tests/test_postgres_readonly.py -q` | all pass |
| Runner integration test (needs PG) | `uv run pytest tests/test_postgres_readonly.py -q -k connection` | 1+ passed (not skipped) |
| Agent runner tests | `uv run pytest tests/test_agent_runner.py -q` | all pass |

The new integration test needs `POSTGRES_DATABASE_URL`. If you cannot provide
one, do Steps 1–3 and report Step 4 as unverified — do not mark done.

## Scope

**In scope**:
- `src/personal_data_warehouse/postgres.py` — store the URL; add
  `read_only_connection()`.
- `src/personal_data_warehouse/postgres_readonly.py` — runner uses a dedicated
  read-only connection; add `close()`.
- `src/personal_data_warehouse/agent_runner.py` — close the runner's connection
  after the run.
- `tests/test_postgres_readonly.py` — add a PG-gated connection-enforcement test.

**Out of scope** (do NOT touch):
- `readonly_sql.py` — keep the keyword blocklist exactly as is; it stays as
  defense-in-depth, not the boundary.
- The proxy HTTP server (`agent_tool_proxy.py`), its bearer token, and its bind
  host — a separate hardening item (proxy binds `0.0.0.0`) is tracked separately;
  do not change bind behavior here.
- The timeline read path (`timeline.py`) — already correct.

## Git workflow

- Branch: `advisor/003-agent-sql-proxy-readonly-connection`
- Commit per step or one logical commit. Message:
  `Isolate agent SQL on a dedicated read-only connection`.
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Store the normalized URL and add a `read_only_connection()` factory

In `src/personal_data_warehouse/postgres.py`, in `PostgresWarehouse.__init__`,
store the normalized URL so a second connection can be opened from it:

```python
        self._database_url = normalized      # add right after computing `normalized`
```

Then add a method on `PostgresWarehouse` (place it near `close()`):

```python
    def read_only_connection(self):
        """Open a dedicated connection for untrusted read-only SQL.

        Configured read-only at the session level (writes raise
        ReadOnlySqlTransaction even under autocommit) with a hard
        statement_timeout, and isolated from the primary write connection so a
        bypass of the app-level SQL blocklist cannot modify the warehouse.
        """
        connection = psycopg2.connect(
            self._database_url,
            options="-c default_transaction_read_only=on -c statement_timeout=30000",
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(f"SET search_path TO {_identifier(self._schema)}")
        return connection
```

`_identifier` is already defined in this module (used a few lines above for the
existing `SET search_path`); reuse it, do not re-import.

**Verify**: `uv run python -c "import personal_data_warehouse.postgres"` → no error
(import still succeeds; syntax OK).

### Step 2: Make the runner use its own read-only connection

In `src/personal_data_warehouse/postgres_readonly.py`, change
`PostgresReadOnlyRunner` to open and own a dedicated connection, and drop the
no-op `SET LOCAL`:

```python
class PostgresReadOnlyRunner:
    def __init__(self, warehouse) -> None:
        self._warehouse = warehouse
        self._connection = warehouse.read_only_connection()

    def query(self, sql: str, *, max_rows: int) -> RawResult:
        with self._connection.cursor() as cursor:
            cursor.execute(sql)
            columns = [description.name for description in cursor.description or ()]
            if max_rows > 0:
                rows = cursor.fetchmany(max_rows)
            else:
                rows = cursor.fetchall()
        return rows_to_raw_result(columns, rows)

    def close(self) -> None:
        try:
            self._connection.close()
        except Exception:
            pass
```

Keep everything else in the file unchanged.

**Verify**: `uv run pytest tests/test_postgres_readonly.py -q` → all existing
tests pass (they use a `FakeRunner`/fake warehouse, so this change doesn't break
them). If any existing test constructs a real `PostgresReadOnlyRunner` with a
fake warehouse lacking `read_only_connection`, STOP and report — that test needs
the fake updated, and you should confirm the intended fake shape.

### Step 3: Close the runner's dedicated connection after each agent run

In `src/personal_data_warehouse/agent_runner.py`, `run_with_warehouse`, ensure
the runner's connection is closed when the run finishes:

```python
    def run_with_warehouse(self, request, *, warehouse, max_rows=50, max_field_chars=3000):
        runner = PostgresReadOnlyRunner(warehouse)
        query_service = PostgresReadOnlyService(
            runner,
            max_rows=max_rows,
            max_field_chars=max_field_chars,
        )
        try:
            with run_agent_tool_proxy(
                query_service=query_service,
                bind_host=self._config.tool_proxy_bind_host,
                public_host=self._config.tool_proxy_public_host,
            ) as tool_env:
                return self.run(request.with_extra_env(tool_env))
        finally:
            runner.close()
```

**Verify**: `uv run pytest tests/test_agent_runner.py -q` → all pass.

### Step 4: Add a PG-gated test proving connection-level enforcement

Add a test to `tests/test_postgres_readonly.py`. It must be gated on
`POSTGRES_DATABASE_URL` the same way the warehouse tests are (import
`make_test_schema` from `tests.conftest` or replicate the skip guard used in
`tests/test_postgres_warehouse.py`). The test:

1. Builds a `PostgresWarehouse(url, schema=make_test_schema())`.
2. Constructs `runner = PostgresReadOnlyRunner(warehouse)`.
3. Asserts `runner._connection is not warehouse._connection` (isolation).
4. Asserts a write executed **directly** on the runner's connection is rejected —
   proving the boundary holds even if the blocklist were bypassed:
   ```python
   import psycopg2
   with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
       with runner._connection.cursor() as cur:
           cur.execute("CREATE TEMP TABLE pdw_ro_probe (x int)")
   ```
   (After a failed statement on an autocommit connection no rollback is needed,
   but call `runner._connection.rollback()` defensively before the next query.)
5. Asserts the timeout is a real session setting:
   ```python
   with runner._connection.cursor() as cur:
       cur.execute("SHOW statement_timeout")
       assert cur.fetchone()[0] in ("30s", "30000ms", "30000")
   ```
6. Cleans up: `runner.close()`; drop the throwaway schema if your fixture doesn't.

**Verify** (requires `POSTGRES_DATABASE_URL`):
`uv run pytest tests/test_postgres_readonly.py -q -k connection` → 1 passed (not skipped).

### Step 5: Confirm no other caller depended on the old shared-connection behavior

`grep -rn "PostgresReadOnlyRunner(" src/ tests/` and confirm the only
constructions are the ones you updated (agent_runner) plus test fakes. If a
caller passes something without `read_only_connection`, STOP and report.

**Verify**: `grep -rn "PostgresReadOnlyRunner(" src/` → only `agent_runner.py`.

## Test plan

- New PG-gated test in `tests/test_postgres_readonly.py`: the runner's connection
  (a) is a distinct object from the warehouse write connection, (b) rejects DDL/
  DML at the connection level, (c) has a real `statement_timeout`.
- Existing `tests/test_postgres_readonly.py` unit tests (fake runner) must still
  pass unchanged.
- Verification: `uv run pytest tests/test_postgres_readonly.py tests/test_agent_runner.py -q` → all pass.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `grep -n "SET LOCAL statement_timeout" src/personal_data_warehouse/postgres_readonly.py` returns nothing (the no-op line is gone)
- [ ] `grep -n "read_only_connection" src/personal_data_warehouse/postgres.py src/personal_data_warehouse/postgres_readonly.py` shows the factory and its use
- [ ] `uv run pytest tests/test_postgres_readonly.py tests/test_agent_runner.py -q` exits 0; the new connection test **passed** (not skipped) with `POSTGRES_DATABASE_URL` set
- [ ] `grep -rn "PostgresReadOnlyRunner(" src/` shows only `agent_runner.py`
- [ ] Only the four in-scope files modified (`git diff --name-only`)
- [ ] `plans/README.md` status row for 003 updated

## STOP conditions

Stop and report back (do not improvise) if:

- `postgres_readonly.py` no longer matches the "Current state" excerpt (e.g. it
  already opens its own connection) — the fix may be partly done.
- Any existing test constructs `PostgresReadOnlyRunner` with a fake warehouse
  that has no `read_only_connection` and updating that fake would change test
  intent — report the fake's shape instead of guessing.
- `default_transaction_read_only=on` via the `options` string does not actually
  reject the write probe in Step 4 (e.g. the Postgres role or connection pooler
  overrides session options) — report; the fix approach needs adjustment (likely
  the least-privilege role in Maintenance notes).
- You cannot obtain a `POSTGRES_DATABASE_URL` — do Steps 1–3, 5 and report Step 4
  unverified; do not mark done.

## Maintenance notes

- **Stronger follow-up (ops, not this plan):** create a dedicated least-privilege
  Postgres role with `SELECT`-only grants and no `pg_read_server_files`
  membership, and point `read_only_connection()` at it via a separate URL env
  (e.g. `PDW_AGENT_READONLY_DATABASE_URL`). That closes the residual risk of
  privileged read functions (`pg_read_file`, `lo_export`) that a read-only
  transaction alone does not block. It needs a DBA `GRANT` on the production
  Postgres, so it's deferred out of this executor plan; track it as an infra task.
- A reviewer should confirm the runner connection is genuinely separate (Step 4's
  `is not` assertion) and that the timeout shows up in `SHOW statement_timeout`.
- If a future feature needs the agent to run *writes* (it shouldn't), it must use
  a different, explicitly-authorized path — not this proxy.
