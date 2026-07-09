# Plan 006: Enforce read-only + a real statement timeout on the Go MCP/query SQL path

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**:
> `git diff --stat 768f164..HEAD -- app/internal/query/postgres.go app/internal/query/sql.go app/internal/query/sql_test.go src/personal_data_warehouse/readonly_sql.py`
> If any of these changed since this plan was written, compare the "Current
> state" excerpts against the live code before proceeding; on a mismatch, treat
> it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: LOW
- **Depends on**: none
- **Category**: security
- **Planned at**: commit `768f164`, 2026-07-07

## Why this matters

Plan 003 already hardened the **Python** agent SQL proxy so untrusted, model-generated SQL runs on a dedicated connection opened `default_transaction_read_only=on` with a real `statement_timeout`. The **Go** MCP query path — the `query` and `sql` tools exposed by the MCP server (`app/internal/query/service.go`) and the timeline read endpoints — never got the same treatment. It still:

1. **Runs on a write-capable connection with no read-only transaction.** `query.PostgresRunner` opens `sql.Open("pgx", cfg.PostgresDatabaseURL)` — the same full-privilege role the app uses to write the credential/session/mutation tables — and executes statements with `db.QueryContext` directly, no read-only transaction. The only write barrier is the lexical `ValidateReadOnlySQL` blocklist, which is a denylist and misses `SELECT … INTO <table>` (Postgres `CREATE TABLE AS`), so a validator gap becomes a real write.
2. **Has no server-side `statement_timeout`.** A pathological or injected query (e.g. a cartesian join, `pg_sleep`) can pin a connection indefinitely; nothing at the DB level caps it.

These tools are reachable by the model through the claude.ai MCP connector, over the same enriched personal data (emails, messages) that can carry prompt-injection. This is the identical threat model that justified plan 003 — just on the other execution path. Both `PostgresRunner` instances (the MCP query runner and the dedicated timeline runner) are **read-only by purpose** (they only ever run `SELECT`s), so making every query run inside a read-only transaction with a real timeout is safe and closes the gap without touching write paths.

The fix has two parts: (a) run every `QueryArgs` statement inside a `READ ONLY` transaction with a `SET LOCAL statement_timeout`, so the database — not a keyword list — enforces read-only and a hard cap; (b) add `INTO` to the lexical blocklists in both Go and Python as defense-in-depth (belt-and-suspenders + a clearer error message).

## Current state

**The Go runner executes on a plain write-capable pool, no read-only tx, no timeout** — `app/internal/query/postgres.go:13-38, 54-107`:

```go
type PostgresRunner struct {
	db *sql.DB
}

func NewPostgresRunner(databaseURL string, timeout time.Duration) (*PostgresRunner, error) {
	// ...
	db, err := sql.Open("pgx", databaseURL)   // full-privilege role (cfg.PostgresDatabaseURL)
	// ... ping with `timeout`, then `timeout` is discarded ...
	return &PostgresRunner{db: db}, nil
}

func (r *PostgresRunner) Query(ctx context.Context, statement string, maxRows int) (RawResult, error) {
	return r.QueryArgs(ctx, statement, nil, maxRows)
}

func (r *PostgresRunner) QueryArgs(ctx context.Context, statement string, args []any, maxRows int) (RawResult, error) {
	// ...
	rows, err := r.db.QueryContext(ctx, statement, args...)   // <-- no read-only tx, no statement_timeout
	// ... scan rows ...
}
```

Both runner instances flow through `QueryArgs`, so fixing it there covers everything:
- MCP `query`/`sql` tools: `app/cmd/pdw-mcp/main.go:28` → `query.NewPostgresRunner(cfg.PostgresDatabaseURL, cfg.QueryTimeout)`; used at `app/internal/query/service.go:306` and `:517` (`s.runner.Query(...)`), each already gated by `ValidateReadOnlySQL`.
- Timeline reads: `app/internal/server/server.go:290` → a dedicated `query.NewPostgresRunner(cfg.TimelineDatabaseURL, cfg.QueryTimeout)`; used via `s.timeline.QueryArgs(...)` in `app/internal/server/timeline.go` (parameterized, app-owned SQL — also read-only).

`cfg.QueryTimeout` defaults to `300 * time.Second` (`app/internal/config/config.go:141`) and is currently only used for the connection ping. It is the natural hard cap to reuse as the per-statement timeout.

**The lexical blocklist misses `INTO`** — `app/internal/query/sql.go:16-33` (`forbiddenKeywords`) has `ALTER, ATTACH, CREATE, DELETE, DETACH, DROP, GRANT, INSERT, KILL, OPTIMIZE, RENAME, REVOKE, SYSTEM, TRUNCATE, UPDATE` — no `INTO`. The first-keyword allowlist (`SELECT, WITH, SHOW, EXPLAIN`) means `COPY`-first statements are already rejected, but `SELECT … INTO new_table …` starts with `SELECT` and passes. The Python copy has the identical gap — `src/personal_data_warehouse/readonly_sql.py:14-30` (`FORBIDDEN_KEYWORDS`), no `INTO`.

Note: `rejectMultipleStatements` (`sql.go:59`) already blocks `;`-separated statements in Go, and `validate_readonly_sql` (`readonly_sql.py:83`) does the same in Python — do not touch that logic.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Go build | `cd app && go build ./...` | exit 0 |
| Go query pkg tests | `cd app && go test ./internal/query/...` | ok, all pass |
| Go full suite | `cd app && go test ./...` | all `ok`/`no test files` |
| Python read-only unit tests | `uv run pytest tests/test_readonly_sql.py -q` | all pass (if the file exists; see Step 4) |
| Python full suite (sanity) | `uv run pytest -q` | no new failures vs. baseline (831 passed, 113 skipped) |

## Scope

**In scope** (the only files you should modify):
- `app/internal/query/postgres.go` — run each `QueryArgs` in a read-only transaction with a `SET LOCAL statement_timeout`; store the timeout on the runner.
- `app/internal/query/sql.go` — add `INTO` to `forbiddenKeywords`.
- `app/internal/query/sql_test.go` — add reject cases for `SELECT … INTO` (and confirm existing allow-cases still pass).
- `app/internal/query/postgres_integration_test.go` (create, PG-gated) — prove writes and long queries are rejected at the connection level.
- `src/personal_data_warehouse/readonly_sql.py` — add `INTO` to `FORBIDDEN_KEYWORDS`.
- `tests/test_readonly_sql.py` (create if absent, else extend) — assert `SELECT … INTO` is rejected by `validate_readonly_sql`.

**Out of scope** (do NOT touch, even though they look related):
- `src/personal_data_warehouse/postgres_readonly.py` and `postgres.py` `read_only_connection()` — the Python runtime path is already fixed by plan 003; only the shared lexical blocklist (`readonly_sql.py`) gets the `INTO` addition here.
- The MCP tool wiring, auth, and the proxy bind host — separate concerns.
- `ValidateReadOnlySQL`'s first-keyword allowlist and `rejectMultipleStatements` — leave the structure intact; only add one keyword.
- The timeline SQL text itself — it is already parameterized and read-only.

## Git workflow

- Branch: `advisor/006-go-query-path-readonly-connection`
- One logical commit. Suggested message (matches the repo's imperative style, e.g. `Isolate agent SQL on a dedicated read-only connection`):
  `Run the Go MCP query path in a read-only transaction with a real timeout`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Store the query timeout on the runner

In `app/internal/query/postgres.go`, add a field and keep the passed-in timeout instead of discarding it after the ping:

```go
type PostgresRunner struct {
	db           *sql.DB
	queryTimeout time.Duration
}
```

In `NewPostgresRunner`, compute an effective timeout and store it on the returned struct. Just before `return &PostgresRunner{...}`:

```go
	effectiveTimeout := timeout
	if effectiveTimeout <= 0 {
		effectiveTimeout = 30 * time.Second
	}
	return &PostgresRunner{db: db, queryTimeout: effectiveTimeout}, nil
```

**Verify**: `cd app && go build ./internal/query/` → exit 0.

### Step 2: Run every query inside a READ ONLY transaction with a statement timeout

In `app/internal/query/postgres.go`, rewrite the body of `QueryArgs` so the statement runs inside a read-only transaction that sets a local `statement_timeout`. Keep all the existing row-scanning/logging logic; only change how the statement is dispatched. The target shape:

```go
func (r *PostgresRunner) QueryArgs(ctx context.Context, statement string, args []any, maxRows int) (RawResult, error) {
	logger := slog.Default().With("component", "postgres")
	started := time.Now()
	logger.DebugContext(ctx, "Postgres query dispatch", "sql", statement, "max_rows", maxRows)

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		logger.ErrorContext(ctx, "Postgres begin read-only tx failed", "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}
	// Rollback is the correct release for a read-only transaction: there is
	// nothing to commit, and after a successful read it is a cheap no-op.
	defer func() { _ = tx.Rollback() }()

	timeoutMs := r.queryTimeout.Milliseconds()
	if timeoutMs <= 0 {
		timeoutMs = 30000
	}
	// SET LOCAL is effective here because we are inside an explicit transaction
	// (unlike the autocommit no-op that plan 003 fixed on the Python side).
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL statement_timeout = %d", timeoutMs)); err != nil {
		logger.ErrorContext(ctx, "Postgres set statement_timeout failed", "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}

	rows, err := tx.QueryContext(ctx, statement, args...)
	if err != nil {
		logger.ErrorContext(ctx, "Postgres query dispatch failed", "sql", statement, "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}
	defer rows.Close()

	// ... KEEP the existing columns/scan/normalize loop exactly as it is now ...
	// (rows.Columns(), the for rows.Next() maxRows cap, Scan, normalizeValue,
	//  rows.Err(), the final DebugContext log, and `return result, nil`.)
}
```

Do not change the scanning loop below `defer rows.Close()` — copy it verbatim from the current implementation. `fmt` is already imported in this file (used elsewhere); if `go build` reports it unused/missing, fix the import block.

**Verify**: `cd app && go build ./...` → exit 0.

### Step 3: Block `SELECT … INTO` in the Go lexical validator

In `app/internal/query/sql.go`, add `"INTO"` to the `forbiddenKeywords` map (keep alphabetical order for readability, between `INSERT` and `KILL`):

```go
	"INSERT":   true,
	"INTO":     true,
	"KILL":     true,
```

**Verify**: `cd app && go build ./internal/query/` → exit 0.

### Step 4: Add reject tests for `SELECT … INTO` (Go)

In `app/internal/query/sql_test.go`, add a case to the existing rejection test (`TestValidateReadOnlySQLRejectsMutationsAndMultipleStatements`) covering:

- `SELECT * INTO evil_table FROM gmail_messages`
- `select id into other from slack_messages`

Both must return a non-nil error. Confirm the existing allow-cases (`SELECT 1`, `WITH ...`, etc.) are unchanged and still pass.

**Verify**: `cd app && go test ./internal/query/...` → ok, all pass (including the new cases).

### Step 5: Mirror the `INTO` block in Python (defense-in-depth)

In `src/personal_data_warehouse/readonly_sql.py`, add `"INTO"` to the `FORBIDDEN_KEYWORDS` set (between `"INSERT"` and `"KILL"`):

```python
    "INSERT",
    "INTO",
    "KILL",
```

Then add/extend a unit test. If `tests/test_readonly_sql.py` exists, add a case; otherwise create it with a minimal test:

```python
import pytest
from personal_data_warehouse.readonly_sql import validate_readonly_sql


def test_validate_readonly_sql_rejects_select_into():
    with pytest.raises(ValueError):
        validate_readonly_sql("SELECT * INTO evil FROM gmail_messages")


def test_validate_readonly_sql_allows_plain_select():
    validate_readonly_sql("SELECT 1")  # must not raise
```

If the real function name/signature differs from `validate_readonly_sql(sql) -> None`, match what `readonly_sql.py` actually exposes (read the file — do not guess).

**Verify**: `uv run pytest tests/test_readonly_sql.py -q` → all pass.

### Step 6: Add a PG-gated Go integration test proving connection-level enforcement

Model this on the existing PG-gated Go test in this repo — read `app/internal/query/postgres_integration_test.go` if it exists to copy its skip guard and connection setup; if it does not exist, create `app/internal/query/postgres_integration_test.go` and use the same env-var gate the Python suite uses (`POSTGRES_DATABASE_URL`), skipping when unset:

```go
func TestPostgresRunnerRejectsWrites(t *testing.T) {
	url := os.Getenv("POSTGRES_DATABASE_URL")
	if url == "" {
		t.Skip("POSTGRES_DATABASE_URL is not set")
	}
	runner, err := NewPostgresRunner(url, 5*time.Second)
	if err != nil { t.Fatalf("open: %v", err) }
	defer runner.Close()

	// A write that bypasses the lexical validator (called directly on the runner)
	// must still fail because the transaction is READ ONLY.
	_, err = runner.Query(context.Background(),
		"CREATE TEMP TABLE pdw_ro_probe (x int)", 1)
	if err == nil {
		t.Fatal("expected read-only transaction to reject CREATE TEMP TABLE")
	}
	// A plain read still works.
	if _, err := runner.Query(context.Background(), "SELECT 1", 1); err != nil {
		t.Fatalf("read query failed: %v", err)
	}
}
```

If you cannot provide `POSTGRES_DATABASE_URL`, implement the test (it will skip) and report Step 6 as unverified — do not mark the plan done.

**Verify** (with `POSTGRES_DATABASE_URL` set): `cd app && go test ./internal/query/ -run TestPostgresRunnerRejectsWrites -v` → PASS (not skipped).

### Step 7: Full-suite sanity

**Verify**:
- `cd app && go test ./...` → every package `ok` or `no test files`.
- `uv run pytest -q` → no new failures vs. the baseline (831 passed, 113 skipped).

## Test plan

- **Go unit** (`sql_test.go`): `SELECT … INTO` (upper and lower case) is rejected; existing allow/reject cases unchanged.
- **Go integration** (`postgres_integration_test.go`, PG-gated): a direct write on the runner is rejected at the transaction level even without the lexical validator; a plain `SELECT` succeeds.
- **Python unit** (`test_readonly_sql.py`): `SELECT … INTO` rejected by `validate_readonly_sql`; plain `SELECT` allowed.
- Use `app/internal/query/postgres_integration_test.go` (or the query package's existing integration test, if present) as the structural pattern for the PG gate.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `cd app && go build ./...` exits 0
- [ ] `grep -n 'BeginTx' app/internal/query/postgres.go` shows a `sql.TxOptions{ReadOnly: true}` transaction in `QueryArgs`
- [ ] `grep -n 'SET LOCAL statement_timeout' app/internal/query/postgres.go` returns a match
- [ ] `grep -n '"INTO"' app/internal/query/sql.go` and `grep -n '"INTO"' src/personal_data_warehouse/readonly_sql.py` both match
- [ ] `cd app && go test ./internal/query/...` passes, including the new `SELECT … INTO` reject cases
- [ ] `cd app && go test ./...` exits 0
- [ ] The PG-gated `TestPostgresRunnerRejectsWrites` **passed** (not skipped) with `POSTGRES_DATABASE_URL` set
- [ ] `uv run pytest tests/test_readonly_sql.py -q` passes; `uv run pytest -q` shows no new failures
- [ ] Only the in-scope files were modified (`cd app && git status` / `git diff --name-only`)
- [ ] `plans/README.md` status row for 006 updated

## STOP conditions

Stop and report back (do not improvise) if:

- The `QueryArgs` body no longer matches the "Current state" excerpt (e.g. it already opens a transaction or already sets a timeout) — the fix may be partly done.
- Reusing `cfg.QueryTimeout` (300s) as the `statement_timeout` breaks a legitimate long-running query in an existing test, or a timeline test starts failing — report; the timeout may need to be a separate, larger config value rather than a change to behavior.
- `db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})` does **not** cause the `CREATE TEMP TABLE` probe in Step 6 to fail (e.g. a connection pooler in front of Postgres swallows the read-only mode) — report; the enforcement approach needs adjustment (likely a dedicated read-only DB role, see Maintenance notes).
- Adding `INTO` to a blocklist breaks a pre-existing legitimate query in the suite (a column/table genuinely named `into`) — report rather than removing the guard.

## Maintenance notes

- **Stronger follow-up (ops, not this plan):** the same least-privilege Postgres role recommended in plan 003 would also back these Go runners; pointing `NewPostgresRunner` at a `SELECT`-only role via a separate URL env removes the residual risk of privileged read functions (`pg_read_file`, `lo_export`) that a read-only transaction alone does not block. Needs a DBA `GRANT`; track as infra.
- A reviewer should confirm both `PostgresRunner` construction sites (`main.go:28` MCP, `server.go:290` timeline) benefit — the fix is in the shared `QueryArgs`, so both do — and that the read-only transaction did not regress timeline latency (the `BEGIN … READ ONLY`/`ROLLBACK` overhead is negligible but confirm the timeline tests still pass).
- The Go and Python lexical blocklists are maintained as two copies (`sql.go` and `readonly_sql.py`). If either grows again, update both. A future consolidation into one shared spec is possible but out of scope here.
- If a future feature legitimately needs a long analytical query beyond `cfg.QueryTimeout`, introduce an explicit larger timeout config rather than removing the cap.
