# Plan 008: Run the Go mutation store's schema DDL once per process, not on every request

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**:
> `git diff --stat 768f164..HEAD -- app/internal/mutations/postgres.go app/internal/mutations/postgres_test.go`
> If either changed since this plan was written, compare the "Current state"
> excerpts against the live code before proceeding; on a mismatch, treat it as a
> STOP condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: perf
- **Planned at**: commit `768f164`, 2026-07-07

## Why this matters

`PostgresStore.EnsureTables` runs the full upstream-mutation schema — 5 `CREATE TABLE`, 2 `ALTER TABLE ADD COLUMN`, and 7 `CREATE INDEX`/`CREATE UNIQUE INDEX` statements (`upstreamMutationSchemaStatements`) — and it is called at the top of **every** store method: `CreateRequest`, `ListRequests`, `GetRequest`, and four more. The mutation-review UI polls `ListRequests`/`GetRequest`, so each poll fires ~14 idempotent-but-catalog-locking DDL statements at the single production Postgres.

Although each statement is `IF NOT EXISTS`, `CREATE INDEX`/`ALTER TABLE` still take catalog locks and contend with live searches and ingestion on the small prod host. This is the exact per-tick-DDL anti-pattern the Python side already fixed for its sensor path (`_ensure_upstream_mutation_tables_once` in `defs/upstream_mutations.py`, added after ~170 failed sensor ticks/day in prod) — but here on the Go request hot path.

The store is a **process singleton** (built once at `app/cmd/pdw-mcp/main.go:38`), so ensuring the schema once per process — latching only on success so a transient first failure can still retry — removes the repeated DDL entirely while preserving first-boot table creation.

## Current state

`PostgresStore` is a singleton constructed once — `app/cmd/pdw-mcp/main.go:38`:

```go
		mutationStore, err := mutations.NewPostgresStore(cfg.PostgresDatabaseURL, cfg.QueryTimeout)
```

The struct and ensure method — `app/internal/mutations/postgres.go:19-22, 81-89`:

```go
type PostgresStore struct {
	db      *sql.DB
	timeout time.Duration
}

func (s *PostgresStore) EnsureTables(ctx context.Context) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	for _, statement := range upstreamMutationSchemaStatements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
```

`EnsureTables(ctx)` is called at the start of all seven public methods — `app/internal/mutations/postgres.go:95, 184, 236, 261, 366, 447, 510` (verify: `grep -n 'EnsureTables(ctx)' app/internal/mutations/postgres.go` returns those seven call sites plus the definition at `:81`).

`upstreamMutationSchemaStatements` is the DDL slice at `app/internal/mutations/postgres.go:2114-2189` (5 CREATE TABLE + 2 ALTER + 7 CREATE INDEX).

The intended semantics to mirror are in `src/personal_data_warehouse/defs/upstream_mutations.py:55-70` — a once-per-process guard that latches so DDL runs once, with a comment explaining the prod incident it fixed.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Go build | `cd app && go build ./...` | exit 0 |
| Mutations pkg tests | `cd app && go test ./internal/mutations/...` | ok, all pass |
| Go full suite | `cd app && go test ./...` | all `ok`/`no test files` |
| Race check (optional) | `cd app && go test -race ./internal/mutations/...` | ok |

## Scope

**In scope** (the only files you should modify):
- `app/internal/mutations/postgres.go` — add a latch so `EnsureTables` runs the DDL only once per process (on success), guarded for concurrency.
- `app/internal/mutations/postgres_test.go` — add a same-package unit test proving the latch short-circuits after success.

**Out of scope** (do NOT touch):
- The `upstreamMutationSchemaStatements` DDL slice — leave every statement exactly as is.
- The seven `EnsureTables(ctx)` call sites — keep them; they now become cheap once the latch is set. Do not remove them (they guarantee first-boot creation for whichever method is called first).
- The Python `_ensure_upstream_mutation_tables_once` — it is the reference, not a target.
- Any other store (`chatgptsession`, `credential_ingest`) — separate.

## Git workflow

- Branch: `advisor/008-mutation-store-ensure-tables-once`
- One logical commit. Suggested message (imperative, matches repo style):
  `Ensure upstream-mutation schema once per process, not per request`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Add a success-latching guard to the store

In `app/internal/mutations/postgres.go`:

1. Add `sync` to the imports if not already present (check the import block; `grep -n '"sync"' app/internal/mutations/postgres.go`).

2. Extend the struct with a mutex and a latched flag:

   ```go
   type PostgresStore struct {
   	db        *sql.DB
   	timeout   time.Duration
   	ensureMu  sync.Mutex
   	ensured   bool
   }
   ```

3. Rename the current DDL loop to an unexported helper and make `EnsureTables` latch on success:

   ```go
   func (s *PostgresStore) EnsureTables(ctx context.Context) error {
   	s.ensureMu.Lock()
   	defer s.ensureMu.Unlock()
   	if s.ensured {
   		return nil
   	}
   	if err := s.ensureTablesNow(ctx); err != nil {
   		return err // not latched: a transient first failure can retry on the next call
   	}
   	s.ensured = true
   	return nil
   }

   func (s *PostgresStore) ensureTablesNow(ctx context.Context) error {
   	ctx, cancel := s.withTimeout(ctx)
   	defer cancel()
   	for _, statement := range upstreamMutationSchemaStatements {
   		if _, err := s.db.ExecContext(ctx, statement); err != nil {
   			return err
   		}
   	}
   	return nil
   }
   ```

   Note: the body of `ensureTablesNow` is the exact current `EnsureTables` body — move it, do not rewrite it.

**Verify**: `cd app && go build ./...` → exit 0.

### Step 2: Unit-test the latch (no Postgres required)

Add a test to `app/internal/mutations/postgres_test.go` (it is already `package mutations`, so it can touch unexported fields). The test proves that once `ensured` is set, `EnsureTables` returns without dialing the database — construct a store with a **nil** `db` and the latch pre-set, and confirm no panic and a nil error (if the latch did not short-circuit, `ExecContext` on a nil `db` would panic):

```go
func TestEnsureTablesLatchesAfterSuccess(t *testing.T) {
	s := &PostgresStore{ensured: true} // db is nil on purpose
	if err := s.EnsureTables(context.Background()); err != nil {
		t.Fatalf("expected latched EnsureTables to no-op, got %v", err)
	}
	// A second call must also be a no-op.
	if err := s.EnsureTables(context.Background()); err != nil {
		t.Fatalf("expected second latched EnsureTables to no-op, got %v", err)
	}
}
```

Add `context` to the test file's imports if needed. This asserts the fast path never touches `db` once latched — the property that eliminates the per-request DDL. (Verifying the DDL runs exactly once against a real database would need a Postgres-backed test; the functional guarantee here is that a latched store issues zero statements, which is what matters for the hot path. If a PG-gated mutations integration harness already exists in this package, you may additionally add an integration test that calls `ListRequests` twice and asserts success — optional.)

**Verify**: `cd app && go test ./internal/mutations/ -run TestEnsureTablesLatchesAfterSuccess -v` → PASS.

### Step 3: Full mutations + module suite

**Verify**:
- `cd app && go test ./internal/mutations/...` → ok, all pass (existing tests must be unaffected).
- `cd app && go test ./...` → every package `ok` or `no test files`.
- Optional: `cd app && go test -race ./internal/mutations/...` → ok (confirms the mutex use is race-clean).

## Test plan

- New same-package unit test in `postgres_test.go`: a store with `ensured=true` and a nil `db` returns nil from `EnsureTables` without panicking, on both first and second call — proving the latch short-circuits before any DB access.
- All existing mutations tests continue to pass unchanged.
- Verification: `cd app && go test ./internal/mutations/...` → all pass.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `cd app && go build ./...` exits 0
- [ ] `grep -n 'ensureMu\|ensured\|ensureTablesNow' app/internal/mutations/postgres.go` shows the mutex, the flag, and the extracted helper
- [ ] `EnsureTables` checks `s.ensured` and latches only after a successful `ensureTablesNow` (inspect the method)
- [ ] `cd app && go test ./internal/mutations/...` passes, including `TestEnsureTablesLatchesAfterSuccess`
- [ ] `cd app && go test ./...` exits 0
- [ ] The seven original `EnsureTables(ctx)` call sites still exist (`grep -c 'EnsureTables(ctx)' app/internal/mutations/postgres.go` ≥ 7)
- [ ] Only the two in-scope files were modified (`git diff --name-only`)
- [ ] `plans/README.md` status row for 008 updated

## STOP conditions

Stop and report back (do not improvise) if:

- `EnsureTables` no longer matches the "Current state" excerpt (e.g. it already caches) — the fix may be partly done.
- `NewPostgresStore` or `main.go` turns out to construct a **new** `PostgresStore` per request rather than once at startup (verify `grep -rn 'NewPostgresStore' app/ | grep -v _test`) — if it is per-request, an instance latch is insufficient; report so the guard can be made process-global instead.
- Adding a `sync.Mutex` to the struct breaks a place that copies `PostgresStore` by value (a `go vet` copylocks warning) — report; the store is used by pointer everywhere here, so this should not happen, but confirm.

## Maintenance notes

- The latch is per-process and resets on restart, so schema changes still apply on the next deploy (fresh process → first call runs the DDL). This matches the Python sensor guard's behavior.
- The latch releases on failure (no latch set), so a Postgres blip during the first request does not permanently prevent table creation — the next request retries. A reviewer should confirm this: the `s.ensured = true` line is reached only after `ensureTablesNow` returns nil.
- The mutex serializes concurrent first-callers so the DDL runs once even under a burst at startup. Under steady state the fast path is a single bool check under a briefly-held lock — negligible.
- If additional store methods are added later, they should keep calling `EnsureTables(ctx)` (now cheap) rather than the raw `ensureTablesNow`.
