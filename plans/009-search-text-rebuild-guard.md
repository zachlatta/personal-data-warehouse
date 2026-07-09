# Plan 009: Skip the `search_text()` recompile when its generated DDL is unchanged

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**:
> `git diff --stat 768f164..HEAD -- src/personal_data_warehouse/postgres.py`
> `postgres.py` is high-churn. Compare the "Current state" excerpts against the
> live code before proceeding; on any mismatch in the search-view ensure path,
> treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED
- **Depends on**: none
- **Category**: perf
- **Planned at**: commit `768f164`, 2026-07-07

## Why this matters

`search_text()` is the warehouse's single cross-source search function — a generated ~450-line PL/pgSQL function that fans out to per-source BM25 indexes. Every time any `ensure_*` table method runs, it calls `_ensure_search_views_if_possible()`, which takes a global advisory lock and then unconditionally issues `DROP VIEW` statements, ensures a large index set, and runs `CREATE OR REPLACE FUNCTION search_text(...)` (plus `search_text_sources()`). Recompiling a large PL/pgSQL function and taking the catalog locks that `CREATE OR REPLACE FUNCTION` implies happens on **every** asset/sensor tick — there are dozens of `ensure_*` call sites across the pipeline, and many sensors run at 30–60s. On the single small production Postgres this is a recurring source of catalog-lock contention against live searches and ingestion (the same class of per-tick-DDL pain already fixed elsewhere, e.g. `defs/upstream_mutations.py`).

The generated function only changes when the code that generates it changes (or when the set of searched tables / the source floor changes). So the recompile is almost always redundant. This plan adds a guard: compute a signature of what determines the generated DDL, store it in a tiny marker table, and skip the `CREATE OR REPLACE FUNCTION` rebuild when the signature already matches **and** the function still exists in the catalog. On a real change (code edit or new source) the signature differs and it rebuilds exactly once; if the function is somehow missing, it rebuilds regardless.

The signature is derived from `inspect.getsource(...)` of the generator method plus the table set and floor — so it auto-invalidates on any edit to the DDL, with **no manual version constant to remember to bump**.

## Current state

The ensure path — `src/personal_data_warehouse/postgres.py:5445-5478`:

```python
    def _ensure_search_views_if_possible(self) -> None:
        # Several Dagster assets can call ensure_* concurrently on deploy. The
        # shared search_text() function/index refresh mutates global Postgres
        # catalog rows, so serialize it to avoid "tuple concurrently updated"
        # races between otherwise-idempotent DDL statements.
        self._command("SELECT pg_advisory_lock(%s)", (SEARCH_SCHEMA_REFRESH_LOCK_ID,))
        try:
            self._ensure_search_views_if_possible_locked()
        finally:
            self._command("SELECT pg_advisory_unlock(%s)", (SEARCH_SCHEMA_REFRESH_LOCK_ID,))

    def _ensure_search_views_if_possible_locked(self) -> None:
        # The person_identities view was removed; drop any copy left behind ...
        self._command("DROP VIEW IF EXISTS person_identities")
        # The legacy cross-source searchable_text view ... has been replaced ...
        self._command("DROP VIEW IF EXISTS searchable_text")
        if all(self._relation_exists(table) for table in self._SEARCHABLE_TEXT_TABLES):
            # Build every bm25 index search_text() references BEFORE (re)creating
            # the function ...
            self._ensure_indexes(self._SEARCHABLE_TEXT_TABLES)
            self._ensure_search_text_function()
```

Key facts confirmed by reading:
- `_ensure_search_text_function` (`postgres.py:5479`) builds the DDL inline and issues it via `self._command(...)` (the big `CREATE OR REPLACE FUNCTION search_text(...)` near `postgres.py:5905`, and `CREATE OR REPLACE FUNCTION search_text_sources()` near `postgres.py:6025`). It is the expensive statement.
- `SEARCH_TEXT_SOURCE_FLOOR` is a module-level constant interpolated into the generated SQL (`postgres.py` uses `str(SEARCH_TEXT_SOURCE_FLOOR)` around line 6018).
- `self._SEARCHABLE_TEXT_TABLES` is the tuple of searched tables (`postgres.py:5419-5432` area).
- Helpers available: `self._command(sql, params=None)` (`postgres.py:6665`), `self._query(sql, params)` (returns rows; used by `_relation_exists` at `postgres.py:6428`), and `_relation_exists`.
- The advisory lock is already held around `_ensure_search_views_if_possible_locked`, so the marker read/write and the rebuild are already serialized across processes — no extra locking needed.

Check whether `hashlib` and `inspect` are already imported at the top of `postgres.py`:
`grep -n '^import hashlib\|^import inspect' src/personal_data_warehouse/postgres.py`. Add whichever is missing.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Import check | `uv run python -c "import personal_data_warehouse.postgres"` | no error |
| Warehouse tests (PG-gated) | `uv run pytest tests/test_postgres_warehouse.py -q` | pass (many need `POSTGRES_DATABASE_URL`; without it they skip) |
| Targeted new test (needs PG) | `uv run pytest tests/test_postgres_warehouse.py -q -k search_schema` | 1+ passed (not skipped) |
| Full suite (sanity) | `uv run pytest -q` | no new failures vs. baseline (831 passed, 113 skipped) |

This plan's new test needs `POSTGRES_DATABASE_URL` (the warehouse cannot be constructed without a real Postgres). If you cannot provide one, implement Steps 1–2, run the import check, and report the test step unverified — do not mark done.

## Scope

**In scope** (the only files you should modify):
- `src/personal_data_warehouse/postgres.py` — add the signature helper, the marker table, the function-exists check, and gate the rebuild in `_ensure_search_views_if_possible_locked`.
- `tests/test_postgres_warehouse.py` — add a PG-gated test that a second ensure does not re-issue the `CREATE OR REPLACE FUNCTION search_text`, and that dropping the marker (or the function) forces a rebuild.

**Out of scope** (do NOT touch):
- The generated SQL inside `_ensure_search_text_function` — do not change what the function does; only gate *whether* it runs.
- The two `DROP VIEW IF EXISTS` legacy-cleanup statements — leave them running unconditionally (they are cheap no-ops once the views are gone; keeping them preserves exact cleanup semantics for old deployments).
- The advisory-lock wrapper `_ensure_search_views_if_possible` — unchanged.
- `_ensure_indexes` and the BM25 index specs — unchanged (index existence is separately cached; gating it is out of scope to avoid the missing-index hazard the current comment warns about).
- Any other `ensure_*` method.

## Git workflow

- Branch: `advisor/009-search-text-rebuild-guard`
- One logical commit. Suggested message (imperative, matches repo style):
  `Skip the search_text() recompile when its generated DDL is unchanged`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Add the signature, marker-table, and function-exists helpers

In `src/personal_data_warehouse/postgres.py`, add `import hashlib` and/or `import inspect` at the top if missing (see the grep in Current state).

Add these three methods to `PostgresWarehouse` (place them right below `_ensure_search_views_if_possible_locked`):

```python
    _SEARCH_SCHEMA_MARKER_TABLE = "pdw_search_schema_state"

    def _search_schema_signature(self) -> str:
        """Signature of everything that determines the generated search DDL.

        Derived from the *source code* of the generator method plus the searched
        table set and the source floor, so ANY edit to the search_text() DDL (or
        adding a source) changes the signature and forces a one-time rebuild —
        no manual version constant to bump. If the source can't be introspected
        (e.g. a zipimport deploy), return "" so the guard never matches and the
        function is rebuilt every time (safe: current behavior)."""
        try:
            source = inspect.getsource(type(self)._ensure_search_text_function)
        except (OSError, TypeError):
            return ""
        payload = "\n".join(
            [
                source,
                ",".join(sorted(self._SEARCHABLE_TEXT_TABLES)),
                str(SEARCH_TEXT_SOURCE_FLOOR),
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _stored_search_schema_signature(self) -> str | None:
        self._command(
            f"CREATE TABLE IF NOT EXISTS {_identifier(self._SEARCH_SCHEMA_MARKER_TABLE)} "
            "(id smallint PRIMARY KEY DEFAULT 1, signature text NOT NULL, "
            "CONSTRAINT pdw_search_schema_state_single_row CHECK (id = 1))"
        )
        rows = self._query(
            f"SELECT signature FROM {_identifier(self._SEARCH_SCHEMA_MARKER_TABLE)} WHERE id = 1"
        )
        if not rows:
            return None
        return rows[0][0]

    def _write_search_schema_signature(self, signature: str) -> None:
        self._command(
            f"INSERT INTO {_identifier(self._SEARCH_SCHEMA_MARKER_TABLE)} (id, signature) "
            "VALUES (1, %s) ON CONFLICT (id) DO UPDATE SET signature = EXCLUDED.signature",
            (signature,),
        )

    def _search_text_function_exists(self) -> bool:
        rows = self._query(
            """
            SELECT 1
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'search_text'
              AND n.nspname = current_schema()
            LIMIT 1
            """
        )
        return bool(rows)
```

Notes:
- `_identifier` is already defined in this module (used throughout for schema/table identifiers) — reuse it; do not re-import.
- Confirm `self._query(sql, params=None)` returns a list of row tuples (it is used that way by `_relation_exists`). If the real helper has a different name/shape, match it (read `postgres.py` — do not guess). `rows[0][0]` assumes tuple rows; adjust if `_query` returns dicts.

**Verify**: `uv run python -c "import personal_data_warehouse.postgres"` → no error.

### Step 2: Gate the rebuild in `_ensure_search_views_if_possible_locked`

Change the method so the expensive rebuild is skipped when the signature matches and the function exists. Keep the two legacy `DROP VIEW` statements unconditional:

```python
    def _ensure_search_views_if_possible_locked(self) -> None:
        self._command("DROP VIEW IF EXISTS person_identities")
        self._command("DROP VIEW IF EXISTS searchable_text")
        if not all(self._relation_exists(table) for table in self._SEARCHABLE_TEXT_TABLES):
            return
        signature = self._search_schema_signature()
        if (
            signature
            and self._stored_search_schema_signature() == signature
            and self._search_text_function_exists()
        ):
            # Generated search DDL unchanged since the last build and the
            # function is present — skip the CREATE OR REPLACE recompile.
            return
        self._ensure_indexes(self._SEARCHABLE_TEXT_TABLES)
        self._ensure_search_text_function()
        self._write_search_schema_signature(signature)
```

(Preserve the original explanatory comments on the `DROP VIEW` lines and the index/function calls — copy them over; they are omitted above only for brevity.)

**Verify**: `uv run python -c "import personal_data_warehouse.postgres"` → no error.

### Step 3: PG-gated test for the guard

In `tests/test_postgres_warehouse.py`, add a test (use the same `POSTGRES_DATABASE_URL` skip guard and throwaway-schema fixture the other tests in this file use — read the top of the file for the `warehouse` fixture / `make_test_schema` helper and reuse it). The test must prove the recompile is skipped when unchanged and forced when drifted. A clean way is to spy on `_command` and look for the `search_text(` function DDL:

```python
def test_search_schema_rebuild_is_skipped_when_unchanged(warehouse):
    # First ensure builds search_text().
    warehouse.ensure_tables()
    assert warehouse._search_text_function_exists()

    # Spy on issued SQL for a second ensure of the search views.
    issued: list[str] = []
    original_command = warehouse._command

    def spy(sql, params=None):
        issued.append(sql)
        return original_command(sql, params)

    warehouse._command = spy
    try:
        warehouse._ensure_search_views_if_possible()
    finally:
        warehouse._command = original_command

    joined = "\n".join(issued)
    assert "CREATE OR REPLACE FUNCTION search_text(" not in joined, (
        "search_text() was recompiled even though its DDL was unchanged"
    )

    # Now drift: drop the marker so the signature no longer matches; the next
    # ensure must rebuild.
    warehouse._command(
        f"DELETE FROM {warehouse._SEARCH_SCHEMA_MARKER_TABLE} WHERE id = 1"
    )
    issued.clear()
    warehouse._command = spy
    try:
        warehouse._ensure_search_views_if_possible()
    finally:
        warehouse._command = original_command
    assert any("CREATE OR REPLACE FUNCTION search_text(" in sql for sql in issued), (
        "search_text() was not rebuilt after the signature marker was cleared"
    )
```

Match the real fixture name and the real `_command` signature/positional-vs-keyword `params` from this file's other tests. If `ensure_tables()` is not the method the fixture uses to build search views, use whatever the file's existing tests call to materialize the schema.

**Verify** (needs `POSTGRES_DATABASE_URL`):
`uv run pytest tests/test_postgres_warehouse.py -q -k search_schema` → 1 passed (not skipped).

### Step 4: Full-suite sanity

**Verify**: `uv run pytest -q` → no new failures vs. baseline (831 passed, 113 skipped; passed may rise by one).

## Test plan

- New PG-gated test in `tests/test_postgres_warehouse.py`:
  - After an initial build, a second `_ensure_search_views_if_possible()` issues **no** `CREATE OR REPLACE FUNCTION search_text(` statement (recompile skipped).
  - After clearing the marker row, the next ensure **does** re-issue it (rebuild forced on drift).
- Structural pattern: the existing PG-gated tests in `tests/test_postgres_warehouse.py` (same skip guard, same throwaway-schema fixture).
- Verification: `uv run pytest tests/test_postgres_warehouse.py -q -k search_schema` → passed (not skipped).

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `uv run python -c "import personal_data_warehouse.postgres"` → no error
- [ ] `grep -n '_search_schema_signature\|_stored_search_schema_signature\|_search_text_function_exists\|pdw_search_schema_state' src/personal_data_warehouse/postgres.py` shows the helpers and marker table
- [ ] `_ensure_search_views_if_possible_locked` returns early (skips `_ensure_search_text_function`) when the signature matches and the function exists (inspect the method)
- [ ] `uv run pytest tests/test_postgres_warehouse.py -q -k search_schema` → **passed** (not skipped) with `POSTGRES_DATABASE_URL` set
- [ ] `uv run pytest -q` → no new failures vs. baseline
- [ ] Only the two in-scope files were modified (`git diff --name-only`)
- [ ] `plans/README.md` status row for 009 updated

## STOP conditions

Stop and report back (do not improvise) if:

- The search-view ensure path no longer matches the "Current state" excerpt (e.g. it already has a guard, or `_ensure_search_text_function` was refactored to return SQL) — the fix may be partly done; reconcile before proceeding.
- `self._query(...)` does not exist or returns a shape incompatible with the helpers (e.g. dict rows) — adjust the helpers to the real return shape and note it, or STOP if unsure.
- The new test shows `search_text()` is **still** recompiled on the unchanged second ensure — the signature is not stable across calls (e.g. `inspect.getsource` returns different text, or `_SEARCHABLE_TEXT_TABLES` is unordered/regenerated). Report; do not weaken the assertion.
- Building the `pdw_search_schema_state` marker table conflicts with an existing object of that name — pick a non-colliding name and note it.

## Maintenance notes

- **Why source-hash, not a version constant:** the signature includes `inspect.getsource` of `_ensure_search_text_function`, so editing the generated DDL automatically changes the signature and triggers exactly one rebuild on the next ensure after deploy. There is intentionally **no** manual version to bump. If the deploy ever runs from a non-source form where `inspect.getsource` fails, the signature is `""`, which never matches — so it safely degrades to the old always-rebuild behavior rather than shipping a stale function.
- A reviewer should confirm: (1) the two legacy `DROP VIEW` statements still run unconditionally; (2) the guard's `and self._search_text_function_exists()` clause is present so a manually-dropped function still gets rebuilt; (3) the marker write happens only after a successful rebuild.
- If a future change makes the generated DDL depend on a helper *outside* `_ensure_search_text_function` (e.g. a new shared SQL fragment builder), fold that helper's source into `_search_schema_signature` too, or the guard could skip a rebuild it should do.
- This plan deliberately does not touch the per-tick `DROP VIEW` no-ops or the index-ensure loop; if profiling later shows those are also hot, gating them behind the same signature is a natural follow-up (with care for the missing-index hazard the existing comment describes).
