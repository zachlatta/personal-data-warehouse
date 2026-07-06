# Plan 002: Stop a transient Slack error from wiping a conversation's sync cursor

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat e118893..HEAD -- src/personal_data_warehouse/postgres.py src/personal_data_warehouse/slack_sync.py`
> If either file changed since this plan was written, compare the "Current
> state" excerpts against the live code before proceeding; on a mismatch,
> treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none (independently executable; its integration test needs `POSTGRES_DATABASE_URL`, see plan 005)
- **Category**: bug
- **Planned at**: commit `e118893`, 2026-07-06

## Why this matters

When Slack returns a transient per-conversation error (a one-off 5xx, a
`channel_not_found`, a mid-stream failure), the sync records the error by writing
a `slack_sync_state` row with `cursor_ts=""`. Because `slack_sync_state` is not
protected in the "preserve non-empty on upsert" map, that empty string
**overwrites the real cursor**. On the next run the conversation has no cursor,
so `_oldest_ts_for_conversation` returns `None` and Slack re-fetches the
conversation's **entire** history from scratch — wasteful API load against
Slack's rate limits, and it can clobber progress made by earlier successful
pages in the same run. The other sync engines (Gmail `gmail_sync.py:192-200`,
Calendar `calendar_sync.py:74`, Contacts `contacts_sync.py:103-112`) deliberately
preserve their cursor on error; Slack is the outlier. The code even documents
having to build high-water recovery around this exact class of bug
(`slack_sync.py:515-524`).

The fix is one entry in an existing table-driven map. It reuses the same
mechanism that already protects storage-pointer columns and WhatsApp names.

## Current state

The error path writes an empty cursor —
`src/personal_data_warehouse/slack_sync.py:421-443` (`_record_conversation_error`):

```python
    def _record_conversation_error(
        self,
        *,
        account: str,
        team_id: str,
        conversation_id: str,
        sync_type: str,
        error: str,
        synced_at: datetime,
        sync_version: int,
    ) -> None:
        self._warehouse.insert_slack_sync_state(
            account=account,
            team_id=team_id,
            object_type="conversation",
            object_id=conversation_id,
            cursor_ts="",          # <-- this blanks the stored cursor on upsert
            last_sync_type=sync_type,
            status="error",
            error=error,
            updated_at=synced_at,
            sync_version=sync_version,
        )
```

The upsert clause honors a per-table "preserve non-empty" map —
`src/personal_data_warehouse/postgres.py:6877-6890` (`_upsert_assignment`):

```python
def _upsert_assignment(*, table: str, column: str, preserve_non_empty: bool) -> str:
    quoted_column = _identifier(column)
    excluded_column = f"EXCLUDED.{quoted_column}"
    if preserve_non_empty:
        return f"{quoted_column} = COALESCE(NULLIF({excluded_column}, ''), {_identifier(table)}.{quoted_column})"
    return f"{quoted_column} = {excluded_column}"
```

The map it consults —
`src/personal_data_warehouse/postgres.py:6731` (`PRESERVE_NON_EMPTY_COLUMNS_BY_TABLE`):

```python
PRESERVE_NON_EMPTY_COLUMNS_BY_TABLE: dict[str, tuple[str, ...]] = {
    "apple_message_attachments": ( ... ),
    "gmail_attachments": ( ... ),
    "whatsapp_media_items": ( ... ),
    "whatsapp_contacts": ( ... ),
    "whatsapp_chats": ("name",),
    "whatsapp_chat_participants": ("display_name",),
}
```

`slack_sync_state` is **absent** from this map, so its `cursor_ts` is assigned
`EXCLUDED.cursor_ts` unconditionally — an empty incoming value wins.

**Why adding `cursor_ts` here is safe** (verified): the only `slack_sync_state`
rows that ever carry a non-empty `cursor_ts` are the `object_type="conversation"`
message-sync rows. The other writer that uses `cursor_ts=""` is the
`object_type="conversation_members"` path
(`slack_sync.py:1009` on error and `slack_sync.py:1045` on success) — those rows
are *always* empty (stored and incoming both `""`), so `COALESCE(NULLIF('',''),
'')` is still `''`: preserve-non-empty is a no-op for them. There is no code path
that deliberately resets a *conversation* cursor to empty; the only time a
conversation row's cursor is legitimately empty is at first creation, when the
stored value is also empty. So preserving non-empty cannot strand a wanted reset.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Slack unit tests (no PG) | `uv run pytest tests/test_slack_sync.py -q` | all pass |
| Warehouse integration tests (needs PG) | `uv run pytest tests/test_postgres_warehouse.py -q` | pass, or `skipped` if no `POSTGRES_DATABASE_URL` |

Set `POSTGRES_DATABASE_URL` (a throwaway/dev Postgres) before running the
integration test, or your new test will silently skip. If you cannot provide a
Postgres, STOP after Step 1–2 and report that the integration test could not be
executed (do not mark done).

## Scope

**In scope**:
- `src/personal_data_warehouse/postgres.py` (add one map entry)
- `tests/test_postgres_warehouse.py` (add one integration test)

**Out of scope** (do NOT touch):
- `slack_sync.py` error path — leave `cursor_ts=""` as written; the fix belongs
  at the upsert layer so it also covers any other empty-cursor writer.
- The `status`/`error` columns — those *should* update on error; only `cursor_ts`
  must be preserved.
- The Slack freshness-selection query that excludes `status="error"` rows
  (`postgres.py` around line 4905) — see Maintenance notes; a separate concern,
  out of scope here.

## Git workflow

- Branch: `advisor/002-preserve-slack-cursor-on-error`
- One commit. Message: `Preserve Slack sync cursor when a conversation errors`.
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Add `slack_sync_state` to the preserve-non-empty map

In `src/personal_data_warehouse/postgres.py`, add an entry to
`PRESERVE_NON_EMPTY_COLUMNS_BY_TABLE` (the dict at line ~6731):

```python
    # A transient per-conversation Slack error records status="error" with an
    # empty cursor_ts; that empty value must never overwrite the real cursor we
    # already synced to (else the next run re-fetches the whole conversation).
    "slack_sync_state": ("cursor_ts",),
```

**Verify**: `grep -n "slack_sync_state" src/personal_data_warehouse/postgres.py | grep -i preserve` OR
`uv run python -c "from personal_data_warehouse.postgres import PRESERVE_NON_EMPTY_COLUMNS_BY_TABLE as p; print(p['slack_sync_state'])"`
→ prints `('cursor_ts',)`.

### Step 2: Confirm the generated upsert SQL now preserves the cursor

Sanity-check that the assignment for `cursor_ts` on `slack_sync_state` is now the
COALESCE form:

```bash
uv run python -c "
from personal_data_warehouse.postgres import _upsert_assignment
print(_upsert_assignment(table='slack_sync_state', column='cursor_ts', preserve_non_empty=True))
"
```

**Verify**: output contains `COALESCE(NULLIF(EXCLUDED."cursor_ts", ''), "slack_sync_state"."cursor_ts")`
(exact identifier quoting may differ; the `COALESCE(NULLIF(...))` shape is what matters).

### Step 3: Add an integration test proving the cursor survives an error write

Add a test to `tests/test_postgres_warehouse.py` that:
1. Ensures the Slack tables exist (call the same `ensure_*` the other Slack tests
   use — find it by searching the file for `slack_sync_state` or
   `insert_slack_sync_state` usages already present).
2. Writes a `conversation` state row with a real `cursor_ts` (e.g. `"1700.0001"`)
   and some `sync_version` V1.
3. Writes a second `conversation` row for the same `(account, team_id,
   object_type, object_id)` with `cursor_ts=""`, `status="error"`, and a
   `sync_version` V2 >= V1 (mirroring `_record_conversation_error`).
4. Reads the row back (via `load_slack_sync_state()` — see `postgres.py:4567`)
   and asserts `cursor_ts == "1700.0001"` (preserved) while `status == "error"`
   (updated).

Model the test structure on the existing PG-gated tests in this file (they use
the `warehouse` fixture and `make_test_schema`; the file begins with a
`pytest.skip("POSTGRES_DATABASE_URL is not set")` gate — reuse that fixture, do
not add a new gate).

**Verify** (requires `POSTGRES_DATABASE_URL`):
`uv run pytest tests/test_postgres_warehouse.py -q -k "slack and cursor"` → 1+ passed (not skipped).

If it reports `skipped`, `POSTGRES_DATABASE_URL` is unset — provide one and
re-run; do not mark the plan done on a skip.

### Step 4: Guard against regression — fail the test without the fix

Temporarily remove the `"slack_sync_state": ("cursor_ts",)` entry, run the new
test, confirm it **fails** (cursor came back `""`), then restore the entry.

**Verify**: after restoring, `uv run pytest tests/test_postgres_warehouse.py -q -k "slack and cursor"` → passed.

## Test plan

- New test in `tests/test_postgres_warehouse.py`: an error write with an empty
  cursor must not overwrite a previously stored non-empty cursor for the same
  conversation key; `status` must still update to `"error"`.
- Pattern to follow: the existing PG-gated Slack tests already in
  `tests/test_postgres_warehouse.py` (same `warehouse` fixture).
- Verification: `uv run pytest tests/test_postgres_warehouse.py -q` → all pass
  (with `POSTGRES_DATABASE_URL` set).

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `uv run python -c "from personal_data_warehouse.postgres import PRESERVE_NON_EMPTY_COLUMNS_BY_TABLE as p; print(p['slack_sync_state'])"` prints `('cursor_ts',)`
- [ ] `uv run pytest tests/test_slack_sync.py -q` exits 0 (no regression in Slack unit tests)
- [ ] `uv run pytest tests/test_postgres_warehouse.py -q` exits 0 with the new test **passed** (not skipped), `POSTGRES_DATABASE_URL` set
- [ ] Only `postgres.py` and `tests/test_postgres_warehouse.py` modified (`git diff --name-only`)
- [ ] `plans/README.md` status row for 002 updated

## STOP conditions

Stop and report back (do not improvise) if:

- `PRESERVE_NON_EMPTY_COLUMNS_BY_TABLE` already contains a `slack_sync_state`
  entry (someone fixed this) — report and stop.
- You cannot obtain a `POSTGRES_DATABASE_URL` to run the integration test — do
  Steps 1–2, then report that Step 3 could not be verified. Do not mark done.
- The `conversation_members` writer analysis in "Current state" no longer matches
  the live code (e.g. a new path deliberately resets a conversation cursor to
  empty), which would make preserve-non-empty unsafe — report before proceeding.

## Maintenance notes

- Related, deliberately deferred: after an error, the row's `status="error"` can
  drop the conversation from Slack **freshness** candidate selection
  (`postgres.py` near line 4905) until a later stage revisits it. This plan fixes
  the data-loss (cursor wipe / full re-sync); it does **not** change freshness
  selection. If errored conversations are observed to stall, that's a separate
  follow-up (make freshness/coverage selection reconsider recently-errored rows).
- A reviewer should confirm no code path intends to reset a `conversation`
  cursor to empty — if one is ever added, it must not rely on the upsert to blank
  the cursor (it now won't).
