# Plan 004: Close the Postgres connection every enrichment/transcription sensor opens each tick

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat e118893..HEAD -- src/personal_data_warehouse/defs/`
> If any of the seven in-scope sensor files changed since this plan was
> written, compare the "Current state" excerpts against the live code before
> proceeding; on a mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: LOW
- **Depends on**: none
- **Category**: perf
- **Planned at**: commit `e118893`, 2026-07-06

## Why this matters

Seven Dagster backlog **sensors** run every 60 seconds in production. Each one
opens a fresh `PostgresWarehouse` (a new psycopg2 TCP connection, plus a
`CREATE SCHEMA IF NOT EXISTS` + `SET search_path` on connect), runs a candidate
check, and returns — **without ever calling `warehouse.close()`**. The socket is
only reclaimed whenever CPython's refcount happens to drop, and any exception
path that retains the frame turns it into a real leaked connection. Across 7
sensors firing every minute on the small, load-sensitive production host
(which has a history of sustained-load incidents), this is needless
connection churn and a latent connection leak.

Two sensors already do this correctly (`chatgpt_backend_ingest.py`,
`upstream_mutations.py` — they wrap the warehouse in `try/finally:
warehouse.close()`). This plan brings the other seven in line with that proven
pattern.

## Current state

The correct pattern already used elsewhere —
`src/personal_data_warehouse/defs/upstream_mutations.py:86-110`:

```python
    warehouse = warehouse_from_settings(settings)
    ...
    try:
        with exclusive_sync_lock(...) as acquired:
            ...
    finally:
        warehouse.close()
```

`PostgresWarehouse.__init__` opens a connection and runs DDL on every
construction, and there is no `__del__`/context manager — `close()` must be
called explicitly (`src/personal_data_warehouse/postgres.py:1221-1231`).

The seven sensors that open a warehouse and never close it (each opens at
`warehouse = warehouse_from_settings(settings)` inside a `def *_sensor(context)`
and then returns a `SkipReason`/`RunRequest`):

| File | Sensor function | `warehouse =` line |
|------|-----------------|--------------------|
| `src/personal_data_warehouse/defs/gmail_attachment_enrichment.py` | `gmail_attachment_enrichment_backlog_sensor` | ~110 |
| `src/personal_data_warehouse/defs/apple_messages_attachment_enrichment.py` | `apple_messages_attachment_enrichment_backlog_sensor` | ~120 |
| `src/personal_data_warehouse/defs/apple_messages_audio_transcription.py` | `apple_messages_audio_transcription_backlog_sensor` | ~127 |
| `src/personal_data_warehouse/defs/apple_voice_memos_enrichment.py` | `apple_voice_memos_enrichment_backlog_sensor` | ~130 |
| `src/personal_data_warehouse/defs/apple_voice_memos_transcription.py` | `apple_voice_memos_transcription_backlog_sensor` | ~110 |
| `src/personal_data_warehouse/defs/whatsapp_audio_transcription.py` | `whatsapp_audio_transcription_backlog_sensor` | ~122 |
| `src/personal_data_warehouse/defs/whatsapp_media_enrichment.py` | `whatsapp_media_enrichment_backlog_sensor` | ~119 |

Representative body —
`gmail_attachment_enrichment.py:104-123`:

```python
def gmail_attachment_enrichment_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="gmail_attachment_enrichment_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(require_gmail=False, require_agent=True)
    warehouse = warehouse_from_settings(settings)
    has_candidate = has_file_enrichment_candidate( warehouse, ... )
    if not has_candidate:
        return SkipReason("No Gmail attachments are waiting for agent enrichment.")

    return RunRequest(tags={"gmail_attachment_trigger": "enrichment_backlog"})
```

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Grep opens vs closes | see Step 3 | every file: closes ≥ 1 |
| Sensor/defs unit tests | `uv run pytest tests/test_schedule_guards.py -q` | all pass |
| New regression test | `uv run pytest tests/test_schedule_guards.py -k sensor_close -q` | 1 passed |

These tests do not require `POSTGRES_DATABASE_URL`.

## Scope

**In scope** (the seven sensor files above) and:
- `tests/test_schedule_guards.py` (add one structural regression test) — if that
  file doesn't fit, use `tests/test_sync_locks.py`; pick whichever already has
  no Postgres gate.

**Out of scope** (do NOT touch):
- `PostgresWarehouse` in `postgres.py` — do **not** add `__del__`/`__enter__`;
  keep the fix inside the sensor functions to minimize blast radius.
- The **asset** functions in these same files (only the `*_sensor` functions leak
  per-tick; assets run rarely and are a separate, lower-value cleanup — leave
  them for now, noted in Maintenance notes).
- The candidate-query SQL itself (`has_file_enrichment_candidate`,
  `load_enrichment_candidates`) — a possible index/sargability optimization is
  deferred (Maintenance notes).
- The two already-correct sensors (`chatgpt_backend_ingest.py`,
  `upstream_mutations.py`).

## Git workflow

- Branch: `advisor/004-close-sensor-warehouse-connections`
- One commit. Message: `Close the warehouse connection each backlog sensor opens`.
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Wrap each sensor's warehouse usage in `try/finally: warehouse.close()`

For **each** of the seven files, once `warehouse = warehouse_from_settings(settings)`
is created, ensure `warehouse.close()` runs on every return path that follows.
Apply this transformation (shown for the gmail sensor; the others differ only in
the candidate check and messages):

```python
    settings = load_settings(require_gmail=False, require_agent=True)
    warehouse = warehouse_from_settings(settings)
    try:
        has_candidate = has_file_enrichment_candidate( warehouse, ... )
        if not has_candidate:
            return SkipReason("No Gmail attachments are waiting for agent enrichment.")
    finally:
        warehouse.close()

    return RunRequest(tags={"gmail_attachment_trigger": "enrichment_backlog"})
```

Rules that make this safe and uniform:
- The `try` starts immediately after `warehouse = warehouse_from_settings(...)`.
- Every statement that **uses** `warehouse` (the candidate check) goes inside the
  `try`.
- The `return SkipReason(...)` for "no candidates" goes **inside** the `try`
  (the `finally` still runs and closes before returning).
- The final `return RunRequest(...)` goes **after** the `try/finally` (it does not
  use the warehouse, and the connection is already closed).
- Do **not** move the early guards that run *before* the warehouse is opened
  (`skip_if_job_in_progress`, `load_settings`, config `SkipReason`s) — those
  return before any connection exists.

Do this for all seven sensors. In files where the candidate check spans several
lines (e.g. `apple_voice_memos_enrichment.py`'s `load_enrichment_candidates(...)`
call), enclose the whole call and its `if not ...: return SkipReason(...)`.

**Verify** (after each file): `uv run python -c "import personal_data_warehouse.defs.gmail_attachment_enrichment"`
(swap module name per file) → no error.

### Step 2: Confirm every sensor file now closes

```bash
for f in gmail_attachment_enrichment apple_messages_attachment_enrichment \
         apple_messages_audio_transcription apple_voice_memos_enrichment \
         apple_voice_memos_transcription whatsapp_audio_transcription \
         whatsapp_media_enrichment; do
  n=$(grep -c "warehouse.close()" src/personal_data_warehouse/defs/$f.py)
  echo "$f: close_count=$n"
done
```

**Verify**: every line shows `close_count` ≥ 1.

### Step 3: Add a structural regression test

Add a test (in `tests/test_schedule_guards.py`) that asserts each of the seven
sensor modules calls `warehouse.close()`. Read the files as text — do not import
the Dagster modules if the test file avoids Dagster imports; a text scan is
sufficient and side-effect free:

```python
from pathlib import Path

SENSOR_FILES_THAT_OPEN_A_WAREHOUSE = [
    "gmail_attachment_enrichment.py",
    "apple_messages_attachment_enrichment.py",
    "apple_messages_audio_transcription.py",
    "apple_voice_memos_enrichment.py",
    "apple_voice_memos_transcription.py",
    "whatsapp_audio_transcription.py",
    "whatsapp_media_enrichment.py",
]

def test_backlog_sensors_close_their_warehouse() -> None:
    defs_dir = Path(__file__).resolve().parents[1] / "src" / "personal_data_warehouse" / "defs"
    missing = []
    for name in SENSOR_FILES_THAT_OPEN_A_WAREHOUSE:
        text = (defs_dir / name).read_text()
        if "warehouse_from_settings(" in text and "warehouse.close()" not in text:
            missing.append(name)
    assert not missing, f"sensor modules open a warehouse but never close it: {missing}"
```

**Verify**: `uv run pytest tests/test_schedule_guards.py -k sensor -q` → passed.

### Step 4: Prove the test catches a regression

Temporarily delete the `warehouse.close()` you added in one file, run the test,
confirm it **fails** naming that file, then restore.

**Verify**: after restoring, `uv run pytest tests/test_schedule_guards.py -q` → all pass.

## Test plan

- New structural test `test_backlog_sensors_close_their_warehouse` in
  `tests/test_schedule_guards.py`: every listed sensor module that opens a
  warehouse also closes it.
- Pattern to follow: existing plain-function tests in `tests/test_schedule_guards.py`
  (no Postgres fixture).
- Verification: `uv run pytest tests/test_schedule_guards.py -q` → all pass.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] Each of the seven sensor files contains `warehouse.close()` (Step 2 loop shows all ≥ 1)
- [ ] Each modified module imports without error (`uv run python -c "import personal_data_warehouse.defs.<module>"` for each)
- [ ] `uv run pytest tests/test_schedule_guards.py -q` exits 0 with the new test passing
- [ ] Only the seven sensor files + the one test file modified (`git diff --name-only`)
- [ ] `plans/README.md` status row for 004 updated

## STOP conditions

Stop and report back (do not improvise) if:

- Any sensor's structure differs materially from the "Current state" pattern
  (e.g. the warehouse is already closed, or it's passed into a helper that closes
  it) — that file may already be fixed; report which.
- A sensor uses the warehouse *after* returning the RunRequest, or holds it in a
  closure, such that closing in `finally` would break it — report; don't force it.
- Wrapping in `try/finally` changes control flow in a file where the warehouse is
  opened before the `skip_if_job_in_progress` early return (it isn't in the seven
  listed, but verify) — report before restructuring guard order.

## Maintenance notes

- **Deferred, same finding:** the asset functions in these files also open a
  warehouse without closing; assets run far less often than the 60s sensor tick,
  so this plan leaves them. A follow-up can give `PostgresWarehouse` a context
  manager (`__enter__/__exit__ → close`) and switch all call sites to
  `with warehouse_from_settings(...) as warehouse:`, which would cover assets and
  future code uniformly.
- **Deferred optimization:** the per-tick candidate probe
  (`has_file_enrichment_candidate` / `load_enrichment_candidates`) uses
  non-sargable predicates (`lower(mime)=ANY`, `filename LIKE '%.pdf'`) that scan
  the eligible set on every no-work tick. If sensor CPU is still a concern after
  this fix, `EXPLAIN` those queries and consider a partial/expression index — but
  verify the plan before adding indexes.
- A reviewer should confirm the `finally` encloses every warehouse-using path and
  that the trailing `return RunRequest(...)` doesn't reference the (now closed)
  warehouse.
