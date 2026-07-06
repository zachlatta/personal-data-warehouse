# Plan 001: Give every Dagster job a unique Postgres advisory-lock id

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat e118893..HEAD -- src/personal_data_warehouse/defs/ src/personal_data_warehouse/sync_locks.py`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: bug
- **Planned at**: commit `e118893`, 2026-07-06

## Why this matters

Several Dagster jobs guard their runs with a Postgres advisory lock keyed on an
integer id. Three pairs of *unrelated* jobs were accidentally given the **same**
id, so in production (where the Postgres lock path is active) each pair is
mutually exclusive: when one holds its lock, the other silently skips its tick
and defers to the next schedule. A long-running holder (e.g. a voice-memo
transcription batch or a Drive full crawl) repeatedly starves its unrelated
partner (Alice import / WhatsApp media enrichment / Gmail attachment enrichment)
even though they touch completely disjoint data. The skip is logged as "another
run is already active", so it looks benign and is easy to miss. Giving each job
a distinct id removes the false contention; adding a uniqueness test prevents a
future re-collision.

## Current state

The advisory lock is keyed **only** on the integer id — the job's `name`
argument only selects which env var / lock-file path the *fallback* file lock
uses, not the Postgres lock key:

`src/personal_data_warehouse/sync_locks.py:64-76` (the Postgres path):

```python
@contextmanager
def exclusive_postgres_advisory_lock(
    postgres_url: str, lock_id: int, *, wait_seconds: float | None = None
) -> Iterator[bool]:
    import psycopg2
    connection = psycopg2.connect(postgres_url)
    connection.autocommit = True
    cursor = connection.cursor()
    acquired = False
    try:
        if wait_seconds is None:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            acquired = bool(cursor.fetchone()[0])
```

So two differently-named jobs sharing one `lock_id` are genuinely mutually
exclusive in Postgres.

The three colliding pairs (verified by grepping every
`*_POSTGRES_LOCK_ID = <number>` in `src/personal_data_warehouse/defs/`):

| id | job A (keep unchanged) | job B (**change this one**) |
|----|------------------------|------------------------------|
| `8_407_112_443` | `src/personal_data_warehouse/defs/google_drive_source_sync.py:24` `GOOGLE_DRIVE_SOURCE_SYNC_POSTGRES_LOCK_ID` | `src/personal_data_warehouse/defs/whatsapp_media_enrichment.py:38` `WHATSAPP_MEDIA_ENRICHMENT_POSTGRES_LOCK_ID` |
| `7_403_111_842` | `src/personal_data_warehouse/defs/contacts_sync.py:21` `CONTACTS_SYNC_POSTGRES_LOCK_ID` | `src/personal_data_warehouse/defs/gmail_attachment_enrichment.py:38` `GMAIL_ATTACHMENT_ENRICHMENT_POSTGRES_LOCK_ID` |
| `7_403_111_840` | `src/personal_data_warehouse/defs/apple_voice_memos_transcription.py:32` `VOICE_MEMOS_TRANSCRIPTION_POSTGRES_LOCK_ID` | `src/personal_data_warehouse/defs/alice_voice_recordings.py:28` `ALICE_VOICE_RECORDINGS_IMPORT_POSTGRES_LOCK_ID` |

The full set of ids currently in use (do **not** reuse any of these for the new
values):

```
7_403_111_837  7_403_111_838  7_403_111_839  7_403_111_840  7_403_111_841
7_403_111_842  7_403_111_843  8_407_112_439  8_407_112_440  8_407_112_441
8_407_112_442  8_407_112_443  8_407_112_444  8_407_112_461  8_407_112_462
8_407_112_463  8_407_112_464  8_407_112_466
```

Repo convention: these constants are module-level literals written with digit
grouping underscores (e.g. `7_403_111_840`). Match that style.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Grep lock ids | `grep -rn "_POSTGRES_LOCK_ID = " src/personal_data_warehouse/defs/` | 21 lines, all distinct numbers |
| Run the new test | `uv run pytest tests/test_sync_locks.py -q` | all pass, incl. the new uniqueness test |
| Full lock/guard tests | `uv run pytest tests/test_sync_locks.py tests/test_schedule_guards.py -q` | all pass |

(These tests do **not** require `POSTGRES_DATABASE_URL`.)

## Scope

**In scope** (the only files you should modify):
- `src/personal_data_warehouse/defs/whatsapp_media_enrichment.py` (one constant)
- `src/personal_data_warehouse/defs/gmail_attachment_enrichment.py` (one constant)
- `src/personal_data_warehouse/defs/alice_voice_recordings.py` (one constant)
- `tests/test_sync_locks.py` (add one test)

**Out of scope** (do NOT touch):
- The three "keep unchanged" constants in the table above — changing them is
  unnecessary and only churns more code.
- `sync_locks.py` itself — the locking mechanism is correct; only the ids collide.
- Any other `*_POSTGRES_LOCK_ID` constant.

## Git workflow

- Branch: `advisor/001-unique-advisory-lock-ids`
- One commit is fine. Message style matches the repo's imperative summaries
  (e.g. `git log` shows "Classify scheduled agent routines and dependabot relays
  as machinery"): `Stop unrelated jobs sharing advisory-lock ids`.
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Reassign the three colliding "change this one" ids

Change **only** the three constants in the right-hand column of the table above,
to these new, currently-unused values:

- `whatsapp_media_enrichment.py:38`:
  `WHATSAPP_MEDIA_ENRICHMENT_POSTGRES_LOCK_ID = 8_407_112_465`
- `gmail_attachment_enrichment.py:38`:
  `GMAIL_ATTACHMENT_ENRICHMENT_POSTGRES_LOCK_ID = 7_403_111_844`
- `alice_voice_recordings.py:28`:
  `ALICE_VOICE_RECORDINGS_IMPORT_POSTGRES_LOCK_ID = 7_403_111_845`

**Verify**: `grep -rn "_POSTGRES_LOCK_ID = " src/personal_data_warehouse/defs/ | grep -oE "[0-9_]+$" | sort | uniq -d`
→ prints nothing (no duplicate values remain).

### Step 2: Add a regression test that fails if any two ids ever collide again

Append a test to `tests/test_sync_locks.py` that scans every
`src/personal_data_warehouse/defs/*.py` file for `_POSTGRES_LOCK_ID = <number>`
declarations and asserts every value is unique. Do this by reading the source
files as text (regex), **not** by importing the `defs` modules — importing them
pulls in Dagster asset/sensor definitions with side effects the test shouldn't
trigger, and this test must run without `POSTGRES_DATABASE_URL`.

Target shape:

```python
import re
from pathlib import Path

def test_advisory_lock_ids_are_unique() -> None:
    defs_dir = Path(__file__).resolve().parents[1] / "src" / "personal_data_warehouse" / "defs"
    pattern = re.compile(r"^([A-Z_]*_POSTGRES_LOCK_ID)\s*=\s*([0-9_]+)", re.MULTILINE)
    by_id: dict[int, list[str]] = {}
    for path in sorted(defs_dir.glob("*.py")):
        for name, raw in pattern.findall(path.read_text()):
            value = int(raw.replace("_", ""))
            by_id.setdefault(value, []).append(f"{path.name}:{name}")
    collisions = {value: names for value, names in by_id.items() if len(names) > 1}
    assert not collisions, f"advisory-lock id collisions: {collisions}"
```

Confirm the parents index is right: `Path(__file__).resolve().parents[1]` must
resolve to the repo root (so that `src/...` exists). `tests/` is directly under
the repo root, so `parents[1]` is correct; if the assertion below fails on a
"no such directory", STOP and report.

**Verify**: `uv run pytest tests/test_sync_locks.py::test_advisory_lock_ids_are_unique -q`
→ 1 passed.

### Step 3: Confirm the guard actually catches a collision (sanity)

Temporarily re-introduce a collision (set the alice id back to
`7_403_111_840`), run the test, confirm it **fails**, then revert. This proves
the test is wired correctly and not vacuously passing.

**Verify**: after reverting, `uv run pytest tests/test_sync_locks.py -q` → all pass.

## Test plan

- New test: `tests/test_sync_locks.py::test_advisory_lock_ids_are_unique` —
  asserts no two `defs` modules declare the same `_POSTGRES_LOCK_ID`.
- Structural pattern to follow: the existing tests in `tests/test_sync_locks.py`
  (plain pytest functions, no Postgres fixture).
- Verification: `uv run pytest tests/test_sync_locks.py -q` → all pass, including
  the 1 new test.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `grep -rn "_POSTGRES_LOCK_ID = " src/personal_data_warehouse/defs/ | grep -oE "[0-9_]+$" | sort | uniq -d` prints nothing
- [ ] `uv run pytest tests/test_sync_locks.py -q` exits 0 and the new uniqueness test is present and passing
- [ ] Exactly three source constants changed (the three "change this one" rows) plus one test added: `git diff --name-only` lists only the four in-scope files
- [ ] `plans/README.md` status row for 001 updated

## STOP conditions

Stop and report back (do not improvise) if:

- The drift check shows any colliding constant already moved to a different value
  since commit `e118893` (someone may have partially fixed this) — re-derive the
  live collisions before changing anything.
- `grep` shows a fourth collision not in the table above — report it; the plan's
  new values were chosen against the id set as of `e118893`.
- The uniqueness test can't locate the `defs` directory (path assumption wrong).

## Maintenance notes

- Whenever a new Dagster job with its own advisory lock is added, it must pick an
  id not already in the set; the new `test_advisory_lock_ids_are_unique` test is
  the guard and will fail CI if two collide.
- A reviewer should confirm the three new ids are not present anywhere else in
  the repo (`grep -rn "8_407_112_465\|7_403_111_844\|7_403_111_845" src/`).
- This does not change lock *semantics*, only the ids, so no production data or
  cursor is affected — the only behavioral change is that the three starved job
  pairs can now run concurrently, which is the intent.
