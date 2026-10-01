# Pipeline health and database integrity

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

## Pipeline Freshness and Health

Every warehouse table also has to declare **which pipeline feeds it and how freshness is
measured**, in `src/personal_data_warehouse/pipeline_health.py`:

- `PIPELINES` — one entry per pipeline (source poller, uploader, enrichment pass, derived
  builder) with its cadence, transport, expected data/run/event intervals, and the sync-state
  table that carries its heartbeat and errors.
- `TABLE_PIPELINES` — one entry per table: its pipeline, its `role`
  (`data` payload / `support` dimension / `state` cursor), the column the pipeline stamps on
  write, and the column holding the row's real-world event time.

**Three intervals, and collapsing them is how a monitor ends up unable to catch anything.**
`expected_run_interval` is how often the pipeline *runs*; `expected_data_interval` is how
often *data legitimately arrives*; `expected_event_interval` is how far behind *the newest
real-world event* may fall. Until 2026-08-23 seven pipelines carried a blunt
`expected_data_interval = 30 days`, so with the ladder's 2x/6x multipliers `pi` — an uploader
that runs every five minutes — could not reach `late` until sixty days, and sat quiet for
five weeks under a green dot. The cadence is not the answer either: a person does not record
a voice memo hourly. Each of those numbers is now set from the source's **own measured gap
distribution** over 730 days (the query is in `pipeline_health.py`), and any interval of a
week or more must carry a `data_basis` saying where it came from
(`test_a_long_data_sla_says_where_its_number_came_from`). `google_contacts` is the instructive
exception: measurement says contact edits really do go 51 days quiet, so its data SLA is
deliberately loose and its **hourly run heartbeat** is what catches it breaking.

**Measure the gap between USES, not between rows** — and check the pipeline has a heartbeat
before you loosen anything. Two pipelines were re-set on 2026-08-27 and each had made the
opposite half of the mistake:

- `pi` carried a 3-day SLA whose basis read "168 gaps, p95 0.06d, max 2.86d". Those are gaps
  between consecutive *events*, and an agent session emits events seconds apart — the number
  described how talkative a session is, never how often Zach opens the tool. Measured over
  distinct days of use, pi has **8 days in its whole life and a longest gap of 40 days**, so
  a 3-day SLA was alarming on Zach not using pi. It is 45 days now, and the uploader
  heartbeat is what says the uploader is alive.
- `alice_voice_recordings` had no run state **at all** — its own registry entry said "a daily
  poller with no heartbeat, so data freshness is the only signal there is." Measured over 17
  months, Zach records on 34 days with a longest gap of **223 days**, so no data SLA can both
  catch the poller dying and stay quiet while he is not recording. It sat `stale`, dragging
  four `marts_voice_memos`/`marts_calendar` views down with it, while the daily poll ran and
  succeeded every day. The poll now stamps `ops.alice_voice_recordings_sync_state` (status,
  error, `last_success_at`) and its data interval is 240 days, which is honestly close to
  mute — the heartbeat is the detector.

The rule both cases point at: **a loose data SLA is only honest when something else is
tight.** Loosening one without a heartbeat is how `manual_finance` (`data=None`) made its own
pipeline stopping in March 2026 "by construction not a detectable event".

`tests/test_pipeline_health.py` enforces this against `POSTGRES_TABLES`, the raw-DDL tables,
and the live schema — the same contract `TIMELINE_TABLE_COVERAGE` has, and the tests also
assert the two registries cover exactly the same tables. **Adding a warehouse table means
adding it to both.**

Data freshness is measured only from `data` tables, deliberately: Slack refreshing its user
directory daily must not make Slack look healthy while message ingestion is frozen. Run
freshness comes from the `state` tables. **Remote-device uploaders report their runs too**:
after every run, `bin/_pdw-upload-lib.sh` (`pdw_post_heartbeat`) posts the wrapper's observed
exit code, duration and error to `POST /ingest/heartbeat`, which upserts one row per
(pipeline, device) into `ops.uploader_heartbeats`. Each uploader pipeline
(`apple_notes`, `apple_messages`, `apple_contacts`, `apple_voice_memos`, `apple_photos`,
`claude_code`, `codex`, `pi`, `openclaw`) declares that table as its `StateSource` with a
`scope_column`/`scope_value` filter, so a LaunchAgent that fires and fails reads `failing`
on `/pipelines` instead of merely `late` — until 2026-08-27 `apple_voice_memos` sat `late`
for fifteen days with no way to say whether the uploader was healthy or the source quiet.
The five-minute uploaders must report within `UPLOADER_RUN_INTERVAL` (30 min: late at 1h,
stale at 3h); the heartbeat is best-effort and never changes the uploader's own exit code.
The `uploader_heartbeats` pipeline row says whether ANY device is reporting at all.

**A failing uploader names what is stuck, and a long streak pushes.** Until 2026-09-30 the
wrappers posted only the exit code, so a 625 MiB voice memo failed 37 runs in a row while
`apple_voice_memos` read `status = failing`, `data_status = ok` (smaller memos kept landing)
and `last_error` NULL — nothing said which file, or for how long. Now the wrapper passes its
run log to `pdw_post_heartbeat`, which sends the run's last `error:` line as `--error`
(`pdw_run_error`, scoped to lines after the run's own `starting` marker); the voice-memos
uploader keeps a per-file failure streak in its state file, so that line reads
`<file> has failed N consecutive run(s) since <T>: <cause>`. The app keeps a per
(pipeline, device) streak on the row (`consecutive_failures`, `failing_since`, computed in the
upsert) and sends one push on the sixth failed run in a row (`uploaderFailureAlertRuns`); a
success resets it.

**The heartbeat post is `pdw heartbeat`, and it resolves credentials the way every other
pdw command does.** `pdw_post_heartbeat` in `bin/_pdw-upload-lib.sh` runs
`pdw heartbeat --pipeline a,b --exit-code N --duration-seconds F --ran-at ISO` (native Go,
`app/cmd/pdw-cli/heartbeat.go`), which reads `pdw login`'s config like `pdw ingest` does.
The history that shaped it: when the post was a `uv run python -m …` DIRECTLY outside the
CLI, it inherited none of the URL/token `pdw ingest` resolved for the uploader beside it. The
five Apple wrappers each happened to export `PDW_API_URL`/`PDW_SECRET_TOKEN` from
`~/.config/pdw/config.json` for their own uploaders; the two agent-sessions wrappers ran
*through* `pdw` and so never did — and their heartbeat therefore failed on **every** run from
the day it shipped, ~288 times a day per Mac, into a launchd error log nobody reads.
`claude_code`, `codex`, `pi` and `openclaw` all sat at `last_run_at` NULL, so for those four
"the uploader died" and "Zach is not using this tool" were indistinguishable — the exact gap
the heartbeat exists to close, open on the four pipelines with no other signal. Credential
resolution lives once in `pdw_export_app_credentials` in the lib and every wrapper inherits it
by sourcing the lib; `tests/test_upload_heartbeat_lib.py` fails if a wrapper hand-rolls the
config read again, or posts a heartbeat without sourcing the lib, and
`tests/test_device_wrappers.py` fails if any wrapper runs anything but `pdw`.

- Collector: the `pipeline_health` Dagster asset (`*/10 * * * *`) probes `max(<column>)` per
  table and writes `ops.pipeline_health` + `ops.pipeline_table_freshness`. It only probes a
  column an index leads with or a table under `PROBE_MAX_UNINDEXED_BYTES`, and records
  `probe_status = 'skipped_unindexed'` otherwise — `timeline.events` (43M rows, 50 GB) is
  monitored through `ops.timeline_sync_state` instead of a full-heap `max(updated_at)`.
- Read surfaces: `marts_ops.pipeline_health` and `marts_ops.table_freshness` compute
  `status` at **read** time (`ok`/`late`/`stale`/`failing`/`attention`/`manual`/`no_data`/
  `unknown`) against each pipeline's own expected interval, so a snapshot older than
  `COLLECTOR_STALE_SECONDS` reports `unknown` rather than presenting stale facts as current.
  Store facts, derive status — the same rule the finance ledger follows.
- UI: the app serves `/pipelines` (linked from the `/timeline` topbar) over
  `GET /api/pipelines`; worst status first, per-table detail behind a click.

### The four levels, and what each one can and cannot see

| level | relation | answers |
| --- | --- | --- |
| 1 — pipelines | `marts_ops.pipeline_health` (+ `marts_ops.table_freshness`) | is this feed still delivering |
| 2 — marts | `marts_ops.mart_view_health` | is the read interface built on anything current |
| 3 — timeline adapters | `marts_ops.timeline_adapter_health` | is THIS kind of data reaching `timeline.events`, including source-cadence-judged ingest watermark lag |
| 3b — priority tiers | `marts_ops.timeline_priority_mix` | how each source's last seven days split across the five tiers; an `unclassified` row is `failing` |
| 3c — agent usage | `marts_ops.agent_usage` | are agents starting at the timeline and scoping by tier (C3), measured daily from their own sessions |
| 3d — search benchmark | `marts_ops.search_benchmark` + `search_benchmark_history` | weekly p50/p90 search latency and labeled MRR through the search tool, with retained trend (C8) |
| 3e — search internals | `marts_ops.search_health` | chunk/vector convergence, orphan completeness, BM25 integrity, and shared-buffer residency |
| 4 — integrity | `marts_ops.collation_health` | did the sort order move under us, and did anything break |

**A mart row names the input that coloured it.** `marts_ops.mart_view_health.cause_pipelines`
lists the declared inputs whose status IS the mart's `input_status`; `stalest_pipeline` is
only the oldest input relative to its SLA and says nothing about the verdict (on 2026-09-30
it named `pi`, ok, beside two agent-session marts in attention because of chatgpt).
`/pipelines` renders it as "attention because of chatgpt".

**No pipeline may declare an interval the collector cannot observe.** The collector runs
every ten minutes and the view judges its snapshot against `now()`, so a snapshot ages by up
to one collector interval. A one-minute run SLA made `timeline_notifications` flap
late/stale between nearly every pair of snapshots while its worker stamped every five
seconds; `test_no_pipeline_declares_an_interval_the_collector_cannot_observe` now refuses
any interval whose late threshold fits inside ten minutes.

**Level 2 exists because a view cannot be probed like a table.** `TABLE_PIPELINES` measures
`max(<written_at>)` over a heap; a view has no stamped column to take a max of and no
`relpages` for the cheapness guard to consult, so the table probe genuinely cannot be pointed
at one. What is cheap and true about a view is measured instead: the freshness of the
**stalest pipeline feeding it**, a **bounded `SELECT 1 FROM <view> LIMIT 1`**, and the
**sha256 of `pg_get_viewdef()`** so a redefinition that silently drops a source table is
visible even though it changes no rows. Views too expensive to probe every ten minutes are
*declared* in `EXPENSIVE_MART_VIEWS` and recorded as `probe_status = 'skipped_expensive'`,
the same honest-skip contract as `skipped_unindexed`.

Two details of the input roll-up are load-bearing, both settled by measuring against
production rather than by argument:

- **Inputs come from `pg_depend`/`pg_rewrite`, closed transitively to base tables** — never a
  hand-written map, which would rot the first time a view was redefined. Both the tables and
  the pipelines they belong to are stored: the tables are the evidence, the pipelines are what
  gets judged.
- **Judged per pipeline, not per table, and ranked by age relative to SLA rather than raw
  age.** A pipeline's own freshness is already a `max()` over its data tables, deliberately;
  applying its interval to one *individual* table breaks that symmetry. Measured 2026-08-23,
  doing so reported four marts `stale` because `derived_finance.transactions` was 1.1 days old
  against `finance_ledger`'s three-hour interval, while the ledger was writing balance
  observations every half hour exactly as designed. So **a mart is never more broken than the
  pipelines feeding it**, and `marts_ops.table_freshness` remains the place to look for a quiet
  table inside a healthy pipeline. Ranking by SLA-relative age matters for the same reason:
  `marts_ai_conversations.events` unions six agent sources whose expectations differ tenfold,
  and raw age would permanently nominate whichever is legitimately the quietest.

**Slack's generic `fatal_error` on `conversations.history` is a page too large to
serve, and the page is retried smaller.** The channel behind the 2026-09-19 red row held
dictionary-dump messages: Slack answered `fatal_error` at any page size of 50 or more and
served the same page at `limit=1`. `iter_cursor_pages_with_cursor` halves the page size
from the same cursor down to 1 before giving up, so a channel with one oversized message
is read past it instead of re-erroring on every sweep forever.

**An errored scope is `failing` only when it is at least 1% of the state table's
scopes; otherwise `attention`.** On a one-row state table (gmail, a transcription run) or
a four-row one (whoop) that is still the very first error, so nothing there changed. On
Slack's ~24k conversation rows, one public channel answering `conversations.history` with
Slack's generic `fatal_error` on every sweep read the whole pipeline `failing` on
2026-09-19 — messages ten minutes fresh, 288/288 runs green — and eleven marts red behind
it. The row is still counted in `state_error_rows` and named in `last_error`; it is the
colour that is proportionate now.

**`newest_event_at` is judged, and `unmeasured` is not `no_data`.** Event lateness escalates
the pipeline's status exactly like write lateness. Two failure modes are deliberately kept
apart from it: `unmonitored` (no data table declares an event column) and `unmeasured` (the
column exists but sits on a large heap with no leading index, so the collector skipped it by
design — `google_drive.modified_time`, `file_attachment_enrichments.ai_processed_at`).
Neither ever colours a pipeline red: a gap in the measurement is not evidence about the data.

Quick check without the UI:

```bash
pdw sql -q "which pipelines are unhealthy" "SELECT pipeline, status, last_write_at, last_error
  FROM marts_ops.pipeline_health WHERE status NOT IN ('ok','manual') ORDER BY status"

pdw sql -q "which marts read something stale" "SELECT view_schema, view_name, status,
  stalest_pipeline, stalest_pipeline_at FROM marts_ops.mart_view_health
  WHERE status NOT IN ('ok') ORDER BY status"

pdw sql -q "collation and index integrity" "SELECT scope, object_name, status, finding, detail
  FROM marts_ops.collation_health WHERE status NOT IN ('ok') ORDER BY status"
```

**Adding a column to a `marts_ops` snapshot table needs no migration line, and that is
new.** `CREATE TABLE IF NOT EXISTS` never revisits an existing table, so until 2026-08-28 a
column added to one of these `TableSpec`s reached every fresh database — and every test, and
CI — while production kept the old shape, and everything naming the column failed on every
run with the suite fully green. It happened three times (`pipeline_health` 2026-08-23,
`pgbackrest_health` 2026-08-27, `agent_usage` 2026-08-28), each repaired by hand-writing one
more `ADD COLUMN IF NOT EXISTS` beside the last — which is the bug, because the author who
adds a column is exactly the author who does not know a migration line is also required.
`ensure_pipeline_health_tables` now reconciles every table in
`PIPELINE_HEALTH_SNAPSHOT_TABLES` against its own spec, and
`test_ensure_restores_any_missing_column_on_every_health_snapshot_table` drops each column
in turn and asserts it comes back. It is scoped to those tables on purpose: their whole
content is rewritten by each collection, so an added column is metadata-only with no heap to
lock. Deriving the DDL from the spec also exposed a disagreement it had been hiding — the
hand-written migration gave the host-saturation gauges the `-1` "unmeasured" default while a
freshly created table gave them `0`, which the view reads as an **idle host**, the one
verdict C6 acts on. `UNMEASURED_SENTINEL_COLUMNS_BY_TABLE` is now the single source for both.

## Collation drift and index corruption

**This database cannot warn you about collation changes, and one has already happened.**
`pg_database.datcollversion` is **NULL** while `pg_database_collation_actual_version()`
reports glibc **2.36**. Postgres only raises its "collation version mismatch" warning when
it has a recorded baseline to compare against, and `ALTER DATABASE ... REFRESH COLLATION
VERSION` refuses to create one from NULL (`ERROR: invalid collation version change`,
`AlterDatabaseRefreshColl`). So the `en_US.utf8` sort order changed underneath the data
silently, and the next change will be silent too.

What that did, found and repaired 2026-08-23: seven btree indexes failed
`bt_index_check` with `item order invariant violated`, and four UNIQUE indexes were
admitting duplicate rows — a `ON CONFLICT` lookup missed the existing row through the
mis-ordered index and INSERTed a second one instead of upserting. 36,825 duplicate rows
had accumulated: `base_apple_messages.chat_messages` 30,043, `base_slack.message_reactions`
6,622, `base_google_calendar.events` 145, `base_apple_notes.notes` 15. Every duplicate group
differed in `sync_version` and `ingested_at`, which is the upsert-became-insert signature.

**There is now a detector, because Postgres will never be one here.** The `collation_health`
Dagster asset (daily 03:41) writes `ops.collation_health`, read through
`marts_ops.collation_health` and rendered on `/pipelines`. It is detector-only — no `REINDEX`,
no `CREATE EXTENSION`, no DDL (`test_the_detector_issues_no_ddl_and_no_repair` pins that), and
a finding keeps the run green so the one signal that matters does not become a permanently red
asset everyone ignores. Four things about it are load-bearing:

- **`datcollversion IS NULL` beside a real actual version IS the finding**, reported as
  `no_baseline`, worded as *"this database cannot detect collation drift; text index ordering
  is unverified"*. Written the obvious way — `recorded <> actual` — the comparison evaluates
  to NULL rather than true and reports CLEAN on exactly the database that has the problem.
- **The observed library version is stored as a fact** every run. With no baseline in
  `pg_database`, the snapshot's own history is the only baseline that will ever exist, so the
  next glibc change is visible as `actual_version` moving.
- **Only collations something actually uses are reported.** All 188 collatable indexes here
  ride the database default and **zero** use ICU, yet **871** ICU collations report drift;
  surfacing those buries the signal on day one, so the query joins through
  `pg_index`/`pg_attribute.attcollation` and reports only collations with a dependent index.
- **`unavailable` describes the extension, not the index.** An index whose last recorded
  amcheck verdict is `unavailable` is restored as `never_checked` (view status `unmeasured`)
  once `amcheck` is installed, and the daily rotation reaches it. Production carried 114 such
  rows as `attention` for weeks after the extension existed — a measurement gap presented as
  a finding, which is the permanently-red-row pattern that buries real ones. `unavailable`
  still reads `attention` while the extension is genuinely missing.
- **The duplicate-key probe applies each index's partial predicate** and skips expression
  indexes (`indkey` containing 0) and heaps over `DIVERGENCE_MAX_HEAP_BYTES` (2 GiB, which
  still covers both tables that actually accumulated duplicates). It is corroboration only:
  it detects duplicate *keys*, and three of the seven damaged indexes had none — they were
  merely mis-ordered. `amcheck` is the rigorous tool for that class, and the published view's
  comment says so.

**How to check by hand, and the two traps.** `amcheck` is the reliable tool
(`SELECT bt_index_check('schema.index'::regclass)`; it raises rather than returning a
value, so check for an exception, not a result):

- **Do not conclude "no duplicates" from a query the planner can answer with the index.**
  A corrupt unique index reports exactly what it believes. `SELECT DISTINCT`, `GROUP BY`
  and `count(DISTINCT ...)` can each read either the heap or the index depending on plan
  shape, and they disagreed by 145 rows on one table here. Force the heap:
  `SET LOCAL enable_indexscan=off; SET LOCAL enable_indexonlyscan=off; SET LOCAL enable_bitmapscan=off;`
- **A duplicate-count sweep is not sufficient.** Three of the seven damaged indexes had
  **no** duplicates — they were merely mis-ordered, which makes an index *miss rows that
  exist* and surfaces as quietly wrong query results, never as a count. Only `amcheck`
  catches that class.
- Any home-grown divergence probe must apply the index's partial predicate
  (`pg_index.indpred`). Ignoring it made one clean partial unique index report 53,035
  phantom excess rows.

Repair order matters: dedupe first (`REINDEX` on a UNIQUE index fails while the heap holds
duplicates), keeping the highest `sync_version` per key because that is what a working
upsert would have left, then `REINDEX INDEX CONCURRENTLY`, then re-verify with `amcheck`.
All 220 btree indexes were swept; the large ones (`base_gmail.messages`,
`base_slack.messages`, `timeline.events`) were clean. All 178 collatable indexes use the
database default collation — the 871 drifted `*-x-icu` collations have no dependent index
and are noise.
