# Agent Notes

Development practices:

* We use TDD for this repo and follow good code practices
* When asked to refactor or change existing code flows, please plan to completely replace the old legacy flow with the new requested flow - including ripping out any and all legacy code
* When querying the database, you can use the pdw CLI
* Before finishing a change, run `uv run pytest`, the canonical full local verification command.
  It self-provisions and removes an extension-complete warehouse Postgres on a random local port
  when `POSTGRES_DATABASE_URL` is absent, reuses an explicitly configured test URL, builds/reuses
  the managed agent image, and locally runs the subscription smokes. First-time auth is
  `uv run personal-data-warehouse-agent-auth login codex` (or `claude` for that provider). Use
  `uv run pytest --unit-only` only as an explicit faster iteration, not final verification;
  missing environment variables never opt tests out. No production database URL is necessary or
  recommended for tests.
  The disposable Postgres runs with `fsync`, `synchronous_commit` and `full_page_writes` off —
  it is thrown away per run, and on Docker-for-Mac each fsync is a virtual block device round
  trip — and the image is published for arm64 as well as amd64 so an Apple-silicon Mac runs it
  natively. Measured 2026-09-20 on porygon (OrbStack, image on the external NVMe) over the same
  167 DB-heavy tests: emulated amd64 with fsync on 352s, native arm64 with fsync off 88s.
  The suite runs under `pytest-xdist` by default (`-n auto` in `pyproject.toml`; `-n 0` for a
  serial run or `--pdb`): each test warehouse is its own `pdw_test_*` namespace and the
  index-refresh advisory lock is keyed per namespace (`index_refresh_lock_key`), which is what
  makes parallel workers safe — with one cluster-wide try-lock, whichever worker lost it
  silently skipped its own schema's index pass and a different index test failed every run.
  Full suite on porygon 2026-09-20: 1,228s → 483s (fsync off) → 261s (native arm64) → ~125s
  (4 workers).
* **`uv run pytest` does not run `go test ./...`, and CI does.** Anything under `app/` — and
  anything that *generates* into it, which in practice means
  `src/personal_data_warehouse/warehouse_catalog.json` — is verified locally only by the Python
  contract tests and by Go tests only after a push. A catalog edit is one JSON file plus
  `scripts/generate_go_warehouse_catalog.py`, so it looks purely Python and is not: on
  2026-08-28 a new mart's catalog comment pushed `TestOverviewGuidanceStaysWithinBudget` past
  its byte cap and reached `main` red behind a fully green `uv run pytest`. Run both when a
  change touches the catalog or `app/`.

## The contracts

These are what the warehouse *is*. Future work — human or agent — honors them rather than
routing around them. Each names what holds it up, because an unenforced contract is one
refactor away from quietly becoming untrue, and several of these have been. **The living grade
is `uv run python scripts/contract_audit.py`** (one verdict per contract, from production);
`tests/test_repo_contracts.py` fails if a contract stated here has no check there. Grade from
that, not from this prose.

- **C1 — everything synced eventually lands on `timeline.events`.** One row per real-world
  event from every source, with `source_table` + `source_pk` drilling back to the
  authoritative row. *Held up by* `TIMELINE_TABLE_COVERAGE`: every table declares itself
  `events`, `detail`, `entity` or `state`, `tests/test_timeline.py` checks that against the
  **live** schema, and a non-`events` table must carry a `no_adapter_reason`.
  `marts_ops.timeline_adapter_health` judges ingest lag against the source's own interval.
  *Gap:* registering an adapter for a new events table is still a silent step.
- **C2 — five priority tiers, and everything is properly categorized.** `self` (Zach did
  it), `direct` (a real person reached him), `cc` (real people, he is peripheral), `noise`
  (bulk or automated, including his own health telemetry), `background` (machinery — the
  warehouse's own or other people's). *Held up by* the `timeline.timeline_priority` enum,
  required per-adapter priority SQL validated before a batch is written, and
  `marts_ops.timeline_priority_mix`. *Gap:* which valid tier an adapter assigns is a
  judgement no test makes. See [Timeline priority tiers](#timeline-priority-tiers).
- **C3 — agents start at the timeline and filter by priority.** The `search` tool or
  `timeline.events` in SQL first, then `marts_*`, then `base_*`; every search entry point
  takes `priorities`, and every hit names its tier. The manual is `pdw readme` / the MCP
  `readme` tool, rendered from `app/internal/guide/` (see [The agent guide](#the-agent-guide)).
  *Held up by* the guide tests, `test_schema_comments_publish_the_start_here_guidance`, and
  `marts_ops.agent_usage`, which measures from real sessions the search-first share (target
  ≥ 60%), priority-filter share (≥ 40%) and SQL-error sessions (< 10%). Its subcommand
  list is pinned to the Go dispatcher by
  `test_the_python_subcommand_lists_are_the_go_dispatchers_commands`.
- **C4 — raw source data for every source is queryable via SQL.** `base_<source>` is a
  faithful copy readable by the `pdw_query` role. *Held up by*
  `test_query_role_reads_public_relations_and_is_denied_private` and the catalog's
  per-layer query-access policy.
- **C5 — multi-source concepts layer `base_* → derived_*/marts_* → timeline`; consumers read
  `timeline → marts_* → base_*`.** **An intelligent transformation must READ from the
  intermediate layer, not one source's raw table**, so a second source is covered the day
  it lands: `base_alice_voice_recordings` sat at 53 recordings and 0 transcripts while every
  registry passed, because transcription scanned the Apple table by name. *Held up by*
  `tests/test_schema_reorg_contract.py` and
  `test_no_enrichment_runner_reads_a_raw_source_table_unaccounted_for`: every enrichment
  runner's raw read is listed with a reason, and a stale exemption fails.
- **C6 — PDW responds fast, and a response over two seconds uses the whole host before
  anyone optimizes further.** CPU, RAM, GPU and disk: an unparallelized plan on an idle
  28-vCPU host is not a query that needs a cleverer algorithm. *Held up by* the per-request
  leg timings every hybrid search logs, the `slow search` warning (CPU/IO pressure, load,
  CPU count) on any search over 2 s, the weekly `marts_ops.search_benchmark` latency and
  saturation verdict over term bags **and** the one-word queries the guide teaches, and
  `cache_residency` in `marts_ops.search_health`. See
  [the performance contract](docs/agents/search.md#performance-contract).
- **C7 — pipeline health is inspectable via SQL and web.** Per source, per mart, per
  timeline adapter, plus integrity: `marts_ops.pipeline_health` / `table_freshness`,
  `marts_ops.mart_view_health` (which names the input that coloured it in
  `cause_pipelines`), `marts_ops.timeline_adapter_health`, `marts_ops.collation_health`,
  all on `/pipelines`. *Held up by* `PIPELINES` + `TABLE_PIPELINES` and
  `tests/test_pipeline_health.py`, including
  `test_no_pipeline_declares_an_interval_the_collector_cannot_observe`. A source being down
  and saying so is this contract working; C11/S1–S3 grade the source.
- **C8 — search is one hybrid path over the timeline, and its quality is measured.** BM25,
  pgvector ANN and a gated literal leg fused by reciprocal rank; embeddings from the
  self-hosted GPU track `timeline.events.seq`. *Held up by* `marts_ops.search_health`
  (chunk/embedding lag, orphan proof, BM25 integrity and bloat), the weekly
  `search_benchmark` asset over the labels in `private.search_benchmark_labels`
  (`docs/search-benchmark.md`), and `src/personal_data_warehouse/search_benchmark.py`.
- **C9 — one obvious way to do a thing, on both surfaces.** One hybrid search, one SQL
  entry point per surface, one schema-discovery call, one way to read a hit's
  conversation (the `context` tool / `pdw context`). *Held up by* `tool.Surface`, the
  `runCall` fence that refuses `pdw call sql|query|search|context|schema_overview|describe_table`
  with the real command, and `app/cmd/pdw-cli/usage_test.go`.
- **C10 — the database is healthy, backed up, and a restore has actually been performed.**
  pgBackRest ships WAL continuously to SFTP on `slowking`, takes periodic backups, and
  runs retention as its own step. *Held up by* `tests/test_pgbackrest_image.py` and
  `marts_ops.pgbackrest_health`, written by the backup loop itself: `attention` when a
  backup attempt or retention (`expire_status`) fails, and when the last restore drill
  (`pgbackrest_restore_drill record`) is older than 45 days. Runbook:
  `~/dev/zachlatta/sysadmin` (`backup-health.md`, `slowking/`).
- **C11 — a source's own SLA is stated and detected, not inferred from the pipeline being
  green.** *Held up by* per-source detectors: `marts_ops.slack_conversation_health`,
  `marts_ops.plaid_item_health`, `marts_ops.simplefin_account_health`, and
  `marts_finance.net_worth.staleness`. *Gap:* every other source rides aggregate freshness.
- **C12 — future developers understand these contracts and honor them.** This file is
  loaded into every session in this repository, so it holds only the contracts and the
  rules every change needs; per-source and incident detail lives in `docs/agents/` and is
  linked below. *Held up by* `tests/test_contracts_doc.py` (every test a contract names
  exists), `test_agents_md_fits_one_sitting` (a byte cap on this file), and
  `test_every_stated_contract_has_a_living_audit_check`.

Three sources carry a contract of their own, because each is where "the pipeline is green"
and "the data is right" have come apart:

- **S1 — Slack: every message, DM, group DM, private channel, public channel and thread is
  synced and current, and a DM lands fast — by polling.** Nothing tells PDW which
  conversations moved (`client.counts` was refused and removed on 2026-10-01), so the
  freshness pass lists DMs and group DMs to find new ones and polls every conversation
  when it is due. *Held up by* `marts_ops.slack_conversation_health` (discovery share,
  poll share for every type, DM landing latency) and
  `test_blanket_freshness_polls_each_conversation_when_it_is_due`. See [Slack](docs/agents/slack.md).
- **S2 — every voice source lands in `base_*`, unifies in `marts_voice_memos.recordings`, is
  transcribed by AssemblyAI, enriched by an agent that can query PDW, and matched to a
  calendar event, with unmatched recordings kept in the same mart.** *Held up by*
  `voice_memo_transcription` / `voice_memo_enrichment` state rows and C5's raw-read
  registry. See [Voice recordings](docs/agents/voice.md).
- **S3 — finances from Plaid, SimpleFIN and manual documents cover expenses, investments,
  the mortgage, liabilities and private equity; multiple witnesses of one account are
  reconciled; receipts are linked to transactions.** *Held up by*
  `marts_ops.plaid_item_health`, `marts_ops.simplefin_account_health`,
  `marts_finance.net_worth.staleness`, `marts_finance.position_coverage`, and the
  `receipt_enrichment` heartbeat. See [Finance](docs/agents/finance.md).

Adding a source touches all of these. The step-by-step list, marked by which steps a test
catches and which fail silently, is [Adding a warehouse source](#adding-a-warehouse-source).

## Where the detail lives

Read the file for the area you are changing before you change it.

| area | file |
| --- | --- |
| search, hybrid retrieval, landing latency, the performance contract and its incidents | [docs/agents/search.md](docs/agents/search.md) |
| pipeline freshness and health, collation drift and index corruption | [docs/agents/pipeline-health.md](docs/agents/pipeline-health.md) |
| Slack: polling, discovery, coverage, huddles, sending, file bytes | [docs/agents/slack.md](docs/agents/slack.md) |
| voice recordings and the Voice Memos uploader | [docs/agents/voice.md](docs/agents/voice.md) |
| finance: Plaid, SimpleFIN, the ledger, manual documents, securities | [docs/agents/finance.md](docs/agents/finance.md) |
| WHOOP and the health mart | [docs/agents/health-sources.md](docs/agents/health-sources.md) |
| agent sessions, Claude Desktop, ChatGPT, Muse, the app's ingest path | [docs/agents/agent-sessions.md](docs/agents/agent-sessions.md) |
| local Mac uploaders and their macOS permissions | [docs/agents/local-uploaders.md](docs/agents/local-uploaders.md) |
| reviewed writes (Notes, Contacts), the iOS app and push | [docs/agents/mutations-and-app.md](docs/agents/mutations-and-app.md) |
| WhatsApp, Hacker News, shared attachment enrichment | [docs/agents/other-sources.md](docs/agents/other-sources.md) |

## The agent guide

**The manual for using the warehouse lives in the binary, not in a skill.** `pdw readme
[full|topic]` (and a bare `pdw`) on the CLI, the `readme` tool over MCP — both rendered from
`app/internal/guide/` (`brief.md`, `readme.md` plus `topics/*.md`, Go templates whose only
branching is the surface's own spelling of each call). **The brief is what a session reads
by default** (~6 KB, capped at 8 KB by `guide_test.go`); `pdw readme full` / `{"topic":
"full"}` is the long form (capped at 14 KB). Measured over twelve real sessions on
2026-09-22, the skill plus the full guide cost 25–45 KB before the first data call
whatever the question was — a one-line phone-number lookup paid eight times its own
answer in preamble — which is why the default is the brief. The same audit added
`pdw context <ref>` (the first-class spelling of `timeline.context()`, naming its columns:
`SELECT *` there returned `metadata` and the full `search_text` of every row, 3.8 KB
against 0.5 KB for the three columns anyone reads, and it was used a quarter as often as
search because the ref had to be quoted inside a quoted statement), a one-line-per-hit
search output (`--full` for the old previews, `-n 10` default, an unknown flag anywhere
is an error rather than query text), and on MCP `connections` + `connection_call` in
place of a flat listing of every connected upstream tool: that list was 196 tools /
153 KB (~38k tokens) of definitions in every MCP session, 142 KB of it proxied, and the
sessions paying it almost never called one (the flat listing is gone, not switchable;
`pdw list` / `pdw call` are unchanged). Since 2026-10-01 `context` is a server tool on both
surfaces and `pdw context` calls it, so a hit's conversation is read one way everywhere. The follow-up the same day: search
previews are cleaned after windowing (tracking URLs, markdown-table scaffolding, zero-width
padding — a newsletter hit's whole 800-char preview had been squarespace-mail.com redirect
links); a textless Slack hit (a file, image or canvas post, ~5,000 a week) is labelled at
**read time** in the search service rather than in the adapter's snippet, because the
adapter edit would change its signature and re-walk 46.8M Slack rows for a label; the SQL
tool hints at `body_markdown_clean` when a statement reads `body_text`/`body_html` from
`base_gmail.messages`; `schema_overview` / `pdw schema` take a schema name or layer prefix
so one domain costs one schema instead of the 36 KB whole; and `marts_ops.agent_usage`
pairs a tool result with its call by `turn_id` (Claude Code's `tool_use_id`, now stamped on
result rows too) before falling back to adjacency, and reads a piped CLI search with no
`Search:` header as unknown rather than failed — adjacency alone had lost the result of
26% of pdw search calls and nearly every proxied MCP call, and the header rule had put 29%
of a fortnight's searches in `search_invalid_or_failed_priority`. The fleet skill is one line that says to read
it. Until 2026-09-09 the guide was a hand-transcribed skill outside this repository that
drifted from the code on every reorg; now `app/internal/guide/guide_test.go` fails when
the guide names a relation the catalog does not have, omits a priority tier or selection,
teaches a command the CLI refuses, indexes a topic that does not exist, or outgrows one
sitting (14 KB for the full page, 8 KB for the brief). So **editing the guide is how a warehouse
change reaches agents**: a change that alters what an agent should do first, which relation a domain
starts at, or a trap it must know is not done until the guide says so — a new source goes
in `topics/sources.md`, a new command in the command map. Keep the main page to what every
session needs and put depth in a topic. The repository is public: no incident amounts, no
people's names, nothing that belongs in a private note.

## Warehouse Schema Layout

The warehouse is organized into four public layers that also sort alphabetically in that
order, so `pdw schema`, `\dn` in psql, and any `ORDER BY table_schema` all read the same way:

| layer | what it holds |
| --- | --- |
| `base_<source>` | faithful provider/source data and full-detail drill-down |
| `derived_<domain>` | modelled facts: normalization, identity resolution, enrichment, history |
| `marts_<domain>` | stable structured read interfaces per domain |
| `timeline` | the cross-source event stream **and** the search interface |

**Start with `timeline`.** `timeline.events` is one row per real-world event from every
source; `timeline.search_text()` / `timeline.search_text_exact()` search the whole corpus.
Each row's `source_table` + `source_pk` drill straight back to the authoritative row. It is
the recommended entry point, not the only truth — plenty of questions are answered directly
from a `base_*` or `marts_*` relation, and relations may flow `base_* → timeline` without
passing through `derived_*` or `marts_*` at all.

Three schemas are hidden from ordinary discovery and are not a query surface:

- `ops` — sync cursors, watermarks, runtime state, with source-prefixed physical names
  (`ops.gmail_sync_state`, `ops.upstream_mutation_operations`, ...). The read-only query role
  can read only the handful the app's own timeline/mutation UI renders.
- `private` — credentials and session snapshots. Never granted to the query role or PUBLIC.
- `internal` — implementation-only helper functions.

### The catalog is the only place to edit

`src/personal_data_warehouse/warehouse_catalog.json` declares every managed table, view,
sequence, function, and type: its stable logical id, layer, domain, physical schema/name,
discoverability, and query-access policy. Python loads it directly
(`personal_data_warehouse.warehouse_catalog`); the Go mirror
(`app/internal/warehouse/catalog_gen.go`) is generated by
`uv run python scripts/generate_go_warehouse_catalog.py` and pinned by a `--check` test.
Adding or moving a warehouse object is one catalog edit plus regeneration, never parallel
hand-edits.

Logical ids (`gmail_messages`, `timeline_events`, ...) are **catalog identifiers**, not SQL.
They are what `timeline.events.source_table` stores and what routing keys on, so they stay
stable when physical names move. Warehouse SQL names relations through an explicit
`@logical_id` marker that `expand_relations` (Python) / `warehouse.ExpandRelations` (Go)
resolves; an unknown marker raises, and a *bare* legacy name is left alone so Postgres
rejects it. There is no rewriter and no compatibility view — that is deliberate: the earlier
bare-identifier rewriter could not tell the `search_text` column from the `search_text()`
function, and a stale unqualified copy silently returned zero rows for 16 days.

### Migrating an old-layout database

`uv run python -m personal_data_warehouse.schema_upgrade --apply` is the one-shot upgrader
(preflight-only by default). It relocates tables with `ALTER ... SET SCHEMA` so large heaps
keep their filenode, rebuilds the marts/search layer from code, and validates the result. It
is deliberately not part of any `ensure_*` path: fresh provisioning only ever creates the
target layout.

### Absence is the epoch, not NULL

Warehouse columns are overwhelmingly `NOT NULL`, and the `TableSpec` layer in `postgres.py`
gives timestamps a sentinel default. So "hasn't happened yet" and "never happened" are
stored as **`1970-01-01 00:00:00+00`**, not as `NULL`. Because the default is applied at
that shared layer, every source inherits the same representation — this is a warehouse-wide
convention, not a per-source quirk.

The symptom you will hit before you understand the cause: `base_whoop.cycles.end_at` holds
the epoch for the cycle still in progress, so `ORDER BY end_at DESC` ranks the *currently
running* cycle as the oldest row in the table. Measured 2026-08-23, the same shape is
everywhere — in the most recent 20,000 `base_apple_messages.messages` rows, `date_read` is
the sentinel 8,220 times and `date_delivered` 16,616 times, with **zero** NULLs in either;
all 18,284 of `base_whatsapp.messages.edited_at`'s absent values are the sentinel too. That
is 41% and 83% of recent messages, so `MIN(date_read)`, `ORDER BY date_read`, and any
"unread" predicate are wrong by default, not in some edge case.

**The `marts_*` read view is the sanctioned place to translate it back**, with
`NULLIF(col, '1970-01-01 00:00:00+00'::timestamptz)`. `marts_ops.pipeline_health` already
does exactly this for `last_write_at`, `newest_event_at`, `last_run_at`, `last_error_at`
and `collected_at`. A view that relies on this should say so in a comment, so the next
reader does not rediscover the convention through a mis-sorted query.

Two rules follow, and the second is the one that bites:

- **Translate every exposed timestamp column, or none.** Since the sources are internally
  consistent, a view that `NULLIF`s `date_read` but forgets `date_delivered` does not
  inherit an inconsistency — it *manufactures* one, and every downstream `ORDER BY`,
  `MIN()`, `COALESCE` and `IS NULL` then disagrees depending on which column was asked.
- **Test it per column.** Seed a row carrying the sentinel, read it back through the view,
  and assert `NULL`. `test_whoop_cycles_view_reports_an_unfinished_cycle_as_null_not_the_epoch`
  is the shape to copy; it is cheap to repeat once per exposed timestamp.

Booleans are the sibling trap: they are `bigint` 0/1 here, not `boolean` (`is_from_me = 1`,
never `= true`). A conforming view over several sources should make the conformed column's
type explicit rather than mixing a bigint from one source with a bool from another.

## Timeline priority tiers

Every `timeline.events` row carries a `priority`, classified per row at sync time by the
adapter's own SQL and stored in the `timeline.timeline_priority` enum. It is the column an
agent filters a timeline read by, and it is the reason "what happened today" does not open
with a newsletter. The enum's declaration order **is** the sort order, most attention first.
The following contract is generated from `warehouse_catalog.json`:

<!-- BEGIN GENERATED TIMELINE PRIORITY CONTRACT -->
<!-- Generated by scripts/generate_go_warehouse_catalog.py; do not edit by hand. -->

| tier | what it means | typical rows |
| --- | --- | --- |
| `self` | Zach initiated it | his sent mail and messages, his notes, photos and voice memos, his agent sessions and the turns he typed into them, his own calendar events, and his card purchases and payments |
| `direct` | a real person reaching him directly | DMs, email addressed to him, small group threads, big group chats for the week he takes part in them, a real `<@id>` ping, and replies in a thread of his that are conversation rather than announcements |
| `cc` | real-people activity he is peripheral to | cc'd mail, private team channels he sits in, big group chats he is not taking part in that week, replies under his channel-wide broadcasts, people talking about him in public, and others editing a file he owns |
| `noise` | bulk or automated traffic | newsletters, notifications, bots, Slackbot file posts, GitHub and CI relays, Gmail's auto-created plus deleted or declined calendar events, his own health telemetry, and public-channel chatter not aimed at him whether or not he is a member |
| `background` | the warehouse's own machinery and other people's background work | enrichment runs, mutation workers, contact-card churn, model answers and tool output in agent sessions, orchestrated (orchestrator-spawned) agent sessions, and Drive files other people change |

**Scope selection guide** — use the single `priorities` mechanism; do not add a competing scope flag:

| intent | priorities | why |
| --- | --- | --- |
| attention or correspondence | `self,direct,cc` | Use the three attention tiers to leave out bulk and background traffic. |
| Zach's own acts or words | `self` | Use self for actions Zach took and words he wrote. |
| prior agent conclusions | `self,background` | Include background because model answers and tool output live there while Zach's prompts live in self. |
| notifications, CI, or telemetry | `noise` | Use noise when automated traffic is the subject rather than a distraction. |
| broad topical discovery or uncertain scope | `all tiers (omit the filter)` | Omit the filter and search all tiers when recall matters or the relevant tier is unknown. |

The default scope is **all tiers**; no priorities filter is applied. `unclassified` is not a sixth tier: the fail-loud sentinel and column default, never valid in steady state; its presence is a bug. It describes rows whose adapter classification did not run and is accepted only so an outage can be found.
<!-- END GENERATED TIMELINE PRIORITY CONTRACT -->

Two of those lines were redrawn on 2026-08-27 after grading twenty random rows per
tier, and each is a *why*, not a mechanism. **A big group chat is one tier for the week**:
the old rule promoted a message to `direct` when Zach had posted within six hours, so a
23-person event-ops WhatsApp group read 140 `cc` / 3 `direct` in one week — the same
people, the same conversation, split by the clock. It is `direct` now when at least two
of the group's messages within ±7 days are his and at least one in ten is
(`_group_week_engaged`); his share in that group is 0.4%, in the groups he actually talks
in 10–55%. **The words Zach typed into an agent session are `self`**: turn rows were
uniformly `background`, so `priorities => ARRAY['self']` could reach a session's title
and never a sentence he wrote; the model's replies and every harness-injected user turn
stay `background`. **A card purchase stays `self`**: it was moved to `background` the same
day on the argument that nobody reads one, and moved back within the hour — a swipe, a
bill payment, a Venmo with a memo are all actions Zach took, and `self` is "Zach did it",
not "Zach would want to read it". Only the sweeps, interest, autopay and payroll a machine
moved are `noise`.

```sql
SELECT event_ts, priority, source, actor, title, snippet
FROM timeline.events
WHERE priority IN ('self', 'direct', 'cc')
  AND event_ts >= now() - interval '1 day'
ORDER BY event_ts DESC LIMIT 100;
```

**The tier is a filter, not just a label, and it is the same filter everywhere.** Reading
`timeline.events` directly, the predicate above is it. Every search entry point takes the
tiers as `priorities`, a `text[]`, and an unknown token raises with the valid list rather
than being dropped into a search of everything:

```sql
SELECT * FROM timeline.search_text('budget approval', 20, priorities => ARRAY['self','direct']);
SELECT * FROM timeline.search_text_exact('invoice 4831', 20, priorities => ARRAY['self']);
```

```bash
pdw search --priority self,direct 'budget approval'      # the CLI form
```

The `search` tool (and its MCP twin) takes the same filter as `"priorities": ["self","direct"]`.
Omitting it searches every tier, which is almost never what an attention question wants:
`noise` alone is most of the corpus, so leaving it in is the usual reason a search comes back
full of newsletters. Every hit carries its own `priority` column, so a filtered search can
always show its work.

**The mix is measured, per source.** `marts_ops.timeline_priority_mix` (also on `/pipelines`)
is one row per (source, tier) over the last seven days — `events_7d`, `events_1d`, `share_7d`,
`newest_event_at` — snapshotted by the `pipeline_health` collector every ten minutes. It is the
surface on which a tier that quietly swallows a source after an adapter edit, or a source that
stops producing `direct` at all, is a number rather than a hunch; any `unclassified` row there
reads `failing`.

`unclassified` is the sixth label and is **not a tier** — it is a fail-loud sentinel for rows
the sync has not classified yet. It is accepted by the `priorities` filter, because scoping a
search to it is how a classification outage is *found*, but any surface that lists it beside
the five real tiers is teaching a sixth tier that does not exist. It must never appear in
steady state; if a query returns `unclassified` rows, an adapter's classification did not run,
and the answer to whatever was asked is wrong rather than merely incomplete. The tiers themselves are heuristics and are
expected to be tuned; the sentinel is not.

**Changing a high-volume adapter's classification SQL is not a cheap edit.** `priority` is
part of the normalized content, so it participates in the content guard (`seq` bumps when it
changes) and it is part of `adapter_signature` in `ops.timeline_sync_state`. Changing the SQL
changes the signature, which resets that adapter's backfill and re-walks **every row it
owns** — slack alone is 46.8M rows, and a past re-walk grew `timeline.events` to 93 GB before
it settled. Batch the change with any other adapter edit you were going to make, expect the
table to bloat while it runs, and plan the vacuum. **The re-walk also writes WAL faster
than the archiver can ship it**: measured 2026-08-26, the slack/gmail re-walk generated
~19 GB/h against pgBackRest pushing ~2 GB/h to the HDD-backed Garage, so the unarchived
backlog grew ~15 GB/h and `pg_wal` passed 62 GB. Throttle it by setting
`TIMELINE_SYNC_BACKFILL_BUDGET_SECONDS` (seconds of each 5-minute run the backfill may
use; unset = the whole 240s budget, `0` = pause the re-walk) on the Dagster deployment
until `pg_stat_archiver` catches up, then unset it. Incremental sync is never throttled
by it, so the timeline stays current while history waits. There is no fallback tier: every adapter
must declare a priority expression, and a NULL or unknown result fails that adapter before
the batch is written. The error is persisted in `ops.timeline_sync_state` and surfaces as
`failing` in `marts_ops.timeline_adapter_health`. The fail-loud engine rollout deliberately
preserves the legacy generated SQL only for signature calculation, so removing the old
`COALESCE(..., 'cc')` wrapper itself does **not** reset all adapter backfills; changing an
adapter's actual classification expression still resets that adapter normally.

## Adding a warehouse source

The only checklist that existed for years was the *photo-source* one further down, which is
the specialized case. This is the general one: fifteen edits, in dependency order, each
marked **ENFORCED** (a test fails if you skip it — the test is named so you can run it) or
**SILENT** (nothing catches it; the source ships subtly wrong and stays that way until
someone notices a gap in an answer).

**Getting the data in**

1. **The collector** — a client uploader under `src/personal_data_warehouse_<source>/`, or a
   Dagster poller under `defs/`. **SILENT.**
2. **The transport** — remote devices POST to the app's `/ingest/<source>/...` endpoints
   (they must not hold the Drive credential); in-process Dagster clients may write Drive
   directly. **SILENT.** See
   [Client uploads via the app](docs/agents/agent-sessions.md#client-uploads-via-the-app-the-write-path-for-remote-devices).
3. **The Dagster reader** — the `<source>_drive_inbox_sensor` + `<source>_drive_ingest` asset
   that promotes inbox objects into the raw table, and its schedule/sensor wiring.
   **SILENT.** A remote-device uploader also posts its run heartbeat via
   `pdw_post_heartbeat` in its `bin/*-upload-*` wrapper and declares
   `state=_uploader_heartbeat("<pipeline>")` in `PIPELINES` — **ENFORCED** for the
   listed uploaders (`test_every_remote_device_uploader_declares_a_run_heartbeat`),
   silent for a brand-new one.

**Making it a warehouse object**

4. **Catalog entry** in `src/personal_data_warehouse/warehouse_catalog.json` — logical id,
   layer, domain, physical schema/name, discoverability, query access. This is the *only*
   place a new relation is declared. **ENFORCED**
   (`test_fresh_database_object_inventory_matches_the_catalog`: a fresh database must contain
   exactly the cataloged objects, and an unknown `@marker` raises instead of passing through).
5. **Regenerate the Go mirror**: `uv run python scripts/generate_go_warehouse_catalog.py`.
   **ENFORCED** (`test_go_catalog_is_generated_from_the_json_catalog`).
6. **`TableSpec`** in `postgres.py` plus the `ensure_<source>_tables()` path that creates it.
   **ENFORCED** (`test_every_postgres_table_spec_is_a_cataloged_table`, plus the fresh-database
   inventory above).
7. **Indexes** in `POSTGRES_INDEXES`, including one leading with the column the freshness
   probe reads. **ENFORCED** for the freshness column
   (`test_the_pipelines_data_tables_are_cheaply_probeable`: the collector refuses `max()` over
   a large unindexed heap, so an unindexed new table reports no freshness at all).

**Making it visible**

8. **`TIMELINE_TABLE_COVERAGE`** — one entry per new table: `events`, `detail`, `entity`, or
   `state`. **ENFORCED** (`test_every_registered_table_is_classified` and
   `test_live_schema_has_no_unclassified_tables`, which checks the **live** schema).
9. **`TABLE_PIPELINES` + a `Pipeline` in `PIPELINES`** — which pipeline feeds the table, its
   role, its write column and its event-time column. **ENFORCED**
   (`test_every_registered_table_has_a_pipeline` and
   `test_pipeline_and_timeline_registries_cover_the_same_tables` — the two registries must
   cover *exactly* the same tables).
10. **Register a timeline adapter** in `TIMELINE_ADAPTERS`. **SILENT** — and this is the
    biggest hole in C1. A table classified `detail`/`entity`/`state` legitimately has no
    adapter, so nothing can tell "correctly not on the timeline" from "forgotten". If your
    source has events, it needs an adapter, and only you will know.
11. **The adapter's pagination contract** — `backfill_sql` pages newest-first by
    `(event_ts, event_id)`, `incremental_sql` oldest-first by `(ingest_ts, event_id)`, both
    returning exactly `TIMELINE_NORMALIZED_COLUMNS`, plus `max_ingest_sql`. **ENFORCED**
    (`test_adapter_sql_carries_the_pagination_contract`).
12. **Assign a priority tier** in the adapter's SELECT. **ENFORCED for presence, SILENT for
    correctness** — an adapter with no priority expression raises at registration, and a
    NULL or unknown label fails the batch before it is written (`TimelinePriorityError`);
    which of the five valid tiers you chose is a judgement no test makes. See
    [Timeline priority tiers](#timeline-priority-tiers).
13. **Seed the adapter in the end-to-end timeline tests** (`_seed_sources` +
    `EXPECTED_SEEDED_EVENTS` in `tests/test_timeline.py`). **ENFORCED** —
    `test_timeline.py` asserts the seed dictionary covers exactly the registered adapters,
    so an unseeded adapter fails the suite instead of never having its SQL run.
14. **Add the `SEARCH_SOURCE_DEFS` token** in `postgres.py`. **ENFORCED** since 2026-08-23
    (`tests/test_repo_contracts.py::test_every_timeline_adapter_has_a_search_source_token`).
    It was silent before, and silent here means two things at once: the source cannot be
    scoped with `sources => ARRAY[...]`, and it falls outside the low-volume BM25 partition,
    so a broad search reaches its rows only by walking past millions of gmail/slack documents.
15. **Document it** — a section in `AGENTS.md` and/or `README.md` with the SQL starting
    points, **and the agent guide** (`app/internal/guide/topics/sources.md`, plus the
    domain topic if one exists), because the guide is what an agent actually reads.
    **SILENT in substance, ENFORCED in accuracy**: nothing requires you to write the
    section, but if you do, every `schema.relation` you name must exist
    (`test_docs_only_name_relations_that_exist` for the docs,
    `TestEveryRelationTheGuideNamesExistsInTheCatalog` for the guide) and may not be a
    pre-reorg name (`test_no_module_names_a_pre_reorg_physical_relation`).

Photo sources have five *additional* registry edits on top of this list — see
[Adding a photo source](docs/agents/local-uploaders.md#adding-a-photo-source-google_photos-takeout-import-manual-imports-).

## Commit and Push Safety

Before committing or pushing, review the complete staged diff line by line for secrets,
credentials, tokens, private URLs, personal data, generated artifacts, and anything else
that should not be public. If there is even a smidgen of doubt about whether a change is
safe to commit or push, stop and check with Zach before proceeding. Never
include other people's names in code, even if their names are public.

Always assume other agents may be running in the same worktree. Before committing, carefully
verify the staged changes and commit only the changes made in the current session unless Zach
explicitly instructs otherwise.

## Deployment / Production

**PDW runs on `mew-coolify`, not on `rotom`.** The Coolify *control plane* (UI, API, the
`coolify` container) still lives on `rotom`, but as of 2026-08-19/21 the app, Dagster, the
warehouse Postgres, and the Dagster Postgres were all migrated to the Coolify server named
`mew-coolify` (a KVM guest on `mew`). `ssh mew-coolify` to inspect the running containers;
`ssh rotom` only gets you the control plane plus Loki, Grafana, and the other apps it still
hosts. The two hosts share one public egress IP, so an outbound-IP symptom does not tell you
which of them made a request.

Find the running containers (names are `<resource-uuid>-<deploy-timestamp>` for apps, bare
uuid for databases) — do not hardcode uuids, they change when a resource is recreated:

```bash
ssh mew-coolify 'sudo -n docker ps --format "{{.Names}}\t{{.Image}}"'
```

Query the warehouse directly as superuser, which is the only way to read `private.*`
credential metadata and the `ops.*` tables the read-only `pdw` role cannot see:

```bash
# Match on the IMAGE: the container name is a bare uuid and says nothing.
ssh mew-coolify 'PG=$(sudo -n docker ps --format "{{.Names}} {{.Image}}" | awk "/pgbackrest/ {print \$1}");
  sudo -n docker exec "$PG" psql -U postgres -c "SELECT ..."'
```

Two traps that cost a session each: `docker` on both hosts needs `sudo -n` (the login user is
not in the `docker` group), and the Python env inside the app/Dagster containers is
`/app/.venv/bin/python`, **not** the `python3` on `PATH` — `/usr/local/bin/python3` is missing
every project dependency, so a `ModuleNotFoundError` there means you used the wrong
interpreter, not a broken image.

**A hung run holds a queue slot, and eight slots is one bad deploy away from starving the
five-minute syncs.** At 14:00Z on 2026-09-09, right after a deploy, seven short jobs
(gmail, contacts, plaid, drive, `pipeline_health`, slack coverage, whatsapp) started
together and hung in their step subprocess without logging a line for 3.5 hours, until the
next deploy cancelled them. `max_concurrent_runs` was 8, so the five-minute Slack freshness
and timeline syncs were created on schedule and then sat `QUEUED` for up to 46 minutes each
(create 14:20, start 14:58) — a DM landing-latency spike with every Slack health number
green, and the second such spike that day after the change-feed episode above. Two things
now hold it: the frequent jobs carry `dagster/max_runtime` tags well under the global
four-hour run-monitoring cap (`tests/test_dagster_job_runtime_caps.py`; the cap itself
stays for the WhatsApp windows and the multi-hour user sync), and the queue allows 12
concurrent runs on a 28-core host at load 2. Diagnose this shape from the Dagster
Postgres, not from `marts_ops.*`: `runs.create_timestamp` far behind `to_timestamp(start_time)`
is a starved queue, and a run whose event log stops at `STEP_WORKER_STARTED` is a hung
step, not a slow one.

Coolify management tooling lives in the `sysadmin` repo at `~/dev/zachlatta/sysadmin`:

- On `crobat` you can obtain a Coolify API key from that repo to drive the Coolify API. See its
  `README.md` and the `rotom/` notes folder for details. `GET $COOLIFY_URL/api/v1/applications`
  reports each app's `destination.server.name`, which is the authoritative answer to "which
  host is this on?"
- The same repo holds the Loki log wrapper used to read production logs — see the
  [Production Logs](#production-logs) section below.

To investigate the production Dagster deployment directly, connect to its Postgres. The
production Dagster Postgres URL is **not** present in this worktree: `.env` is gitignored and
only exists in the parent (non-worktree) checkout. Read `PROD_DAGSTER_URL` from the parent
repo's env file at `~/dev/zachlatta/personal-data-warehouse/.env`. In production, Dagster reads
the same connection string from `DAGSTER_POSTGRES_URL` (see `docker/dagster.yaml`).

## Production Logs

Production runs as a Coolify app on the `mew-coolify` server (managed by the
Coolify instance on `rotom`). The best way to read
its logs is the Loki wrapper in the `sysadmin` repo:
`~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs`.

That script talks to Loki over Tailscale, so it only works from a machine on
the tailnet. Zach's dev machines `crobat` and `porygon` are both on the tailnet
and have access. Before assuming you can use it, confirm you are actually on
`crobat` or `porygon` by running `hostname` (or `scutil --get LocalHostName`)
and checking the output equals one of those. If you are anywhere else, stop and
ask Zach instead of guessing.

Once you have confirmed you are on `crobat` or `porygon`, useful starting points:

```bash
# Recent app/container logs for the PDW deployments (they live on mew-coolify).
~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs \
  --format-logs --since 1h '{job="coolify",server="mew-coolify"}'

# Filter to a specific container by resource UUID (see warning below).
~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs \
  --format-logs --since 1h \
  '{job="coolify",server="mew-coolify"} | json | container_name =~ "(?i).*<resource-uuid>.*"'

# Host-level system logs. Match both hosts when you are not sure which one served a request.
~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs \
  --format-logs --since 1h '{job="machine",server=~"rotom|mew-coolify"}'
```

Logs predating the 2026-08-19/21 migration are under `server="rotom"`, so widen the
selector to `server=~"rotom|mew-coolify"` for any window that spans it.

### Pin to the right deployment before reading logs

The Coolify fleet hosts many apps, several with confusingly similar
names. A loose name filter like `container_name =~ ".*dagster.*"` can silently
match more than one and return logs for the wrong app. Don't filter by guessed
names — first ask the Coolify API for the deployment's exact resource UUID, then
filter on that. Coolify names each container `<resource-uuid>-<deploy-timestamp>`,
so the UUID is an unambiguous key.

The Coolify API URL and key live in the `sysadmin` repo's gitignored `.env`
(`~/dev/zachlatta/sysadmin/.env`) as `COOLIFY_URL` and `COOLIFY_API_KEY`:

```bash
set -a && source ~/dev/zachlatta/sysadmin/.env && set +a
curl -fsS -H "Authorization: Bearer $COOLIFY_API_KEY" \
  "$COOLIFY_URL/api/v1/applications" \
  | jq -r '.[] | "\(.uuid)\t\(.name)\t\(.fqdn // "-")"'
```

Find the UUID for the exact app name you want, plug it into the
`container_name` filter above, then sanity-check the output: every line's
`coolify[...]` tag should share one `<resource-uuid>-<deploy-timestamp>` prefix.
More than one prefix means the filter is still too broad.

See `~/dev/zachlatta/sysadmin/README.md` and the script's `--help` for the
full set of selectors and flags.

