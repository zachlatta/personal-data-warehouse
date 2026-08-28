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

## The eleven contracts

Everything after this section is detail. These eleven are what the warehouse *is*, and
future work — human or agent — is expected to honor them rather than route around them.
Each one names what holds it up, because an unenforced contract is one refactor away from
quietly becoming untrue, and several of these have been.

- **C1 — everything synced eventually lands on `timeline.events`.** One row per real-world
  event, from every source, with `source_table` + `source_pk` drilling back to the
  authoritative row. *Held up by* `TIMELINE_TABLE_COVERAGE`: every warehouse table must
  declare itself as `events`, `detail`, `entity`, or `state`, and `tests/test_timeline.py`
  checks that against the **live** schema, so a new table that skips the registry fails the
  suite. *Gap:* nothing forces a source's event table to actually have an adapter — the
  classification is a human's word.
- **C2 — the timeline has five priority tiers and everything is properly categorized.**
  `self`, `direct`, `cc`, `noise`, `background`, in that attention order, stored in the
  `timeline.timeline_priority` enum. *Held up by* the enum itself (an invalid label is a
  Postgres error), required per-adapter priority expressions, runtime validation that rejects
  NULL/unknown outputs before a batch is written, and the classification tests. Failures are
  recorded in `marts_ops.timeline_adapter_health`. *Gap:* which valid tier an adapter assigns
  is a judgement no test can make for you. See [Timeline priority tiers](#timeline-priority-tiers).
- **C3 — agents start at the timeline and can filter by priority.** The `search` tool or
  `timeline.events`/`timeline.search_text()` in SQL, then one hop out to the source row.
  *Held up by* the catalog's `START HERE` guidance being published as real schema comments
  (`test_schema_comments_publish_the_start_here_guidance`) so discovery cannot disagree with
  the docs — and, since 2026-08-27, **measured**: `marts_ops.agent_usage` (daily
  `agent_usage` asset over `marts_ai_conversations.events`) reports, per agent source, the
  share of PDW sessions whose first call was a search, the share of searches carrying a
  priority filter, the share of SQL that went straight to `base_*`, and the SQL-error
  session rate, judged against targets (search-first ≥ 60%, priority filter ≥ 40%,
  error sessions < 10%). The baseline on 2026-08-26 was 27% / 6% / 35% / 27%. The
  `search` tool hints when an unscoped result came back mostly noise, and the query tool
  hints before running a raw-table text scan; this row is how we know whether that lands.
- **C4 — raw source data for every source is queryable via SQL.** `base_<source>` is a
  faithful copy, discoverable, and readable by the read-only `pdw_query` role. The timeline
  is the recommended entry point, never the only truth. *Held up by*
  `test_query_role_reads_public_relations_and_is_denied_private` and the catalog's
  query-access policy per layer.
- **C5 — multi-source concepts layer `base_* → derived_*/marts_* → timeline`; consumers read
  the other way, `timeline → marts_* → base_*`.** Raw rows never learn about identity;
  identity and enrichment live in `derived_*`; `marts_*` is the stable read interface.
  **An intelligent transformation must also READ from the intermediate layer, not from one
  source's raw table** — enrichment, transcription, extraction and fingerprinting take their
  candidates from a `marts_*`/`derived_*` relation so a second source is covered the day it
  lands. Saying only where the *output* lives is what let
  `base_alice_voice_recordings` sit at 53 recordings with 0 transcripts while every enforced
  registry passed: the transcription pass scanned `base_apple_voice_memos.files` by name.
  Identity-layer scanners that genuinely must read raw are the documented exception.
  *Held up by* `tests/test_schema_reorg_contract.py` (layer/schema-name consistency, the
  catalog as the only editable authority, no pre-reorg name anywhere in the source **or the
  docs**) and, for the input half, `tests/test_repo_contracts.py`
  (`test_no_enrichment_runner_reads_a_raw_source_table_unaccounted_for`): every enrichment
  runner's raw reads must be listed with a reason, a stale exemption fails, and a newly
  added `*_enrichment.py` / `*_transcription.py` / `*_extraction.py` module that is not
  registered fails too.
- **C6 — PDW responds fast, and saturates the host before anyone optimizes further.**
  *Held up by* nothing automatic. It is the contract that has cost the most: see
  [Performance contract](#performance-contract).
- **C7 — pipeline health for every source, mart, and timeline data type is inspectable via
  SQL and web.** Four levels, at parity in SQL and on `/pipelines`: pipelines
  (`marts_ops.pipeline_health` / `marts_ops.table_freshness`), the marts read interface
  (`marts_ops.mart_view_health`), per-adapter timeline currency
  (`marts_ops.timeline_adapter_health`), and index integrity
  (`marts_ops.collation_health`). *Held up by* `PIPELINES` + `TABLE_PIPELINES` and
  `tests/test_pipeline_health.py`, which asserts the freshness registry and the timeline
  registry cover *exactly* the same tables, and that every catalogued mart gets a health row.

- **C8 — search is one hybrid path over the timeline, and its quality is measured.** The
  `search` tool fuses BM25, pgvector ANN and a gated literal leg by reciprocal rank; the
  semantic corpus converges on `timeline.events.seq`, so embeddings track the timeline rather
  than a schedule. *Held up by* `marts_ops.search_health` (chunk/embedding lag as data, not a
  flag), the source-token and pool-partition tests in `tests/test_repo_contracts.py`, and the
  labeled benchmark in `src/personal_data_warehouse/search_benchmark.py`
  (`docs/search-benchmark.md`), and — since 2026-08-27 — the weekly `search_benchmark`
  Dagster asset, which runs the same measurement through the app's own `search` tool and
  writes p50/p90 latency over fixed term-bag probes plus MRR / hit@k over the labeled cases
  into `marts_ops.search_benchmark` (`attention` when p50 > 2s or MRR < 0.30; `unknown`
  after ten days), rendered on `/pipelines`. The labels now live in
  `private.search_benchmark_labels`, loaded with
  `uv run python -m personal_data_warehouse.search_benchmark publish-labels` and exported
  with `pull-labels`, so a lost gitignored directory can no longer make retrieval quality
  unmeasurable — which it did once, in 2026-08. First run, 2026-08-27 04:44Z: **MRR 0.434
  over 68 labeled cases** (hit@1 24, hit@5 36, hit@10 44, found 49), latency unmeasured
  because every unscoped probe timed out on a cold HNSW cache — and the row said so, rather
  than reporting zero. Second run, 12:02Z the same day, after all four BM25 indexes were
  rebuilt and the HNSW index prewarmed: **MRR 0.450** (hit@1 24, hit@5 37, hit@10 48, found
  54), unscoped hybrid **p50 2.09s / p90 3.54s / max 3.59s** — `attention` by 90ms against
  the 2s goal, which is the honest distance left.
- **C9 — one obvious way to do a thing, on both surfaces.** There is exactly one hybrid
  search, one SQL entry point per surface, one schema-discovery call. *Held up by*
  `tool.Surface` splitting MCP-only and CLI-only tools, the `runCall` fence that refuses
  `pdw call sql|query|search|schema_overview|describe_table` with a redirect, and the usage
  drift tests in `app/cmd/pdw-cli/usage_test.go`, which derive the command list from the
  dispatcher rather than a hand-kept list.
- **C10 — the database is backed up, and a restore has actually been performed.** pgBackRest
  ships WAL continuously to an encrypted S3 repository and takes periodic fulls. *Held up by*
  `tests/test_pgbackrest_image.py` for the settings that make it possible, and by nothing at
  all for the part that matters: whether a backup EXISTS. On 2026-08-26 the stanza reported
  `status: error (no valid backups)` and had for a day, while WAL archiving kept working and
  every PDW health surface stayed green, because backups appear in none of them. A backup you
  have not restored is a hypothesis; the restore drill is the test.
- **C11 — a source's own SLA is stated and detected, not inferred from the pipeline being
  green.** A feed can be healthy in aggregate while the part that matters is frozen. *Held up
  by* per-source detectors where they exist — `marts_ops.slack_conversation_health` judges the
  SHARE of conversations re-listed per type, because `marts_ops.pipeline_health` rolls Slack up
  as one pipeline and ~19k public-channel messages a day kept it `ok` straight through a total
  group-DM outage — and, since 2026-08-27, the share of live public channels actually
  re-READ (`history_polled_fraction`), because being listed is not being read: 11,488
  public channels Zach is not in sat frozen at their backfill behind a 99.2% discovery
  number while PDW held 40% of the month's public-channel messages; `marts_ops.plaid_item_health` does the same per institution, because a
  re-link that mints a second live Item double-counts net worth while the pipeline stays
  green; `marts_finance.net_worth.staleness` judges each manual valuation against its own
  kind's refresh. *Gap:* every other source rides aggregate freshness. Latency, as opposed
  to freshness, is still unmeasured almost everywhere.

Adding a source touches all eleven. The step-by-step list, marked by which steps a test
catches and which fail silently, is [Adding a warehouse source](#adding-a-warehouse-source).

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
with a newsletter. The enum's declaration order **is** the sort order, most attention first:

| tier | what it means | typical rows |
| --- | --- | --- |
| `self` | Zach initiated it | his sent mail and messages, his notes and voice memos, his agent sessions **and the turns he typed into them**, his own calendar events, his card purchases and payments |
| `direct` | a real person reaching him directly | DMs, email addressed to him, small group threads, big group chats **for the week he takes part in them**, a real `<@id>` ping, replies in a thread of his that is a conversation rather than an announcement |
| `cc` | real-people activity he is peripheral to | cc'd mail, private team channels he sits in, big group chats he is not taking part in that week, replies piling under his `<!channel>` broadcasts, people talking *about* him in public, others editing a file he owns |
| `noise` | bulk or automated traffic | newsletters, notifications, bots, Slackbot file posts, GitHub/CI relays, Gmail's auto-created calendar events, his own health telemetry, and public-channel chatter not aimed at him — **member or not** |
| `background` | the warehouse's own machinery and other people's background work | enrichment runs, mutation workers, the model's turns in agent sessions, orchestrator-spawned agent sessions, Drive files other people change |

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

## Timeline search and hybrid retrieval

Text search runs through three timeline functions plus one app tool:

- `timeline.search_text(query, max_results, sources, since, priorities)` — ranked BM25. Hits carry
  `event_ts` (mirror of `occurred_at`), `title`, `source_table`, `source_pk` for one-hop
  drill-down; the `text` preview is windowed around the first matched term; `sources`
  accepts familiar aliases (`apple_messages`, `voice_memos`, `drive`, ...); a partially
  failed fan-out WARNs and a fully failed one raises — an empty result is never a broken
  search layer in disguise.
- `timeline.search_text_exact(...)` — literal substring, recency-ordered, and it also
  matches number-format variants of the needle (thousands separators both ways, phone
  punctuation stripped).
- `timeline.context(ref, before, after)` — the **conversation** around a hit, chosen per
  source: a Gmail hit returns its thread, a Slack hit returns its thread when it is in one
  and otherwise the messages around it in its channel, an iMessage/WhatsApp hit returns the
  rest of that chat, and an agent-session turn returns the neighbouring turns of the same
  session. Everything else returns the neighbouring events of the hit's `(source, context)`
  stream. See [Conversation context](#conversation-context-what-timelinecontext-returns).

The named parameters are exactly `max_results`, `sources`, `since` and `priorities`; anything
else is an invented name and raises. `priorities` is the attention filter documented under
[Timeline priority tiers](#timeline-priority-tiers) — `priorities => ARRAY['self','direct']`
in SQL, `--priority self,direct` on the CLI, `"priorities": [...]` on the tool.

### Conversation context: what `timeline.context()` returns

A hit is one line. What makes it readable is the conversation it was said in — and for the
sources people actually ask about, that is **not** the rows sharing its `context` column.
`context` is a DISPLAY label, and two of them are actively misleading:

| source | what `context` stores | what the generic walk returned |
| --- | --- | --- |
| `gmail` | the mailbox account | 1,187 emails across 472 threads in one account-week (2026-08-27), so a 15-a-side window was thirty strangers and never the reply |
| `slack` (mpim) | the literal string `group DM` | 65 distinct group DMs and 635 events interleaved over 30 days |
| `slack` (channel) | `#name` | the channel, but a threaded reply read as bare channel chatter |

So a conversational source resolves its neighbours in the **source table**, whose indexes
already express the real conversation, and joins the resolved ids back to `timeline.events`
by its primary key:

| adapter | the conversation | index that serves it |
| --- | --- | --- |
| `gmail_email` | the thread | `base_gmail.messages (account, thread_id, internal_date DESC)` |
| `slack_message` | the thread when Slack says the row is a reply or a parent with replies, else the conversation | `base_slack.messages (account, team_id, conversation_id, thread_ts)` / `(…, message_datetime DESC)` |
| `apple_message` | the chat | `base_apple_messages.chat_messages (account, chat_id, message_date DESC)` |
| `whatsapp_message` | the chat | `base_whatsapp.messages (account, chat_id, message_at DESC)` |

Every other adapter keeps the generic `(source, context)` walk, which is the right answer
where `context` IS the identity (an agent session's `<source>|<session_id>`, a calendar id,
a folder path) or where the events are not a conversation at all (photos, health, finance).
Which adapters do is **declared**, in `TIMELINE_CONTEXT_STREAMS` /
`TIMELINE_CONTEXT_GENERIC_ADAPTERS`, never inferred — `test_every_adapter_declares_how_its_conversation_is_read`
fails a new adapter that made no decision, the same hole `TIMELINE_TABLE_COVERAGE` closes
for C1.

Four things here are load-bearing and each was paid for:

- **Nothing about `timeline.events` changed.** Not the stored `context`, so no
  `adapter_signature` change and no 46.8M-row Slack re-walk; and no new index on the 45 GB
  heap, because the source tables already had the right ones.
- **The anchor is a plpgsql record, not a joined CTE.** Putting it in a `WITH` makes it
  referenced twice, so Postgres materializes it and every ordering bound becomes a join
  qual against an opaque CTE scan instead of an index qual. Measured 2026-08-27 on the
  busiest Slack channel: each leg alone ran 183ms (one reference, so the CTE inlined) and
  the two together **timed out past the 60s budget**. As a record it is 162-295ms for 31
  rows; the Gmail thread walk is 165-212ms.
- **The walk goes outward from the anchor in both directions**, rather than materializing
  the conversation and slicing it — production Gmail threads run past 60 messages and a
  Slack channel past a million.
- **A stream that resolves nothing falls through to the generic walk**, never to an empty
  transcript. An iMessage with no chat row, or a source row deleted out from under its
  timeline event, gets neighbours-in-time rather than silence.

**Known gap:** the stored Slack `context` is still `group DM` for every mpim, and that
column also keys the semantic chunk windows in `derived_search.chunks`, so all 65 group DMs
are still embedded as one hourly conversation. Fixing it is an adapter edit that re-walks
every Slack row — batch it with the next one, and read
[Timeline priority tiers](#timeline-priority-tiers) for the WAL/vacuum playbook first.

A **broad** (unscoped) `search_text` call does not fan out per source: it pools candidates
from two index-ordered BM25 scans — the global index for the high-volume adapters, a partial
index (`timeline_events_search_text_bm25_lowvol_idx`) for the low-volume tail — and applies
the per-source floor to that pool. The old per-source fan-out ran eighteen branches serially
in one plpgsql loop, so its wall clock was the SUM of every branch (6.9s warm, 21.7s cold on
the production corpus) while one index-ordered scan of the same index is tens of
milliseconds. A **scoped** call (`sources => ARRAY[...]`) still runs the per-source
branches. Several things there are load-bearing and easy to undo by accident.

**The pool carries the scan's ORDINAL, not a score.** Each partition is
`ORDER BY <bm25 operator> LIMIT n`, so its rows already emerge in exact relevance order and
the ordinal IS the rank; naming the operator in the pool's SELECT list as well re-tokenizes
every pooled document to recover a number the ordering already encoded. Because the two
partitions split by adapter and every source's adapters live entirely in one of them, the
per-source floor needs no score at all — only the cross-partition fill does, and the top-k
of two score-sorted lists is contained in each list's own first k, so at most a couple of
hundred candidates are ever scored instead of ~5,800. Measured on production 2026-08-26,
end to end through the two-statement shape the function actually uses: a broad pool went
575ms → 85ms warm and 1.1-2.2s → 0.3-1.2s cold, and with `priorities => ARRAY['self']`,
where every surviving document is one of Zach's own large ones, 11.5s → 0.47s warm and
13.6-37.3s → 1.5-7.6s cold (p50 2.4s). The score *vectors* of the top 50 are
byte-identical before and after on every query tested; only ties reorder.
**Measure this the way the function does it** — one `array_agg` statement — because
`SELECT count(*) FROM (…)` lets the planner delete the unused score expression, which made
an early measurement of this read the whole cost as free.

The ordinal is honest where `bm25_get_current_score()` is not: it is assigned
after an explicit ORDER BY, so it is right on the seq-scan-and-sort plan a small or new
table gets, which is exactly where that helper returns a garbage constant. It stays banned
(`test_search_text_scores_with_the_bm25_operator_not_the_scan_helper`).

The pool pins `enable_sort = off` for the scans (the planner has no cost model for the bm25
operator and otherwise re-scores every row of a selective adapter filter, ~5.6ms per
document) — but **only** for the scans: the pool is collected into arrays in its own
statement and the hint is restored before the ranking runs, because leaving it over the
whole plan left the planner no sane way to feed the window function and one query then ran
for five MINUTES. The only window allowed under the hint is the bare `row_number() OVER ()`
that captures the ordinal, which needs no sort.
The low-volume partition needs its OWN index — scanning those adapters through the global
index walks past millions of gmail/slack documents and took 15-16s on an unlucky query.
And the pool depth is a measured trade (`SEARCH_TEXT_BROAD_POOL`): deeper gives the
per-source floor more to promote, up to a point where latency grows and scores do not.

**An attention-scoped broad call reads its own pair of indexes, and the literal tier
predicate beside them is load-bearing.** `priorities => ARRAY['self']` is the query C3
exists for, and it was the slowest thing in the layer: `self` is 1.01% of 49M rows and
`self` + `direct` is 2.72%, so filling a 5,000-row pool from the global index walks ~500k
score-ordered documents and pays a *random heap visit* on each one to read the tier —
measured 2026-08-26 on novel queries, cold 15-20s, and through the app's multi-leg hybrid
past the 60s statement ceiling twice. When every requested tier is one the partial
`..._bm25_attention_idx` / `..._bm25_attention_lowvol_idx` pair contains, the same
two-partition pool is taken from them instead. Three things there are easy to get wrong:

- **The subset test is the whole safety argument.** Those indexes hold only `self` and
  `direct`, so a call for `noise` (82% of the corpus), for `cc`, or with no filter at all
  must fall back to the general pair. Serving it from the attention pair would return
  *silently empty*, which is the worst failure this layer has.
- **The scan repeats `priority IN ('self','direct')` as a LITERAL**, beside the runtime
  `priorities` filter that does the actual selecting. A partial index is only usable when
  the planner can prove the query implies its predicate, and `priorities` is a runtime
  array it can prove nothing about. Drop the literal and the planner picks the global index
  — and vchord-bm25 then RAISES, because `to_bm25query()` pins an index by name and checks.
- **The big attention index deliberately carries no adapter list.** Its predicate is the
  tier alone and the high-volume partition adds `adapter NOT IN (...)` as an ordinary
  filter, because a predicate derived from the adapter registry needs
  `rebuild_on_definition_change`, and that rebuild is a non-concurrent DROP+CREATE — minutes
  of exclusive lock on the 45 GB timeline heap. The low-volume attention index must carry
  the list to be the low-volume partition at all, and at 90k documents it rebuilds in 59s.

The pair cost 14 MB + 533 MB against the global index's 10.2 GB and took 59s + 4m35s to
build `CONCURRENTLY` on production (1,333,278 documents). Measured there the same day on
twelve novel term-bag queries per tier, **alternating which implementation ran first** so
neither one gets the other's warmed cache — an unbalanced order is worth 5-10x here and
will tell you whatever you want to hear:

| call | before (p50 / p90) | after (p50 / p90) |
| --- | --- | --- |
| `priorities => ARRAY['self']` cold | 13.7s / 24.2s | **2.3s / 8.7s** |
| `priorities => ARRAY['self']` warm | 10.7s / 20.8s | **1.7s / 1.9s** |
| `priorities => ARRAY['self','direct']` cold | 8.7s / 15.4s | **1.1s / 1.5s** |
| `priorities => ARRAY['self','direct']` warm | 7.1s / 16.5s | **1.0s / 1.4s** |
| unscoped (must not regress) cold | 1.7s / 3.3s | 2.0s / 3.7s |
| unscoped warm | 0.19s / 0.79s | 0.21s / 0.70s |
| `ARRAY['noise']` (must not regress) cold | 1.8s / 2.5s | 2.0s / 2.9s |
| `ARRAY['noise']` warm | 1.9s / 2.3s | 2.0s / 2.2s |

The unscoped and `noise` rows run byte-identical SQL before and after — verified by
comparing `(ref, score)` for the whole top 50, which matched 50/50 on both — so their
difference is measurement noise, and it is the honest scale for reading the tier rows.

**The attention path returns a DIFFERENT ranking, and that is not a bug to fix.** On three
common-term queries the top 50 overlapped the old path's by 30, 25 and 34 of 50, with an
identical source mix (same twelve sources, same per-source counts) — so the divergence is
*within* each source. It is the IDF: BM25 statistics come from the index being scanned, and
an index over `self` + `direct` has different term frequencies than one over all 49M rows.
That is textbook — the collection you compute IDF over should be the collection you retrieve
from — and it is the same property the low-volume partition has had all along. Scored back
on the global scale the new top 50 averages -12.7 against the old -13.5 with an identical
best hit, and re-scoring the attention pool globally instead did **not** restore the old
ranking (31/25/35 overlap), which is what proves the shift comes from the deeper candidate
pool and the sub-corpus statistics rather than from the score column. There is currently no
labelled benchmark to put an MRR on this (`.search-eval/ground_truth.json` is gone), so it
is stated rather than quantified.

A **scoped** call needs none of this and does not get it: its branch limit is `max_results`,
not the pool depth, so it stops after ~20 matching rows rather than 5,000. Measured the same
day, `sources => ARRAY['gmail'], priorities => ARRAY['self']` was 275ms before the index
existed. The depth of the pool, not the tier filter, is what made the broad path expensive
— and that is also why a *rare* term never showed the problem: for a query only a few
thousand documents match, the global scan exhausts the postings before the tier filter can
make it walk. Benchmark this with common words, or you will measure nothing.

**A scoped search still pays the operator per returned row, and for Google Drive that is
the whole cost.** Measured 2026-08-26, `sources => ARRAY['google_drive']` at depth 50 takes
5.1-8.6s, and the decomposition is 161ms to find the rows, 441ms to window their previews,
and **2.7s to score them** — ~53ms per multi-megabyte Drive document. Moving the score off
the discarded rows cannot help there, because a scoped single-source search discards
nothing: the 50 rows it scores are the 50 it returns. Lowering the depth or scoping by
`since` is the lever an agent has today.

**Fusion weights are conditional on query shape, and they were measured offline.**
`scripts/search_fusion_lab.py collect` gathers each hybrid leg's raw evidence once per
labeled query (the same SQL helpers the app calls, the same four query embeddings) and
`score` re-fuses it under a weight grid in seconds, so a fusion change is measured before
it is written into `search_hybrid_fuse`. Measured 2026-08-26 on 67 scored queries: four
ANN legs return hundreds of candidates each, and at the old 1.5 semantic weight semantic
ranks 1-16 outvoted a correct BM25 #1 on every term bag (keyword alone hit@1 7/20, hybrid
2/20). Now semantic weight 1.0, literal 3.0, and for a query that is NOT sentence-shaped
(the app's own function-word test, repeated in SQL) the BM25 head, ranks 1-5, counts
double: MRR 0.339 -> 0.394, hit@1 15 -> 20, hit@10 40 -> 44, found@50 unchanged at 51.
Flat lexical 2.0 / semantic 0.5 scored MRR 0.400 but lost seven found@50 -- a head
bonus, not a flat weight, is what keeps semantic recall. Re-run the lab before touching
`SEARCH_HYBRID_*_WEIGHT`. Confirmed end to end through the deployed `search` tool on
2026-08-27, same 57 queries before and after: hybrid MRR 0.408 -> 0.489, hit@1 18 -> 24,
hit@10 33 -> 37, found@50 44 -> 43 (the one loss a statement timeout), while keyword mode --
the control, untouched by the change -- moved 0.362 -> 0.352. Term bags gained most
("acon 20k mini magazines order" 33 -> 1, "mixam magazine order quote" 17 -> 3); the
regressions are long term bags where BM25's head was wrong ("DDP duties customs prepaid
shipment AGH Fulfillment" 21 -> 40).

**A Drive file id finds the file itself.** The `drive_file` search document carries
`file_id` (since 2026-08-26), because an agent holding an id from a URL or an email
searched for it verbatim and got only the emails that mentioned the file -- the id had
lived solely in `event_id`/`source_pk`, which no search reads.

**The label file rots with the adapters, and the harness now says so instead of scoring
it.** On 2026-08-26 the voice-memo adapter's event ids changed shape (`apple_voice_memos|`
prefix, from the mart rewrite) and twelve labels went stale in one day; scored as misses
they read as a 0.15 MRR regression. `search_benchmark run` now sets aside a case whose every
ref has left the timeline and lists it under "NOT SCORED"; refs are still stamped as
`adapter:event_id`, so re-resolve them after any adapter id change. The benchmark set is
73 cases as of 2026-08-26, 28 of them the exact queries live agents issued that week
(`origin: live-agent-2026-08-26`), kept off-repo at `~/.config/pdw/search-eval/`.

**Phrase a search as the words the answering record would contain, not as the question.**
Sentence-shaped queries score MRR 0.27 on the labeled benchmark where term-bag queries score
0.53, and rewording the nine questions that returned nothing useful — "how long our money
lasts at the current pace of expenses" → "runway burn rate months of cash remaining" —
recovered five of them, from nothing in the top 50 to ranks 10, 10, 12, 15 and 48. The
`search` tool says this in its description and attaches a `hint` to any sentence-shaped
query, because the caller is itself a model and can act on it; that is why query rewriting
is guidance here rather than another model in the search path.

- The app's `search` tool — hybrid retrieval over **three** legs fused by reciprocal rank:
  BM25, pgvector ANN (one leg per query representation, see below), and — for a query of
  at most `SEARCH_HYBRID_EXACT_MAX_WORDS` words — literal substring. The literal leg is
  what makes identifier-shaped questions work ("admin/api-keys", a Drive file id, a
  person's name), where BM25 tokenization and embeddings both fail: adding it took the
  labeled benchmark from MRR 0.292 to 0.403 and answered three queries that previously
  had nothing in the top 50. It stays gated because ungated it scored *worse*. Machine
  tokens (digits or identifier punctuation) search bounded plain-document retrieval chunks
  through `search_chunks_text_trgm_idx`, not multi-megabyte timeline documents; symbolic
  tokens rank an earlier chunk occurrence ahead of a late mention, while opaque ids containing
  digits preserve recency. Conversation windows retain full-document exact matching because a
  window's `event_id` is its last member, not necessarily the member containing the literal;
  the full path runs only when the chunk index first confirms a matching chat window. Ordinary
  alphabetic names also retain full-document exact matching. Hybrid falls
  back to keyword with an explicit `fallback_reason` when embeddings or pgvector are
  unavailable. Agent sessions are indexed per turn (`kind = 'agent_turn'`); the session
  roll-up row carries headline fields only.

The semantic layer (`src/personal_data_warehouse/search_index.py`): `derived_search.chunks`
is derived from `timeline.events` by the `search_chunks` asset (cursor =
`timeline.events.seq`, so it converges whenever the timeline does — timeline sync also
resets a source adapter's backfill whenever that adapter's SQL changes, via
`adapter_signature` in `ops.timeline_sync_state`). Chat sources chunk as per-(context,
hour) conversation windows; other sources chunk per event with big documents split.
`derived_search.chunk_embeddings` holds one 512-dim halfvec per distinct chunk text per
model (content-sha keyed), filled by the `search_chunk_embeddings` asset through any
OpenAI-compatible `/v1/embeddings` endpoint (`SEARCH_EMBEDDINGS_BASE_URL` / `_API_KEY` /
`_MODEL` / `_DIMENSIONS` on the Dagster AND app deployments). Unconfigured or
pre-pgvector hosts skip loudly, never red.

**The production embedding server runs on `mew` (the GPU box, `ssh mew`) and is managed
by Coolify** as the `personal-data-warehouse-embeddings` application. Coolify targets the
physical `mew` server directly; the GPU is deliberately not passed through to the
`mew-coolify` VM. Its Git-backed deployment definition is
`embeddings/docker-compose.yaml`, the HF cache persists at
`/opt/pdw-embeddings/hf-cache`, and it serves `Qwen/Qwen3-Embedding-4B` on the RTX 3080 Ti,
bound to the tailnet at `http://100.104.110.27:8485/v1`. 4B was chosen
over 0.6B off the 2026 MTEB standings (multilingual 69.45 vs 64.33; the 8B leader does
not fit 12 GB) — the family tops open self-hosted models and the GPU is otherwise idle.
Queries (the app's Go client only, never the Python document indexer) are wrapped in the
instruction prefix from `SEARCH_EMBEDDINGS_QUERY_PREFIX`, per Qwen3-Embedding's
instruction-asymmetric training. The app embeds the instructed **and** raw query in one
batched request; sentence-shaped queries also get instructed and raw deterministic
content-word forms in that request. BM25, literal, and one ANN leg per vector run concurrently
on separate pooled Postgres connections, then `timeline.search_hybrid_fuse` combines their
compact evidence; `timeline.search_hybrid` is the compatible direct-SQL wrapper over the same
helpers. Separate legs, not a blend: the instructed and raw forms land in different
neighbourhoods and each retrieves answers the other misses, so averaging them into one vector
averages the difference away — measured on the labeled benchmark, blending scored MRR 0.234
where two legs scored 0.300. The content-word forms raised the expanded live-agent benchmark
from MRR 0.305 to 0.321 and hit@1 from 7 to 8 without reducing hit@5, hit@10, or found@50. Their
ANN legs deliberately use only `max(200, 2 * max_results)` candidates; repeating the original
vectors' 1,000-row floor made warm searches slower, while the 200-row floor preserved every
headline quality metric. In an in-container pooled A/B over eight queries twice, the parallel
path kept median latency flat (1.108s → 1.102s) while cutting mean 2.10s → 1.25s, p90
3.79s → 1.82s, and max 6.40s → 3.01s. The instruction
*text* matters as much as its presence (0.240 vs 0.300 for two wordings of the same task),
so re-measure with `search_benchmark` before changing it. Write the instruction's newline as
the two characters `\n`, which the app decodes: Coolify truncates an environment value at a
real newline, and production silently ran with the instruction's second half missing. `SEARCH_EMBEDDINGS_QUERY_RAW_WEIGHT`
is removed; the app logs a warning if it is still set. None of this changes document
embeddings. Find and inspect the live Coolify-managed container with
`ssh mew 'docker ps --filter label=coolify.resourceName=personal-data-warehouse-embeddings'`;
deploy changes through Coolify rather than running a replacement container by hand. The
compose definition pins `--auto-truncate` (required: the model's
32k context exceeds TEI's default batch limit) **and `--max-client-batch-size 256`**
(the indexer posts 128-text batches; TEI's default cap of 32 makes them 413 —
this exact omission broke a relaunch once already).
Wider-than-512 vectors are MRL-truncated + renormalized client-side on both the Python
and Go sides, so the server honoring the `dimensions` parameter is optional. pgvector
ships in the warehouse postgres image; a host that predates it degrades (no embedding
column, no HNSW, no `search_hybrid`) until the DB container is rolled onto the current
image.

## Performance contract

PDW is supposed to answer fast, and **before anyone optimizes further, confirm we are
actually using the host.** This is C6, it is enforced by nothing, and it has already cost
three incidents.

**The budget hierarchy, innermost first.** Each layer's budget must sit *below* the next one
out, so the innermost timer fires first and the failure carries a useful message:

| budget | value | where |
| --- | --- | --- |
| Postgres statement timeout, per user query | 60s | `config.QueryTimeout` (`PDW_QUERY_TIMEOUT`), applied as `SET LOCAL statement_timeout` |
| Python read-only runner | 30s | `postgres.py`, `-c statement_timeout=30000` on the read-only connection |
| `pdw sql` client wait | 75s | `defaultSQLTimeout` — deliberately *above* the server budget |
| Public edge cutoff | ~100s | Cloudflare in front of the app; not configurable from here |

The ordering is the whole point. The CLI waits longer than the server so a slow query comes
back as the server's SQL timeout error — which carries a rewrite hint — instead of a
client-side abort that leaves the statement burning server-side and invites a blind retry.
The incident that produced this table was the opposite arrangement: a 10s client wait, a
~100s edge cutoff and a 300s server budget, so every slow query was abandoned by its caller
while the database kept working on it. If you change one of these numbers, change it knowing
which of the others it must stay under.

**Saturate the host before optimizing.** Measured 2026-08-23: a warm identifier search burned
3.9s on **one** core while the 28-vCPU box sat 90-96% idle and `parallel_workers_launched`
came back **0**. An unparallelized plan on an idle 28-vCPU host is not a query that needs a
cleverer algorithm; it is a query that has not been allowed to use the machine. Check
`EXPLAIN (ANALYZE, BUFFERS)` for `Workers Launched`, and check the box's actual utilization,
*before* reaching for a new index, a narrower scan, or a rewrite. The measured wins in the
search layer came from exactly this discipline in reverse — the pooled two-partition BM25
scan replaced eighteen serial per-source branches whose wall clock was the SUM of every
branch (6.9s warm, 21.7s cold) with two index-ordered scans.

**A documented performance number that silently regressed is itself a bug, so re-measure
before you quote one — and prefer the living number.** Since 2026-08-27 the weekly
`search_benchmark` asset writes p50/p90 hybrid latency and labeled MRR into
`marts_ops.search_benchmark` (on `/pipelines`), and `scripts/contract_audit.py` times three
novel searches on demand; the tables below are the history of how each fix was measured,
not a promise the current deployment keeps. Measured 2026-08-26 through the CLI on the
audit day, before the host-saturation fixes in this section landed, hybrid search ran
4.7-13.9s (median ~9.5s) with a 28-core VM at load 19.7 and 29% iowait — the point of the
benchmark row is that the next such regression is a red row, not a doc line. This section claimed the pooled scan returned the global top 200 in
36ms until 2026-08-26, when it was actually 2.9s — the pool had grown a per-row bm25 score
in its SELECT list, which re-tokenizes every pooled document. Re-measured 2026-08-26 on the
production corpus, novel queries every time (a repeated query is ~3x faster and will tell
you what you want to hear), and through the two-statement shape the function really uses:

| path | before | after |
| --- | --- | --- |
| broad `search_text`, warm | 0.58-0.93s | 0.08-0.17s |
| broad `search_text`, cold | 1.1-2.2s | 0.3-1.2s |
| `priorities => ARRAY['self']`, warm | 9.3-12.1s | 0.45-0.58s |
| `priorities => ARRAY['self']`, cold | 13.6-37.3s | 1.5-7.6s (p50 2.4s) |
| scoped `sources => ARRAY['google_drive']` | 5.1-8.6s | unchanged — see the search section |
| `search_text_exact`, novel needle | 1.3-3.7s | unchanged |

**The query tool warns before the timeout, not after.** A statement that pattern-matches
(`ILIKE`, `LIKE`, `~`, `regexp_*`, `position()`) over a raw `base_*` table with no `timeline.`
reference gets a `hint` attached to its result before it runs — that shape was every
statement timeout in 14 days of agent sessions (324 such calls), and the timeout hint
arrived ten seconds too late. The statement still executes; `pdw sql` prints the hint on
stderr so scripted `--output` consumers keep clean rows on stdout.

`search_text_exact` is a different shape and is **not** the same defect: it already launches
six parallel workers and its cost is the 7 GB trigram index plus the ILIKE recheck's heap
I/O. It is not comparable to hybrid's `search_hybrid_exact` leg (0.32s) either — that leg
searches the bounded `derived_search.chunks` documents, not multi-megabyte timeline ones.

**The search working set does not fit in RAM, so what evicts it decides the latency.**
Measured 2026-08-26 on `mew-coolify` (28 vCPU, 26 GB, `shared_buffers` 10 GB): the
HNSW index is 8.5 GB, the global BM25 index 10.7 GB, the chunk heap 7.4 GB, the chunk
embeddings 8.6 GB. `fincore` on the data files found the HNSW index **2% resident** in the
page cache and the BM25 index 34%, and `pg_stat_statements` put the semantic leg at
**93% I/O wait** (2.5s of a 2.7s mean, ~12,400 blocks read per call). The same leg is
100ms when its pages are cached: a broad ANN scan `EXPLAIN (ANALYZE, BUFFERS)`'d cold at
17.3s (14,125 reads) and 0.1s warm on the very next run, and the BM25 pool scan 6.1s
cold / 0.15s warm. Nothing about the queries was wrong; the pages were simply gone every
time. `/proc/pressure/io` read `full avg10=32%` -- the disk was the saturated resource,
not the CPU -- and three jobs were the reason:

| job | reads per run | cadence | what it was doing |
| --- | --- | --- | --- |
| `search_chunk_embeddings` drain | 8.2 GB + 6.7 GB per slab | every 10 min | anti-joined the whole 7.9M-row chunk heap against the embeddings from the newest chunk down, **every run**, because its keyset cursor was a local variable |
| Slack thread backfill (`missing_replies_only`) | 8.0 GB | every ~5 min | walked all 1.2M thread parents to find the ones with no fetched replies -- and found **zero**, the backlog having drained weeks earlier |
| a leaked `pdw_test_*` schema probe | 28 GB | ~17 min, for a day | a test run pointed at the production URL left a full `base_slack` copy behind, and the freshness collector kept `max()`-ing it |

Together that was on the order of 150 GB of reads an hour against a 12 GB page cache.
The fixes are structural, not tuning: the drain now keeps **two persisted cursors** in
`ops.search_chunk_sync_state` (row `embeddings`) -- a `built_at` watermark for freshly
built chunks and a resumable newest-first keyset for the one-time historical walk -- and
both walks are index-only over `search_chunks_built_at_sha_idx` /
`search_chunks_ts_chunk_sha_idx`, bounded to 500k / 250k index rows per run. The thread
backfill remembers a drained walk (`ops.slack_sync_state` row
`thread_backfill_walk/drained`) and checks only the last 30 days, index-bounded, for six
hours before walking everything again. The semantic leg's chunk join reads
`search_chunks_sha_cover_idx` and never touches the chunk heap. **Before optimizing a
search query on this host, run `fincore` on its index files and read
`shared_blk_read_time / total_exec_time` for its statement in `pg_stat_statements`**; a
leg that is 90% I/O wait needs its pages kept, not a better plan. The A/B that says so:
`hnsw.ef_search` 1000 -> 300 at the same 1,000-candidate pool kept a 9.2/10 top-10
overlap and changed warm latency by 4ms, so shrinking the scan buys almost nothing once
the index is resident.

**A BM25 index can be corrupt while `indisvalid` says true, and only a scan finds out.**
The OOM kill on 2026-08-27 (a backend at 6.5 GB anon RSS during a deploy build) took the
cluster through crash recovery and left `..._bm25_lowvol_idx` and `..._bm25_attention_idx`
with bad pages (`invalid page index at block N, SQLSTATE XX001`), plus a half-built
`_ccnew` from a `REINDEX CONCURRENTLY` the crash interrupted. Every low-volume source then
failed keyword AND hybrid search, and no health surface moved: `amcheck` does not cover
pg_textsearch, and nothing read the indexes. `marts_ops.search_health` now carries a
`bm25_indexes` row, written by the chunk builder every five minutes from a one-row scan
through each timeline BM25 index (`probe_bm25_indexes`); a corrupt index is `failing`
with the index name and error in `last_error`. Repair is `REINDEX`; the CONCURRENTLY form
deadlocks readily against the timeline sync's `ShareUpdateExclusiveLock` (retry rather
than diagnose, and drop the `_ccnew` leftover first) and, measured 2026-08-27 on all
three rebuilt indexes, **produces an index about twice the size of a plain build** (lowvol
198 MB vs 96 MB, attention 1.2 GB vs 527 MB, global 11 GB vs 5.5 GB) -- bloat that is
paid straight out of the page cache the search working set needs. The three indexes
rebuilt CONCURRENTLY that night all read `invalid page index` again after the next clean
restart, so a rebuild is not done until it has been **validated cold**: evict it from
`shared_buffers` (`pg_buffercache_evict_relation`), `echo 3 > /proc/sys/vm/drop_caches`
on the guest, then scan it with a multi-term `to_bm25query` (with the partial index's
predicate in the WHERE clause, or the planner picks the global index and the query raises
for an unrelated reason). Every index passing that scan was also the state left behind
for the next restart to test. `search_benchmark.py smoke` is the manual check: one call
per source token, failing sources named.

**What warm actually costs, measured 2026-08-27 after the rebuilds** (serial, depth 10,
eight labeled queries, host quiet): hybrid p50 **3.2s**, p90 5.0s; keyword p50 0.56s.
The same sample taken on the cold cache earlier that night was hybrid p50 26s with six of
eight calls past the 60s budget. The BM25 index is 5.5 GB after a plain rebuild, so
"HNSW and BM25 cannot both be resident" is no longer true as stated: 8.6 + 5.5 GB against
~11 GB of usable page cache plus 8 GB of shared buffers, warm BM25 first and HNSW last.

**Large rewrites are their own budget.** The `timeline.events` priority column change from
`bigint` to the enum failed twice before it worked: the table is 43M rows and the rewrite
drags every index with it. Dropping the indexes first and rebuilding them after brought it
down to ~50 minutes and shrank the table from 63 GB to 45 GB. Assume any `ALTER TYPE` on a
table that size is an outage-shaped operation and plan it as one.

A source-scoped agent-session ANN search uses a deliberately smaller candidate pool
(`4 * max_results`, bounded to 40-200 per vector). Agent-session chunks are only 3.05% of
the global HNSW: asking for 1,000 qualifying rows made one vector leg scan 97,245 embeddings
and take 31.2s, so two legs exceeded the app's 60s statement budget. A 40-row leg took 2.25s;
agent sessions have p95 three chunks per event, so the 4x pool still covers the requested
event depth. Broad and every other scoped search retain the deeper 20x / 1,000-2,000 pool.

Google Drive is the opposite filtered-ANN case: reducing the pool did not fix it. A 40-row
HNSW leg still walked 14,737 global embeddings and took 16.0s. For exactly the Drive scope,
hybrid instead scans all 223k Drive chunks by exact cosine distance, source-first behind an
`OFFSET 0` plan barrier. PostgreSQL launches three workers for that scan; it measured 7.0s
cold and 0.66s warm while returning the full 1,000-row pool, so it is both faster and more
exact than filtered ANN. Fetch chunk text only *after* the top-k: doing it below the sort
detoasts every Drive document. Broad and mixed-source searches still use the global HNSW.

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

**The heartbeat post runs OUTSIDE the pdw CLI, so it needs credentials of its own.**
`pdw_post_heartbeat` shells out to `uv run python -m personal_data_warehouse.uploader_heartbeat`
directly, which inherits none of the URL/token `pdw ingest` resolves for the uploader beside
it. The five Apple wrappers each happened to export `PDW_API_URL`/`PDW_SECRET_TOKEN` from
`~/.config/pdw/config.json` for their own uploaders (they keep `pdw` out of the exec chain to
protect their TCC grants); the two agent-sessions wrappers run *through* `pdw` and so never
did — and their heartbeat therefore failed on **every** run from the day it shipped, ~288
times a day per Mac, into a launchd error log nobody reads. `claude_code`, `codex`, `pi` and
`openclaw` all sat at `last_run_at` NULL, so for those four "the uploader died" and "Zach is
not using this tool" were indistinguishable — the exact gap the heartbeat exists to close,
open on the four pipelines with no other signal. Credential resolution now lives once in
`pdw_export_app_credentials` in `bin/_pdw-upload-lib.sh` and every wrapper inherits it by
sourcing the lib; `tests/test_upload_heartbeat_lib.py` fails if a wrapper hand-rolls the
config read again, or posts a heartbeat without sourcing the lib.

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
| 3 — timeline adapters | `marts_ops.timeline_adapter_health` | is THIS kind of data reaching `timeline.events` |
| 3b — priority tiers | `marts_ops.timeline_priority_mix` | how each source's last seven days split across the five tiers; an `unclassified` row is `failing` |
| 3c — agent usage | `marts_ops.agent_usage` | are agents starting at the timeline and scoping by tier (C3), measured daily from their own sessions |
| 3d — search benchmark | `marts_ops.search_benchmark` | weekly p50/p90 search latency and labeled MRR through the search tool (C8) |
| 4 — integrity | `marts_ops.collation_health` | did the sort order move under us, and did anything break |

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
   [Client uploads via the app](#client-uploads-via-the-app-the-write-path-for-remote-devices).
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
    points. **SILENT in substance, ENFORCED in accuracy**: nothing requires you to write the
    section, but if you do, every `schema.relation` you name must exist
    (`test_docs_only_name_relations_that_exist`) and may not be a pre-reorg name
    (`test_no_module_names_a_pre_reorg_physical_relation`).

Photo sources have five *additional* registry edits on top of this list — see
[Adding a photo source](#adding-a-photo-source-google_photos-takeout-import-manual-imports-).

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

## pdw CLI Full Disk Access vs self-updates (macOS)

macOS TCC keys a Full Disk Access grant to the binary's code-signing designated
requirement. Unsigned darwin binaries only carry the Go linker's ad-hoc signature, whose
requirement is the cdhash of that exact build — so every pdw self-update used to silently
invalidate pdw's FDA grant (System Settings still showed the toggle on). Fixed by signing
release binaries with a stable identity **in the release workflow**:

- **The `pdw-cli-release.yml` build job signs both darwin binaries** with a pinned,
  sha256-verified `rcodesign` (signs Mach-O from plain PEM files on the Linux runner — no
  macOS runner, keychain, or trust settings involved), using the self-signed 100-year
  `pdw-codesign` certificate from the repo Actions secrets `PDW_CODESIGN_KEY` /
  `PDW_CODESIGN_CERT`, under the stable identifier `com.zachlatta.pdw`. The designated
  requirement — `identifier "com.zachlatta.pdw" and certificate root = H"<cert hash>"` — is
  therefore identical for every release, so TCC grants survive self-updates. Signing runs
  before packaging so `SHA256SUMS` covers the signed bytes; a release build with missing
  secrets **fails loudly** (only unreleased fork-PR dry-runs may skip signing), and
  `selfupdate/workflow_test.go` pins the whole contract.
- **Per-Mac setup is just the grant itself**: install a released binary (`pdw update
  --force` or a release tarball), then toggle pdw on once in System Settings → Privacy &
  Security → Full Disk Access. Done forever on that Mac. Granted on porygon 2026-07-14.
- **If pdw's FDA breaks anyway**, check `codesign -d --verbose=2 ~/.local/bin/pdw`: it must
  show `Identifier=com.zachlatta.pdw` and `Authority=pdw-codesign`. `Signature=adhoc` means
  a local `go build` or pre-signing binary is installed — replace it with a release
  (`pdw update --force`); the existing grant starts matching again with no new GUI toggle.
- **The signing identity must never be regenerated casually**: a new certificate is a new
  requirement, which means a new manual FDA toggle on every Mac that granted against it.
  The canonical copy lives in the GitHub Actions secrets; the original key/cert (plus the
  rcodesign used to mint them) are kept as a local backup in `~/.config/pdw/codesign/` on
  porygon. If the key is ever lost, generate a new one (openssl self-signed cert with the
  `codeSigning` EKU), update both secrets, and expect one re-toggle per Mac.

This covers pdw's own grant (needed by `pdw ingest claude-desktop`). The uploader
LaunchAgents dodge the problem differently — they exec `uv run python` directly without pdw
in the chain — and their `/bin/zsh`/`uv`/venv-python grants (including the uv python
path-drift gotcha described below) are unchanged.

## iOS app and push notifications

`mobile/` is an Expo app over the app's own HTTP API — the timeline, the mutation
review queue, and push notifications; see `mobile/README.md`. Three server pieces
back it, all behind the static bearer the CLI uses:

- `GET|POST /api/mutations/requests…` (`app/internal/mutations/api.go`) is THE review
  surface: list/get, approve, deny, drop one email, edit an email before approval
  (`…/mutations/<id>/update-email`), and mark a dead request superseded
  (`…/supersede`, offered when `can_supersede`). Every `gmail.send_email` mutation
  carries an `email` view-model (delivery mode, variants with the selected one marked,
  each body split into editable part / signature / quoted thread, the reply thread) so
  no client re-derives it. The actor is `app:<client_name>`. It executes nothing.
  **The browser UI is a client of this same API**: `/mutation-review`, `/timeline` and
  `/search` are one static single-page app (`app/internal/webapp`, ES modules embedded in
  the binary, no build step, no CDN) that renders nothing server-side and authenticates
  with `PDW_SECRET_TOKEN` exactly like the phone (`Bearer web:<token>`). The old
  server-rendered review pages and their password cookie are gone; a still-set
  `PDW_MUTATION_UI_*` variable is reported as deprecated at startup.
- `POST /api/push/register` stores the device's Expo push token in
  `private.push_devices` (`app/internal/push`); `POST /api/push/test` sends to every
  active device and returns the fan-out report.
- A request landing in `pending_review` fires `mutations.Config.RequestCreated`, which
  the server wires to the push notifier. Delivery is asynchronous and bounded; a
  `DeviceNotRegistered` ticket flips that row to `disabled` with the reason, so an
  unreachable phone is a fact in the table rather than a quietly shrinking fan-out.

**Every timeline row carries its deep link, and the detail carries the conversation.**
`GET /api/timeline`, `/api/timeline/item` and `/api/timeline/item/context` attach
`open: {url, label, app_url?}` to each row (`app/internal/server/timeline_links.go`),
computed from `adapter` + `source_pk` + `metadata` alone so the list pays no extra query:
a Slack permalink (`<domain>.slack.com/archives/<conv>/p<ts>`, with `thread_ts`/`cid` for
a reply, domain from `base_slack.teams`), Gmail `#all/<message_id>` in that account,
Calendar's base64 `eid`, Drive `open?id=`, Google Contacts, ChatGPT/claude.ai conversations,
`wa.me` for a 1:1 WhatsApp chat, `imessage:`/`sms:` for a 1:1 iMessage chat, and the
Notes `showNote?identifier=` scheme. `app_url` is the native scheme a phone tries first
and falls back from — the iOS app never calls `canOpenURL`, which would need every scheme
declared in the native Info.plist. A source with no honest URL (health, photos, voice memos,
a group chat, a CLI agent transcript) gets **no** link rather than a guessed one.
`/api/timeline/item` also inlines `context` — `timeline.context(ref, 15, 15)`, the same
function agents call — with `is_anchor` on the row asked about; both UIs render it as a
scrollable transcript with earlier/later controls up to the function's 50-a-side cap.

Push is delivered through the Expo push service (`exp.host`), not APNs directly.
`PDW_EXPO_ACCESS_TOKEN` is optional on the app deployment. A simulator cannot
receive push; the app reports `unsupported` there and everything else still works.

**Notifications are rich, and the server is where their UX is iterated.**
`push.Notification` (`app/internal/push/notifier.go`) carries `subtitle`, an https
`image_url` (or `image_storage_file_id`, signed into an `/objects/` link), a `category`
for action buttons, `route`, `thread_id`, `collapse_id`, `interruption_level`, `badge`
and `sound`, and `Validate()` refuses what iOS or Expo would otherwise drop silently — an
unknown category shows no buttons, an `http` image is blocked by App Transport Security
in the extension. Three senders share it: the `notify` tool (`pdw call notify --data
'{…}'`, also on MCP), `POST /api/push/send` (same JSON, for curl), and the mutation hook,
whose alert is now `mutation_review` with Approve / Deny / Review buttons that act without
opening the app. Categories live in `app/internal/push/categories.go` and are published
at `GET /api/push/categories`; the app registers them on launch, so a new button is a Go
edit — only handling a *new action id* needs `mobile/src/lib/push.ts`. Images on iOS need
the Notification Service Extension target (`mobile/targets/notification-service`, added
at prebuild by `@bacons/apple-targets`), which reads Expo's `body._richContent.image`
from the payload; a build without it shows the same push as text, and changing it is a
native build, not an OTA update. Settings → "Send test push" sends an image + subtitle +
Open button, which is how a phone proves the extension is installed.

## Voice recordings (a multi-source domain, one pipeline)

Voice has **three** sources — `base_apple_voice_memos.files` (the Mac uploader),
`base_alice_voice_recordings.recordings`, and the audio rows of
`base_apple_notes.attachments` (call recordings, voicemails and the Notes app's own
recorder; collapsed to one recording per attachment on its newest revision) — and exactly
one of everything downstream.
**Start at `marts_voice_memos.recordings`**: one row per recording from either source,
with the resolved title, summary, transcript, participants, action items and calendar
match as real columns. `marts_voice_memos.transcript_segments` holds the speaker-labelled
utterances.

```sql
SELECT source, recorded_at, title, summary
FROM marts_voice_memos.recordings
WHERE transcript IS NOT NULL ORDER BY recorded_at DESC LIMIT 20;
```

**A provider rejection is the transcription pipeline's state, and it reads `failing`.**
`derived_voice_memos.transcription_runs` records every AssemblyAI rejection as
`status = 'error'` with the response text, and `voice_memo_transcription` declares it as its
`StateSource`. On 2026-08-27 every call had returned `400 … account balance is negative` for
hours while the row read green and 45 recordings across three sources sat untranscribed,
attributed to a quiet uploader. A successful retry overwrites the error row, so the failure
clears itself; a persistent one is a billing or credential action, not a pipeline bug.

**A history table needs an `error_window`; a current-state table must never have one.**
`StateSource` was built for `ops.*_sync_state`, where ONE upserted row per scope IS that
scope's state today -- ageing a row out there would hide a live outage, which is why
`whoop` (26 hours hard-down reading `ok` once) has no window. `transcription_runs` is the
opposite shape: one row per attempt, never revisited. Measured 2026-08-27 after the
`rejected` split landed, two rows from **2026-05-02** still pinned the pipeline red --
`"Upload failed, please try again"`, a genuinely retryable message so correctly not
`rejected`, on recordings of `size_bytes = 0`, which the candidate query excludes, so
nothing would ever retry them and clear it. A live outage re-stamps `requested_at` on
every run and stays inside the window; an error that has stopped being re-stamped is
history, not state. Seven days, and the reported reason is windowed with the count so the
banner cannot quote a failure that no longer colours the row.

**A recording the provider will never accept is `rejected`, not `error`, and that
distinction is what keeps the row readable.** The error count behind
`state_error_rows` is over the WHOLE runs table with no time bound, so a single
permanently unacceptable recording pins `voice_memo_transcription` to `failing`
forever. Production had eleven of them -- "no spoken audio", "audio duration is too
short", "does not appear to contain audio", oldest 2026-05-01 -- which means the row
was **already red** when the balance outage arrived on 2026-08-27 and the StateSource
added to catch that outage could not have caught it. `rejected` is terminal for the
candidate query and sits outside `StateSource.error_statuses`, exactly as slack's
`gone` does. The classifier is an **allow-list** of recognised rejections
(`PERMANENT_VOICE_MEMO_TRANSCRIPTION_REJECTION_PATTERNS`), never "whatever is not
retryable": mistaking a permanent rejection for a transient one costs one wasted API
call, while mistaking a TRANSIENT failure for permanent silently retires the recording
and hides the outage. An unrecognised error stays `error` and stays red on purpose.

**"Out of credits" was the wrong account, and the balance alone cannot tell you which.**
The 2026-08-27 rejection was real, but it belonged to an AssemblyAI account whose balance
had gone negative -- while the account Zach was reading a positive balance on was a
*different* one, whose key production did not hold. Both statements were true at once, so
the disagreement is not evidence that the API is lying. The check that settles it takes one
call and names the account: post a transcript with the key production actually has and read
`project_id` off the response. Rotating the key is the repair, and it is only complete when
the value in the Coolify **Dagster** deployment changes -- the app deployment carries no
`ASSEMBLYAI_API_KEY`, so updating the repo `.env` alone fixes nothing in production.

**The speech model mishears "Hack Club" as "HackPad", and the transcript is left
alone on purpose.** Universal-3.5 Pro produces `HackPad` on some audio even though
`Hack Club` sits in `keyterms_prompt`, and AssemblyAI's `custom_spelling` cannot repair
it -- the API rejects a multi-word `to` field, and "Hack Club" is two words. Measured
2026-08-27 across seven recordings re-run through both models: `Hack Club` 29 -> 23 and
`HackPad` 0 -> 3, with **all** of the loss inside one acoustically hard recording and
five of seven preserving the term exactly. The corpus says which one is real -- 504 of
632 stored transcripts contain `Hack Club`, 12 contain `HackPad` -- so a `HackPad` hit
is almost always the mishearing rather than the defunct Dropbox product.

**No stored transcript is unreachable by this today, and that is a fact with a shelf
life.** Measured 2026-08-27, every one of the 12 transcripts containing `HackPad` also
contains `Hack Club` somewhere else, so a `Hack Club` search currently misses **zero**
recordings. What the mishearing costs is a *count*, not a document -- until a short
recording arrives whose every mention is misheard, at which point that one really does
become unreachable. Search both spellings when the answer depends on completeness. The repair is
deliberately NOT a rewrite of `transcript`: editing the provider's words would destroy
the evidence that the mishearing happened and would corrupt a genuine mention. Instead
`ASR_CONFUSION_HINTS` in `apple_voice_memos_enrichment.py` carries the (heard, intended,
why) triple, `enrichment_system_prompt()` hands it to the enrichment agent, and the
agent resolves the term in `title`, `summary`, `participants` and `action_items` while
leaving `transcript` untouched and recording what it did in `evidence`. Adding a future
mishearing is one tuple. The prompt version bump (`...-agent-v7`) re-enriches within
`VOICE_MEMOS_ENRICHMENT_LOOKBACK_WEEKS` rather than the whole corpus.

**Universal-3.5 Pro is the model, with the older two as fallback.**
`ASSEMBLYAI_SPEECH_MODELS` is `("universal-3-5-pro", "universal-3-pro", "universal-2")`,
sent as the `speech_models` fallback chain. Those three are the only slugs the API accepts;
an unknown one is a 400 that names the valid list, so a typo fails loud instead of quietly
transcribing at a lower quality. `speech_model_used` on the response records which one ran,
and it is what `derived_voice_memos.transcription_runs.model` stores -- read that column
rather than assuming the head of the chain served the request.

**That mart is the INPUT to transcription and enrichment, not only an output.** Both
passes (`defs/apple_voice_memos_transcription.py`, `defs/apple_voice_memos_enrichment.py`)
take their candidates from it, so a new voice source is transcribed and enriched by
existing code the day its raw table lands. They used to scan
`base_apple_voice_memos.files` by name, and the mart hardcoded `NULL` transcript/summary
for the other branch, which made the NULLs self-fulfilling: Alice sat at 53 recordings, 0
transcripts and 0 summaries while every ENFORCED registry passed. See C5.

The three derived tables — `derived_voice_memos.transcription_runs`, `.transcript_segments`
and `.enrichments` — are keyed by **`source` first**, because a `recording_id` is unique
only inside its own source. Without that column a second source's run upserts onto the
first source's row, so the domain could not have stored a second transcript even if
something had produced one.

One timeline adapter (`voice_memo`, source `voice_memos`, kind `voice_memo`, priority
`self`) covers every source over the mart; `timeline.events.metadata->>'voice_source'`
says which one, and `source_pk` carries `{source, account, recording_id}`. Search scopes
them together under `sources => ARRAY['transcript']`.

## Local Voice Memos Upload Scheduler

This Mac is intended to run the local Voice Memos uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.voice-memos-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist`
- Wrapper script: `bin/voice-memos-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest voice-memos --mode incremental` (the wrapper runs the pdw CLI, which execs `uv run python -m personal_data_warehouse_voice_memos.cli`)
- Main run log: `~/Library/Logs/personal-data-warehouse/voice-memos-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/voice-memos-upload.heartbeat`
- Status helper: `bin/voice-memos-upload-status`

Each run also performs the **enriched-title write-back**: memos that still carry an
app-assigned name ("New Recording N" / geocoded location names — detected by the
`0x1000` auto-named bit in `ZFLAGS`, or the literal `New Recording N` pattern for
pre-flag-era rows) are renamed in the Voice Memos app to the newest completed
`derived_voice_memos.enrichments` title. Hand-typed titles are never overwritten (the
gate is enforced at plan time and re-checked inside the write transaction). The rename
is a proper Core Data save against `CloudRecordings.db` via PyObjC
(`writeback.py` + `store_writer.py`): the model comes from the store's own
`Z_MODELCACHE`, migration is disabled (incompatible future stores fail loudly), and the
save records persistent history under the author
`com.zachlatta.pdw.voice-memo-writeback`, which `voicememod` exports to CloudKit so the
rename syncs to all devices. Kill switch: `VOICE_MEMOS_WRITEBACK_ENABLED=0`. Manual
runs: `pdw ingest voice-memos --writeback-only [--writeback-dry-run] [--writeback-limit N]`,
`--no-writeback` for upload-only. Titles are fetched from the app's `/api/tools/sql`
endpoint with the same `PDW_API_URL`/`PDW_SECRET_TOKEN` the uploader already uses.

Use these commands when inspecting or repairing it:

```bash
bin/voice-memos-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
tail -80 ~/Library/Logs/personal-data-warehouse/voice-memos-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/voice-memos-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
```

Do not replace this with cron unless there is a specific reason. On current macOS, LaunchAgents
behave better for user-session jobs and are easier to inspect with `launchctl`.

If the run log shows `PermissionError: [Errno 1] Operation not permitted` for
`~/Library/Group Containers/group.com.apple.VoiceMemos.shared/Recordings`, the LaunchAgent is
loaded correctly but macOS Full Disk Access is blocking the background process. Grant Full Disk
Access to the executable chain used by the job, especially `/bin/zsh`, the `pdw` binary (`~/.local/bin/pdw`), `/opt/homebrew/bin/uv`, and
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`. The python lives under a
versioned uv directory, so its real path **drifts on every uv python patch bump** (e.g.
`cpython-3.12.12-…` → `cpython-3.12.13-…`), silently breaking the previously-granted FDA. Don't
hardcode it — derive the current target with
`uv run python -c 'import sys,os;print(os.path.realpath(sys.executable))'`, grant FDA to that, then
kickstart the LaunchAgent again. Because the path changes under you, re-check it whenever an
uploader starts failing with a permission error after working fine before.

## Local Apple Notes Upload Scheduler

This Mac is intended to run the local Apple Notes uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.apple-notes-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist`
- Wrapper script: `bin/apple-notes-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest apple-notes --mode incremental` (the wrapper runs the pdw CLI, which execs `uv run python -m personal_data_warehouse_apple_notes.cli`)
- Main run log: `~/Library/Logs/personal-data-warehouse/apple-notes-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/apple-notes-upload.heartbeat`
- Status helper: `bin/apple-notes-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/apple-notes-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload
tail -80 ~/Library/Logs/personal-data-warehouse/apple-notes-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/apple-notes-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload
```

The uploader only sees this Mac's local NoteStore, and macOS only pulls Notes iCloud changes
while Notes.app is running — with the app quit, the store silently freezes and the uploader
reports healthy `selected=0` runs while edits made on other devices never arrive. Each run
therefore ensures Notes.app is running (launched hidden via `open -g -j -a Notes`; see
`notes_app.py`). Set `APPLE_NOTES_OPEN_NOTES_APP=0` to disable. If apple_notes data looks
stale despite healthy runs, check the `NoteStore.sqlite-wal` mtime — days old means iCloud
delivery is stalled, not the uploader.

If the run log shows `PermissionError` or SQLite `authorization denied` for
`~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite`, the LaunchAgent is loaded
correctly but macOS Full Disk Access is blocking the background process. Grant Full Disk Access to
the executable chain used by the job, especially `/bin/zsh`, the `pdw` binary (`~/.local/bin/pdw`), `/opt/homebrew/bin/uv`, and
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`. The python lives under a
versioned uv directory, so its real path **drifts on every uv python patch bump** (e.g.
`cpython-3.12.12-…` → `cpython-3.12.13-…`), silently breaking the previously-granted FDA. Don't
hardcode it — derive the current target with
`uv run python -c 'import sys,os;print(os.path.realpath(sys.executable))'`, grant FDA to that, then
kickstart the LaunchAgent again. Because the path changes under you, re-check it whenever an
uploader starts failing with a permission error after working fine before.

## Writing to Apple Notes (apple_notes mutations)

Apple Notes is the first **write-back** source that reviewed mutations reach. Everything
else `propose_mutation` supports — Gmail, Calendar, Contacts — has a server API the cloud
worker can call. Notes has none: iCloud publishes no write endpoint, and the desktop app
keeps note bodies as gzipped protobuf inside a Core Data store whose decryption key and
auth token sit behind OpenAI-style team-scoped entitlements. **The only supported way to
change a note is to ask Notes.app itself, on a Mac that is signed in.** So the proposal
and review halves live with every other mutation type, and the executor is local.

```
agent  → propose_mutation apple_notes.create_note / apple_notes.update_note
       → ops.upstream_mutation_operations, status pending_review
human  → /mutation-review approves it
Mac    → the apple-notes uploader run claims provider apple_notes and applies it
         through Notes.app over AppleScript
Mac    → the same run's upload stage ships the changed note back
warehouse → base_apple_notes.notes, then timeline.events
```

The round trip closes inside one uploader run because mutations are applied **before** the
scan, not after: an approved edit reaches the warehouse in that cycle instead of waiting
five minutes for the next one.

### The two operations

- `apple_notes.create_note` — `folder` (default `PDW Agent`, created if missing), optional
  `name`, required `body`.
- `apple_notes.update_note` — `note_id` plus any of `name`, `body`, `append_body`.

**`body` and `append_body` are not interchangeable and the difference is destructive.**
`body` replaces the entire note; `append_body` leaves it alone and adds to the end. They
are rejected together at proposal time. Prefer `append_body`: the executor cannot tell an
intentional rewrite from a stale read, so it records the pre-edit body in the mutation's
`result_json.previous_body` — that is a recovery path, not a guard.

**A note has two identifiers and they look nothing alike.** Notes' AppleScript `id` is
`x-coredata://<store-uuid>/ICNote/p<Z_PK>`; `base_apple_notes.notes.note_id` is the store's
ZIDENTIFIER, a bare UUID. An agent can only discover the second one, so the executor
accepts either and resolves a UUID through a snapshot of the local store. Requiring the
Core Data form would have meant the one id a proposal can find is the one the executor
rejects.

**Title is the first line, not the `name` property.** Notes recomputes a note's name from
its body, so setting `name` alone does not survive; the executor promotes it into a leading
heading and, on update, rewrites the existing first line.

### Why the cloud worker must not claim these

`LOCAL_ONLY_MUTATION_PROVIDERS` in `defs/upstream_mutations.py` excludes `apple_notes` from
both the sensor's count and the worker's claim. Without that exclusion the cloud worker
claims the row, fails it as unknown-provider, and bumps `attempt_count` every ten seconds
while the Mac that could have applied it never sees an approved row. Any future source
whose upstream is a local app belongs in that tuple **and** needs a local worker, or its
rows sit approved forever with nothing reporting that they are stuck.

A create is also never reclaimed from a stale `executing` claim: replaying it makes a
second note. Only `apple_notes.update_note` is in the reclaim set.

### macOS Automation permission — the part that will break

The executor sends AppleEvents, so the calling chain needs **Automation → Notes**, which is
separate from the Full Disk Access the uploaders already hold. Three things learned the
hard way on porygon, 2026-08-24:

- **An unanswered prompt wedges the whole machine's AppleEvents, and it does not look like
  a permission problem.** `tccd` blocks in `CFUserNotificationReceiveResponse` waiting for a
  click nobody will make on an unattended Mac, and every AppleEvent to a *TCC-protected*
  app (Notes, Contacts, Calendar, Reminders) then hangs to `-1712 AppleEvent timed out` —
  while unprotected apps (TextEdit, Music) answer instantly, which is what makes it read as
  "Notes is broken" rather than "consent is pending". Restarting Notes does not clear it;
  the pending dialog does. Diagnose with
  `sample $(pgrep -f 'tccd$') 1 -mayDie -f /tmp/t.txt` and grep for
  `CFUserNotificationReceiveResponse`.
- **A grant row with `flags = 1` is a pending prompt, not an allow.** Flipping `auth_value`
  to 2 while leaving `flags = 1` re-prompts forever. A working row reads
  `auth_value = 2, auth_reason = 3, flags = NULL`. The user TCC database is readable and
  writable only from a chain that already holds Full Disk Access — in practice a LaunchAgent,
  not an SSH shell — and `tccd` caches, so a change needs `kill -9 $(pgrep -f 'tccd$')`.
- **TCC attributes the event to `uv`, at its versioned Cellar path.** The chain is
  launchd → `/bin/zsh` → `uv` → python → `osascript`, and the grant lands on
  `/opt/homebrew/Cellar/uv/<version>/bin/uv`. That path **drifts on every uv upgrade**,
  exactly like the Full Disk Access python-path drift documented above, so a working
  executor will start returning `blocked_missing_credentials` after a `brew upgrade` with no
  code change. The executor classifies `-1743` as `blocked_missing_credentials` rather than
  a failure precisely so this reads as "a human must re-grant on that Mac".

`APPLE_NOTES_MUTATIONS_ENABLED=0` pauses the local worker without touching the uploader.
`pdw ingest apple-notes --mutations-only` applies approved mutations and skips the upload;
`--no-mutations` does the reverse.

SQL starting points: `base_apple_notes.notes` for the resulting note, and
`ops.upstream_mutation_operations` for the mutation's own status, result and error.

## Local Apple Messages Upload Scheduler

This Mac is intended to run the local Apple Messages uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.apple-messages-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist`
- Wrapper script: `bin/apple-messages-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest apple-messages --mode incremental` (the wrapper runs the pdw CLI, which execs `uv run python -m personal_data_warehouse_apple_messages.cli`)
- Main run log: `~/Library/Logs/personal-data-warehouse/apple-messages-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/apple-messages-upload.heartbeat`
- Status helper: `bin/apple-messages-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/apple-messages-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload
tail -80 ~/Library/Logs/personal-data-warehouse/apple-messages-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/apple-messages-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload
```

If the run log shows `PermissionError` or SQLite `authorization denied` for
`~/Library/Messages/chat.db`, the LaunchAgent is loaded correctly but macOS Full Disk Access is
blocking the background process. Grant Full Disk Access to the executable chain used by the job,
especially `/bin/zsh`, the `pdw` binary (`~/.local/bin/pdw`), `/opt/homebrew/bin/uv`, and
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`. The python lives under a
versioned uv directory, so its real path **drifts on every uv python patch bump** (e.g.
`cpython-3.12.12-…` → `cpython-3.12.13-…`), silently breaking the previously-granted FDA. Don't
hardcode it — derive the current target with
`uv run python -c 'import sys,os;print(os.path.realpath(sys.executable))'`, grant FDA to that, then
kickstart the LaunchAgent again. Because the path changes under you, re-check it whenever an
uploader starts failing with a permission error after working fine before.

Apple Messages SQL starting points are `base_apple_messages.messages`, `base_apple_messages.chats`,
`base_apple_messages.handles`, `base_apple_messages.chat_handles`,
`base_apple_messages.chat_messages`, and `base_apple_messages.attachments`, with the resolved
read view at `marts_messages.apple_messages`.

## Local Apple Contacts Upload Scheduler

This Mac is intended to run the local Apple Contacts uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.apple-contacts-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-contacts-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.apple-contacts-upload.plist`
- Wrapper script: `bin/apple-contacts-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `uv run python -m personal_data_warehouse_apple_contacts.cli --mode incremental`
- Main run log: `~/Library/Logs/personal-data-warehouse/apple-contacts-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/apple-contacts-upload.heartbeat`
- Status helper: `bin/apple-contacts-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/apple-contacts-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-contacts-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-contacts-upload
tail -80 ~/Library/Logs/personal-data-warehouse/apple-contacts-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/apple-contacts-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.apple-contacts-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-contacts-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-contacts-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-contacts-upload
```

The uploader snapshots every `AddressBook-v22.abcddb` under
`~/Library/Application Support/AddressBook`, including local and account/iCloud stores. It sends
changed cards and tombstones through the app's `/ingest/apple-contacts/batch` endpoint. Dagster's
`apple_contacts_drive_inbox_sensor` consumes them into `base_apple_contacts.cards`; `marts_contacts.contacts`
unions active Apple and Google cards and `marts_contacts.contact_points` provides normalized phones/emails
for identity joins. `marts_messages.apple_messages` uses those points to resolve Messages senders.

If the run log shows `PermissionError` or SQLite `authorization denied` for an Address Book
store, grant Full Disk Access to `/bin/zsh`, `/opt/homebrew/bin/uv`, the repo venv Python, and the
current real uv Python path, then kickstart the LaunchAgent.

## Local Apple Photos Upload Scheduler

This Mac is intended to run the local Apple Photos uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.photos-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.photos-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.photos-upload.plist`
- Wrapper script: `bin/photos-upload-launchd`
- Run cadence: every 1800 seconds with `RunAtLoad`
- Command: `uv run python -m personal_data_warehouse_photos.cli --mode incremental --limit 100` (override the bounded-run default with `PHOTOS_UPLOAD_LIMIT`; the wrapper runs uv DIRECTLY — pdw self-updates invalidate TCC grants attributed to it, so it must stay out of every uploader exec chain; credentials are read from `~/.config/pdw/config.json`)
- Main run log: `~/Library/Logs/personal-data-warehouse/photos-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/photos-upload.heartbeat`
- Status helper: `bin/photos-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/photos-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.photos-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.photos-upload
tail -80 ~/Library/Logs/personal-data-warehouse/photos-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/photos-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.photos-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.photos-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.photos-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.photos-upload
```

If the run log shows `PermissionError` for `~/Pictures/Photos Library.photoslibrary`, the
LaunchAgent is loaded correctly but macOS Full Disk Access is blocking the background process —
the same FDA/uv-python-path-drift story as the other uploaders above.

The uploader also needs the macOS Photos privacy grant used by PhotoKit. PhotoKit runs in the
hidden native app `~/Library/Application Support/personal-data-warehouse/photos-helper/PDW Photos
Exporter.app`; every helper call goes through LaunchServices so TCC consistently attributes the
grant to `com.zachlatta.pdw.photos-exporter`. Do not replace this with a loose executable: macOS
attributes a command-line PhotoKit request to its responsible parent (Ghostty interactively,
launchd when scheduled), producing a grant that works in only one context. The first scheduled
export requests access automatically, or request it ahead of time with `uv run python -m
personal_data_warehouse_photos.cli --authorize`; either path must show **PDW Photos Exporter** as
the requester. Grant **Full Access** (Selected Photos is insufficient), then kickstart the
LaunchAgent. The helper is rebuilt only when its checked-in Swift source or privacy plist changes;
because it is ad-hoc signed, such a change requires authorization again. If a run reports that
Photos access was denied or limited, repair PDW Photos Exporter in System Settings → Privacy &
Security → Photos. LaunchServices invocation is asynchronous and the uploader waits on the app's
redirected result files; do not use `open -W`, which races short-lived helper instances and can
fail with `initial call to kevent() failed: No such process` even when the app launched.

The uploader snapshots `Photos.sqlite` (never reads the live DB) for metadata and candidate
selection, but deliberately never reads `Photos Library.photoslibrary/originals` for media:
under Optimize Mac Storage that tree is only an incomplete cache. Every selected resource is
exported through PhotoKit with iCloud network access enabled, so Photos downloads the complete
original before upload. Scanner selection is limited to `ZBUNDLESCOPE = 0`: nonzero bundle scopes
are transient syndicated/shared records that Photos stores in `ZASSET` but does not expose as
user-library `PHAsset`s. The native helper fetches with `includeAllBurstAssets` +
`includeHiddenAssets`: burst-stack members (`ZVISIBILITYSTATE = 2`) and hidden assets are ordinary
rows in `Photos.sqlite` but are invisible to a default PhotoKit fetch, so without those flags they
are permanently "not available through PhotoKit". Photo and video assets request PhotoKit's
original resource type; Live Photos also request the original paired-video resource under the
still's ZUUID with `role=live_video`. A missing asset, failed iCloud download, empty export, or
size mismatch is a loud run failure that retries later—never a successful local-only coverage
count. Repeated
failures on one file back off exponentially (30 min doubling to 7 days) and are dropped from
selection while backed off, so a file PhotoKit will never export cannot consume the run's
`--limit` slots; after 5 attempts it also stops failing the run and is reported as
`deferred=`/`failed=` in the summary instead. That demotion requires an upload to have succeeded
since the streak began, so a real outage (revoked Photos access, dead network) stays loudly red
rather than going quietly green. `--retry-failed` clears every backoff for an immediate retry.
Complete bytes then go through
`POST /ingest/photos/file/resumable` + `/ingest/photos/metadata`. The app creates a
scoped Google Drive resumable session after its normal content-sha dedup check; the uploader
streams the export to Drive in 16 MiB chunks, resumes from Drive's acknowledged byte after
timeouts, and verifies Drive's final sha256 + size before uploading the envelope. Photo files
therefore have no app/Cloudflare body ceiling and never permanently defer for size. Edited
renditions are not uploaded yet (originals only; the run log counts assets with adjustments).
The scheduled 100-resource limit bounds disk/network work and still walks the backlog because
incremental state selection happens before the limit. For a manual backfill batch, use
`pdw ingest apple-photos --mode incremental --limit N`; `full` is only for intentionally
re-exporting already-complete resources.

Serverside, `photos_drive_inbox_sensor` + `photos_drive_ingest` consume the inbox into
`base_apple_photos.files`; the `photo_identity` asset dedups renditions into logical photos
(`derived_photos.assets` + `derived_photos.asset_files` link/audit rows, 256-bit dhash fingerprints in
`derived_enrichment.media_fingerprints`, 1280px JPEG thumbnails in Drive); `photo_enrichment` runs the
vision agent once per logical photo over `marts_photos.canonical_renditions`; the `photo`
timeline adapter emits one event per photo with the AI caption in `search_text`.

Photos SQL starting points are `base_apple_photos.files` (raw renditions), `derived_photos.assets` (one row
per deduplicated logical photo), `derived_photos.asset_files` (identity links + `match_method`/
`match_score` dedup audit), `marts_photos.photos` (assets + caption + rendition counts),
`marts_photos.files` (all renditions across sources), and timeline `source = 'photos'`. Free-text
search: `timeline.search_text()` with `sources => ARRAY['photo']`.

### Adding a photo source (google_photos Takeout import, manual imports, ...)

The photos pipeline is multi-source by construction; Apple Photos is just the first source.
`PHOTO_SOURCE_RELATIONS` in `src/personal_data_warehouse/relations.py` is THE extension point —
it drives Drive-ingest routing, the identity runner's scan, and the `marts_photos.files` union.
To add a source:

1. **Raw table**: add `<source>` to `SOURCE_RAW_SCHEMAS`, a `("<source>_files", "<source>",
   "files")` relation row, and a `TableSpec(PHOTO_SOURCE_FILE_COLUMNS, ...)` in `postgres.py`
   (same shared column list and provenance primary key as `apple_photos_files`), then add the
   table to `_PHOTO_TABLES` and `TIMELINE_TABLE_COVERAGE` (a `detail` of `photo_assets`).
2. **Registry**: one entry in `PHOTO_SOURCE_RELATIONS` (`"<source>": "<source>_files"`). Unknown
   sources fail loud at ingest — register before uploading.
3. **Uploader**: post the shared envelope (`personal_data_warehouse_photos/envelope.py`,
   `source="<source>"`, native id + role per file, raw record under a source-named key like
   `takeout_sidecar`) to `/ingest/photos/file/resumable` + `/ingest/photos/metadata` via
   `IngestClient.upload_photo_file_path`/`upload_photo_metadata`. Live/motion
   components upload under the same native id with `role=live_video`; edited outputs use
   `role=edited`.
4. **Precedence**: slot the source into `PHOTO_SOURCE_PRECEDENCE`
   (`src/personal_data_warehouse/photo_identity.py`) so canonical-field resolution knows who
   wins when renditions disagree.
5. Nothing else: identity/dedup (incl. the burst guard and cross-source perceptual merge),
   thumbnails, enrichment, timeline, and search all follow automatically from the registry.

## Local Agent Sessions Upload Scheduler

Captures AI agent CLI session transcripts (Claude Code + Codex + OpenClaw + pi) so every device's
sessions are queryable in the warehouse. The append-only transcripts are tailed and shipped,
line by line, through the same Drive-inbox pipeline as Apple Messages/WhatsApp.

> The macOS LaunchAgent below runs on Zach's Macs (crobat, porygon) for Claude Code/Codex/pi. The
> **openclaw VM** runs the same uploader for OpenClaw sessions via a systemd user timer — see
> [OpenClaw Agent Sessions](#openclaw-agent-sessions-openclaw-vm) below.

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.agent-sessions-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.agent-sessions-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.agent-sessions-upload.plist`
- Wrapper script: `bin/agent-sessions-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest agent-sessions --mode incremental` (the wrapper runs the pdw CLI, which execs `uv run python -m personal_data_warehouse_agent_sessions.cli`)
- Main run log: `~/Library/Logs/personal-data-warehouse/agent-sessions-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/agent-sessions-upload.heartbeat`
- Status helper: `bin/agent-sessions-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/agent-sessions-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.agent-sessions-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.agent-sessions-upload
tail -80 ~/Library/Logs/personal-data-warehouse/agent-sessions-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/agent-sessions-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.agent-sessions-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.agent-sessions-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.agent-sessions-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.agent-sessions-upload
```

The uploader reads `~/.claude/projects/**/*.jsonl`, `~/.codex/sessions/**/rollout-*.jsonl`,
`~/.openclaw/agents/main/sessions/<sessionId>.jsonl`, and
`~/.pi/agent/sessions/**/*.jsonl` (override with `AGENT_SESSIONS_CLAUDE_PROJECTS_DIR` /
`AGENT_SESSIONS_CODEX_SESSIONS_DIR` / `AGENT_SESSIONS_OPENCLAW_SESSIONS_DIR` /
`AGENT_SESSIONS_PI_SESSIONS_DIR`; set one to empty to disable that tool on a host). Each
tool's directory that doesn't exist on a given machine is simply skipped, so the same uploader
binary works everywhere. The OpenClaw scan ignores the `<sessionId>.trajectory.jsonl` runtime
trace and the `.json` sidecars next to each transcript. It tracks a byte offset per file,
coalesces new lines across files into full-size gzipped JSONL batches, and posts them through the
app's ingest endpoint (see below), which writes them into the `agent-sessions/inbox/` Drive
folder. The `--limit` flag bounds a run (useful for a first backfill). In Dagster, the
`agent_sessions_drive_inbox_sensor` + `agent_sessions_drive_ingest` asset consume the batches.

## Client uploads via the app (the write path for remote devices)

Every *remote-device* uploader (agent-sessions, voice-memos, apple-notes, apple-messages) writes
through the app — those devices are untrusted and must not hold the Drive credential. Each device
POSTs domain payloads to the app's semantic ingestion endpoints (`POST /ingest/<source>/<type>`,
e.g. `/ingest/agent-sessions/batch`, `/ingest/apple-messages/batch` + `/attachment`,
`/ingest/voice-memos/audio` + `/metadata`, `/ingest/apple-notes/body` + `/attachment` +
`/revision`). The app owns the Drive credential, folder ids, object keys, `kind` values, and
`pdw_*` tags; the device holds none of that. The app writes byte-identical Drive objects, so the
Dagster `*_drive_ingest` readers are unchanged.

**Exception — the in-process WhatsApp client writes directly to Drive.** It runs *inside* the
trusted prod Dagster deployment (co-located with the app on mew-coolify) and already holds the full
Drive read+write credential the readers use, so the app indirection buys nothing and only re-adds
a Cloudflare 100 MiB body cap on its large media (WhatsApp videos). It builds the same Drive
`ObjectStore` the `whatsapp_drive_ingest` reader builds and writes `whatsapp/inbox/batches/` +
`whatsapp/inbox/media/` objects itself (byte/tag-identical to what the app would have written),
deduping by content sha. There is **no** `/ingest/whatsapp/*` endpoint — see
[WhatsApp Client](#whatsapp-client-linked-device). (Note `claude_desktop_client` also runs in
Dagster but still posts to the shared `/ingest/agent-sessions/batch`: small payloads, no cap
problem.)

Every uploader therefore needs the warehouse URL and the app secret token. The canonical source
is pdw's own config: because the uploaders run via `pdw ingest <source>`, the pdw CLI resolves the
URL + token the way it does for every other command (`pdw login`, then `PDW_API_URL` /
`PDW_SECRET_TOKEN`) and passes them down — so a single `pdw login` configures uploads too, with no
separate ingest URL to manage. The client reads `PDW_API_URL` (legacy alias: `MCP_BASE_URL`) for
the URL and `PDW_SECRET_TOKEN` (legacy alias: `MCP_SECRET_TOKEN`) for the signing key. Without any
of them the uploader fails fast. On the app side, ingestion turns on automatically when the object
store is configured; per-source folders default to `PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID` and
can be overridden with `PDW_INGEST_<SOURCE>_FOLDER_ID` (e.g.
`PDW_INGEST_AGENT_SESSIONS_FOLDER_ID`). Uploads are authenticated with the same HMAC scheme as
signed download links, bound to the endpoint and the body's sha256, and the app dedups by stable
content sha. `<SOURCE>_STORAGE_BACKEND` / `<SOURCE>_GOOGLE_DRIVE_FOLDER_ID` now only provision the
Dagster reader's Drive access (the reader still reads Drive directly); they no longer affect how
clients write.

### Large uploads and the Cloudflare 100 MiB cap

The public app hostnames are fronted by **Cloudflare**, which hard-caps request bodies at **100
MiB** on non-Enterprise plans (it answers `413 Payload Too Large` before the request reaches the
app, whose own cap is `PDW_INGEST_MAX_OBJECT_BYTES`, default 512 MiB). Voice memos in particular
routinely exceed 100 MiB, so a client posting to the Cloudflare URL silently fails on big files —
and because a per-file failure used to re-raise, a single oversized memo wedged the whole run.

The upload client (`ingest_client.py`, shared by every uploader) handles this two ways:

- **Prefer a Tailscale-direct origin.** When `PDW_INGEST_TAILSCALE_HOST` names a tailnet node
  (e.g. `mew-coolify`, the host the app runs on) — or `PDW_INGEST_DIRECT_URL` gives an explicit base — the client
  resolves that node's current tailnet IPv4 via the `tailscale` CLI and, if it answers `/healthz`
  as the app, sends uploads straight there over plain HTTP (Tailscale/WireGuard is the transport
  encryption) with the public `Host:` header so Traefik still routes to the app. That bypasses
  Cloudflare entirely and lifts the ceiling to the app's 512 MiB cap. Off-tailnet (probe fails) it
  transparently falls back to the public `PDW_API_URL`. These are set in the gitignored repo `.env`
  on the tailnet machines, so the committed repo stays generic. `PDW_TAILSCALE_BIN` overrides the
  CLI path.
- **Defer what the route still can't carry.** `IngestClient.effective_max_upload_bytes` reports the
  real ceiling for the chosen route (512 MiB direct, else min(app cap, 100 MiB)). The voice-memos
  runner defers any recording above it (like its partial/age deferrals) instead of 413-ing and
  wedging — so e.g. a lone 588 MiB memo is skipped while every other memo uploads.
- **Photos do not use the capped body route.** Their signed start request is tiny, and their full
  bytes go straight into an app-created Drive resumable session in 16 MiB chunks. This works
  through the public app URL and has no 100/512/640 MiB per-file ceiling; Drive's acknowledged
  range handles lost responses and its final sha256 + size gate the metadata upload.

Agent-session SQL starting points are the source-owned raw event tables
`base_claude_code.events`, `base_codex.events`, `base_openclaw.events`, `base_pi.events`, `base_claude_desktop.events`, and
`base_chatgpt.events` (one row per transcript/conversation line; `device` tags the machine where
applicable). Cross-source querying uses `marts_ai_conversations.events`, and per-session roll-ups
(counts, token sums, title, cwd/git, first prompt) use `marts_ai_conversations.sessions`. Free-text
content is available through `timeline.search_text()` with `source = 'agent_session'`. (Not to be
confused with `ops.ai_processing_agent_runs` / `ops.ai_processing_agent_run_events`, which log the
warehouse's own internal enrichment agent.)

### OpenClaw Agent Sessions (openclaw VM)

OpenClaw runs on the `openclaw` Ubuntu VM (libvirt/KVM guest on `rotom`; reach it with
`ssh openclaw`, or `ssh -J rotom openclaw` when direct TCP is wedged — pings work but SSH can
time out, a known rotom-side issue). It writes one JSONL transcript per session under
`~/.openclaw/agents/main/sessions/`. Because the VM is Linux (no launchd), the uploader runs as
a **systemd user timer** (zrl has `Linger=yes`, so user units run without an active login).

- Checkout: `~/dev/zachlatta/personal-data-warehouse` (clone of `main` via a read-only GitHub
  deploy key; `core.sshCommand` points at `~/.ssh/pdw_deploy_key`); runs via `uv`
  (`~/.local/bin/uv`).
- Env: `~/dev/zachlatta/personal-data-warehouse/.env` holds the **app-ingest** config (the VM has
  no Drive credential): `PDW_API_URL` (the app, `https://data-warehouse-mcp.zachlatta.com`),
  `PDW_SECRET_TOKEN` (= the app's `PDW_SECRET_TOKEN`/`MCP_SECRET_TOKEN`),
  `AGENT_SESSIONS_STORAGE_BACKEND=http_app`, and `AGENT_SESSIONS_ACCOUNT=zach@zachlatta.com`
  (tags the envelope `account` + keys the upload-offset state DB — keep it stable).
  `AGENT_SESSIONS_CLAUDE_PROJECTS_DIR=`/`AGENT_SESSIONS_CODEX_SESSIONS_DIR=` are blanked so the
  VM uploads only OpenClaw sessions. `device` auto-resolves to the hostname `openclaw`. Uploads
  POST to the app, which writes the batch into the Drive inbox the Dagster ingest reads — see
  [Client uploads via the app](#client-uploads-via-the-app-the-only-write-path).
- Systemd unit: `personal-data-warehouse-agent-sessions-upload.{service,timer}` (user scope).
- Checked-in templates: `ops/systemd/personal-data-warehouse-agent-sessions-upload.{service,timer}`.
- Wrapper: `bin/agent-sessions-upload-systemd`; status helper: `bin/agent-sessions-upload-status-systemd`.
- Run cadence: every 300s (`OnUnitActiveSec=300s`, `Persistent=true`), mirroring the macOS cadence.
- Run log: `~/.local/state/personal-data-warehouse/agent-sessions-upload.run.log`;
  heartbeat: `~/.local/state/personal-data-warehouse/agent-sessions-upload.heartbeat`.

Inspect or repair it (from `ssh -J rotom openclaw`):

```bash
~/dev/zachlatta/personal-data-warehouse/bin/agent-sessions-upload-status-systemd
systemctl --user list-timers personal-data-warehouse-agent-sessions-upload.timer --all
systemctl --user start personal-data-warehouse-agent-sessions-upload.service   # run once now
journalctl --user -u personal-data-warehouse-agent-sessions-upload.service -n 80 --no-pager
tail -80 ~/.local/state/personal-data-warehouse/agent-sessions-upload.run.log
```

Install / reinstall the units after editing the templates:

```bash
mkdir -p ~/.config/systemd/user
cp ops/systemd/personal-data-warehouse-agent-sessions-upload.* ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now personal-data-warehouse-agent-sessions-upload.timer
```

To pull new code: `cd ~/dev/zachlatta/personal-data-warehouse && git pull && uv sync`. Because
uploads go through the app, end-to-end also depends on the **app** (`/ingest/agent-sessions/batch`)
and the **prod Dagster** reader both running `main` (the app writes the object tags the Dagster
reader expects, and the reader carries `openclaw_event_row`). Land/deploy code on both before
relying on the timer.

## Claude Desktop Sessions (claude.ai)

Captures normal Claude conversations from the Claude Desktop app so they're queryable in
the warehouse alongside the agent-CLI sources. They land in the source-owned
`base_claude_desktop.events` raw table and are also exposed through `marts_ai_conversations.events`,
`marts_ai_conversations.sessions`, and `timeline.search_text()`, normalized by
`claude_desktop_event_row` in `agent_sessions_drive_ingest.py`.

Unlike Claude Code/Codex/OpenClaw, **the desktop app keeps no transcripts on disk** - it is a
claude.ai wrapper; conversations live server-side. So this source is **authed clientside, polled
serverside**:

- **Clientside auth (native Go in the `pdw` CLI - all local-machine logic lives in the CLI, not
  Python):** `pdw ingest claude-desktop` decrypts the desktop app's `sessionKey` cookie (Chromium
  cookie store + macOS Keychain AES key) and pushes the session credential
  (`account`/`session_key`/`org_id`) to the app's HMAC-signed `/ingest/claude-desktop/credential`
  endpoint. Implementation: `app/cmd/pdw-cli/claudedesktop.go` (Keychain via `security`, cookie DB
  via the macOS-bundled `sqlite3`, AES/PBKDF2 from the Go stdlib). `--dry-run` prints what would be
  pushed without contacting the app.
- **App credential endpoint (Go):** `app/internal/server/credential_ingest.go` verifies the same
  object-upload HMAC as the other ingest endpoints and upserts the credential into the
  `private.claude_desktop_credentials` Postgres table (keyed by account). Registered in `NewMux`
  whenever `POSTGRES_DATABASE_URL` is set.
- **Serverside poller (Dagster):** `defs/claude_desktop_client.py` - the `claude_desktop_client`
  asset + `claude_desktop_client_keepalive_sensor` (5-min cadence) read the credential from
  Postgres and poll the claude.ai API (`personal_data_warehouse_claude_desktop/{api,sync,state}.py`).
  The `sessionKey` alone authenticates the API, so it works from prod's IP - no Cloudflare cookies
  needed. It fetches conversations changed since the per-conversation `updated_at` cursor
  (`ops.claude_desktop_conversation_state`, Postgres-durable) and ships one `conversation` header line +
  one `message` line per turn through the SAME `/ingest/agent-sessions/batch` path as the other
  agent sources. Re-shipping a whole conversation when it gains a turn is cheap (warehouse dedupes
  by `(source, session_id, event_uuid)` into `base_claude_desktop.events`).

The `sessionKey` rotates ~monthly; the desktop app refreshes it, and the clientside LaunchAgent
re-pushes it hourly so the server's copy stays fresh.

Env: `CLAUDE_DESKTOP_ACCOUNT` (keys the credential + cursor; falls back to
`AGENT_SESSIONS_ACCOUNT`/`APPLE_MESSAGES_ACCOUNT`/`VOICE_MEMOS_ACCOUNT`/`GMAIL_ACCOUNTS[0]` - must
match between the clientside push and the serverside poller), `CLAUDE_DESKTOP_ENABLED` (default on;
set `0` to pause the poller), `CLAUDE_DESKTOP_ORG_ID` (override the org from the cookie),
`CLAUDE_DESKTOP_BASE_URL` (default `https://claude.ai`), and clientside-only
`CLAUDE_DESKTOP_COOKIES_PATH` / `CLAUDE_DESKTOP_KEYCHAIN_SERVICE` / `CLAUDE_DESKTOP_KEYCHAIN_ACCOUNT`.

> End-to-end depends on the **app** (`/ingest/claude-desktop/credential` + `/ingest/agent-sessions/batch`)
> and **prod Dagster** (the poller + the `claude_desktop_event_row` reader) both running `main`. Land
> and deploy code on both before relying on the LaunchAgent. The serverside poller is unofficial-API
> access to claude.ai; treat it like the WhatsApp linked-device client (small ToS/account risk).

### Local Claude Desktop Auth Scheduler

The Mac with the Claude Desktop app pushes the credential through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.claude-desktop-auth`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.claude-desktop-auth.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.claude-desktop-auth.plist`
- Wrapper script: `bin/claude-desktop-auth-launchd` (sources the repo `.env`, then runs
  `pdw ingest claude-desktop`; the Go command does not load `.env` itself)
- Run cadence: every 3600 seconds with `RunAtLoad`
- Main run log: `~/Library/Logs/personal-data-warehouse/claude-desktop-auth.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/claude-desktop-auth.heartbeat`
- Status helper: `bin/claude-desktop-auth-status`

Inspect or repair it:

```bash
bin/claude-desktop-auth-status
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.claude-desktop-auth
tail -80 ~/Library/Logs/personal-data-warehouse/claude-desktop-auth.run.log
pdw ingest claude-desktop --dry-run   # verify cookie decryption without pushing
```

Install / reinstall the plist after editing the template:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.claude-desktop-auth.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.claude-desktop-auth 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.claude-desktop-auth.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.claude-desktop-auth
```

If `pdw ingest claude-desktop` fails reading the Keychain or cookie store, macOS Full Disk Access
is likely blocking the background process from
`~/Library/Application Support/Claude/Cookies` or the `Claude Safe Storage` Keychain item. Grant
Full Disk Access to `/bin/zsh` and the `pdw` binary (`~/.local/bin/pdw`), then kickstart again.
pdw's grant survives self-updates only because release binaries are signed with the stable
identity — see
[pdw CLI Full Disk Access vs self-updates](#pdw-cli-full-disk-access-vs-self-updates-macos);
if it breaks, make sure a signed release build is installed (`pdw update --force`).

### Why the ChatGPT session expires, and the hourly auth LaunchAgent

**The ChatGPT credential dies exactly 10 days after capture, by construction.** The
`accessToken` is an RS256 JWT with `exp - iat = 864000s`, and ChatGPT mints it **only during
a browser sign-in**. Replaying the captured cookie at `/api/auth/session` returns *that same
cached token forever* - measured 2026-08-22, twelve days after capture and two days after the
token's own expiry, chatgpt.com still returned the token issued at capture time, even when the
rotated `Set-Cookie` was honored and replayed across consecutive calls. Nothing server-side
renews it. The 2026-08 cycle is the shape to recognize: published 08-10 11:17:30, 286 green
polls/day for ten days, first failure 08-20 12:20 - one sensor tick after the JWT expired.
Do not go looking for rate limits, IP binding, or a flaky cookie; check `token_expires_at`
in `private.chatgpt_sessions` first.

Two mechanisms follow from that:

- **`chatgpt-auth`, an hourly LaunchAgent** (below) re-publishes the browser's session, so the
  server's copy is never staler than the browser's. **Chrome renews the token by itself** as
  long as it stays running and signed in - porygon's Chrome re-minted at 2026-08-20 11:36:10,
  nineteen minutes after the previous token expired at 11:16:47, with nobody at the keyboard.
  Hourly re-publishing plus a permanently-running Chrome is therefore hands-off. What it cannot
  survive is Chrome being quit or signed out for ten days: the agent will then faithfully
  republish a token that is already dead.
- **An early warning instead of a silent stop.** Every successful poll writes the token's real
  expiry to `private.chatgpt_sessions.token_expires_at`; within two days of it the credential
  reads `action_required` on `/pipelines` **while polling continues** (it never sets
  `expired_at`, which is the separate "rejected, stop polling" mark). `publish-session` prints
  the same warning to stderr, so it lands in the agent's run log and in
  `bin/chatgpt-auth-status`.

**chatgpt.com is behind a Cloudflare managed challenge.** Plain `requests`/`urllib` gets a 403
with `cf-mitigated: challenge`; the identical cookie under `curl_cffi` Chrome impersonation gets
a 200 (measured from the prod host, 2026-08-22). `ChatGPTBackendClient` therefore defaults to a
`curl_cffi` session, exactly like `personal_data_warehouse_claude_desktop.api`. Never "simplify"
it back to `requests` - the symptom is an opaque `ChatGPTAuthError: session expired`, because a
403 is classified as an auth failure.

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.chatgpt-auth`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.chatgpt-auth.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.chatgpt-auth.plist`
- Wrapper script: `bin/chatgpt-auth-launchd` (sources the repo `.env`, then runs
  `pdw chatgpt publish-session --non-interactive`)
- Run cadence: every 3600 seconds with `RunAtLoad`
- Main run log: `~/Library/Logs/personal-data-warehouse/chatgpt-auth.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/chatgpt-auth.heartbeat`
- Status helper: `bin/chatgpt-auth-status`

**It runs on porygon**, whose Chrome holds the chatgpt.com login and stays running around the
clock - which is exactly what keeps the token renewing. A laptop that sleeps or quits Chrome is
a worse host, even though `publish-session` works there too (crobat's Chrome `Profile 1` also
has the login). Whichever Mac hosts it, that Mac's Chrome must stay running and signed in.

```bash
bin/chatgpt-auth-status
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.chatgpt-auth
tail -80 ~/Library/Logs/personal-data-warehouse/chatgpt-auth.run.log
pdw chatgpt publish-session --dry-run   # verify cookie decryption without publishing
```

Install / reinstall the plist after editing the template:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.chatgpt-auth.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.chatgpt-auth 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.chatgpt-auth.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.chatgpt-auth
```

The agent reads the browser's "Chrome Safe Storage" keychain item through `/usr/bin/security`,
so it needs the login keychain auto-unlocked by loginwindow **and** an ACL that says **Always
Allow** (plain "Allow" is one-shot and the next tick fails). Grant it once by running
`pdw chatgpt publish-session` from a GUI-attached terminal on that Mac and clicking Always
Allow; on porygon this grant is already in place, and the sibling `claude-desktop-auth` agent
proves the launchd session can read the keychain there. When it later fails, decode
`security`'s exit status before theorising - 36 is
"cannot prompt" and says nothing, 51 on *every* item means the login keychain password
diverged from the account password. This is a keychain problem, not TCC: TCC fails on a file
path with `Operation not permitted`.

Claude Desktop SQL starting points are `base_claude_desktop.events` for raw rows and
`marts_ai_conversations.events` / `marts_ai_conversations.sessions` filtered to
`source = 'claude_desktop'` for unified querying (one `meta` row per conversation carrying the
title/model, then `user`/`assistant` rows per turn; `session_id` is the claude.ai conversation
uuid). Free-text is in `timeline.search_text()` under `source = 'agent_session'`.

## WhatsApp Client (linked device)

WhatsApp syncs through a real WhatsApp Web multidevice client (neonize, Python bindings over
whatsmeow), not a local-store scanner. The client registers as a linked device on Zach's
WhatsApp account, holds a persistent connection, and receives live messages plus history sync.

It runs in-process with the production Dagster deployment; no separate image or service:

- Asset/job: `whatsapp_client` / `whatsapp_client_job` (`src/personal_data_warehouse/defs/whatsapp_client.py`)
- The client runs in bounded windows (`WHATSAPP_CLIENT_RUN_SECONDS`, default 10800s) so it
  never trips Dagster run monitoring (`max_runtime_seconds: 14400`); the
  `whatsapp_client_keepalive_sensor` relaunches it whenever no run is active. WhatsApp queues
  messages for offline linked devices, so the seconds between windows lose nothing.
- A Postgres advisory lock prevents two concurrent connections on one session, which would
  corrupt the device state.
- Records land in the same Drive layout as Apple Messages, but the WhatsApp client writes them
  **directly** (it holds the Drive credential), not through the app's ingest endpoints: it builds
  the same `ObjectStore` the reader uses (`google_drive_spec(..., source="whatsapp")`) and writes
  JSONL.gz envelope batches to `whatsapp/inbox/batches/` and media blobs to `whatsapp/inbox/media/`
  itself (kind `whatsapp_export_batch` / `whatsapp_media_item`, deduped by content sha). Because
  the write skips Cloudflare, large media (videos) over the 100 MiB public-body cap upload fine.
  The `whatsapp_drive_inbox_sensor` + `whatsapp_drive_ingest` asset consume and promote them
  unchanged. The batch/media object keys + `pdw_*` tags live in
  `src/personal_data_warehouse_whatsapp/batcher.py`.
- Session state is canonical in Postgres table `whatsapp_client_sessions` as a bytea SQLite
  snapshot keyed by `WHATSAPP_ACCOUNT` + `WHATSAPP_SESSION_KEY` (default `default`). neonize
  still requires a SQLite filename at runtime, so `WHATSAPP_SESSION_PATH` is only a disposable
  cache path restored from Postgres before each run and snapshotted back after pairing,
  connect, contact dumps, flushes, and shutdown.

Enabling and pairing (first time):

1. The client is enabled by default once WhatsApp is configured; leave
   `WHATSAPP_CLIENT_ENABLED` unset or set it to `1`. Set it to `0` only when the client needs
   to be paused. `WHATSAPP_SESSION_PATH` may be left as the default runtime cache path; it does
   not need a persistent volume. Optionally set `WHATSAPP_PAIR_PHONE=<E.164 number without +>`
   to pair with an 8-character code instead of a QR.
2. Wait for the keepalive sensor to launch `whatsapp_client_job` (or launch it from the
   Dagster UI) and open the run logs.
3. Scan the QR printed in the logs (WhatsApp > Settings > Linked Devices > Link a Device), or
   enter the logged pairing code. After pairing, the client snapshots the session into Postgres
   and history sync chunks arrive automatically.

Other env vars: `WHATSAPP_ACCOUNT`, `WHATSAPP_GOOGLE_DRIVE_FOLDER_ID` (both fall back to the
Apple Messages values), `WHATSAPP_SESSION_KEY`, `WHATSAPP_CLIENT_ID` (normally leave unset),
`WHATSAPP_FLUSH_INTERVAL_SECONDS`, `WHATSAPP_MEDIA_BYTES_PER_FLUSH`,
`WHATSAPP_MEDIA_COUNT_PER_FLUSH`, `WHATSAPP_DOWNLOAD_HISTORY_MEDIA` (default **on**: all
attachments — live and history-sync — are downloaded to object storage. Set it to `0` as an
escape hatch if the history-media backfill causes load/ban-risk issues; WhatsApp often will not
serve very old media, so expect some `whatsapp_media_items.is_missing = true` rows for old
messages even with it on).

For local pairing/debugging there is a CLI: `uv run personal-data-warehouse-whatsapp-client`
(requires `brew install libmagic` on macOS). It requires `POSTGRES_DATABASE_URL` because
Postgres is the session source of truth. `--session-file` only selects the runtime cache file.

Caveats: unofficial clients violate WhatsApp ToS and carry a small account-ban risk. neonize is
pinned exactly (0.4.3.post0) and **must be bumped when WhatsApp rejects the bundled whatsmeow
version** — the failure looks like `Client outdated (405) connect failure` in the run logs. In
2026-07 the stale pin crash-looped the client for ~2.5 days (~3k red runs) because goneonize also
panics (SIGABRT) marshaling the empty ClientOutdated event. Two rules keep that from recurring:
subscribe only to events the warehouse consumes (`_register_event_handlers` — goneonize marshals
KeepAliveRestored/StreamReplaced/ClientOutdated as empty protos and panics on zero-byte marshal,
and Go only marshals subscribed events), and keep the pin fresh enough for WhatsApp's version
gate. neonize's Go shared library is pre-fetched in the Dockerfile (`import neonize.client` at
build).

WhatsApp SQL starting points are `base_whatsapp.messages`, `base_whatsapp.chats`,
`base_whatsapp.chat_participants` (group rosters: one row per member with admin flags),
`base_whatsapp.contacts`, and `base_whatsapp.media_items`, with the resolved read view at
`marts_messages.whatsapp_messages`. Group subjects and rosters are populated by
a once-per-run-window `get_joined_groups()` dump in the client (history sync never carries
them); `whatsapp_chats.name` is preserved against later empty-name history rows.

Downloaded WhatsApp image (and document-PDF) media is enriched the same way Gmail attachments
are: the `whatsapp_media_enrichment` asset (`defs/whatsapp_media_enrichment.py`) scans
`whatsapp_media_items` for stored blobs (`is_missing = 0`), runs each through the agent-container
vision pipeline, and upserts the structured text into the shared `file_attachment_enrichments`
table — the renamed, source-agnostic successor to `gmail_attachment_enrichments` that both Gmail
and WhatsApp write to (keyed by `content_sha256` + `ai_provider`/`ai_model`/`ai_prompt_version`,
each source under its own `task_type`/`prompt_version`). The runner, image prep, agent prompt,
and candidate query all live in `file_attachment_enrichment.py`; each source is a
`FileEnrichmentSource` descriptor. That enrichment text is folded into the parent WhatsApp
message's timeline search document and surfaced by `search_text()` under `source = 'whatsapp'`.

## ChatGPT (consumer) - server-side backend poll

Normal ChatGPT conversations (the consumer product, not the API) land in the source-owned
`base_chatgpt.events` raw table (`source = 'chatgpt'`), alongside the other AI conversation sources,
and roll up through `marts_ai_conversations.sessions` with free-text in `timeline.search_text()`
(`subsource = 'chatgpt'`).

Why this one is different: the **ChatGPT desktop app** (`~/Library/Application Support/com.openai.chat`)
stores conversations **encrypted** (`conversations-v3-*/*.data`), and both the decryption key and
the app's auth token live in the macOS **data-protection keychain** under OpenAI's team access
group (`2DC432GLL2.com.openai.chat`). That is an `errSecMissingEntitlement` wall: a code-signing
check on the calling binary, not a user-consent gate, so no local helper can read them. We
therefore do **not** read the desktop app. Instead the warehouse polls ChatGPT's backend API
**server-side** using a chatgpt.com **web session** captured from a browser.

Two pieces:

- **Client-side setup (manual, interactive): `pdw chatgpt publish-session`.** Reads the
  chatgpt.com session cookie from a local Chrome-family browser (Chrome/Brave/Edge/Arc; auto-detected
  or `--browser`), decrypting it with the browser's *legacy*, consent-readable "<Browser> Safe
  Storage" keychain item (a one-time "allow" prompt); see `chatgpt_cookies.py`. It validates the
  session against `/api/auth/session`, then POSTs the full cookie header (HMAC-signed, like every
  other ingest) to the app endpoint `POST /ingest/chatgpt/session`, which upserts it into Postgres
  `private.chatgpt_sessions` (`app/internal/chatgptsession`). The cookie never goes to Drive. Re-run
  this whenever the server reports the session expired. Flags: `--account` (defaults through the
  same account fallback), `--session-key`, `--dry-run`.
- **Server-side poll (Dagster): `chatgpt_backend_ingest` asset + `chatgpt_backend_ingest_sensor`.**
  The sensor fires every `CHATGPT_POLL_INTERVAL_SECONDS` (default 300) once a session is published
  (it *skips* with a "run publish-session" reason before first setup, so a missing session never
  floods failures). The asset reads the stored session, exchanges it for a short-lived `accessToken`
  (`chatgpt_backend.py`), walks `backend-api/conversations` newest-first, fetches each conversation
  whose `update_time` is newer than the per-conversation watermark in `ops.chatgpt_conversation_sync`,
  and normalizes the message tree via `chatgpt_conversation_to_event_rows`
  (`agent_sessions_drive_ingest.py`; depth-first `seq`, `tool`/`tool_use` detection, `model_slug`,
  reasoning -> `thinking`). Re-ingest is idempotent in `base_chatgpt.events` (PK
  `source,session_id,event_uuid`).

**Fail-loud / self-heal:** when the session is rejected (logout/expiry), the backend client raises
`ChatGPTAuthError`, the asset re-raises it with *"run `pdw chatgpt publish-session`"* and the run
goes **red** in monitoring; never a silent skip. The fix is one local re-run of publish-session.

Prod config (Coolify, on the **Dagster** deployment): ChatGPT polling is enabled by default once
an account label is available (`CHATGPT_ACCOUNT`, falling back to the agent-sessions/gmail
account); `CHATGPT_CLIENT_ENABLED=0` pauses it. Optional:
`CHATGPT_POLL_INTERVAL_SECONDS`, `CHATGPT_PAGE_SIZE`, `CHATGPT_MAX_CONVERSATIONS_PER_RUN` (bound a
first backfill), `CHATGPT_SESSION_KEY`, `CHATGPT_BASE_URL`. The **app** auto-exposes
`/ingest/chatgpt/session` whenever it has Postgres; no extra config. This is an unofficial API
(same ToS/ban-risk class as the WhatsApp client); it reads only the configured account. ChatGPT SQL
starting points: `base_chatgpt.events` plus `marts_ai_conversations.events` /
`marts_ai_conversations.sessions` filtered to `source = 'chatgpt'`, and `private.chatgpt_sessions`
(credential) / `ops.chatgpt_conversation_sync` (per-conversation watermark).

## Health (two WHOOP sources, one read interface)

**Start at `marts_health`, not at either `base_whoop*` schema.** WHOOP arrives twice — the
public developer API (`base_whoop`, summary grain) and the app API (`base_whoop_private`,
high resolution) — and reading either alone is wrong in a different direction. The public
source has no time series, no strain components, no sleep debt and no sport catalog; the
private source is missing rows the public one has (measured 2026-08-26: 305 public sleeps
vs 294 private, 268 public workouts vs 257). `marts_health.{cycles,sleeps,recoveries,workouts}`
conforms them, LEFT-joined from the public row outward, with `has_private_detail` saying
whether the higher-resolution row existed.

```sql
SELECT start_at, strain, sleep_need_seconds, has_private_detail
FROM marts_health.cycles ORDER BY start_at DESC LIMIT 7;
```

Two conforming rules are the reason the mart exists, and both are silent 1000x errors
without it:

- **`hrv_rmssd_milli` is the only HRV column.** `base_whoop_private.recoveries` records it
  in **seconds**; `base_whoop.recoveries` in milliseconds. The mart publishes one column,
  in milliseconds.
- **Every duration is `*_seconds`.** Public sleep-stage totals are milliseconds
  (`total_rem_sleep_time_milli`) and private ones are seconds; the mart exposes no
  `*_milli` duration at all, so two sources' columns cannot be added together by accident.

The epoch sentinel is translated on **every** exposed timestamp, which is what makes
`marts_health.cycles.end_at` NULL for the cycle still running instead of sorting oldest.
`test_health_mart_translates_every_exposed_timestamp_or_none` seeds an all-sentinel row and
checks the whole column list, per view, because translating some columns and not their
siblings manufactures an inconsistency the sources do not have.

The four WHOOP timeline adapters still read `base_whoop` directly and are unchanged:
repointing them would change `adapter_signature` and re-walk those adapters for no
behavioural gain, and the private tables are deliberately classified `detail` so the same
health events are not emitted twice.

## WHOOP (health)

Read-only OAuth sync against the WHOOP v2 API, running as the `whoop_sync` Dagster asset on
`whoop_sync_every_five_minutes`. Six source-owned tables — `base_whoop.profiles`,
`base_whoop.body_measurements`, `base_whoop.cycles`, `base_whoop.recoveries`,
`base_whoop.sleeps`, `base_whoop.workouts` — plus `ops.whoop_sync_state` (per-collection
watermark and status) and `private.whoop_oauth_tokens` (the credential). Cycles, recoveries,
sleeps and workouts each get a timeline adapter; profile and body measurements stay source
entities rather than repeated events.

```sql
SELECT start_at, strain, average_heart_rate FROM base_whoop.cycles ORDER BY start_at DESC LIMIT 30;
SELECT start_at, sleep_performance_percentage FROM base_whoop.sleeps WHERE nap = 0 ORDER BY start_at DESC LIMIT 30;
```

**A cycle is not its start date.** It runs sleep-onset to next sleep-onset, so the day it
reports is the day it is *awake* for: an onset at 11:12 PM Friday ending 12:07 AM Sunday is
the **Saturday** cycle. The in-progress cycle stores `end_at = 1970-01-01T00:00Z` — the
warehouse-wide "absent" sentinel, not NULL — so `ORDER BY end_at DESC` ranks the running
cycle *oldest*; bound on `start_at` instead.

**The credential rotates on every refresh, and that is the whole operational story.** WHOOP
refresh tokens are single-use: a successful refresh invalidates the pair that produced it, so
two concurrent refreshes have one winner and one permanently dead loser. Three production
incidents in 2026-07/08 came from exactly that. The repaired design has one authority —
`private.whoop_oauth_tokens` — and a `WHOOP_TOKEN_JSON_B64` env value may populate an *absent*
row once and can never replace an existing one. Every credential mutation (bootstrap,
scheduled refresh, direct CLI refresh, explicit reauthorization) takes the same Postgres
advisory lock, and refresh additionally holds the account row lock from the provider call
through the commit; a racer with the pre-rotation token waits and adopts the winner rather
than spending a consumed token.

A dead refresh token — the token endpoint answering 400/401/403, which no retry can clear —
records `status = 'action_required'` for every collection, fails the first no-progress run,
and then *skips* later ticks for that same rejected fingerprint so one dead credential cannot
generate hundreds of identical red runs. It stays `attention` on `/pipelines` until a real
success clears it, so an unchanged `action_required` row is an active incident, not a quiet
pipeline:

```bash
pdw sql -q "is WHOOP authentication healthy" \
  "SELECT pipeline, status, last_write_at, last_run_at, last_error
   FROM marts_ops.pipeline_health WHERE pipeline = 'whoop'"
```

Repair it by re-running the OAuth flow from a terminal with production database access:
`uv run personal-data-warehouse-whoop-auth --install` (add `--manual --no-browser` when the
browser is on another machine). The next tick sees the new fingerprint and self-heals with no
deployment restart. Never paste the callback URL, authorization code, or token into chat,
logs, or a commit. Full runbook, including the cross-host reverse-tunnel procedure:
[`docs/whoop-oauth-operations.md`](docs/whoop-oauth-operations.md).

## WHOOP private API (health, high resolution)

`base_whoop` is the *public* developer API, and it is summary-grain: one row per cycle,
sleep, recovery and workout, with **no time series at all**. Per-6-second heart rate, the
sleep hypnogram, the journal, and the trend metrics (VO2 max, weight, body composition,
steps) have no public endpoint whatsoever. Source `whoop_private` is the second WHOOP
source that fills that in by calling the endpoints `app.whoop.com` itself calls. Full
reconnaissance, including the dead ends nobody should re-walk:
[`docs/whoop-private-api.md`](docs/whoop-private-api.md).

**It is a separate pipeline from `whoop`, on purpose.** It has its own credential and its
own cadence, so `marts_ops.pipeline_health` reports `whoop` and `whoop_private`
independently and one of them dying is never hidden by the other still writing.

### SQL starting points

```sql
-- the day's heart rate, one reading every six seconds
SELECT sample_at, heart_rate FROM base_whoop_private.heart_rate_samples
WHERE sample_at >= now() - interval '1 day' ORDER BY sample_at;

-- the readings inside one workout, offset from its start
SELECT elapsed_seconds, heart_rate FROM marts_health.workout_heart_rate_samples
WHERE workout_id = '<id>' ORDER BY sample_at;

-- last night's hypnogram
SELECT stage, started_at, ended_at FROM base_whoop_private.sleep_events
ORDER BY started_at DESC LIMIT 50;
```

| relation | what it holds |
| --- | --- |
| `base_whoop_private.heart_rate_samples` | continuous heart rate at 6s, every hour of every day (`step` 6/60/600 are the only values the API accepts; 6 is the only one stored) |
| `marts_health.workout_heart_rate_samples` | that same series joined to each workout's own bounds, with `elapsed_seconds` |
| `base_whoop_private.sleep_events` | the hypnogram: one row per LIGHT / REM / SWS / DISTURBANCES stage |
| `base_whoop_private.journal_entries` | the journal answers Zach typed; **the only table here with a timeline adapter** |
| `base_whoop_private.cycles`, `.sleeps`, `.recoveries`, `.workouts` | high-resolution copies of the public rows (strain components, sleep debt, HRV/RHR components, zone durations, GPS) |
| `base_whoop_private.sports` | the 204-sport catalog resolving a workout's `sport_id` |
| `base_whoop_private.documents` | Tier-2 raw UI payloads kept as `raw_json`, keyed `(kind, doc_key)`: `trend`, `stress`, `cardio_details`, `sleep_deep_dive`, `strain_deep_dive`, `behavior_impact`, `health_tab` |
| `ops.whoop_private_sync_state` | per-collection watermark, status and error |
| `private.whoop_private_sessions` | the credential |

**The Strain Coach target lives in `documents`, and it is not in strain units.**
`kind = 'strain_deep_dive'` (one row per day) carries WHOOP's recommended strain in its
`SCORE_GAUGE` item as `score_target`, with the optimal band as
`lower_optimal_percentage` / `higher_optimal_percentage`. All three are **gauge
fractions: multiply by 21**. The scale is linear, so `gauge_fill_percentage * 21`
reproduces the displayed strain and is the check that the fields still mean what they
did. Two sibling kinds landed with it: `behavior_impact` (one row per day — WHOOP's own
attribution of yesterday's journal behaviors to today's recovery, which nothing else in
the warehouse can reconstruct) and `health_tab` (one current row under
`doc_key = 'current'` — WHOOP Age, Pace of Aging, Health Monitor statuses). Every day-keyed
kind — those two plus `stress` and `sleep_deep_dive` — is walked backwards to the
account's first cycle, bounded by `WHOOP_PRIVATE_DOCUMENTS_BACKFILL_DAYS_PER_RUN`;
**the documents table is the cursor**, so an interrupted backfill resumes with no
watermark to repair. That budget is set by bytes, not by the rate limit: a recent
`stress` day is ~1.7 MB and `sleep_deep_dive` ~935 KB, against ~5 KB and 326 bytes for
the other two, so lower it (not the kind list) if the pull ever needs to be lighter. Those
are *wire* bytes and they overstate the disk by ~13x: the walk finished 2026-08-24 at the
first cycle (2025-10-23) with 306 days of each kind stored in a 75 MB table.

**The GPS route is `cardio_details`, keyed by workout, and its sweep is cursored by the
same table.** `kind = 'cardio_details'` (one row per `activity_id`) carries the workout's
`map` — `gps_coordinates` as a lat/lng point list (a 1.7 mi run is ~1,200 points), plus
display-string `gps_metrics` — and `base_whoop_private.workouts.gps_data_json` carries only
the distance/elevation summary. Most workouts have no `map` because WHOOP had no phone GPS
for them (measured 2026-08-27: 47 maps across 260 workouts), not because the pull skipped
them. Each run asks for the workouts it just pulled first and then spends what is left of
`WHOOP_PRIVATE_MAX_WORKOUT_REQUESTS` (25) on stored workouts with no document at all,
newest first. Until 2026-08-27 only the run's own newest-25 window was ever asked, so a
workout that landed late — backdated, edited, or restated by a later cycles pull — fell
out of the window and was never fetched; production held three such workouts.

**Heart rate is one series at one grain, and it covers every hour, not just
workouts.** Until 2026-08-26 it was minute-grain all day plus a six-second copy
scoped to workouts, in a second table. The private API in fact serves true
six-second heart rate for **any** window — verified against the live API that day
at one, sixty and two hundred and forty days back — so the workout scoping was
buying nothing but a duplicate of the same readings, and that
workout-scoped table was retired (dropped in place by `ensure_whoop_private_tables`;
`marts_health.workout_heart_rate_samples` is the replacement, and it is a view). It is 14,400 rows a
day and ~5.2M a year, which is the trade that was made deliberately: storage is
recoverable, an unrecorded resolution is not.

**Changing the grain re-walks the history, by itself.**
`ops.whoop_private_sync_state.collection_signature` records what a collection's
rows depend on beyond the window they cover, exactly as `adapter_signature` does
for a timeline adapter. A cursor alone cannot express this: heart rate had
already walked back past the account's first cycle, so it was *finished*, and
resuming it would have left every historic row at the old grain forever. A run
that reads a different signature starts that collection's backfill again from
now — no `force_full_sync`, no operator who has to know. The walk is bounded by
`WHOOP_PRIVATE_HEART_RATE_CHUNKS_PER_RUN` (48 six-hour chunks, twelve days a
run), so a year of history is ~30 runs. The old grain is deleted over exactly
each window the new one has just written, *after* it is written: two grids in one
table double-count every minute whose old timestamp misses the new grid, and
deleting first would leave the series briefly empty.

**The backfill floor is the account's first cycle, not `full_sync_start`.** A
member has no heart rate before they had a WHOOP. Left at the configured floor
the walk spends ten years of runs asking for windows that cannot contain a
reading — production had reached 2025-02-03 for an account whose first cycle is
2025-10-23.

**Only `journal_entries` reaches `timeline.events`** (adapter `whoop_private_journal`,
source `whoop_private`, priority `self` — Zach opened the app and answered the question
himself). The private cycles/sleeps/recoveries/workouts are classified `detail` of the
`base_whoop` row they duplicate: those events are already on the timeline through the four
public adapters, and a second adapter over the private copies would emit a duplicate of
every health event onto a 43M-row table. Read a health *event* from `timeline.events`, then
drill into `base_whoop_private.*` for the resolution the public row does not carry.

### Auth: a captured browser session, not OAuth

MFA is mandatory on this account, so there is no unattended password grant and no login to
implement. The web app's session is captured from ordinary Chrome cookies on `.whoop.com`
(the same Safe Storage keychain machinery `chatgpt_cookies.py` uses) and published to the
warehouse, exactly like the ChatGPT session:

- `whoop-auth-token` — the bearer, an AWS Cognito JWT, **24 hours**.
- `whoop-auth-refresh-token` — opaque, **30 days**, and **every refresh returns a new one**.

Persisting the rotation slides the 30-day window forward, so this source is hands-off
indefinitely: unlike ChatGPT's 10-day token, nothing here expires on a fixed clock while
the sync keeps running. The refresh goes to
`POST /auth-service/v2/whoop/refresh` with the **refresh** token in the `Authorization`
header and an **empty body** — sending it as `{"refresh_token": ...}` returns 401, which is
what makes every published recipe fail. Rotations are persisted under the same advisory
lock the public WHOOP credential uses; three production incidents came from treating a
rotating credential casually.

When the refresh window does lapse, `ops.whoop_private_sync_state` goes `action_required`
and `/pipelines` shows it. Repair it from the Mac whose Chrome holds the whoop.com login:

```bash
pdw whoop publish-session          # add --dry-run to verify cookie decryption first
```

### Unit traps

- **`hrv_rmssd` is in SECONDS here.** The public API's `base_whoop.recoveries.hrv_rmssd_milli`
  is milliseconds. Mixing the two is a 1000x error, so the private table stores
  `hrv_rmssd_seconds` *and* a derived milliseconds column rather than one ambiguous name.
- **`during`, `days` and `optimal_sleep_times` are PostgreSQL range notation**
  (`['start','end')`). Parse the bounds; do not cast the string to a timestamp.
- Day boundaries are user-local — take `timezone_offset` from the bootstrap response first.
- The cycle carries `predicted_end` + `data_state`, which is a cleaner in-progress signal
  than the warehouse's epoch sentinel.
- Rate limits are 2,000 per 5 minutes and 144,000 per day (~20x the public API), so a
  backfill is not limit-constrained.

## Plaid Finance

Personal financial data is linked through Plaid and stored in the source-owned `plaid` schema.
Raw/query tables are `base_plaid.items`, `base_plaid.accounts`, `base_plaid.transactions`,
`base_plaid.investment_securities`, `base_plaid.investment_holdings`, `base_plaid.investment_transactions`,
`base_plaid.liabilities`, and `ops.plaid_sync_state`. Finance-domain read views are
`marts_finance.accounts`, `marts_finance.transactions`, `marts_finance.investment_holdings`,
`marts_finance.investment_transactions`, and `marts_finance.liabilities`. Access tokens are
isolated in `private.plaid_item_tokens`. Warehouse initialization provisions the NOLOGIN
`PDW_QUERY_POSTGRES_ROLE` (default `pdw_query`), revokes `private` from it/`PUBLIC`, and both Go and
Python read-only query runners assume that role for every user-authored query; never bypass this
boundary or expose the token table through normal query surfaces.

Configure `PLAID_ACCOUNT`, `PLAID_CLIENT_ID`, `PLAID_SECRET`, and `PLAID_ENV` on the machine doing
interactive linking and in the production Dagster deployment. `pdw ingest plaid link` opens the
localhost Plaid Link flow and persists the exchanged token; repeat it once per institution.
`pdw ingest plaid items` lists what is linked; `pdw ingest plaid unlink <item-id>` retires one
(revokes it at Plaid, then deletes exactly that Item's rows — see the re-link trap below).
`pdw ingest plaid sync` performs an immediate pull. Production uses the `plaid_finance_sync` asset
and `plaid_finance_sync_every_thirty_minutes` schedule. Account, holding, and liability responses
are authoritative snapshots: reconcile missing accounts/holdings/liabilities rather than leaving
stale current rows. Product errors must persist a redacted `ops.plaid_sync_state` row before the run
fails. The exception is a permanent Item error — `NO_ACCOUNTS`, `ITEM_LOGIN_REQUIRED`, and the rest
of `PLAID_ACTION_REQUIRED_ERROR_CODES` — which no retry can clear: those record status
`action_required` (keeping the prior cursor and last-success time so re-linking resumes instead of
replaying), warn in the run log, count in the asset's `action_required` metadata, and leave the run
green. Otherwise one dead institution keeps the every-30-minutes schedule permanently red and
buries the transient failures that are worth paging on. Repair by re-running
`pdw ingest plaid link` for that institution; find them with
`SELECT * FROM ops.plaid_sync_state WHERE status = 'action_required'`.
**A cleared `action_required` is not the finish line.** Link only sometimes repairs the existing
Item; it can just as well mint a NEW `item_id` with NEW account ids for the same real accounts and
leave the dead Item linked beside it (this happened on 2026-07-25). Both Items then sync: every
balance is counted twice in `marts_finance.net_worth` and the transaction overlap is duplicated.
After any re-link, assert the item count too —
`SELECT institution_name, count(*) FROM base_plaid.items GROUP BY 1 HAVING count(*) > 1` — and retire
the leftover with `pdw ingest plaid unlink <item-id>` (`--dry-run` first; the id may be an
unambiguous prefix). The ledger side self-heals from there: plaid account identity resolves by
owner + institution + mask + side, so the surviving Item's accounts merge back into the logical
accounts they duplicated, and the residue is pruned.
Optional products default to read-only `transactions,investments,liabilities`; no
payment/money-movement Plaid products are requested.
New Links request `PLAID_TRANSACTIONS_LOOKBACK_DAYS` of Transactions history, defaulting to Plaid's
730-day maximum; the same setting controls the Investments transaction query window. Transactions
is the required Link product, while configured Investments and Liabilities are additional
consented products so partial-product institutions remain linkable. Sync marks products absent
from an Item's Plaid product metadata as `unsupported` without failing supported products. Plaid
cannot expand an existing Item's Transactions history grant, so Items created with a shorter
window must be removed and linked again. Preserve and verify warehouse history during that
migration before deleting rows belonging to the old Item.
Run `uv run python scripts/plaid_linking_report.py` after linking/live verification to refresh the
mode-0600, gitignored `reports/plaid-linking-report.private.md` artifact with every institution and
anonymous account status plus last-pull evidence. See the README's **Plaid Finance Sync** section
for all settings and safe aggregate verification queries.

## Finance Ledger (stocks and flows)

The derived `finance` schema is the cross-source ledger over the finance sources (Plaid +
manual_finance). Every source is a witness to one of two fact types: a **flow** (money moved: a
transaction) or a **stock** (something was worth X at time T: a balance, valuation, or principal).
The ledger stores **facts only** — no categories or other opinions; categorization is a future
enrichment layer.

- `derived_finance.accounts` — one row per logical account/asset/liability (kinds incl.
  checking/credit/brokerage/ira/mortgage/property/vehicle/private_fund/receivable), resolved across sources
  via `derived_finance.account_links` (photos-identity pattern: raw rows never learn about identity;
  deterministic `fa_<sha>` ids; delete links + rerun replays every decision). Identity evidence is
  owner + institution + mask + side for **both** sources: plaid account ids are item-scoped, so a
  re-link that mints a new Item would otherwise fork every account and double-count net worth.
  Two simultaneously-live plaid accounts never merge (nothing says which is authoritative) —
  retire the dead Item and the survivor adopts the older account on the next run, leaving residue
  with no links, which is pruned with its observations.
- `derived_finance.observations` — append-only per-day values (PK account_id/as_of/kind/source; NUMERIC
  money, DATE days). The `finance_ledger` asset (schedule `7,37 * * * *`, after each `*/30` Plaid
  sync) snapshots every live Plaid account's balance daily — Plaid itself only keeps
  current-state, so this table IS the balance history.
- `derived_finance.transactions` — the unified deduped flow ledger (+
  `derived_finance.transaction_links` audit): one row per real-world money movement across Plaid and
  uploaded statements. Amounts are signed NUMERIC, **positive = inflow to the account** (Plaid's
  positive-out is negated at ingest; document rows carry explicit in/out). Cross-source dedup at
  the Plaid/statement overlap seam: same account + exact amount + dates within ±3 days merge
  (Plaid wins field precedence; `match_method` records source_id/pending_id/fuzzy_amount_date).
  Pending Plaid rows merge into their posted successor via `pending_transaction_id`; the
  transactions table is reconciled to current source rows every run (derived state — raw rows
  never touched). Statement balances become `balance` observations (`principal` on mortgage
  accounts); valuation docs (Zillow screenshots, fund positions) become `valuation` observations
  and found property/vehicle/private_fund accounts.
- Net worth: `marts_finance.net_worth` (latest observation per account, signed by side; net worth
  = `SUM(signed_value)`) and `marts_finance.net_worth_history` (forward-filled daily
  assets/liabilities/net series). **Every net-worth row says how stale it is**: `age_days`,
  `expected_refresh_days` (3 for Plaid-fed kinds, 35 for a mortgage statement, 120 for
  property / vehicle / private_fund / receivable valuations) and `staleness`
  (`ok` / `late` / `stale` at 1x / 3x). Measured 2026-08-26 the private-fund valuation was
  4.5 months old and the mortgage 8 weeks with nothing flagging either; quote a net worth
  with its stalest input, not as today's number. `marts_finance.accounts` (accounts + latest observation) and
  `marts_finance.transactions` (the ledger joined to accounts) REPLACED the old Plaid passthrough
  views of the same names; the plaid-specific `marts_finance.investment_*` / `marts_finance.liabilities`
  passthroughs remain.

## Manual Finance Documents (manual_finance)

Manually uploaded finance documents — bank/credit/brokerage/mortgage statements, property/vehicle
valuation screenshots, private-fund position docs, CSV/OFX/QFX exports — land in
`base_manual_finance.documents` (one row per doc, native id = content sha) with agent extractions in
`derived_finance.document_extractions`. The mortgage servicer is not Plaid-supported, so mortgage
statements are the mortgage's only source.

- Upload: `pdw ingest manual-finance <files-or-dir>` (uploader package
  `src/personal_data_warehouse_manual_finance/`). The folder-per-account organization
  (`<institution>-<name>-<mask>/statement.pdf`) is preserved as `original_path` (the primary
  account-resolution hint) and as the object key's account segment:
  `manual-finance/inbox/<account-folder>/<date>-<sha><ext>`. Content-sha dedup + sha-keyed local
  state make re-runs cheap; `--limit`, `--mode full`, `--root` supported.
- Transport: `/ingest/manual-finance/file` + `/metadata` (photos pattern, HMAC-signed,
  provenance-sha metadata dedup that excludes `original_path` — moving a file updates the hint
  instead of duplicating). Dagster `manual_finance_drive_inbox_sensor` + `manual_finance_drive_ingest`
  consume the inbox and promote objects to `manual-finance/library/` keeping the account segment.
- **Agent-first extraction** (`manual_finance_extraction.py`): bank files are structured in
  terrible ways, so there are NO format-specific parsers and no deterministic path that bypasses
  the agent. Input prep only chooses what the agent sees (pypdf text layer when rich; pdftoppm
  page renders — `MANUAL_FINANCE_RENDER_MAX_PAGES`, default 10 — when scanned; raw text for
  CSV/OFX/RTF; normalized JPEG for screenshots). The agent runs with read-only warehouse access
  (`run_with_pdw`), gets known `derived_finance.accounts` + `original_path` as context, and returns
  a strict-schema payload (transactions[]/balances[]/valuations[] with decimal-string money)
  mapped into typed columns; bumping `PROMPT_VERSION` re-extracts without clobbering. Retry cap:
  agent failures count per run in `agent_runs`; permanent input-prep failures record status
  `unreadable` and are excluded within the error window.
- Config: folder ids fall back to the shared Drive folder; set
  `PDW_INGEST_MANUAL_FINANCE_FOLDER_ID` (app) + `MANUAL_FINANCE_GOOGLE_DRIVE_FOLDER_ID` (Dagster
  reader) to use a dedicated `manual-finance` subfolder inside the existing PDW Drive folder.

Finance SQL starting points: `marts_finance.net_worth`, `marts_finance.net_worth_history`,
`derived_finance.accounts`, `derived_finance.observations`, `base_manual_finance.documents`,
`derived_finance.document_extractions`, plus the existing `base_plaid.*` / `marts_finance.*` views.

## Securities: trades, tax lots, and coverage

The cash ledger records that money left a brokerage account; it cannot say which security, how
many shares, or at what price. Those are the facts a purchase **lot** is made of, and they live
in their own layer.

**Do not answer a holdings/returns/cost-basis question from
`marts_finance.investment_transactions`** — that view is a Plaid passthrough, and Plaid's
lookback is a hard **730 days** (trade history starts 2024-07-16). Reading it alone is how an
agent concluded in 2026-08 that pre-2024 lots were "not reconstructable from Plaid" and told
the user to ask the broker, while the buys sat in the statement corpus back to 2018.

Use instead:

| relation | what it answers |
| --- | --- |
| `marts_finance.security_transactions` | every share movement, both sources, deduped |
| `marts_finance.tax_lots` | FIFO lots: acquired_on, basis, term, unrealized gain |
| `marts_finance.position_coverage` | **how much of a position actually has a lot history** |

- `derived_finance.security_transactions` is one row per real share movement across Plaid and
  manual statements, with `derived_finance.security_transaction_links` recording how each source
  row resolved (`source_id` founded it, `security_quantity_date` merged it into a Plaid twin).
  The ~20-month statement/Plaid overlap **must** dedup — a doubled trade yields a confidently
  wrong lot. Sides are `buy` / `sell` / `transfer_in` / `transfer_out`.
- `derived_finance.tax_lots` is the FIFO reduction, rebuilt wholesale each run (it is a
  reduction, not accumulated state, so it must be able to shrink). It never invents a basis:
  `basis_known = 0` for a transferred-in lot (the real basis is at the origin account), and a
  sale with no acquisition becomes an `unmatched_sale` row rather than a negative position.
  `method` records the lot election used — FIFO is a *choice*, and the broker's own election
  governs at tax time.
- **`asset_class` is not cosmetic.** An option prints under the underlying's ticker but one
  contract is 100 shares. Plaid compounds this by labelling option trades `type = 'equity'` on
  the security while the transaction name reads *"buy 2.000 QBIT call with strike of $12.00"*.
  Options therefore get their own `security_key` and are excluded from spot pricing/coverage.
- **Check `position_coverage` before quoting a return.** `coverage_status` is `complete` /
  `partial` / `none` / `lots_exceed_holding` / `basis_mismatch` / `no_holding`. The last three
  mean either more open lots than shares held (a missing disposal), reconstructed basis
  disagreeing materially with the provider's independent position basis, or open lots in an
  account that holds none of that security at all. A percentage alone can only understate these
  problems because it is capped at 100.
- **A trade is only deduped against its twin inside ONE ledger account, so an account
  misresolution double-books the position and nothing downstream can tell.** That is what
  happened to Robinhood crypto, and it survived six weeks because it was invisible from every
  direction. Robinhood reports crypto as its own Plaid account while its crypto statements live
  in their own upload folder; the folder's `derived_finance.account_links` row was made from the
  one statement extracted at the time, which printed the *brokerage* account number in its
  header, and links were consulted before resolution and never revisited — so 48 statement
  crypto trades sat in the brokerage account, where nothing could merge them into the Plaid rows
  describing the same trades. `marts_finance.tax_lots` then reported phantom open BTC/ETH/SOL
  lots — worth more than the whole real position — in an account that never held a coin.
  Three things now hold this shut, and each was independently necessary:
  - **Document account links are re-resolved every run** and an evidence-backed match supersedes
    a stored link (`links_relinked` in the run summary). A link is a derived decision, and this
    is what makes "delete the links and rerun replays every decision" actually true. A group
    that matches nothing keeps the account its own documents founded, so a private-fund folder
    is never orphaned. Statement observations are reconciled to the current corpus for the same
    reason — otherwise a relinked group leaves balances behind on its old account.
  - **Quantity equality tolerates the coarser source's rounding.** Plaid prints crypto share
    counts to six decimals; on a 0.003183 BTC buy that is 1.4e-4 *relative*, wider than
    `QUANTITY_MATCH_TOLERANCE`, so even in one account five of those buys still would not have
    merged. A quantity now also matches when the finer value rounds exactly to the coarser one,
    bounded to sub-microshare differences so a share count printed without decimals can never
    absorb a trade half a share larger.
  - **`marts_finance.position_coverage` reports lots the account does not hold.** It used to
    start from held positions, so the phantom lots produced no row and the real crypto account
    read `complete` throughout. It is a FULL join now, and open lots with no holding behind them
    read `no_holding` — but only for an account whose holdings Plaid actually reports, because a
    statement-only account has no feed to disagree with and judging its lots against silence
    would make every reconstructed position look broken.

Extraction contract: `PROMPT_VERSION = manual-finance-agent-v2` captures per-trade
`ticker`/`cusip`/`quantity`/`price_per_share`/`trade_side`/`fees` plus a `positions[]` snapshot
of the statement's portfolio summary. v1 stored a brokerage buy as an anonymous cash debit.
Bumping the version re-extracts the corpus without clobbering v1 (the extractions PK includes
the prompt version). `price_is_derived = 1` marks a price computed from amount/quantity because
the document did not print one.

**Known limits — state them when the answer depends on them.** Neither is modelled, and both
show up as a `partial` / `lots_exceed_holding` coverage status rather than a wrong-looking
number:

- **Stock splits.** A split changes the share count with no trade, so a lot opened from a
  pre-split statement carries pre-split quantities against a post-split holding.
- **Wash sales.** Lots are raw FIFO facts. A harvested loss inside the ±30-day wash window is
  still shown as realized, because disallowance is a tax *opinion*, not a fact the sources
  witness. Say so before anyone trades on a harvesting number.

External verification scripts (they hit the real agent and real source data, so run them
deliberately):
`scripts/verify_manual_finance_extraction_v2.py <pdf>...` checks the agent against a statement's
printed detail; `scripts/verify_securities_ledger_e2e.py <extraction json>...` replays real Plaid
data plus real extractions through the production runner into a throwaway schema. The ledger's
full-replay contract is deterministic test coverage rather than an operator script:
`test_crypto_relink_converges_to_the_same_state_as_a_full_replay` first reproduces the frozen
Robinhood link, repairs it incrementally, deletes all derived finance state, and proves a clean
rebuild from the complete source corpus produces exactly the same links, trades, and lots.

## Shared file-attachment enrichment

**Start at `marts_files.attachments`**: one conformed row per attachment from every source
that holds bytes — `base_gmail.attachments`, `base_whatsapp.media_items`,
`base_apple_messages.attachments`, `base_apple_notes.attachments` — with `source`,
`parent_id` (the message or note), `attachment_id`, `filename`, `mime_type`, `size_bytes`,
`content_sha256`, `is_stored` (bigint 0/1: the bytes are in the object store), `is_deleted`,
`occurred_at` and the `storage_*` columns. Apple Notes attachments appear once per note
revision. That view is the **input** to the file (vision), audio (AssemblyAI) and
deterministic-text enrichment passes and to the receipt pass's attachment-evidence check —
never the raw tables. Until 2026-08-27 each `FileEnrichmentSource` /
`AudioEnrichmentSource` / `TextExtractionSource` named its raw table, which is the C5 hole:
a receipt that arrived over WhatsApp was invisible to the receipt pass, and Apple Notes
attachments were enriched by nothing. `ALLOWED_RAW_ATTACHMENT_SOURCES` in
`tests/test_repo_contracts.py` is empty on purpose; adding an entry re-opens the hole.

`gmail_attachment_enrichments` was renamed to `file_attachment_enrichments` and generalized into
a single source-agnostic enrichment pipeline (`file_attachment_enrichment.py`). To add a new
attachment source: add its UNION branch to `marts_files.attachments`
(`_ensure_files_mart_views`), then define a `FileEnrichmentSource` whose `table` is
`marts_files_attachments` and whose `stored_predicate` is `a.source = '<source>' AND
a.is_stored = 1` (the descriptor exists only to give that source its own task_type/prompt
version), and wire a Dagster asset/sensor that runs `FileAttachmentEnrichmentRunner` with it —
see `defs/gmail_attachment_enrichment.py` and `defs/whatsapp_media_enrichment.py`. The table
rename migrates in place via `ensure_*` (`ALTER TABLE IF EXISTS … RENAME`), preserving
existing rows.

## Slack file bytes and "who sent this image?"

### Getting a Slack file's bytes: `get_object` already does this

`get_object` takes a `base_slack.files.file_id` (`F...`) directly and returns metadata plus a
signed `download_url` that needs no further auth. The app resolves the file live through
`files.info` across every configured workspace token and downloads `url_private`
(`app/internal/objectstore/slack.go`, wired in `server.go`); it already holds the tokens.

```bash
pdw call get_object --data '{"storage_file_id":"F0EXAMPLE123"}'
curl -L -o poster.png "<download_url from the response>"
```

**Do not build a second Slack fetch path.** A 2026-08-16 session concluded pdw could not fetch
Slack file bytes and guessed an answer; it had tried the *public* `slack-files.com` permalink,
which 404s unless the file was explicitly shared publicly. `url_private` needs the bearer token
and `get_object` supplies it. Sampled 2026-08-18 across recent, 2016-era and mid-era files,
images and non-images: 19/19 returned bytes, including a 20 MB PNG.

Slack's sharpest trap is already handled there: an unauthorized `files.slack.com` GET returns
**200 with an HTML login page**, not a 4xx, and the store rejects that rather than returning it
as content.

### Identifying an image: fingerprints, then plain SQL

Slack image files are fingerprinted with the same 256-bit dhash the photos pipeline uses, into
the same `derived_enrichment.media_fingerprints` table. `derived_slack.file_fingerprints` links
a Slack file to the content sha its bytes hash to (PK `account, team_id, file_id` — one download
per file, not per share) and carries the status/attempts/backoff that make the backfill
resumable. **The bytes are never stored**: ~905k live Slack images total ~552 GB versus ~200
bytes per fingerprint, and a named file's bytes are one `get_object` call away.

There is **no new command**. Hash the picture, then run ordinary SQL:

```bash
uv run python -c "from personal_data_warehouse.slack_image_lookup import lookup_sql_for_image; \
  print(lookup_sql_for_image('/path/to/poster.png'))"     # prints ready-to-run SQL
```

Paste that into `pdw sql` (or the `query` tool). It ranks `marts_slack.image_fingerprints` by
`bit_count` XOR distance and **resolves the uploader** by joining `base_slack.users` — the join
the 2026-08-16 session never made. It reports **real_name, @handle and display_name separately**
because Slack keeps all three and they differ (a real row: `Real Name` / `@realname110` /
`realname`); that session was asked for the *handle* specifically.

The hash must come from that Pillow code path — a fingerprint computed by a different resampler
does not error, it silently stops matching.

Read distances as: **0–6 the same image**, 7–16 very likely, 17–28 possibly related, beyond that
check by eye. Verified on the real corpus: every re-encode/rescale of the motivating poster
hashes to distance **0**, while two *different* 11x17 posters sit at **124–125**. Byte size is
useless here — one re-encode was *larger* than the original, and the two copies in the incident
differed by 1.3 MB.

Backfill: the `slack_file_fingerprints` Dagster asset (hourly `:19`) takes a bounded
newest-first slice (`SLACK_FILE_FINGERPRINT_LIMIT`, default 300; `..._RUN_SECONDS`, default
900). Newest-first because recency is what people ask about. It fetches bytes **through the
app's `get_object`**, so it holds no Slack credential of its own and there is one Slack-file
implementation to fix. A 429 ends the slice cleanly without burning the file's retry budget.
**The table is the cursor**, so it resumes by itself; there is no watermark to repair.

Two gotchas worth knowing:

- **Print artwork breaks photo defaults.** The motivating poster is 420,750,000 pixels (11x17
  inches at 1500 DPI), far past Pillow's ~89 MP decompression-bomb guard. `compute_dhash` takes
  an opt-in `max_pixels` (Slack uses 512 MP) that is scoped and restored, so the photos pipeline
  keeps its own posture. Without it that exact file is `undecodable` forever.
- **A DM's `name` is the other user's id**, and a group DM's is `mpdm-a--b--c-1`. Render by
  `conversation_kind`, never by name, or a DM prints as a channel that does not exist.

Coverage is not proof of absence — only fingerprinted files are searchable:

```bash
pdw sql --output json -q "slack fingerprint coverage" \
  "SELECT status, count(*) FROM derived_slack.file_fingerprints GROUP BY 1 ORDER BY 2 DESC"
```

End-to-end check against real Slack bytes in a throwaway schema (writes nothing to prod):
`uv run python scripts/verify_slack_image_lookup.py --file-id <F...> [--probe copy.png]`.

## Slack conversation discovery, and the page-1 trap

**Slack sync has two independent halves, and only one of them was ever monitored.**
`conversations.list` *discovers* conversations into `base_slack.conversations`; every other
stage (freshness, coverage, read-state, members) then runs off those **cached** rows with
`use_existing_conversations=True`. So a conversation that discovery never sees is not
merely stale — it is invisible to every downstream stage forever, and no amount of healthy
message throughput anywhere else will reveal it.

Discovery walks the list in bounded slices, `SLACK_ASSET_METADATA_CONVERSATION_PAGE_LIMIT`
pages per metadata run. Until 2026-08-24 that walk **restarted at page 1 on every run and
stopped after one page**, because no cursor was persisted. Measured against the live Slack
API that day, the damage was:

| type | live | missing from the warehouse |
| --- | --- | --- |
| `mpim` | 2,788 | **172** — every group DM created after 2026-05-18 |
| `public_channel` | 13,157 | **1,948** — essentially every channel created after 2026-05-18 |
| `im` | 3,628 | 0 |
| `private_channel` | 114 | 0 |

1,181 real messages existed in Slack and nowhere in PDW. `im` and `private_channel`
survived only by luck — 114 private channels fit inside one 200-row page, and page 1 of the
`im` list happens to carry the newest DMs. For `mpim`, page 1 is the *oldest* 200 and every
new group DM lands on pages 12-13, which the walk could never reach. 2,354 of 2,597 mpim
rows still carried `synced_at = 2026-05-18T15:34:34Z`, the last full multi-page walk.

Three things now hold this up, and the second is the one that is easy to undo:

- **The walk is resumable.** `_refresh_active_conversations` stores its `conversations.list`
  cursor in `ops.slack_sync_state` under `object_type = 'conversation_list'` (`object_id` is
  the conversation type), resumes from it, and stores `''` at the end of the list so the next
  pass starts over and keeps cycling. A cursor Slack rejects (`invalid_cursor`) restarts the
  walk rather than wedging that type.
- **Both rotations are driven by state, not the wall clock.**
  `_metadata_conversation_types` picks the conversation type whose walk is furthest behind
  (preferring one mid-walk); `_coverage_stage` picks the coverage stage that has gone
  longest without running, from a `coverage_stage` row per stage in `ops.slack_sync_state`
  written only *after* the work happens. A clock rotation **silently forfeits a stage's
  turn whenever that run loses the shared Slack lock**, and a lock-starved stage still
  returns `MaterializeResult` with `skipped_due_to_lock: true` and a green run. Measured:
  six of every eight metadata runs did nothing (mpim metadata went 11.5 hours between
  refreshes), and **38 of 54 coverage runs over six hours — 70% — were no-ops**, which is
  why the 1,929 newly discovered public channels attempted exactly one backfill in their
  first fifty minutes. Reverting either rotation to the clock reintroduces this. The tell
  in Dagster is a materialization whose metadata carries no work counters at all
  (`conversations_seen` absent rather than zero).
- **`marts_ops.slack_conversation_health` judges the SHARE re-listed, per type.** This is
  the detector that was missing. `marts_ops.pipeline_health` aggregates Slack as a single
  pipeline and ~19k public-channel messages a day kept it `ok` with `state_error_rows = 0`
  through a total group-DM outage.

```sql
SELECT conversation_type, live_count, refreshed_count, refreshed_fraction, status,
       discovery_status, oldest_conversation_synced_at
FROM marts_ops.slack_conversation_health ORDER BY status, conversation_type;
```

**`refreshed_fraction` is the number that means something — not `max(synced_at)`, and not
the single oldest row either.** A page-1-only walk re-stamps the first 200 rows every hour,
so `max()` looks perfect while the tail is months old; that is exactly how this hid for
three months. But `min()` over-fires in the other direction: a conversation archived
*upstream* after we last listed it keeps `is_archived = 0` forever, because the only path
that would correct the flag is the same walk that excludes archived rows — so roughly 1% of
rows can never be re-stamped and the oldest-row rule is permanently red. Measured after the
repair: im 100%, mpim 100%, private_channel 99.1%, public_channel 99.2%, against 200 of
2,597 (**7.7%**) during the outage. Thresholds are `ok` >= 95%, `late` >= 75%, `stale`
below.

The status is also deliberately about the **sync attempt**, not about messages: mpim had
eleven legitimate zero-message days between 2026-07-11 and 2026-08-18, so alerting on "no
group DM messages" is a guaranteed false positive. It would also have been wrong here —
group DMs really were silent from 2026-08-20T18:57 onward, which is exactly what Slack
itself reports.

## Slack channels Zach is not in: discovery is not coverage

**Being listed is not being read, and for four months those were the same word
here.** Discovery walks `conversations.list` and stamps every public channel;
`marts_ops.slack_conversation_health` reported 99.2% of them re-listed and `ok`.
But nothing then *asked* most of them for messages. Freshness asks the change
feed, which only knows the ~690 conversations Zach participates in; coverage
only offers a channel whose history is not yet complete, so a channel drops out
of it for good the moment its backfill finishes. A public channel he can read
but has not joined was therefore fetched exactly once and then frozen.

Measured 2026-08-27 against Slack's own admin analytics (`slack.public_channel_analytics`
in the Hack Club warehouse, which is per-channel-per-day ground truth):

| | Slack | PDW |
| --- | --- | --- |
| public channels posting on 2026-08-21 | 718 | 205 |
| public-channel messages that day | 71,636 | 24,116 |
| public-channel messages in August | 1,662,044 | 662,927 (40%) |
| non-member public channels untouched for 14 days | — | 10,711 of 11,488 |

`#arvutitrack` posted 17,646 messages that day and PDW held none of them.
The monthly capture ratio ran 64-80% from December to June and fell to 51% in
July and 40% in August as more channels finished their backfill and froze.

**The fix is a sweep, and the thing that makes a sweep work is stamping a poll
that found nothing.** `slack_workspace_public_sweep_sync` (`4-59/5 * * * *`, on
its own advisory lock — it has ~13k conversations to walk and on the shared lock
it would get whatever six stages with hundreds of candidates left over, the same
reason freshness has its own) polls live public channels regardless of membership, in
two buckets: `hot` (a message within `SLACK_ASSET_SWEEP_HOT_DAYS`, default 7) and
`cold` (everything else), each ordered by *when we last polled it*
(`ops.slack_sync_state.updated_at`, NULLS FIRST so a never-synced channel goes
first). A channel with a cursor is resumed at it, so a quiet channel costs one
`conversations.history` call; a channel with no cursor is streamed in full, which
is how the 601 channels created in July 2026 and never fetched get their history.

Thread replies are deliberately **not** fetched inline: a page of 200 parents can
carry dozens of threads, and one busy channel would eat the whole run. They are
drained by `slack_workspace_thread_backfill_sync`, which selects parents whose
replies we do not hold from any conversation, newest first.

`SLACK_ASSET_SWEEP_HOT_LIMIT` (20) and `SLACK_ASSET_SWEEP_COLD_LIMIT` (40) are a
**rate budget, not a preference**: they are the sustained call rate this stage
adds (~12/min) against a measured ~39/min `conversations.history` ceiling shared
with freshness, threads, coverage and read-state. Cold is the larger bucket
because there are ~11.8k cold channels to ~1.5k hot ones, which puts the full
rotation at about a day — inside the four-day SLA the health view judges. Raise
them for a catch-up and put them back; every stage aborts gracefully on the
shared rate-limit budget, so over-asking does not fail runs, it starves the
other stages.

- **Membership is deliberately not a filter, and `is_member` is not trustworthy
  anyway.** `base_slack.conversations.is_member` said 202 while the change feed
  covers 316 channels; it is refreshed only by whatever last wrote the
  conversation row.
- **A poll that returns nothing must still be recorded**
  (`touch_slack_conversation_sync_state`, which advances `updated_at` and
  preserves the cursor, sync type and status). The candidate order is by last
  poll, so an unstamped poll re-picks the same channels forever and the tail is
  never reached. This is the one invariant that makes the rotation a rotation.
- **`marts_ops.slack_conversation_health` now judges both halves.**
  `history_polled_fraction` is the share of live conversations asked for new
  messages within a cycle, and it is judged **only for `public_channel`** —
  for the other three types the change feed authoritatively reports that nothing
  happened, so "not polled" is evidence of nothing. `status` is the worse of the
  two halves; both are on `/pipelines`.
- **Ground truth for this lives outside PDW.** The Hack Club warehouse's
  `slack.public_channel_analytics` is Slack's own admin analytics, one row per
  channel per day with `messages_posted_count`. Compare against it rather than
  against a feeling that search results look thin.

## Slack change feed: how the sync knows what to fetch

**Slack's public API cannot tell you which conversations have new messages.**
`conversations.list` returns no last-message marker at all — only `updated`, which tracks
topic and member edits. So with an app token the only way to find a new message is to call
`conversations.history` on every conversation. Measured 2026-08-24: the freshness pass
attempts **950 conversations per five-minute cron** against a token ceiling of **~39
`conversations.history` calls/minute** (37-call burst, then a steady `Retry-After: 10`).
It is ~5x oversubscribed, so it spends ~10 minutes of every hour asleep on 429s *while
holding the exclusive Slack lock* — which is why 70% of coverage runs and 83% of metadata
runs were lock-skipped no-ops, and why backfills never drained.

**`client.counts` answers the same question in one request** — but only for a real
signed-in session, which is why `private.slack_sessions` exists. The credential is two
pieces that are useless apart: an `xoxc-` token from the Slack desktop app's localStorage
and the `d` cookie. Capture and publish both with:

```bash
pdw slack publish-session            # add --dry-run to check without publishing
```

Run it from a **GUI terminal** on the Mac signed in to Slack (SSH cannot reach the
keychain) and choose **Always Allow** — a one-shot "Allow" makes every later run fail. The
`d` cookie is good for ~13 months and rolls forward with use, so this is setup, not a
chore.

**What the feed does and does not cover.** Measured on the real workspace: 316 channels
(exactly the 317 the account belongs to), 237 open DMs, 137 open group DMs — 690 total. It
is complete for everything Zach participates in and **silent about the ~13k public channels
he is not a member of**, which keep the slow coverage sweep. `slack_change_feed.py` reports
that coverage rather than assuming it.

Three behaviours are load-bearing and each failure would be silent:

- **An entry with no `latest` marker is ignored, not fetched.** Treating unknown as changed
  restores the blanket poll this replaces.
- **A failed `client.counts` raises; it never returns an empty list.** "Nothing changed" and
  "we could not ask" must not look alike — the empty reading would stop ingestion silently.
- **Any failure degrades to the old polling path** (`SlackChangePlan.usable = False`), so a
  revoked or missing session costs throughput and never coverage. `SLACK_ASSET_USE_CHANGE_FEED=0`
  forces that fallback.

**`derived_slack.inbox_items` is refreshed incrementally, and the watermark is the reason.**
Every Slack stage ends by refreshing that snapshot (`refresh_slack_account_state_items`).
Until 2026-08-27 each call rebuilt it from scratch — thirty days of every member
conversation's messages, read from the heap once per UNION branch, then every previous
row re-inserted as a tombstone — at 44s mean, ~40 calls an hour, 22.6 CPU-hours in 46h:
the single largest statement on the host while **eleven** conversations changed per five
minutes. It now records a watermark in `ops.slack_sync_state`
(`object_type = 'account_state_refresh'`, object_id `<team>`; `<team>:full` for the last
full rebuild) and recomputes only conversations whose row or messages were stamped after
it, minus a one-hour overlap, because a stage stamps rows with the `synced_at` it computed
at *start* and may commit minutes later. Items of a changed conversation that are not
re-emitted are tombstoned by `container_id`; items past the 30-day window are tombstoned
without touching their conversation; a full rebuild still runs once a day, so anything the
overlap misses is wrong for at most a day. Concurrent refreshes take
`pg_try_advisory_xact_lock` and **skip** rather than queue — four of them used to wait up
to eleven minutes on each other for an identical result. Delete the two state rows to
force a full rebuild.

### Local Slack Auth Scheduler

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.slack-auth`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.slack-auth.plist`
- Wrapper script: `bin/slack-auth-launchd`; status helper: `bin/slack-auth-status`
- Run cadence: every 3600 seconds with `RunAtLoad`
- Run log: `~/Library/Logs/personal-data-warehouse/slack-auth.run.log`

**When it fails, `security` timing out after 60 seconds is the usual reason and it is not
a hung binary.** It means the keychain put up a prompt no LaunchAgent can answer: either the
login keychain is locked (Mac asleep or at the login screen) or the ACL on "Slack Safe
Storage" is a one-shot "Allow" rather than "Always Allow". The command says so in its own
output. This is benign in itself — the `d` cookie is valid for ~13 months, so an hour (or a
week) of missed re-publishes changes nothing; only a genuinely rotated session needs the
agent to succeed. Repair by unlocking the Mac and running it once from a GUI terminal,
choosing **Always Allow**.

It runs on the Mac signed in to the Slack desktop app (**crobat**), not on porygon, because
that is where the session lives. **The wrapper execs `uv run python -m
personal_data_warehouse.slack_setup` directly and deliberately keeps `pdw` out of the exec
chain**: macOS attributes the "Slack Safe Storage" keychain grant to the binaries in that
chain, and pdw replaces its own binary on every release, so routing through `pdw slack`
would let a routine update silently revoke the grant. The photos uploader avoids Full Disk
Access loss the same way. Credentials still come from `pdw login`'s config file.

**Enterprise Grid is a live trap here.** Hack Club is an Enterprise Grid org, so a client
session's `auth.test` returns the **org** id `E09V59WQY1E` where the app token returns the
**workspace** id `T0266FRGM` — and all ~45M warehouse rows are keyed by the workspace. Storing
one as the other would not error; it would write a second parallel copy of Slack. The capture
refuses to put an `E` id in `team_id`, the publish endpoint rejects it, and
`pdw slack publish-session` resolves the workspace through `base_slack.teams.enterprise_id`
rather than guessing (an org covering several workspaces raises instead).

## Slack huddles: metadata yes, content no

**Huddle metadata is in the warehouse; huddle content never will be.** It is easy to
conclude huddles are missing entirely — Slack publishes no API that lists them, and none
that exposes huddle audio or Slack-AI huddle notes. But every huddle posts a message with
`subtype = 'huddle_thread'` whose payload carries a `room` object with `created_by`,
`date_start`, `date_end`, `has_ended` and the full `participant_history`. 5,942 of those
were already being ingested, unreachable only because they sat inside `raw_json`.

`marts_slack.huddles` is that, parsed: one row per huddle with `huddle_id`, `huddle_name`,
`created_by`, `started_at`, `ended_at`, `duration_seconds`, `participant_user_ids`,
`participant_count`, and the conversation it happened in.

```sql
SELECT started_at, conversation_name, huddle_name, duration_seconds, participant_count
FROM marts_slack.huddles
WHERE 'U09UE480JHH' = ANY(participant_user_ids)
ORDER BY started_at DESC LIMIT 20;
```

Two traps. `date_start`/`date_end` are epoch **integers** inside the JSON, not the
`timestamptz` the rest of the warehouse uses, and a huddle still running carries `0` — the
view converts both, reporting a live huddle as `ended_at IS NULL` rather than 1970 or a
negative duration. And a huddle's `participant_history` is everyone who ever joined, not
who was there at any one moment.

**What was said in a huddle is not in PDW and cannot be made to be.** Zach makes real
decisions in huddles, so absence of a decision in the warehouse is never evidence that the
decision was not made. Say so rather than reporting a confident negative.
