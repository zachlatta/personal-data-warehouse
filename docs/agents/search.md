# Search: hybrid retrieval, landing latency and the performance contract

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

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
[Timeline priority tiers](../../AGENTS.md#timeline-priority-tiers) — `priorities => ARRAY['self','direct']`
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
[Timeline priority tiers](../../AGENTS.md#timeline-priority-tiers) for the WAL/vacuum playbook first.

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

**The pooled scan is one-shot dynamic SQL with the query text inlined, and that is
the difference between 190ms and 28 seconds.** Measured 2026-09-19 on production, same
words, same session: the pool as a static plpgsql statement — an SPI cached plan with
`query` as a parameter to `to_bm25query()` — took 14–28s on queries whose matches
include a few multi-megabyte `self`-tier documents (77 Drive files for one two-word
probe), while the identical statement with the query as a literal took 4–190ms, and a
`PREPARE`d statement was just as slow under `force_custom_plan`, so it is the
cached-plan path itself that makes pg_textsearch re-score every returned document
rather than trust the index order. The scoped branches always inlined the query
(`%1$L`); the two pools now do too, with `priorities` and `since` passed through
`USING`, and `test_search_text_pool_is_one_shot_dynamic_sql_with_the_query_inlined`
refuses a static `to_bm25query(query, …)` inside the pool. The symptom to recognise:
`auto_explain` shows the index scan returning a few hundred rows with ~20k buffers and
~130ms of I/O, and the statement still takes 25 seconds — the time is in the per-row
re-scoring, which no node attributes. A related trap found the same day: a plain
`REINDEX` of a BM25 index wrote `pg_class.reltuples = 212,042` for the 61M-row
timeline heap (a 280x under-estimate); `ANALYZE timeline.events` repaired it in 19s.
Check `reltuples` after any index rebuild on that table.

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
  of exclusive lock on the 45 GB timeline heap.
  **Only the Dagster image performs that rebuild** (`PDW_INDEX_DEFINITION_OWNER=1`, set in
  the `Dockerfile`; every other process logs the drift and leaves the index alone). Every
  client that opens the warehouse runs `ensure_*` — the Mac uploaders and resident mutation
  workers carry the production URL — and a checkout one deploy behind reads the CURRENT
  index as drifted: from 2026-09-14 to 09-16 porygon (on the pre-Hacker-News commit) and
  Dagster rebuilt both low-volume BM25 indexes to each other's definition ~16 times an
  hour each, 30-45s of exclusive lock on `timeline.events` per rebuild, and every Slack
  freshness pass sat minutes behind that lock (DM landing p95 ~30 min). `git pull` on the
  Mac is the immediate repair; the ownership flag is what keeps it from recurring. The low-volume attention index must carry
  the list to be the low-volume partition at all, and at 90k documents it rebuilds in 59s.

**The pair holds `cc` too since 2026-09-10, because the scope the advice recommends has
to be the scope the index serves.** Every surface says "attention = `self,direct,cc`", and
the pair held only `self` and `direct` — the original argument being that `cc` was 6.9M
rows. The 2026-08-26 re-tiering shrank `cc` to 1.18M (self 524k, direct 830k, cc 1.18M,
background 1.18M, noise 55.4M on 2026-09-10), and from then on the recommended scope was
the one shape the subset test rejected: a `self,direct,cc` search fell through to the
global pair and walked the corpus with a heap visit per document. Measured 2026-09-09 on
novel queries: unscoped hybrid p50 1.4–1.9s, `self,direct,cc` **9.2 / 10.2 / 10.0s**, and
the weekly benchmark row read 11.2s for the same scope beside 5.3s unscoped. The
optimized set is `optimized_bm25_priorities` in the catalog (2.5M of 59M rows) and the
production pair was rebuilt in the same maintenance window as the plain REINDEX below.
Measured 2026-09-10 02:23Z, novel queries, thirty minutes after the window and with the
startup prewarm's I/O settled (`io some avg60` 2.7%): unscoped hybrid 1.29 / 1.27 / 1.57s,
`self,direct,cc` **1.5 / 1.3 / 4.9 / 1.5 / 4.1s** (p50 1.5s, from 10s). The window itself:
attention pair rebuilt with `cc` in 4m26s (944 MB + 17 MB), plain `REINDEX` of the global
index in 13m51s (**12 GB → 6.2 GB**) and of the low-volume one in 1m29s (235 → 117 MB);
`cache_residency` went from 9.5% to 38% of a working set that shrank from 23.8 to 17.6 GB.
Two things to know before repeating it: a plain `REINDEX` of the global index blocks
timeline writes and global searches for the whole fourteen minutes, and the deploy that
follows (a changed index fingerprint) runs its own 26 GB prewarm at startup — the first
searches after it read 16s / 45s, which is the prewarm's I/O, not the rebuild.

Repeated 2026-09-19 14:27–14:49Z after the 09-14..16 index-definition flap had doubled
every BM25 index again: plain `REINDEX` of the global index in 15m50s (**12 GB → 6.36 GB**),
the attention index in 4m41s (1.9 GB → 968 MB), the low-volume pair in 47s + 30s
(298 → 148 MB, 41 → 39 MB), each with `lock_timeout = 90s` and a retry loop so the
rebuild queues behind readers instead of wedging them; no lock waiters were seen. All
four passed the cold probe after `pg_buffercache_evict_relation` + `drop_caches`, the
Dagster prewarm fired by itself on the relfilenode change, and `cache_residency` read
**35.9%** of an 18.1 GB working set (from 10.3% of 25.7 GB) within ten minutes.

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

**Search with the fewest, most distinctive words the answering record would contain — not
the question, and not a long bag of generic terms.** Re-measured 2026-08-27 on the labeled
benchmark (68 cases, hybrid, depth 50): bare identifiers score MRR 0.675 (hit@10 19/20),
term bags 0.417 (23/30), sentence-shaped questions 0.290 (8/18). Rewording the nine
questions that returned nothing useful — "how long our money lasts at the current pace of
expenses" → "runway burn rate months of cash remaining" — recovered five of them, from
nothing in the top 50 to ranks 10, 10, 12, 15 and 48. But the sharper finding is inside the
term-bag stratum: **adding generic words to a distinctive anchor hurts.** "Mt Foolery" ranks
#1 while "Woody Mt Foolery cancelled postponed weather" is absent from the top 50; "Sunbeam
Marrakesh" #5 against "customs duty charged to receive package shirt Sunbeam" #41; the
term bags that miss are the ones with no name, number or identifier in them ("meeting recording NASA", "DDP duties customs
prepaid shipment AGH Fulfillment" #45). Each generic term dilutes both the BM25 score and the
embedding neighbourhood, and rank fusion then averages the anchor away. So: search an
identifier alone; anchor on a name, a product, an amount or a subject-line phrase; prefer
several short searches over one long one; and on a miss drop words rather than add them.
The `search` tool says this in its description and attaches a `hint` to a sentence-shaped
query and to a long query with no anchor, because the caller is itself a model and can act
on it; that is why query rewriting is guidance here rather than another model in the
search path. Natural-language questions at 0.29 remain a retrieval-side problem — an agent
will always sometimes pass a user's question through — and are not fixed by guidance.

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
  digits preserve recency. A conversation window's `event_id` is its last member, not
  necessarily the member containing the literal, so a matching window is resolved to its
  member by re-reading only that window's events (same `source`, `context` and hour, through
  `timeline_events_context_time_idx`) for the newest matching windows. Until 2026-10-01 this
  ran `search_text_exact` over every chat event instead -- a trigram scan of the 45 GB heap
  that took 1.6-11s ("Mt Foolery" 7.3s, an amount 10.7s) and was, with the chunk part, the
  host's largest block reader (155 GB in three days). A needle found in more than
  `SEARCH_HYBRID_EXACT_MAX_CHUNK_MATCHES` (5,000) chunks is a word every BM25 hit already
  contains, and the leg returns nothing for it ("Robinhood": 113,227 chunks, grouped for
  7.4s). Measured against the live function over the 25 short labeled needles: identical
  refs and MRR, p50 1.29s -> 0.30s, p90 4.4s -> 1.1s. Since
  2026-09-19 ordinary alphabetic names take the chunk path too: keeping them on the
  full-document recheck cost ~1 GB of heap read and ~2s per two-word search (6.4 GB across
  six probes, the host's largest search-cache evictor) for one labeled proper name at rank 1
  instead of 2.
- **The broad BM25 leg scores a bounded prefix of a huge candidate and previews only what it
  returns.** Its floor and fill candidates are ranked with the bm25 operator, which
  re-tokenizes the document it is given: on 2026-09-30 a one-word search ("Sonoma") spent
  1.86s scoring three 5 MB Drive candidates that did not make the top 10, and 170ms more
  windowing their previews. A candidate over `SEARCH_TEXT_SCORE_SCAN_CHARS` (200k, the same
  prefix chunks and previews read) is scored on that prefix -- kept and ranked last if the
  prefix scores zero, never dropped -- and previews are windowed after `LIMIT`. Keyword MRR
  was identical over the 74 labels (0.3016) and one-word searches went 3.8s -> 0.3s with the
  same top 10.
- **Every hybrid search logs where its time went.** `search completed` carries
  `embed_ms`/`lexical_ms`/`exact_ms`/`semantic_ms`/`fuse_ms` and the slowest leg; a search
  over two seconds also logs `slow search` with the host's CPU and I/O pressure, load and
  CPU count, which is how C6's "is the host saturated" question is answered per request.
- Hybrid falls back to keyword with an explicit `fallback_reason` when embeddings or
  pgvector are unavailable. Agent sessions are indexed per turn (`kind = 'agent_turn'`); the
  session roll-up row carries headline fields only.

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

**The first measured verdict says the opposite of the 2026-08-23 one, and that is the point
of measuring it.** `marts_ops.search_benchmark` carries the host's own pressure beside the
latency (C6), and the first run to write it — 2026-08-28 15:45 — read **`io_bound`**: I/O
pressure `full avg10` 11.4%, CPU pressure `some avg10` **0.04%**, load **3.12 on 28 cores**,
with hybrid p50 2.59s. So the machine is not short of CPU and the query is not short of
workers; it is waiting on pages, exactly as the working-set section below describes. Read
`saturation` before deciding what kind of problem a slow search is: `cpu_bound` and `idle`
call for a better plan or more parallelism, `io_bound` calls for keeping the index resident.

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
Measured 2026-08-26 on `mew-coolify` (28 vCPU, 26 GB; `shared_buffers` was then 10 GB,
and is **8 GB as of 2026-09-01**): the HNSW index was 8.5 GB, the global BM25 index
10.7 GB, the chunk heap 7.4 GB, the chunk
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
The fixes are structural, not tuning: the drain now keeps **two persisted keysets** in
`ops.search_chunk_sync_state` (row `embeddings`) -- `(built_at, chunk_id)` for freshly
built chunks and `(event_ts, chunk_id)` for the resumable one-time historical walk -- and
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

**The warmer was the evictor, and a forced warm is never free.** Measured 2026-09-20 on
production: `timeline_sync` forced a full search-index warm after every hourly reconcile of
`gmail_email`, `slack_message` and `slack_file` — three independent hourly clocks, so 63
BM25 warms and 17 HNSW loads in 24 hours, ~1.5 TB pushed through an 18 GB cache, with
`pg_prewarm` the largest block reader in `pg_stat_statements` (31 TB since 09-02) and
residency at 19.6% while cold novel searches took 5–8 s. Two things made it worse than
the sum: the HNSW `buffer` pass loaded a 10.3 GB graph into 8 GB of `shared_buffers`, so
every warm evicted every other page Postgres held; and the global BM25 index was
**13.4 GB on disk with 6.4 GB of live segments** — pg_textsearch spills and merges park
displaced pages for deferred reclaim and never truncate the file, so a day after a plain
REINDEX to 6.36 GB the warm was reading 7 GB of dead pages every time. Now the
post-reconcile repair measures residency and goes through the same floor and 24-hour
interval as the cold-cache guard (`restore_search_cache_after_reconcile`), the warm has
no `buffer` pass, and `marts_ops.search_health` carries a `bm25_index_bloat` row
(`resident_bytes` = live segment bytes, `total_bytes` = file bytes, `attention` below
60% live) so the 2x file is a number. The repair for the file is a plain `REINDEX` in a
maintenance window; pg_textsearch 1.4.0 adds `bm25_compact()` for keeping it there.
The RAM question this answered: mew has 2×16 GB DDR4 in A2/B2 with A1/B1 free (128 GB
max) — but do not buy sticks to hold a cache the warmer is flushing.

**`pg_prewarm` in `prefetch` mode is a hint, not a warm, and it lied once.** On
2026-09-19 a forced warm reported 3.46M blocks in 26 seconds and `fincore` on the index
files found **1% of HNSW and 10% of the global BM25 index** resident afterwards:
`prefetch` is an asynchronous `posix_fadvise(WILLNEED)` that the kernel dropped under
memory pressure, and the state row recorded a success. The warm modes are now `read`
(synchronous, 10 GB of HNSW in 9.6s and 6.7 GB of BM25 in 4.4s on the same host) then
`buffer` for HNSW. Verify a warm with `fincore` inside the Postgres container
(`docker exec <pg> fincore -b <relfilepath>*`), never with the log line — and subtract
`Shmem` (the 8 GB of shared buffers) from `/proc/meminfo`'s `Cached` before reading it
as file cache: the host's real page cache is ~10 GB, which is why a 10 GB HNSW graph
cannot stay resident there while the sync workload runs beside it.

**A cold cache with no index-identity change is re-warmed by the five-minute search
health pass, at most once a day.** `prewarm_search_indexes_if_needed` fires on a schema
signature, a database restart, or a REINDEX (the relfilenode fingerprint) — and none of
those happened between 2026-09-16 and 09-19, while the 09-14..16 index-definition flap
(`bm25_attention_lowvol_idx` rebuilt 1,705 times, ~57 TB through the page cache in nine
days) had left residency at 10% for three days with unscoped hybrid at 3-4s. The
`search_chunks` asset now calls `rewarm_search_indexes_if_cold` after it records
`cache_residency`: below `SEARCH_RESIDENCY_REWARM_FLOOR` (20%) and more than
`SEARCH_RESIDENCY_REWARM_MIN_INTERVAL` (24h) since the last warm, it forces one. The
interval is the point: a forced warm that did not stick means something else is evicting
the cache, and the answer to that is `pg_stat_statements ORDER BY shared_blks_read DESC`,
not another 20 GB read every five minutes.

The second coordinate on the fresh keyset is non-negotiable: one chunk-builder batch
stamps thousands of rows with the same `built_at`. A timestamp-only cursor once stopped in
the middle of such a group and permanently skipped 2,525 chunks while every scheduled run
reported green. After both cursors converge, a daily covering-index anti-join independently
proves there are no chunk SHAs without an embedding and repairs any it finds; its own
`orphaned_chunks` health row means cursor convergence can no longer hide that class of loss.

**The 2026-09-01 recurrence had a different pair of evictors and is why the gauges and
keysets are now stricter.** Both HNSW (then 9.2 GB) and BM25 (global 11 GB) were 0%
resident in shared buffers *and* the OS cache. A Slack adapter watermark probe wrapped
`max(synced_at)` around its full user/conversation joins every five minutes (~13 TB/day),
although the bare indexed max takes about 0.15 ms; the adapter now uses the bare probe
while retaining the old joined SQL only for signature compatibility, so deployment does
not reset a 47M-row backfill. The unbounded missing-replies walk contributed another
~16 TB/day: a public sweep kept discovering parents, so its drained cooldown never held.
That walk now persists and resumes the full `(message_datetime, message_ts,
conversation_id)` keyset on every bounded page. Slack upserts also refuse to update when
only the local `synced_at`/`sync_version` observation changed, so a sweep does not keep
feeding old rows back into either the thread walk or timeline adapter. Read the
`cache_residency` row in `marts_ops.search_health` for the current **shared-buffer** fact;
use `fincore`/`mincore` separately when the OS page-cache distinction matters.

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
eight calls past the 60s budget. The BM25 global index is 5.5 GB **immediately after a
plain rebuild**; that is a rebuild target, not a current-size promise. On 2026-09-01 the
global/attention indexes were back at the concurrent-build sizes (about 11 GB / 1.08 GB,
versus 5.5 GB / 527 MB after plain rebuild), which the living residency + latency rows now
make visible. After a safe plain-`REINDEX` window, prewarm BM25 and then HNSW with
`pg_prewarm`; the extension is provisioned with the health layer, but warming is an
operator action rather than a startup scan that can fight recovery. Thus
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

## Landing latency: how long a source takes to reach the timeline

**Measure it from `timeline.events.first_seen_at - event_ts`, never from a `base_*`
table's `ingested_at`/`synced_at`.** Those columns are re-stamped by every rescan and
upsert: `base_apple_messages.messages.ingested_at` read 5-20 *hours* behind on 2026-09-25
for messages the timeline had held within five minutes, because the nightly full rescan
rewrote the column. `first_seen_at` is the first time the row existed in PDW.

```sql
SELECT source, count(*) AS n,
  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM first_seen_at - event_ts))/60) AS p50_min,
  round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM first_seen_at - event_ts))/60) AS p95_min
FROM timeline.events
WHERE first_seen_at >= now() - interval '24 hours' AND event_ts >= now() - interval '3 days'
  AND first_seen_at >= event_ts
GROUP BY 1 ORDER BY p50_min;
```

Measured 2026-09-25 before the fast lane below: WhatsApp p50 4 min / p95 6, Slack DMs
4 / 9, iMessage 5 / 9, Gmail 3 / 6, agent sessions 6-7 / 9, Apple Notes 8 / 11, Drive
17 / 30 (its 30-minute cron), Contacts 47 (hourly), Slack public channels ~3 h / ~6 h
(the non-member sweep rotation, by design), voice memos ~4 h (transcription), WHOOP ~8 h
(the cycle's own clock), Hacker News ~7 days (a comment's `event_ts` is when it was
posted; the frontier walk finds it later). **The chat sources were fast at the source and
slow on the schedule**: every one was two serial five-minute clocks -- the source's own
poll or upload, then the `timeline_sync` tick -- and two uniform 0-5 minute waits is ~5 min
expected, ~10 min worst, which is exactly what the tiers read. Two things in the 7-day
window were outages, not cadence: WhatsApp read ~7 days for 09-11..09-18 (the device was
unpaired until 09-19 and the backlog landed at once) and OpenClaw the same shape until the
SQLite-store fix on 09-20.

**The fast lane removes the second clock.** An ingest asset that just wrote rows --
`whatsapp_drive_ingest`, `apple_messages_drive_ingest`, `agent_sessions_drive_ingest`,
`gmail_mailbox_sync` and the Slack *freshness* stage (`slack_workspace_sync`; coverage,
sweeps and metadata are history and stay on the scheduled pass) -- calls
`land_sources_on_timeline` (`timeline_fast_lane.py`) before it returns, which runs
`TimelineSyncEngine.run_incremental` for that source's adapters only, in the same run: no
queue slot, no subprocess start, no wait for `*/5`. It is **incremental only** and never a
correctness dependency: backfill, refresh, prune, reconcile, retire and first contact stay
with the five-minute `timeline_sync`, which is unchanged and is still what makes the
timeline converge (C1). Three rules keep the two from fighting:

- **Each adapter's incremental pass holds a per-adapter advisory lock**
  (`hashtext('<schema>|timeline_incremental|<adapter>')`), taken by both the fast lane and
  the scheduled pass, and both *skip* rather than wait -- the scheduled pass still loads the
  state row so refresh/backfill/reconcile keep their turn.
- **The stored watermark only ever moves forward**, enforced in `_save_state` itself
  (row-comparison `CASE` in the upsert), so a scheduled pass that saves a stale in-memory
  copy after the fast lane advanced the watermark cannot rewind it.
- **The fast lane only touches an adapter the scheduled pass has initialized**
  (`backfill_done` with a matching `adapter_signature`); first contact and a definition
  reset are the scheduled pass's decisions.

It never raises into the ingest run (a failure is a warning plus an `errors` entry in the
asset's `timeline_fast_lane` metadata), it runs only when the ingest wrote rows, and
`TIMELINE_FAST_LANE_ENABLED=0` on the Dagster deployment turns it off with no other effect.

The client side moved too: the iMessage and Apple Notes LaunchAgents carry `WatchPaths`
on the store's SQLite WAL (`chat.db-wal`, `NoteStore.sqlite-wal`) with a 60-second
`ThrottleInterval`, so an upload fires seconds after the app writes rather than at the next
five-minute tick (the tick stays as the floor); and the WhatsApp client flushes every 20 s
instead of 60 (`WHATSAPP_FLUSH_INTERVAL_SECONDS`), snapshotting its session only when a
flush actually shipped something. The remaining floor for WhatsApp and iMessage is the
Drive inbox sensor's 60-second minimum interval plus one Dagster run start; for Slack DMs it
is the freshness cron itself. Expected after all of it: WhatsApp and iMessage ~1-2 min,
Slack DMs ~3 min, Gmail ~3 min. Re-measure with the query above rather than quoting these.
