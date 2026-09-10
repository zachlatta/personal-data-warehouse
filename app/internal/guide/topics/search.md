# Search

Search is one hybrid path over `timeline.events`: BM25 keyword retrieval, semantic
retrieval (one ANN leg per query representation) and a gated literal-substring leg, fused
by reciprocal rank. {{if .CLI}}`pdw search`{{else}}The `search` tool{{end}} is the only entry point that runs all of it; the SQL
functions below are the pieces an SQL-native workflow can call on its own.

## Query shape decides the result

Measured on the labeled benchmark (68 cases, hybrid, depth 50): a bare identifier scores
MRR 0.68 (hit@10 19/20), a term bag 0.42, a sentence-shaped question 0.29. Inside the
term-bag stratum, **adding generic words to a distinctive anchor hurts** — "Mt Foolery" #1
against "Mt Foolery cancelled postponed weather" absent from the top 50; "Sunbeam
Marrakesh" #5 against "customs duty charged to receive package shirt Sunbeam" #41. The
queries that miss are the ones with no name, number or identifier in them.

So: anchor on a name, a product, an amount, an id or a subject-line phrase; search an
identifier **alone**; rewrite a question into the words the answering record would use
("how long our money lasts" → "runway burn rate months cash remaining"); prefer several
short searches over one long one; on a miss drop words, never add. The tool attaches a
`hint` to a sentence-shaped query and to a long query with no anchor — re-issue rather
than paging deeper.

## Modes

- `hybrid` (default): semantic + keyword + literal. Falls back to keyword with an explicit
  `fallback_reason` when embeddings or pgvector are unavailable.
- `exact`: case-insensitive literal substring, recency-ordered. Use it for an email
  address, phone number, amount, URL, path, file id or error string. Number-format
  variants match (`1441.52` finds `1,441.52`; phone punctuation is ignored). A Google
  Drive file id searched this way returns the file itself, not only the mail about it.
- `keyword`: BM25 only, for when you specifically want lexical ranking.

## Scoping

- `priorities` — the attention tiers (`{{.Attention}}` for correspondence, `self` for
  Zach's own words, `self,background` for prior agent conclusions, `noise` when automation
  is the subject). Omit it for broad discovery. An unknown tier errors with the valid list.
- `sources` — aliases are accepted: `gmail`/`email`, `slack`, `apple_messages`/`imessages`,
  `whatsapp`, `calendar`, `drive`/`gdrive`, `contacts`, `notes`, `photos`,
  `voice_memos`/`transcripts`, `agent_session`/`agent_sessions`, `finance`, `whoop`,
  `slack_files`, `mutations`. An unknown token errors with the valid set.
- `since` — a lower bound on event time (`2026-03-01`). It is also the lever that makes
  a scoped Google Drive search cheap: Drive documents are multi-megabyte and scoring 50
  of them costs seconds.
- `max_results` — default 20. Raise it for recall work only; a scoped search pays per
  returned row.

## Reading a hit

Every hit carries `priority`, `ref`, `source_table` and `source_pk`. `source_table` is the
catalog's logical id (`gmail_messages`, `slack_messages`), not a physical name;
`source_pk` is the JSON primary key of the authoritative row. One hop reaches the raw
record; usually the better hop is the conversation:

```sql
SELECT * FROM timeline.context('<ref>', 5, 5);
```

`timeline.context(ref, before, after)` returns the hit's REAL conversation, chosen per
source: a Gmail hit returns its thread, a Slack hit its thread when it is a reply or a
parent with replies and otherwise the messages around it in that conversation, an
iMessage or WhatsApp hit the rest of that chat, an agent turn the neighbouring turns of
its session. Everything else returns the neighbouring events in time of the same
`(source, context)` stream. It is bounded at 50 a side and costs ~200ms in production.

## The SQL functions

```sql
SELECT * FROM timeline.search_text('budget approval', 20, priorities => ARRAY['self','direct']);
SELECT * FROM timeline.search_text_exact('invoice 4831', 20, sources => ARRAY['gmail']);
SELECT * FROM timeline.search_text('offer letter', 20, since => '2026-01-01');
```

The named parameters are exactly `max_results`, `sources`, `since` and `priorities`; any
other name raises. Hits carry `occurred_at` and a `text` preview windowed around the
first matched term (`timeline.events` itself uses `event_ts` and `snippet`).
`timeline.search_hybrid` exists in the schema but is **not callable from plain SQL** — it
takes a precomputed query embedding only the app can produce (calling it with text fails
with 42883). Hybrid retrieval from SQL does not exist; use {{if .CLI}}`pdw search`{{else}}the `search` tool{{end}}.

## What a miss means

- A zero-result response says how to recover (drop words, widen tiers, try `exact`).
  A partial search-layer failure is a warning in the response; a total one is an error.
  An empty result is never a broken search layer in disguise.
- `ERROR: invalid page index at block N (SQLSTATE XX001)` means a BM25 index was left
  corrupt by a Postgres crash. It is not a query problem: report it and check
  `marts_ops.search_health` component `bm25_indexes`.
- **The search document is not the whole row.** For agent sessions it is the title, the
  first prompt, the working directory and the user/assistant turns — tool calls and tool
  results are not in it. For Slack, WhatsApp and iMessage it is the message plus any
  attachment enrichment text. A path or error that only ever appeared in tool output is
  found in `marts_ai_conversations.events`, not by search.
- A negative from search is bounded by coverage: check `marts_ops.pipeline_health` and
  the source's own health row (topic `ops`) before reporting that something never happened.
