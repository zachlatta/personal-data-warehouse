# Personal Data Warehouse (PDW) — the agent guide

One warehouse for everything about Zach: Gmail, Slack, Google Calendar, Google Drive,
Google Contacts, iMessage and SMS, WhatsApp, Apple Notes, Apple Photos, Apple Voice Memos,
WHOOP health, Plaid and statement-backed finances, meeting and voice-memo transcripts, and
every prior AI agent session across all providers and machines. Any question about Zach's
own life starts here — not from priors, not from a web search, and not from a single-source
connector, which cannot join the half of the story that lived in another app. Reads are
safe. Writes are proposals a human approves (topic `mutations`).

This document is the contract for using it well. Read a topic when your question enters
its domain{{if .CLI}} (`pdw readme <topic>`){{else}} (`readme` with `{"topic": "<name>"}`){{end}}; the index is at the end.
Never trust a relation or column name from memory, including one you read here: the
warehouse is re-layered often, and the discovery calls below are what is current.

## The workflow: search first, SQL second

1. **Search** for any text, topic, person, phrase, or identifier:
{{if .CLI}}   `pdw search --priority {{.Attention}} '<fewest distinctive words>'`{{else}}   `search` with `{"query": "<fewest distinctive words>", "priorities": {{jsonList (split .Attention)}}}`{{end}}
   Every hit carries `priority`, `ref`, `source_table` and `source_pk`.
2. **Read the conversation** around a useful hit — a Gmail hit returns its thread, a Slack
   hit its thread or channel, a chat hit the rest of that chat, an agent turn its
   neighbouring turns:
{{if .CLI}}   `pdw sql -q 'context around a hit' "SELECT * FROM timeline.context('<ref>', 5, 5)"`{{else}}   `query` with `{"queries": [{"question": "context around a hit", "sql": "SELECT * FROM timeline.context('<ref>', 5, 5)"}]}`{{end}}
3. **Structured questions** (aggregates, joins, predicates, drill-down) are SQL, walked
   in layer order: bounded `timeline.events` filtered by priority → `marts_*` (stable
   per-domain read views) → `base_*` (raw provider rows, reached from a hit's
   `source_table`/`source_pk`). `derived_*` holds the modelled facts between them.
4. **Before any SQL**, confirm every relation's columns:
{{if .CLI}}   `pdw columns <schema.relation>` — and `pdw schema` only to find a relation you do not know.{{else}}   `describe_table` with `{"relation": "<schema.relation>"}` — and `schema_overview` only to find a relation you do not know.{{end}}

Do not open with schema discovery because SQL may be useful later; do not `ILIKE` a raw
`base_*` body column (it times out, and the SQL tool warns on that shape); do not guess a
second name after a 42703/42P01 — re-check the columns and read the server's hint. This
order is measured: `marts_ops.agent_usage` grades every agent source on search-first
sessions (target ≥ 60%), searches carrying a priority filter (≥ 40%), and SQL-error
sessions (< 10%).

## Command map

{{if .CLI -}}
| You want | Run |
| --- | --- |
| This guide, or one topic | `pdw readme [topic]` (bare `pdw` prints the guide) |
| Search every source | `pdw search [--priority TIERS] [--source NAMES] [--since DATE] [--mode hybrid\|keyword\|exact] [-n N] '<terms>'` |
| Read-only SQL | `pdw sql --output json -q '<why>' '<SQL>'` (multi-line SQL: `--file q.sql` or stdin) |
| One relation's exact columns | `pdw columns <schema.relation>` |
| Every relation with row estimates | `pdw schema` |
| The other tools (`get_object`, `notify`, `propose_mutation_help`, `propose_mutation`) | `pdw list`, `pdw describe <tool>`, `pdw call <tool> --data '<json>'` |
| A local uploader or credential publisher | `pdw ingest <source>`, `pdw slack\|chatgpt\|whoop publish-session` (topic `ingest`) |
| Setup and upkeep | `pdw login`, `pdw config show`, `pdw version`, `pdw update --check` |

Commands agents invent that do not exist: `pdw query`, `pdw schema_overview`,
`pdw describe_table`, `pdw call sql|query|search|schema_overview|describe_table` (each is
refused with the real command), and `pdw --version` (it is `pdw version`). Always pass
`--output csv|json|nd-json` and a real `-q` intent in scripts; the SQL tool logs the
intent server-side.
{{- else -}}
| You want | Call |
| --- | --- |
| This guide, or one topic | `readme` (`{}` or `{"topic": "<name>"}`) |
| Search every source | `search` `{"query": "...", "priorities": [...], "sources": [...], "since": "YYYY-MM-DD", "mode": "hybrid|keyword|exact", "max_results": N}` |
| Read-only SQL | `query` `{"queries": [{"question": "<why>", "sql": "<SQL>"}], "format": "csv|json|ndjson"}` |
| One relation's exact columns | `describe_table` `{"relation": "<schema.relation>"}` |
| Every relation with row estimates | `schema_overview` `{}` |
| Bytes of a stored attachment, photo, recording or Slack file | `get_object` `{"storage_file_id": "<id>"}` |
| A reviewed write | `propose_mutation_help` `{}`, then `propose_mutation` (topic `mutations`) |
| A push notification to Zach's phone | `notify` |

`query` takes an array so several statements can share one call; each needs a `question`,
which is logged as the caller's intent. The same warehouse is reachable from a shell as
the `pdw` CLI (`pdw readme` prints this guide with CLI spellings); local uploaders and
credential publishers exist only there.
{{- end}}

## Search well

Search with the **fewest, most distinctive words the answering record would contain** — a
name, an id, a product, an amount, a subject-line phrase — not the question, and not a
long bag of generic terms. Measured on the labeled benchmark: a bare identifier scores
MRR 0.68, a term bag 0.42, a sentence-shaped question 0.29, and **adding generic words to
a distinctive anchor hurts** ("Mt Foolery" ranks first; the same anchor inside
"Mt Foolery cancelled postponed weather" is not in the top 50). Search an identifier alone. Prefer several short
searches over one long one. On a miss, drop words rather than add them; when the tool
attaches a `hint`, act on it.

- `hybrid` (default) fuses semantic, BM25 and a gated literal leg by rank; `exact` for a
  literal phrase, email address, phone, amount, URL, path or id (number-format variants
  match: `1441.52` finds `1,441.52`); `keyword` for BM25 only.
- Scope by `sources` (`gmail`, `slack`, `apple_messages`, `whatsapp`, `calendar`, `drive`,
  `contacts`, `notes`, `photos`, `voice_memos`/`transcripts`, `agent_session`, `finance`,
  `whoop`, ...; an unknown token errors with the valid list) and `since` when the request
  gives a natural scope. A Drive file id searched with `exact` returns the file itself.
- **Priority is the filter that turns the whole corpus into an answer.** `noise` is most
  of the timeline, so an attention question without a tier filter comes back as
  newsletters. The tiers and when to use each are below.
- On a search timeout, narrow the scope or lower `max_results`; do not retry unchanged.

More in topic `search`.

## Priority tiers

Every `timeline.events` row carries one of five tiers, classified at sync time; the enum
order is the attention order and the same filter works everywhere ({{if .CLI}}`--priority`, `priorities => ARRAY[...]` in SQL{{else}}`priorities`, `priorities => ARRAY[...]` in SQL{{end}}).

| tier | meaning | typical rows |
| --- | --- | --- |
{{range .Tiers}}| `{{.Name}}` | {{.Meaning}} | {{.TypicalRows}} |
{{end}}
| intent | priorities |
| --- | --- |
{{range .Selections}}| {{.Intent}} | {{if .Priorities}}`{{join .Priorities ","}}`{{else}}omit the filter{{end}} — {{.Guidance}} |
{{end}}
`{{.Sentinel.Name}}` is not a sixth tier: it is {{.Sentinel.Meaning}}. If it appears in a
result, an adapter's classification did not run and the answer is wrong, not merely incomplete.

## SQL rules that prevent the recurring failures

- **Columns first, every time.** Undefined-column and undefined-relation errors are the
  fleet's #1 SQL failure and all come from guessed names. Names that keep being guessed
  wrong: `slack.messages` → `base_slack.messages` (time column `message_datetime`);
  `gmail.messages` → `base_gmail.messages` (`from_address`, not `from_email`);
  `google_calendar.events` → `base_google_calendar.events`; `google_contacts.contacts` →
  `base_google_contacts.cards`; `timeline_events` → `timeline.events`;
  `marts.ai_conversation_events` → `marts_ai_conversations.events`.
- **Booleans are `bigint` 0/1** (`is_from_me = 1`, never `= true`).
- **Absence is the epoch, not NULL.** "Never happened" is stored as
  `1970-01-01 00:00:00+00` in nearly every timestamp column (`date_read`, `end_at`,
  `edited_at`, ...), so `MIN()`, `ORDER BY ... ASC` and `IS NULL` are wrong by default on
  `base_*`; the `marts_*` views translate it back to NULL. Bound on the start column.
- JSON columns are `text` on older sources (cast before `->>`) and real `jsonb` on newer
  ones; some address columns are `text[]` (`ILIKE` on them raises — `unnest` first);
  `round(x, 2)` on double precision fails (`round(x::numeric, 2)`).
- **Budget: 60s per statement server-side**, and the SQL tool warns before running a
  pattern-match over a raw `base_*` table. Respond to a timeout by making the query
  cheaper (search functions, `LIMIT`, date bounds), not by retrying.
- `ops.*` sync state is mostly unreadable by the query role; use `marts_ops.*` (topic `ops`).

More in topic `sql`.

## Where things live

| domain | start at |
| --- | --- |
| everything, cross-source | `timeline.events`, `timeline.search_text()`, `timeline.search_text_exact()`, `timeline.context()` |
| mail | `base_gmail.messages`, `base_gmail.attachments`, `marts_inbox.gmail_threads` |
| chat | `marts_messages.messages` (iMessage + WhatsApp, sender-resolved), `base_slack.messages`, `marts_inbox.slack_items`, `marts_slack.huddles` |
| calendar and contacts | `base_google_calendar.events`, `marts_contacts.contacts`, `marts_contacts.contact_points` |
| files, notes, photos | `base_google_drive.files`, `derived_documents.google_drive_file_texts`, `base_apple_notes.notes`, `marts_photos.photos`, `marts_files.attachments` |
| voice and transcripts | `marts_voice_memos.recordings`, `marts_voice_memos.transcript_segments`, `marts_calendar.events_with_voice_memos` |
| health | `marts_health.cycles`, `.sleeps`, `.recoveries`, `.workouts`; high-resolution `base_whoop_private.*` |
| finance | `marts_finance.net_worth`, `.transactions`, `.accounts`, `.commitments`, `.tax_lots`, `.position_coverage` |
| prior agent sessions | `marts_ai_conversations.sessions`, `marts_ai_conversations.events` |
| is the data current | `marts_ops.pipeline_health`, `marts_ops.table_freshness`, `marts_ops.timeline_adapter_health` |

Topic `sources` has the per-domain detail; `agent-sessions`, `finance`, `health` and
`slack` go deeper.

## Absence is not evidence

The warehouse is complete for what it syncs and silent about what it cannot. Before
reporting a negative, check the source's freshness (`marts_ops.pipeline_health`,
`marts_ops.slack_conversation_health`) and the known gaps: nothing said in a Slack huddle
reaches PDW (metadata only, `marts_slack.huddles`); a bank may hand Plaid only ~90 days
of history, so older spending lives in the statement corpus
(`base_manual_finance.documents`); Slack public channels Zach is not a member of are
swept slowly and were frozen for months before 2026-08-27. State the window and the
freshness of what you read.

## Writes

Nothing here writes upstream directly. {{if .CLI}}`pdw call propose_mutation_help` lists the supported
types; `pdw call propose_mutation --data '<json>'`{{else}}`propose_mutation_help` lists the supported
types; `propose_mutation`{{end}} queues a request for human review and returns an
`approval_url`. Gmail (send, archive, labels), Google Calendar, Google Contacts, Slack
mark-read and Apple Notes are supported. Topic `mutations`.

{{if .CLI -}}
## Setup

`pdw login` saves the warehouse URL and token to `~/.config/pdw/config.json`; `PDW_API_URL`
and `PDW_SECRET_TOKEN` override it. The binary self-updates in the background — leave that
on, and when behaviour looks stale run `pdw version` and `pdw update --check` before
debugging anything else. `pdw ingest <source>` runs a local uploader against the machine's
own data and writes to the warehouse: never run it speculatively (topic `ingest`).

{{end -}}
## Topics

{{range .Topics}}- `{{.Name}}` — {{.Summary}}
{{end}}
{{if .CLI}}Read one with `pdw readme <topic>`.{{else}}Read one with `readme` and `{"topic": "<name>"}`.{{end}}
