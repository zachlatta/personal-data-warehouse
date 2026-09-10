# SQL

{{if .CLI}}`pdw sql` is the one way to run SQL from the CLI{{else}}`query` is the one way to run SQL over MCP{{end}}: read-only, one bounded complete
result, a 60-second statement budget. Every statement carries a plain-English
`question`, logged as the caller's intent.

{{if .CLI -}}
```sh
pdw sql --output json -q 'newest attention events today' \
  "SELECT event_ts, priority, source, actor, title FROM timeline.events
   WHERE priority IN ('self','direct','cc') AND event_ts >= now() - interval '1 day'
   ORDER BY event_ts DESC LIMIT 50"
pdw sql -q 'why' --file query.sql      # multi-line SQL from a file
pdw sql -q 'why' < query.sql           # or stdin; both avoid shell quoting
```

Always pass `--output csv|json|nd-json` in a script (the default prints a note on stdout)
and a real `-q`. The server's SQL error, timeout and shape hints arrive on stderr; rows
stay clean on stdout.
{{- else -}}
```json
{"queries": [{"question": "newest attention events today",
              "sql": "SELECT event_ts, priority, source, actor, title FROM timeline.events WHERE priority IN ('self','direct','cc') AND event_ts >= now() - interval '1 day' ORDER BY event_ts DESC LIMIT 50"}],
 "format": "json"}
```

Several statements may share one call; each returns its own result or error.
{{- end}}

## The layers, and the order to walk them

| layer | what it holds | when to read it |
| --- | --- | --- |
| `timeline` | one row per real-world event from every source, plus the search functions | first: what happened, who said it, attention filtering |
| `marts_<domain>` | stable, conformed read views (identity resolved, units normalised, sentinels translated) | structured questions inside one domain |
| `derived_<domain>` | modelled facts: identity links, ledger observations, enrichment, transcripts, chunks | when a mart's provenance matters |
| `base_<source>` | a faithful copy of the provider's rows, every field | drill-down from a hit's `source_table`/`source_pk`; a field no mart exposes |

Raw rows never learn about identity; the marts do. A question about "who" is answered
from a mart (`marts_contacts.contacts`, `marts_messages.messages`), not by joining
handles by hand. `ops`, `private` and `internal` are hidden and not a query surface; the
read-only role cannot read most of `ops.*` — freshness is `marts_ops.table_freshness`.

## Columns first

Every SQL failure class that recurs comes from a guessed name. Before writing a statement:
{{if .CLI}}`pdw columns <schema.relation>`{{else}}`describe_table`{{end}} for **every** relation it references — exact column names, exact
Postgres types (`text[]` and `bigint` rather than information_schema's `ARRAY`), indexes
and a row estimate. On a 42703 (column) or 42P01 (relation) error, the server names real
candidates: read them, re-check, and do not guess a second name from the first failure.

## Type traps

- **Booleans are `bigint` 0/1.** `is_from_me = 1`, `is_deleted = 0`. Never `= true`.
- **Absence is the epoch sentinel, not NULL.** Columns are overwhelmingly `NOT NULL` and
  "has not happened" is stored as `1970-01-01 00:00:00+00`: `base_whoop.cycles.end_at`
  for the cycle in progress, `base_apple_messages.messages.date_read` on unread
  messages (41% of recent rows), `edited_at`, `authorized_at`, and so on. `MIN()`,
  `ORDER BY col ASC` and `IS NULL` are therefore wrong by default on `base_*`; the
  `marts_*` views translate the sentinel to NULL on every exposed timestamp. Bound and
  sort on the start column.
- **JSON is `text` on older sources** (Slack, Gmail, Calendar, Apple, WhatsApp, agent
  session tables): cast before `->>`. Newer tables carry real `jsonb`
  (`base_whoop_private.documents.raw_json`). The column type from describe settles it.
- **Some address columns are `text[]`.** `base_gmail.messages.from_address` is `text`,
  but `to_addresses`, `cc_addresses` and `bcc_addresses` are arrays: `ILIKE` on them
  raises 42883; use `EXISTS (SELECT 1 FROM unnest(to_addresses) a WHERE a ILIKE '%…%')`.
- `round(x, 2)` on double precision fails; `round(x::numeric, 2)`.
- Compare time columns to timestamps, never to epoch integers.
- Ranges (`during`, `days` in WHOOP private tables) are Postgres range notation
  (`['start','end')`): read the bounds, do not cast the string.

## The budget

| budget | value |
| --- | --- |
| server statement timeout | 60s |
{{if .CLI}}| `pdw sql` client wait | 75s, deliberately above the server's, so the server's error (with its hint) is what you see |
{{end}}| public edge | ~100s |

A slow query is answered by making it cheaper: the search functions instead of a body
scan, a `LIMIT`, an indexed time bound, a `sources` scope. A pattern match (`ILIKE`,
`~`, `regexp_*`, `position()`) over a `base_*` table with no `timeline.` reference is the
shape behind every timeout in a fortnight of sessions, and the tool warns before running
it. `timeline.events` is ~50M rows: bound it by `event_ts` and `priority`, and never
`count(*)` it — the row estimate in the schema overview is the sizing number.

## Row-count and freshness questions

- Row estimates: `schema_overview` prints `(~N rows, estimated)` per relation.
- Freshness: `SELECT pipeline, status, last_write_at, newest_event_at FROM marts_ops.pipeline_health`
  and `marts_ops.table_freshness` (`last_write_at`, `data_age_seconds`) per table.
- Which sources feed the timeline and how far behind each is:
  `marts_ops.timeline_adapter_health`.
