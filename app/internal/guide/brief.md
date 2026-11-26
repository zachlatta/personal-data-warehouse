# PDW — the agent guide (brief)

One warehouse for everything about Zach: Gmail, Slack, Calendar, Drive, Contacts, iMessage,
WhatsApp, Notes, Photos, screenshots, voice-memo transcripts, WHOOP health, finances, and every prior AI
agent session. Any question about Zach's own life starts here. Reads are safe; built-in
writes need human approval. Relation and column names change often — never trust memory,
including this page. This is the brief; {{if .CLI}}`pdw readme full`{{else}}`readme` with `{"topic": "full"}`{{end}} is the long form.

## Workflow: search first, SQL second

1. **Search** with the fewest, most distinctive words (name, id, subject, amount) — not the question. On a miss drop words.
{{if .CLI}}   `pdw search --priority {{.Attention}} '<terms>'` (`-n 10`; `--source`, `--since`, `--mode exact`, `--full`){{else}}   `search` `{"query": "<terms>", "priorities": {{jsonList (split .Attention)}}}` (also `sources`, `since`, `mode: exact`){{end}}
2. **Read the conversation** around a hit (email/Slack thread, chat, or neighbouring agent turns):
{{if .CLI}}   `pdw context '<ref>'` (`--full` for whole texts); a hit's `sql:` line reads its source row verbatim{{else}}   `context` `{"ref": "<ref>"}` (`before`/`after` 5, `full: true` for whole texts); `drill_sql` reads its source row verbatim{{end}}
3. **SQL** only for aggregates, joins, predicates and drill-down, walking
   `timeline.events` (bounded, priority-filtered) → `marts_*`/`derived_*` → `base_*`.
4. **Columns before SQL, every time, and wait for them:** {{if .CLI}}`pdw columns <schema.relation>`; `pdw schema` only for unknown relations.{{else}}`describe_table` `{"relation": "<schema.relation>"}`; `schema_overview` only for unknown relations.{{end}}
   Read the output before writing SQL (parallel or fallback calls are still guesses).

Never open with schema discovery or `ILIKE` a raw `base_*` body column (times out);
after 42703/42P01 read the server's hint (it lists the real columns) instead of guessing.

{{if .CLI -}}
## Command map

| You want | Run |
| --- | --- |
| This brief / the full guide / one topic | `pdw readme [full\|topic]` (bare `pdw` prints the brief) |
| Search | `pdw search [--priority TIERS] [--source NAMES] [--since DATE] [--mode hybrid\|keyword\|exact] [-n N] [--full] '<terms>'` |
| Conversation around a hit | `pdw context '<ref>' [--before N] [--after N] [--full]` |
| Read-only SQL | `pdw sql -q '<why>' '<SQL>'` (CSV by default; `--output json\|nd-json`; multi-line SQL via `--file` or stdin) |
| One relation's exact columns | `pdw columns <schema.relation>` |
| Every relation with row estimates, or one schema / layer | `pdw schema [marts_finance\|marts\|base_]` |
| Other tools (`get_object --save <path>` for file/screenshot bytes, `notify`, `propose_mutation*`, `<conn>__<tool>`) | `pdw list`, `pdw describe <tool>`, `pdw call <tool> --data '<json>'`; built-ins return a plain JSON object (no MCP content array); `--output text` prints it |
| Put cards in Zach's Feed tab, read his engagement | `pdw feed push\|edit\|remove\|restore\|history\|stats` (topic `feed`) |
| Setup | `pdw login`, `pdw version`, `pdw update --check`; uploaders: `pdw ingest <source>` (topic `ingest`) |

Not commands: `pdw query` (`pdw sql`), `pdw tools`/`pdw connections` (`pdw list`), `pdw schema_overview`, `pdw describe_table`, `pdw call sql|query|search|context`.
{{- else -}}
## Tool map

| You want | Call |
| --- | --- |
| This brief / the full guide / one topic | `readme` (`{}`, `{"topic": "full"}` or `{"topic": "<name>"}`) |
| Search | `search` `{"query", "priorities", "sources", "since", "mode", "max_results"}` |
| Conversation around a hit | `context` `{"ref", "before", "after", "full"}` |
| Read-only SQL | `query` `{"queries": [{"question", "sql"}], "format": "csv|json|ndjson"}` (CSV by default) |
| One relation's exact columns | `describe_table` `{"relation"}` |
| Every relation with row estimates, or one schema / layer | `schema_overview` `{}` or `{"schema": "marts_finance"}` |
| Stored screenshot/photo/file bytes | `get_object` `{"storage_file_id", "inline": true}` (auto-resizes PNG/JPEG ≤ 1568 px; use preview/thumbnail ids below) |
| A reviewed write | `propose_mutation_help`, then `propose_mutation` (topic `mutations`) |
| Connected upstream MCP servers (skills, tasks, other warehouses) | `connections` to list, `connection_call` to invoke (topic `connections`) |
| A push notification to Zach's phone | `notify` |
| Put cards in Zach's Feed tab, read his engagement | `feed_push`, `feed_update`, `feed_history`, `feed_stats` (topic `feed`) |
| The `pdw` CLI in your own shell (a sandbox, VM or container) | `cli_authorize`, then run the one-line command it returns (24 h token) |
{{- end}}

## Priority tiers (the filter that turns the corpus into an answer)

| tier | meaning |
| --- | --- |
{{range .Tiers}}| `{{.Name}}` | {{.Meaning}} |
{{end}}
| intent | priorities |
| --- | --- |
{{range .Selections}}| {{.Intent}} | {{if .Priorities}}`{{join .Priorities ","}}`{{else}}omit the filter{{end}} |
{{end}}
`{{.Sentinel.Name}}` is not a tier: {{.Sentinel.Meaning}}.

## SQL traps

- PDW is PostgreSQL 18, not ClickHouse. `timeline.events` has `occurred_at`, `actor`, `snippet`,
  `search_text` and `adapter` + `event_id`; `ref`, `who` and `text` exist only on
  `timeline.search_text()` rows. There is no `event_ts` (renamed `occurred_at` on 2026-10-25).
- Booleans are `bigint` 0/1 (`is_deleted = 0`, never `= true` or `NOT is_deleted`). Absence is the epoch
  `1970-01-01`, not NULL, on `base_*`; `marts_*` translate it. Name the time column per source
  (`base_gmail.messages.internal_date`, `base_slack.messages.message_datetime`, `timeline.events.occurred_at`).
- Read mail bodies from `body_markdown_clean`, not `body_text` (tracking URLs). JSON is `text` on
  older sources — cast before `->>`; `jsonb` needs `::text` before `LIKE`. 60s statement budget;
  a timeout means narrow, not retry.
- Keys are `<thing>_id`, almost never a bare `id`: `base_gmail.messages.message_id`, `base_feed.items.item_id`.
  Slack: `conversation_id` (not `channel_id`), `user_id`/`username` (not `user_name`), channel names in
  `base_slack.conversations`. `search_text()` rows name the sender `who` (not `actor`); agent `events`
  have `role`, `occurred_at` (no `actor`, `created_at`). Regex repeat counts max out at 255: use `.{0,255}`, or `substr(col, greatest(1, strpos(col, x) - 300), 600)`.
- `ops.*` is unreadable by the query role; `marts_ops.*` is the health surface (topic `ops`).
  A mutation's status: `ops.upstream_mutation_requests.id` (not `request_id`), with `context_json`
  and `result_json`; its operations carry `request_id` and the type in `operation`.

- **Screenshots (through PDW, not SSH):** phone screenshots → {{if .CLI}}`pdw search --source photos`{{else}}`search` `sources: ["photos"]`{{end}} / `marts_photos.photos` (`thumbnail_storage_file_id`); periodic frames (Mac & phone) → {{if .CLI}}`pdw search --source screen_timelapse` (omit `--priority` — frames are `noise`){{else}}`search` `sources: ["screen_timelapse"]` (omit `priorities` — frames are `noise`){{end}} or `marts_screen_timelapse.active_screens` (`preview_storage_file_id`); raw `base_screen_timelapse.screenshots`; view bytes via {{if .CLI}}`pdw call get_object --data '{"storage_file_id":"<id>"}' --save x.png`{{else}}`get_object` `{"storage_file_id":"<id>","inline":true}`{{end}}. (Manual Mac PNGs are not in PDW.)

## Where things live

`timeline.events` (everything) · mail `base_gmail.messages`, `marts_inbox.gmail_threads` · chat
`marts_messages.messages`, `base_slack.messages` · calendar/contacts `base_google_calendar.events`,
`marts_contacts.contacts` · files/notes/photos `base_google_drive.files`, `base_apple_notes.notes`,
`marts_photos.photos`, `marts_files.attachments` · screens `marts_screen_timelapse.active_screens` ·
voice `marts_voice_memos.recordings` · health `marts_health.*` · finance `marts_finance.net_worth`,
`.transactions` · sessions `marts_ai_conversations.sessions`, `.events` · freshness
`marts_ops.pipeline_health`.

Absence is not evidence: check freshness before reporting a negative, and state the window.

## Topics

{{if .CLI}}`pdw run script.js`{{else}}`run`{{end}} composes dependent reads in server-side JavaScript; topic `scripts` has the API and limits.

{{range .Topics}}- `{{.Name}}` — {{.Summary}}
{{end}}
{{if .CLI}}Read one with `pdw readme <topic>`; `pdw readme full` is the complete guide.{{else}}Read one with `readme` and `{"topic": "<name>"}`; `{"topic": "full"}` is the complete guide.{{end}}
