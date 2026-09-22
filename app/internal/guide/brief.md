# PDW — the agent guide (brief)

One warehouse for everything about Zach: Gmail, Slack, Calendar, Drive, Contacts, iMessage,
WhatsApp, Notes, Photos, voice-memo transcripts, WHOOP health, finances, and every prior AI
agent session. Any question about Zach's own life starts here. Reads are safe; built-in
writes need human approval. Relation and column names change often — never trust memory,
including this page. This is the brief; {{if .CLI}}`pdw readme full`{{else}}`readme` with `{"topic": "full"}`{{end}} is the long form.

## Workflow: search first, SQL second

1. **Search** with the fewest, most distinctive words the answering record would contain
   (a name, an id, a subject line, an amount) — not the question. On a miss drop words.
{{if .CLI}}   `pdw search --priority {{.Attention}} '<terms>'`  (`-n 10` default; `--source`, `--since`, `--mode exact` for ids/paths/amounts; `--full` for long previews){{else}}   `search` `{"query": "<terms>", "priorities": {{jsonList (split .Attention)}}}` (also `sources`, `since`, `mode: exact` for ids/paths/amounts){{end}}
2. **Read the conversation** around a hit (its email thread, Slack thread/channel, chat, or
   neighbouring agent turns):
{{if .CLI}}   `pdw context '<ref>'` (or `pdw sql -q why "SELECT event_ts, actor, snippet FROM timeline.context('<ref>', 5, 5)"`){{else}}   `query` `{"queries": [{"question": "why", "sql": "SELECT event_ts, actor, snippet FROM timeline.context('<ref>', 5, 5)"}]}`{{end}}
3. **SQL** only for aggregates, joins, predicates and drill-down, walking
   `timeline.events` (bounded, priority-filtered) → `marts_*` → `base_*`.
4. **Columns before SQL, every time:** {{if .CLI}}`pdw columns <schema.relation>`; `pdw schema` only to find a relation you do not know.{{else}}`describe_table` `{"relation": "<schema.relation>"}`; `schema_overview` only to find a relation you do not know.{{end}}

Do not open with schema discovery; do not `ILIKE` a raw `base_*` body column (it times out);
after a 42703/42P01 read the server's hint — it lists the real columns — instead of guessing again.

{{if .CLI -}}
## Command map

| You want | Run |
| --- | --- |
| This brief / the full guide / one topic | `pdw readme [full\|topic]` (bare `pdw` prints the brief) |
| Search | `pdw search [--priority TIERS] [--source NAMES] [--since DATE] [--mode hybrid\|keyword\|exact] [-n N] [--full] '<terms>'` |
| Conversation around a hit | `pdw context '<ref>' [--before N] [--after N]` |
| Read-only SQL | `pdw sql -q '<why>' '<SQL>'` (CSV by default; `--output json\|nd-json`; multi-line SQL via `--file` or stdin) |
| One relation's exact columns | `pdw columns <schema.relation>` |
| Every relation with row estimates | `pdw schema` |
| Other tools (`get_object`, `notify`, `propose_mutation_help`, `propose_mutation`, connected `<connection>__<tool>`) | `pdw list`, `pdw describe <tool>`, `pdw call <tool> --data '<json>'` |
| Setup | `pdw login`, `pdw version`, `pdw update --check`; uploaders: `pdw ingest <source>` (topic `ingest`) |

Not commands: `pdw query`, `pdw schema_overview`, `pdw describe_table`, `pdw call sql|query|search` (each is refused with the real one).
{{- else -}}
## Tool map

| You want | Call |
| --- | --- |
| This brief / the full guide / one topic | `readme` (`{}`, `{"topic": "full"}` or `{"topic": "<name>"}`) |
| Search | `search` `{"query", "priorities", "sources", "since", "mode", "max_results"}` |
| Read-only SQL | `query` `{"queries": [{"question", "sql"}], "format": "csv|json|ndjson"}` (CSV by default) |
| One relation's exact columns | `describe_table` `{"relation"}` |
| Every relation with row estimates | `schema_overview` `{}` |
| Bytes of a stored attachment, photo, recording or Slack file | `get_object` `{"storage_file_id"}` |
| A reviewed write | `propose_mutation_help`, then `propose_mutation` (topic `mutations`) |
| Connected upstream MCP servers (skills, tasks, other warehouses) | `connections` to list, `connection_call` to invoke (topic `connections`) |
| A push notification to Zach's phone | `notify` |
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

- Booleans are `bigint` 0/1 (`is_deleted = 0`, never `= true`). Absence is the epoch
  `1970-01-01`, not NULL, on `base_*`; `marts_*` translate it. Name the time column per source
  (`base_gmail.messages.internal_date`, `base_slack.messages.message_datetime`, `timeline.events.event_ts`).
- Read mail bodies from `body_markdown_clean`, not `body_text` (tracking URLs). JSON is `text` on
  older sources — cast before `->>`. 60s statement budget; a timeout means narrow, not retry.
- `ops.*` is unreadable by the query role; `marts_ops.*` is the health surface (topic `ops`).

## Where things live

`timeline.events` (everything) · mail `base_gmail.messages`, `marts_inbox.gmail_threads` · chat
`marts_messages.messages`, `base_slack.messages` · calendar/contacts `base_google_calendar.events`,
`marts_contacts.contacts` · files/notes/photos `base_google_drive.files`, `base_apple_notes.notes`,
`marts_photos.photos`, `marts_files.attachments` · voice `marts_voice_memos.recordings` · health
`marts_health.*` · finance `marts_finance.net_worth`, `.transactions` · prior agent sessions
`marts_ai_conversations.sessions`, `.events` · freshness `marts_ops.pipeline_health`.

Absence is not evidence: check freshness before reporting a negative, and state the window.

## Topics

{{range .Topics}}- `{{.Name}}` — {{.Summary}}
{{end}}
{{if .CLI}}Read one with `pdw readme <topic>`; `pdw readme full` is the complete guide.{{else}}Read one with `readme` and `{"topic": "<name>"}`; `{"topic": "full"}` is the complete guide.{{end}}
