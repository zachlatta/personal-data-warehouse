# Personal Data Warehouse App

This is the Go app that fronts the Postgres warehouse. It exposes a tool
registry over two surfaces:

- **MCP** at `/mcp` — the default flow, used by Claude connectors. OAuth-protected.
- **HTTP API** at `/api/tools` — for CLI and script use. Static-bearer protected.

Each tool declares which surfaces it appears on:

- **MCP-only**: `query` — the single read-only SQL operation for an LLM; each bounded result is returned in full.
- **CLI-only**: `sql` — psql-style "give me the whole result" for terminal/script use.
- **Both**: `search`, `schema_overview`, `describe_table`, and the `propose_*`
  mutation tools.

## Environment

Required:

```bash
POSTGRES_DATABASE_URL=...
PDW_SECRET_TOKEN=...
MCP_BASE_URL=https://your-public-coolify-domain
```

Optional:

```bash
MCP_ADDR=:8080
MCP_MAX_ROWS=100000
MCP_MAX_FIELD_CHARS=4000
MCP_QUERY_TIMEOUT=60s
PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
PDW_OBJECT_STORE_GOOGLE_TOKEN_JSON_B64=<authorized-user-token-json>
PDW_INGEST_AGENT_SESSIONS_FOLDER_ID=<optional-source-folder-id>
PDW_INGEST_APPLE_MESSAGES_FOLDER_ID=<optional-source-folder-id>
PDW_INGEST_WHATSAPP_FOLDER_ID=<optional-source-folder-id>
PDW_INGEST_VOICE_MEMOS_FOLDER_ID=<optional-source-folder-id>
PDW_INGEST_APPLE_NOTES_FOLDER_ID=<optional-source-folder-id>
```

`MCP_MAX_FIELD_CHARS` bounds text previews and other descriptive tool fields;
the MCP `query` result itself is returned without per-field truncation, up to
`MCP_MAX_ROWS`.

`PDW_SECRET_TOKEN` is the shared secret. It does triple duty: signing key for
the MCP OAuth bearer tokens, the value entered on the OAuth authorize page
during connector setup, and the raw bearer the HTTP API expects in
`Authorization: Bearer <client_name>:<token>`. It must be at least 32
characters; use a high-entropy random value. Rotating it invalidates existing
MCP sessions and any CLI/API clients holding the old token.

Every authenticated request must identify the calling client by name (e.g.
`claude`, `codex`, `hermes`, `claude-cli`) so the per-request log line shows
who's calling. See the HTTP API and Claude Connector sections below.

`MCP_SECRET_TOKEN` is still read as a fallback for one release so existing
deployments keep working — set `PDW_SECRET_TOKEN` and drop the legacy var
when convenient.

The mutation proposal tools and the review UI at `/mutation-review` are always on: the review
UI is the web app (`internal/webapp`, also serving `/timeline` and `/search`), a static SPA over
the same JSON API the iOS app uses, authenticated with `PDW_SECRET_TOKEN` like every API client.
The old `PDW_MUTATION_UI_PASSWORD` / `PDW_MUTATION_UI_SESSION_SECRET` /
`PDW_MUTATION_UI_SESSION_TTL_SECONDS` settings are ignored and logged as deprecated.

Set `PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID` and `PDW_OBJECT_STORE_GOOGLE_TOKEN_JSON` (or
`_B64`) to enable object downloads and client upload ingestion. Local uploaders POST signed domain
payloads to `/ingest/...`; the app owns the Drive credential, object keys, `kind` values, and
`pdw_*` tags. Per-source `PDW_INGEST_<SOURCE>_FOLDER_ID` vars are optional and otherwise fall back
to the shared object-store folder.

## Run Locally

```bash
cd app
set -a; source ../.env; set +a
export PDW_SECRET_TOKEN=choose-a-random-local-secret-at-least-32-chars
export MCP_BASE_URL=http://localhost:8080
go run ./cmd/pdw-mcp
```

Endpoints:

```text
http://localhost:8080/mcp           # MCP transport (OAuth-protected)
http://localhost:8080/api/tools     # HTTP API tool list (static-bearer)
http://localhost:8080/api/tools/{name}  # Invoke a tool
```

## Claude Connector Setup

1. Deploy this server at a public HTTPS URL.
2. In Claude, add a custom connector with the MCP URL:

```text
https://your-public-coolify-domain/mcp
```

3. Claude will start the OAuth flow.
4. On the authorization page, enter a **client name** (e.g. `claude`, `claude-laptop`, `claude-work`) — this is what shows up in the server logs for every request the connector makes — and `PDW_SECRET_TOKEN`.

## Coolify

Create a new Dockerfile-based app using this repository.

Use:

```text
Dockerfile path: app/Dockerfile
Exposed port: 8080
```

Set:

```bash
POSTGRES_DATABASE_URL=...
PDW_SECRET_TOKEN=...
MCP_BASE_URL=https://your-public-coolify-domain
```

Do not reuse the root `Dockerfile`; that one runs Dagster.

## HTTP API

Tools marked CLI-only or "both" are reachable here. The MCP-only `query` tool returns `404 tool_not_found`.

### Auth

```http
Authorization: Bearer <client_name>:<PDW_SECRET_TOKEN>
```

The client name is required (e.g. `codex`, `hermes`, `claude-cli`) — it's
logged on every authenticated request so you can tell connectors apart. A
bare `Bearer <token>` (no name) is rejected with `401`. Names must be 1–64
characters, with no `:` (it separates name from token) and no control
characters.

The OAuth flow at `/oauth/*` is MCP-only; the HTTP API uses the raw shared
secret directly. Tokens are compared in constant time.

### Endpoints

`GET /api/tools` — list all tools with their JSON Schema input definitions:

```json
{
  "data": [
    {
      "name": "sql",
      "title": "Run SQL",
      "description": "...",
      "input_schema": { "type": "object", "properties": { ... } }
    }
  ]
}
```

`POST /api/tools/{name}` — invoke a tool. Request body is the raw tool input
JSON (same shape MCP uses); response wraps the tool's output in `data`:

```bash
curl -sS https://your-host/api/tools/sql \
  -H "Authorization: Bearer codex:$PDW_SECRET_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"question":"What are three Voice Memo recording IDs?","sql":"SELECT recording_id FROM apple_voice_memos_enrichments LIMIT 3","format":"json"}'
```

```json
{
  "data": {
    "question": "What are three Voice Memo recording IDs?",
    "sql": "SELECT recording_id FROM apple_voice_memos_enrichments LIMIT 3",
    "format": "json",
    "column_names": ["recording_id"],
    "total_rows": 3,
    "rows": [{"recording_id": "..."}, ...]
  }
}
```

### Errors

```json
{ "error": { "code": "tool_not_found", "message": "no tool named foo" } }
```

| Status | Code                 | When                                                                 |
|--------|----------------------|----------------------------------------------------------------------|
| 401    | _(plain text)_       | Missing/invalid `Authorization: Bearer <name>:<token>` header        |
| 404    | `tool_not_found`     | Unknown tool name, or unknown path under `/api`                      |
| 400    | `invalid_input`      | Request body or proposal fails the tool's input validation           |
| 405    | `method_not_allowed` | Wrong HTTP method (POST on `/api/tools`, GET on `/api/tools/{name}`) |
| 502    | `tool_error`         | Tool infrastructure failed unexpectedly (e.g. Postgres unreachable)  |
| 500    | `schema_error`       | Server-side schema derivation bug                                    |

**Partial success returns 200.** A `query` call with three statements where
one fails returns `200` with per-statement `error` fields in the body — same
as MCP, where `IsError=true` would still carry the partial results. Inspect
`data.results[].error` to detect this case.

## Tools

There is one SQL operation on each surface. MCP `query` accepts a batch of `{question, sql}` statements and returns every bounded result in full. CLI/HTTP `sql` accepts one statement and returns its full result with a 1,000,000-row safety cap. Neither flow creates cursors or requires follow-up helper tools.

When mutation review is enabled, the server also exposes:

- `propose_mutation` — single entry point that takes `title`, `reason`, `mutations: [...]`,
  and optional `context`. Each entry in `mutations` carries a `type` (e.g. `gmail.send_email`,
  `gmail.archive_threads`, `gmail.modify_thread_labels`, `calendar.update_event`, or
  `slack.mark_conversation_read`) plus that type's payload fields.
  `gmail.modify_thread_labels` accepts exact label display names or Gmail label IDs in
  `add_labels` / `remove_labels`; `create_and_add_labels` explicitly creates missing user labels
  by display name and applies them, while reusing same-name labels on retry. Batching multiple
  mutations into one call groups them under one review request.
- `propose_mutation_help` — zero-argument tool that returns the catalog of supported mutation
  types with field-by-field descriptions and worked examples. Call this first to see how to
  shape each mutation entry.

These tools only create rows in the `upstream_mutation_requests` and `upstream_mutations` tables.
They return an approval URL under `/mutation-review` (the web app, which reviews through
`/api/mutations/*`); the actual Gmail, Calendar, Contacts, or Slack write is still performed later
by the existing approved-mutation worker. Slack mark-read uses the private xoxc + `d`-cookie pair
published by `pdw slack publish-session`; credentials never enter the proposal payload.

SQL starting points:

- Gmail: `clean_gmail_inbox`, `gmail_messages`, `gmail_attachments`, `gmail_attachment_enrichments`
- Slack: `clean_slack_inbox`, `slack_messages`, `slack_conversations`, `slack_users`
- Apple Notes: `apple_notes` for latest note state and searchable bodies, `apple_note_revisions`
  for every observed version and tombstone, and `apple_note_attachments` for attachment metadata
- Apple Messages/iMessage/iMessages/SMS/RCS: `apple_messages` for latest message state
  and searchable decoded bodies,
  `apple_message_chats`, `apple_message_handles`, `apple_message_chat_handles`,
  `apple_message_chat_messages`, and `apple_message_attachments`
- Transcripts: `apple_voice_memos_enrichments`, `apple_voice_memos_transcription_runs`,
  `apple_voice_memos_transcript_segments`, `clean_calendar_with_transcripts`,
  `clean_transcripts_no_calendar_match`

General search flow for pdw clients — raw source tables serve structured predicates (keys,
senders, time ranges, joins); **all text search goes through the `timeline.*` functions in SQL** over the
unified timeline document. Raw message/body columns are deliberately not text-indexed:

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

- Ranked keyword search (the default): `SELECT * FROM timeline.search_text('offer letter', 50)`
  searches the unified timeline BM25 document and returns
  `(source, subsource, context, who, occurred_at, account, ref, text, score, event_ts, title,
  source_table, source_pk, priority)` ranked across **every** timeline source (`score` lower / more
  negative = better; `event_ts` duplicates `occurred_at` so `timeline.events` column lists
  work unchanged). The `text` preview is windowed around the first matched term. A hit
  carries its own drill-down: `source_table`/`source_pk` point straight at the source
  relation; `ref` (`<adapter>:<event_id>`) still joins to `timeline.events` when the
  normalized row is wanted.
- Surrounding conversation: `SELECT * FROM timeline.context(ref, 5, 5)` returns the
  `timeline.events` rows of the conversation the hit belongs to — an email's thread, a Slack
  message's thread or its channel, the rest of an iMessage/WhatsApp chat, the neighbouring
  turns of an agent session — so a hit reads as a conversation without a raw-table
  excursion. Sources that are not conversations return their neighbours in time.
- Literal substring/phrase/id search: `SELECT * FROM timeline.search_text_exact('offer letter', 50)` —
  the same document and hit shape, matched exactly (trigram-indexed, case-insensitive, LIKE
  wildcards treated literally), ordered by recency, with the returned text windowed around the
  first match. Number-format variants of the needle also match (thousands separators both
  ways, phone punctuation stripped), so `1441.52` finds `1,441.52` and `(415) 516-3303`
  finds `+14155163303`. Use this instead of post-filtering `search_text()` output with an
  outer `ILIKE` or scanning raw body columns; needles must be at least 3 characters.
- Both take the same optional args:
  `(query, max_results, sources => ARRAY['slack','gmail'], since => '2026-03-01',
  priorities => ARRAY['self','direct'])`, with `max_results` capped server-side.
  `priorities` scopes the search to the generated contract above and is pushed
  into every scan rather than applied afterwards, so a narrow tier still gets
  a full top-k. Omitting it (or passing an empty array) searches every tier, and
  an unknown token raises listing the valid set. Call `SELECT * FROM timeline.search_text_sources()` to discover
  valid source tokens; familiar aliases (`apple_messages`, `apple_notes`, `voice_memos`,
  `drive`, `contacts`, ...) resolve to the right token, and an unknown token raises listing
  the valid set. Attachment/media enrichments, Drive extracts, transcripts, and other
  detail text are folded into the parent timeline event's `search_text` document;
  agent-session transcripts are indexed per turn (`kind = 'agent_turn'`) so a hit lands on
  the matching turn rather than a whole-session blob. BM25 ranking
  is OR'd, stemmed whole-word matching, so a noisy top-N never proves absence — for "find every
  mention of X" use `search_text_exact()` and vary the needle. New cross-source text is picked
  up by adding it to the relevant timeline adapter's search document.
- A broken search layer is loud: a partially failed fan-out raises a SQL WARNING naming the
  failed sources, and if every source branch fails the call errors instead of returning an
  empty (silently wrong) result set.
- Detailed follow-up: use the timeline hit's `source_table`/`source_pk` to query the canonical
  source tables directly for complete rows, joins, attachments, thread context, etc.
- Tool-level entry point: the `search` tool wraps this contract for callers that don't want to
  write SQL. Its default `hybrid` mode embeds the query through an OpenAI-compatible embeddings
  API (`SEARCH_EMBEDDINGS_BASE_URL`, `SEARCH_EMBEDDINGS_API_KEY`, `SEARCH_EMBEDDINGS_MODEL`,
  `SEARCH_EMBEDDINGS_DIMENSIONS`) and fans BM25, one instructed semantic leg,
  and the short-literal leg over separate pooled Postgres connections. It then calls
  `timeline.search_hybrid_fuse`; `timeline.search_hybrid` remains the compatible direct-SQL
  wrapper over the same helpers. Machine tokens (ids, paths, emails and version-like strings) search
  bounded plain-document chunks through the `derived_search.chunks.text` trigram index instead
  of rechecking multi-megabyte timeline documents. Matching conversation windows and ordinary
  alphabetic names retain full-document exact matching: a window's representative ref is its
  last event, which may not be the event containing the literal. It also
  returns a `hint` on a sentence-shaped query, advising the caller to re-issue with the few
  most distinctive words the answering record would use — measured to recover five of nine
  otherwise-unanswerable benchmark questions — and a second `hint` on a long query with no
  anchor (no name, number or identifier), because adding generic words to a distinctive
  term measurably hurts. Instruction-tuned models can
  prepend `SEARCH_EMBEDDINGS_QUERY_PREFIX` on queries only (write its newline as the two
  characters `\n`: a real newline in an environment value does not survive every deploy
  pipeline, and a truncated instruction retrieves measurably worse). With a prefix set the client embeds
  only the instructed query. The former instructed/raw plus sentence content-word fanout was removed
  after the benchmark grew to 73 live-agent-shaped cases, including source/time/priority scopes: the
  single instructed ANN leg improved offline MRR from 0.342 to 0.361 and found@50 from 55 to 56 before
  the accompanying fusion retune, while removing 50% of ANN scans for term bags and 75% for sentences.
  A verified-hybrid, interleaved in-container A/B on the production host reduced mean warm
  end-to-end latency 32%, p50 17%, and p90 10%, with the maximum flat. When embeddings are not
  configured or `search_hybrid` is not installed (no pgvector), it automatically falls back to
  keyword search and reports a `fallback_reason`. Agent-session-only searches bound each ANN
  leg to 4x the requested depth (40-200 rows): those chunks are 3.05% of the global HNSW, and
  the former 1,000-row floor made each filtered vector leg take about 31 seconds. Other scopes
  use a measured 400-800 row pool. Drive-only semantic legs use a three-worker exact scan of the 223k
  Drive chunks instead: even a 40-row filtered-HNSW leg took 16 seconds, while the exact scan
  returned the then-full 1,000-row candidate pool in 7.0 seconds cold and 0.66 seconds warm. Broad
  and mixed-source searches keep the global HNSW. Modes `keyword` and `exact` force
  `timeline.search_text` / `timeline.search_text_exact` directly. Takes
  `query`, `max_results` (default 20; raise it explicitly for recall work), `sources`, `since`, and `mode`.
  The first-class CLI uses this same tool without JSON quoting: `pdw search --source
  gmail,slack 'offer letter'`. It prints a compact hit list by default; `--output json` returns
  the full machine-readable response. SQL-native workflows can still call
  `timeline.search_text`, `timeline.search_text_exact`, and `timeline.context` through `pdw sql`.

### `query`

The single MCP SQL operation. It executes read-only Postgres SQL and returns every bounded result in full, including long text fields. Each statement must include `question`, a concise plain-English description of the intent; legacy bare SQL arrays are rejected.

```json
{
  "name": "query",
  "input": {
    "queries": [
      {
        "question": "What is the most recent completed Voice Memo transcript?",
        "sql": "SELECT recording_id, transcript FROM marts_voice_memos.recordings WHERE transcription_status = 'completed' ORDER BY recorded_at DESC LIMIT 1"
      }
    ],
    "format": "csv"
  }
}
```

Only read-only statements are allowed: `SELECT`, `WITH`, `SHOW`, and `EXPLAIN`. `format` may be `csv` (default), `json`, or `ndjson`. Each query object produces one result with `question`, `sql`, `column_names`, `total_rows`, and `rows`; errors are per statement. Results over `MCP_MAX_ROWS` are rejected with a clear request to narrow the SQL. No field is silently truncated and no `query_id` cursor is created.

For long transcripts, message bodies, attachment text, or Apple Notes bodies, select the needed row and column directly with a narrow predicate or `LIMIT`. For phrase discovery across large text, start with `search` rather than a raw-table pattern scan.

### `schema_overview`

It also exposes a schema overview MCP tool:

```json
{
  "name": "schema_overview",
  "input": {}
}
```

`schema_overview` returns one text block: a preamble covering the conventions a
caller cannot infer from any column name, then one line per relation grouped by
schema.

```text
-- HOW TO USE THIS: relation names + keys + row counts only. For any other column, call
--   describe_table('gmail.messages')  →  every column with its exact Postgres type.
...
# gmail (4 relations)
  gmail.messages    ~1.2M   31 cols  pk(account,message_id)  time: internal_date
```

Each line carries the relation's planner row estimate, column count, primary
key, and primary event-time column. It deliberately does **not** list columns:
at 108 relations the full catalog was ~61KB, big enough that some clients
spilled it to a file rather than rendering it, and far too big to survive in an
agent's context until the SQL was written. What actually happened was that
callers read it once, lost it, and guessed — 70% of failed warehouse queries in
30 days of transcripts were SQLSTATE 42703 (undefined column). Columns moved to
`describe_table`, which is cheap enough to call per relation, and the overview
now costs ~18KB.

`timeline.events` keeps its columns inline, because every `timeline.search_text*()` result
hands back a ref into it and a second round trip on every search is not worth
the bytes saved.

The `time:` field is curated, not inferred. Where a relation has several
plausible event-time columns and no curated entry, it lists the candidates and
says `(ambiguous — confirm with describe_table)` rather than picking one:
naming the wrong timestamp yields a query that runs, returns rows, and answers
a different question than the caller asked.

### `describe_table`

```json
{
  "name": "describe_table",
  "input": {"relation": "gmail.messages"}
}
```

The authoritative column list for one relation — every column with its exact
`format_type` (`text[]`, `bigint`, `timestamp with time zone`, not
`information_schema`'s "ARRAY"), plus indexes and row estimate:

```text
# whatsapp.messages (~123,456 rows, estimated)
# indexes:
#   btree (account, chat_id, message_id) [primary key]
#   gin (body_text gin_trgm_ops) WHERE (is_deleted = 0)

account (text),chat_id (text),is_from_me (bigint),message_at (timestamp with time zone),...
```

It takes a schema-qualified name, a bare table name (resolved when only one
schema has it), or a database-qualified one (the leading segment is dropped).
Every failure names concrete candidates instead of sending the caller back to
the catalog empty-handed: an ambiguous bare name lists every schema that has
it, an unknown name lists the closest matches, and a known-wrong name
(`slack_channels`) is answered with the right one.

It is registered on both surfaces. `pdw columns <table>` is the CLI spelling
and calls this tool, so both surfaces return byte-identical output.

## CLI: `pdw`

`cmd/pdw-cli` builds the `pdw` command: a small command-line client that
consumes `/api/tools`. It discovers every tool the server exposes at runtime,
so it stays in sync without changes when new tools are added. (The source
directory and release artifacts keep the historical `pdw-cli` name so that
binaries installed before the rename can still self-update — see
[Self-update](#self-update).)

```bash
cd app
go build -o /tmp/pdw ./cmd/pdw-cli

# One-time setup. Stores URL+token in $XDG_CONFIG_HOME/pdw/config.json
# (defaults to ~/.config/pdw/config.json) with mode 0600. A pre-rename
# ~/.config/pdw-cli/config.json is still read as a fallback.
/tmp/pdw login \
  --base-url http://localhost:8080 \
  --token "$(pass show pdw)" \
  --client laptop
# or run without flags for an interactive prompt.

/tmp/pdw list                     # name/title/description table
/tmp/pdw list --json              # raw JSON tool list
/tmp/pdw describe sql             # title + description + input JSON Schema
/tmp/pdw call schema_overview     # zero-input NON-SQL tool
/tmp/pdw columns gmail.messages   # describe_table: columns + types + indexes for one relation
/tmp/pdw sql -q 'Find offer letters' "SELECT * FROM timeline.search_text('offer letter', 50)"
/tmp/pdw sql 'SELECT 1'                  # SQL is the only positional; defaults to CSV + an output-format note
/tmp/pdw sql -q 'What is one?' 'SELECT 1'  # -q records the caller's intent in server logs
/tmp/pdw sql --output json -q 'What time is it?' 'SELECT now()'
/tmp/pdw sql --output nd-json -q 'Which recent Gmail messages exist?' 'SELECT * FROM gmail_messages LIMIT 3'
/tmp/pdw sql --no-timeout -q 'Run a long query' 'SELECT ...'  # opt out of the default 10-second timeout
/tmp/pdw sql -q 'Find calendar transcripts mentioning Vercel' --file query.sql  # SQL from a file
/tmp/pdw sql -q 'Recent Slack messages' < query.sql                            # SQL from stdin
/tmp/pdw config show              # prints config with the token redacted
/tmp/pdw logout                   # removes the config file
```

Running SQL has exactly one path: the `sql` command. The read-only query tool
is named `sql` over the CLI/HTTP API and `query` over MCP, so `pdw call sql`
and `pdw call query` are both rejected with a pointer to `pdw sql`. This
keeps SQL off the JSON-quoting `call` path. `call` is for non-SQL tools only.
`pdw sql` cancels a query after 10 seconds by default; pass `--no-timeout` for
a long-running query that should wait indefinitely on the client side.

Values resolve in this order: **`--flag` > environment variable > config
file > default**. Env vars (`PDW_API_URL`, `PDW_SECRET_TOKEN`,
`PDW_CLIENT_NAME`) and flags (`--base-url`, `--token`, `--client`) still
work for one-off invocations, scripts, and CI. Server errors surface as
non-zero exits with the structured `code`/`message`/`http <status>`
envelope on stderr.

### Self-update

`pdw update` replaces the running binary with the latest GitHub release
from `zachlatta/personal-data-warehouse`, verifying the download against
`SHA256SUMS`. Release artifacts keep the historical `pdw-cli` name (the
asset `pdw-cli_<version>_<os>_<arch>.tar.gz` packs a single `pdw-cli` file,
and tags are `pdw-cli/v*`) so that binaries installed before the `pdw-cli` →
`pdw` rename can still self-update; `pdw update` writes the new binary back to
whatever path the running binary occupies, so it keeps the `pdw` name on disk.
Releases are produced automatically by `.github/workflows/pdw-cli-release.yml`:

- **Every push to `main`** that touches any file under `app/**` builds binaries
  for `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64` and
  publishes a release tagged `pdw-cli/v0.0.<commit-count>-sha.<short-sha>`.
  The commit count is monotonic, so `pdw update` always sees newer
  builds without waiting for a manual tag.
- **`pdw-cli/v*` git tags** publish a release tagged with the version you
  pushed (e.g. `pdw-cli/v0.1.0`).
- Each release is force-marked `--latest` so it shows up at
  `/releases/latest` even though the `-sha.<short>` suffix would normally
  be classified as a semver pre-release.
- Pull requests just run tests + build to check the matrix; they never
  publish.

```bash
pdw version        # prints the build version baked in via -ldflags
pdw update --check # report whether a newer release exists
pdw update         # download, verify SHA256, atomically replace this binary
pdw update --force # reinstall even if already on the latest version
pdw update --repo other/fork --github-api https://api.github.com  # alt source
```

Override the GitHub repo with `PDW_REPO` or `--repo` (legacy `PDW_CLI_REPO`
is still honored; the test suite uses both `--repo` and `--github-api` to
drive end-to-end fakes).

#### Background auto-update

You rarely need to run `pdw update` by hand: every invocation also kicks off a
throttled background self-update. On each run `pdw` checks a stamp file next to
its config (`$XDG_CONFIG_HOME/pdw/auto-update.json`); if more than five minutes
have passed since the last attempt, it records a fresh stamp and spawns a
detached copy of itself running the hidden `__auto-update` worker, which does
the same download-verify-replace as `pdw update`. The foreground command never
blocks on it and never fails because of it — the refreshed binary is simply
picked up on the next invocation. The five-minute debounce means a burst of
calls (e.g. an agent firing many queries) costs at most one GitHub check.

Auto-update is skipped for local `dev` builds (so a hand-built binary is never
clobbered), for the `update`/`version`/`help` commands, and whenever
`PDW_NO_AUTO_UPDATE=1` (or `true`/`yes`/`on`) is set.

## Verify

```bash
cd app
go test ./...
go build -o /tmp/pdw-mcp ./cmd/pdw-mcp
go build -o /tmp/pdw ./cmd/pdw-cli
```

To verify against the real Postgres URL from the repository `.env`:

```bash
set -a; source ../.env; set +a
go test ./internal/query -run TestPostgresRunnerUsesRealPostgresDatabaseURL -count=1
```
