# Personal Data Warehouse App

This is the Go app that fronts the Postgres warehouse. It exposes a tool
registry over two surfaces:

- **MCP** at `/mcp` — the default flow, used by Claude connectors. OAuth-protected.
- **HTTP API** at `/api/tools` — for CLI and script use. Static-bearer protected.

Each tool declares which surfaces it appears on:

- **MCP-only**: `query`, `get_rows`, `get_field`, `grep_rows` — cursor-style
  tools designed for an LLM stepping through results.
- **CLI-only**: `sql` — psql-style "give me the whole result"
  for terminal/script use; no caching, no field truncation.
- **Both**: `schema_overview`, the `propose_*` mutation tools.

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
MCP_QUERY_CACHE_MAX_BYTES=268435456
MCP_GET_FIELD_MAX_CHARS=200000
MCP_QUERY_CACHE_TTL=30m
MCP_DEBUG_CACHE_TOOL=false
MCP_QUERY_TIMEOUT=300s
PDW_MUTATION_UI_PASSWORD=...
PDW_MUTATION_UI_SESSION_SECRET=...
PDW_MUTATION_UI_SESSION_TTL_SECONDS=43200
PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
PDW_OBJECT_STORE_GOOGLE_TOKEN_JSON_B64=<authorized-user-token-json>
PDW_INGEST_AGENT_SESSIONS_FOLDER_ID=<optional-source-folder-id>
PDW_INGEST_APPLE_MESSAGES_FOLDER_ID=<optional-source-folder-id>
PDW_INGEST_WHATSAPP_FOLDER_ID=<optional-source-folder-id>
PDW_INGEST_VOICE_MEMOS_FOLDER_ID=<optional-source-folder-id>
PDW_INGEST_APPLE_NOTES_FOLDER_ID=<optional-source-folder-id>
```

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

Set `PDW_MUTATION_UI_PASSWORD` to enable the mutation proposal tools and the review UI at
`/mutation-review`. `PDW_MUTATION_UI_SESSION_SECRET` should be a separate high-entropy value; if it
is omitted, the process generates an ephemeral signing secret and browser sessions are invalidated
on restart.

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
PDW_MUTATION_UI_PASSWORD=...
PDW_MUTATION_UI_SESSION_SECRET=...
```

Do not reuse the root `Dockerfile`; that one runs Dagster.

## HTTP API

Tools marked CLI-only or "both" are reachable here. MCP-only tools
(`query`, `get_rows`, `get_field`, `grep_rows`) return `404 tool_not_found`.

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
| 400    | `invalid_input`      | Request body is not valid JSON for the tool's input schema           |
| 405    | `method_not_allowed` | Wrong HTTP method (POST on `/api/tools`, GET on `/api/tools/{name}`) |
| 502    | `tool_error`         | Tool handler returned an error (e.g. Postgres unreachable)           |
| 500    | `schema_error`       | Server-side schema derivation bug                                    |

**Partial success returns 200.** A `query` call with three statements where
one fails returns `200` with per-statement `error` fields in the body — same
as MCP, where `IsError=true` would still carry the partial results. Inspect
`data.results[].error` to detect this case.

## Tools

The MCP server exposes cursor-based query tools. `query` executes SQL once, caches the full result in the server process, and returns a `query_id` handle for follow-up calls. Cached results expire after `MCP_QUERY_CACHE_TTL` and are evicted least-recently-used when the process-wide `MCP_QUERY_CACHE_MAX_BYTES` cap is reached. If the server restarts, old `query_id`s are invalid and the caller should re-run `query`.

The CLI/HTTP API exposes a separate `sql` tool that runs one
SQL statement and returns the entire result body in one response — no caching,
no field truncation, just a safety cap of 1,000,000 rows. Use it the way
you'd use `psql` interactively or from a shell script.

When mutation review is enabled, the server also exposes:

- `propose_mutation` — single entry point that takes `title`, `reason`, `mutations: [...]`,
  and optional `context`. Each entry in `mutations` carries a `type` (e.g. `gmail.send_email`,
  `gmail.archive_threads`, `calendar.update_event`) plus that type's payload fields. Batching
  multiple mutations into one call groups them under one review request.
- `propose_mutation_help` — zero-argument tool that returns the catalog of supported mutation
  types with field-by-field descriptions and worked examples. Call this first to see how to
  shape each mutation entry.

These tools only create rows in the `upstream_mutation_requests` and `upstream_mutations` tables.
They return an approval URL under `/mutation-review`; the actual Gmail, Calendar, or Contacts
write is still performed later by the existing approved-mutation worker.

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
senders, time ranges, joins); **all text search goes through the `search.*` functions** over the
unified timeline document. Raw message/body columns are deliberately not text-indexed:

- Ranked keyword search (the default): `SELECT * FROM search_text('offer letter', 50)`
  searches the unified timeline BM25 document and returns
  `(source, subsource, context, who, occurred_at, account, ref, text, score)` ranked across
  **every** timeline source (`score` lower / more negative = better). `ref` is a
  timeline ref (`<adapter>:<event_id>`); join to `timeline.events` and then use
  `source_table`/`source_pk` for full source rows.
- Literal substring/phrase/id search: `SELECT * FROM search_text_exact('offer letter', 50)` —
  the same document and hit shape, matched exactly (trigram-indexed, case-insensitive, LIKE
  wildcards treated literally), ordered by recency, with the returned text windowed around the
  first match. Use this instead of post-filtering `search_text()` output with an outer `ILIKE`
  or scanning raw body columns; needles must be at least 3 characters.
- Both take the same optional args:
  `(query, max_results, sources => ARRAY['slack','gmail'], since => '2026-03-01')`, with
  `max_results` capped server-side. Call `SELECT * FROM search_text_sources()` to discover
  valid source tokens. Attachment/media enrichments, Drive extracts, transcripts, and other
  detail text are folded into the parent timeline event's `search_text` document. BM25 ranking
  is OR'd, stemmed whole-word matching, so a noisy top-N never proves absence — for "find every
  mention of X" use `search_text_exact()` and vary the needle. New cross-source text is picked
  up by adding it to the relevant timeline adapter's search document.
- Detailed follow-up: use the timeline hit's `source_table`/`source_pk` to query the canonical
  source tables directly for complete rows, joins, attachments, thread context, etc.

### `query`

Executes read-only Postgres SQL, caches each result under a generated `query_id`, and returns a preview.
Each SQL statement must include `question`, a concise plain-English question this SQL statement is
trying to answer. Legacy `sql` array input is rejected.

```json
{
  "name": "query",
  "input": {
    "queries": [
      {
        "question": "What is the most recent completed Voice Memo transcript?",
        "sql": "SELECT recording_id, transcript FROM apple_voice_memos_enrichments WHERE status = 'completed' ORDER BY created_at DESC LIMIT 1"
      }
    ],
    "preview_rows": 1,
    "format": "csv"
  }
}
```

Apple Notes bodies can be long, so query the row first and then use `get_field` for the full body:

```json
{
  "name": "query",
  "input": {
    "queries": [
      {
        "question": "What are the most recently modified non-deleted Apple Notes?",
        "sql": "SELECT note_id, title, modified_at, body_text, body_html FROM apple_notes WHERE is_deleted = 0 ORDER BY modified_at DESC LIMIT 5"
      }
    ],
    "preview_rows": 5,
    "format": "csv"
  }
}
```

Only read-only statements are allowed: `SELECT`, `WITH`, `SHOW`, and `EXPLAIN`.

Each query object in `queries` gets its own `query_id`. The server logs `question` with the SQL,
query_id, row count, duration, errors, and follow-up cached-result tool calls. `format` may be `csv`,
`json`, or `ndjson`; `csv` is the default. Query results over `MCP_MAX_ROWS` are rejected with a clear
error. Long preview fields are truncated to `MCP_MAX_FIELD_CHARS`, and truncation metadata is returned as structured data:

```json
{
  "query_id": "f00d...",
  "total_rows": 1,
  "column_names": ["recording_id", "transcript"],
  "preview": "recording_id,transcript\nabc,first 4000 chars...\n# TRUNCATIONS: [{\"row\":0,\"column\":\"transcript\",\"returned\":4000,\"total\":24168}]",
  "truncations": [
    {"row": 0, "column": "transcript", "returned": 4000, "total": 24168}
  ]
}
```

For CSV previews, the same truncation array is also emitted as a trailing parseable line prefixed with `# TRUNCATIONS: `. Do not compute substring offsets in SQL. Use `get_field` for long fields.

### `get_rows`

Returns a row slice from a cached query result without re-executing SQL.

```json
{
  "name": "get_rows",
  "input": {
    "query_id": "f00d...",
    "offset": 50,
    "limit": 25
  }
}
```

`format` can be overridden per call; otherwise it inherits the original `query` format. Long fields are truncated the same way as previews, with structured `truncations`.

### `get_field`

Returns a character chunk from one cached cell. This is the right tool for reading transcripts, email bodies, attachment text, or any long text column end-to-end.

```json
{
  "name": "get_field",
  "input": {
    "query_id": "f00d...",
    "row": 0,
    "column": "transcript",
    "offset": 0,
    "length": 200000
  }
}
```

The response includes `total_chars`, `returned_chars`, `offset`, `value`, and `eof`. `length` is capped by `MCP_GET_FIELD_MAX_CHARS`.

Use `get_field` the same way for Apple Notes `body_text`, `body_html`, `body_markdown`, Apple
Messages `body_text`, or `raw_metadata_json` columns after querying the relevant tables.

Reading a 24 KB transcript now takes exactly two MCP calls:

1. `query` the row and receive a `query_id` plus a truncated preview.
2. `get_field` with `row: 0`, `column: "transcript"`, `offset: 0`, and `length: 200000`.

Do not use the old six-query `substring(transcript, start, length)` pattern.

### `grep_rows`

Regex-searches cached rows without re-executing SQL and returns match context.

```json
{
  "name": "grep_rows",
  "input": {
    "query_id": "f00d...",
    "pattern": "weighted projects",
    "columns": ["transcript"],
    "limit": 20,
    "context_chars": 200
  }
}
```

Use this to find where a phrase appears across cached transcripts or email bodies before fetching the full field with `get_field`.

### `schema_overview`

It also exposes a schema overview MCP tool:

```json
{
  "name": "schema_overview",
  "input": {}
}
```

`schema_overview` returns one text block: a leading note on how to reference
the tables, then a section per table or view headed by its bare name (the name
you use directly in `FROM`/`JOIN`):

```text
-- Reference these tables by their bare name in FROM/JOIN (e.g. FROM gmail_messages). Do not prefix them with the database name ("postgres.").

# table_name

column1,column2,column3
sample row 1
sample row 2
```

The heading is the bare table name on purpose: it is what `FROM` expects.
Earlier versions printed `# database.table_name`, which led callers to write
`FROM postgres.table_name` — invalid in Postgres, where qualification is
`schema.table`, not `database.table`. It uses `current_database()` and
`information_schema` against the current Postgres schema, then samples up to
three rows per table. Sample cell values are capped at 15 characters to keep the preview compact; truncation metadata is included when a preview value is shortened.

### `_debug_cache_status`

When `MCP_DEBUG_CACHE_TOOL=true`, the server also exposes `_debug_cache_status` to show live `query_id`s, ages, and process-wide cache size.

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
/tmp/pdw columns gmail_messages   # column names + types for one table
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

- **Every push to `main`** that touches `app/**` (CLI, client, selfupdate,
  shared `tool`/`api`/`auth` packages, or `go.mod`/`go.sum`) builds binaries
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
