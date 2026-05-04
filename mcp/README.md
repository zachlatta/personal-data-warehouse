# Personal Data Warehouse MCP

This is a Go remote MCP server for querying the ClickHouse warehouse from Claude connectors.

## Environment

Required:

```bash
CLICKHOUSE_URL=...
MCP_SECRET_TOKEN=...
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
```

`MCP_SECRET_TOKEN` is the shared setup secret and token signing key. It must be at least 32 characters; use a high-entropy random value. During Claude connector setup, the OAuth page asks for this value. After a successful login, Claude uses bearer tokens issued by the server. Rotating `MCP_SECRET_TOKEN` invalidates existing sessions.

## Run Locally

```bash
cd mcp
set -a; source ../.env; set +a
export MCP_SECRET_TOKEN=choose-a-random-local-secret-at-least-32-chars
export MCP_BASE_URL=http://localhost:8080
go run ./cmd/pdw-mcp
```

The MCP endpoint is:

```text
http://localhost:8080/mcp
```

## Claude Connector Setup

1. Deploy this server at a public HTTPS URL.
2. In Claude, add a custom connector with the MCP URL:

```text
https://your-public-coolify-domain/mcp
```

3. Claude will start the OAuth flow.
4. Enter `MCP_SECRET_TOKEN` on the authorization page.

## Coolify

Create a new Dockerfile-based app using this repository.

Use:

```text
Dockerfile path: mcp/Dockerfile
Exposed port: 8080
```

Set:

```bash
CLICKHOUSE_URL=...
MCP_SECRET_TOKEN=...
MCP_BASE_URL=https://your-public-coolify-domain
```

Do not reuse the root `Dockerfile`; that one runs Dagster.

## Tools

The server exposes cursor-based query tools. `query` executes SQL once, caches the full result in the server process, and returns a `query_id` handle for follow-up calls. Cached results expire after `MCP_QUERY_CACHE_TTL` and are evicted least-recently-used when the process-wide `MCP_QUERY_CACHE_MAX_BYTES` cap is reached. If the server restarts, old `query_id`s are invalid and the caller should re-run `query`.

### `query`

Executes read-only ClickHouse SQL, caches each result under a generated `query_id`, and returns a preview.

```json
{
  "name": "query",
  "input": {
    "sql": ["SELECT id, transcript FROM voice_memo_transcripts WHERE id = 'abc'"],
    "preview_rows": 1,
    "format": "csv"
  }
}
```

Only read-only statements are allowed: `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `DESC`, and `EXPLAIN`.

Each SQL string in `sql` gets its own `query_id`. `format` may be `csv`, `json`, or `ndjson`; `csv` is the default. Query results over `MCP_MAX_ROWS` are rejected with a clear error. Long preview fields are truncated to `MCP_MAX_FIELD_CHARS`, and truncation metadata is returned as structured data:

```json
{
  "query_id": "f00d...",
  "total_rows": 1,
  "column_names": ["id", "transcript"],
  "preview": "id,transcript\nabc,first 4000 chars...\n# TRUNCATIONS: [{\"row\":0,\"column\":\"transcript\",\"returned\":4000,\"total\":24168}]",
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

`schema_overview` returns one text block with a section per table:

```text
# database.table_name

column1,column2,column3
sample row 1
sample row 2
```

It uses `currentDatabase()` and `SHOW TABLES` against the current ClickHouse database, then samples up to three rows per table. Sample cell values are capped at 15 characters to keep the preview compact; truncation metadata is included when a preview value is shortened. It does not require access to ClickHouse `system.*` metadata tables.

### `_debug_cache_status`

When `MCP_DEBUG_CACHE_TOOL=true`, the server also exposes `_debug_cache_status` to show live `query_id`s, ages, and process-wide cache size.

## Verify

```bash
cd mcp
go test ./...
go build -o /tmp/pdw-mcp ./cmd/pdw-mcp
```

To verify against the real ClickHouse URL from the repository `.env`:

```bash
set -a; source ../.env; set +a
go test ./internal/query -run TestClickHouseRunnerUsesRealClickHouseURL -count=1
```
