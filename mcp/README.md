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
MCP_MAX_ROWS=100
MCP_MAX_FIELD_CHARS=4000
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

The server exposes a query MCP tool:

```json
{
  "name": "query",
  "input": {
    "sql": ["SELECT * FROM gmail_messages LIMIT 5"]
  }
}
```

Only read-only statements are allowed: `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `DESC`, and `EXPLAIN`.

Each statement returns CSV text to keep tool responses compact. Results are automatically capped by `MCP_MAX_ROWS`. Long string fields are truncated to `MCP_MAX_FIELD_CHARS`. When truncation happens, the response includes a second small truncation CSV with instructions for fetching the full field with `length(column)` and chunked `substring(column, start, size)` queries.

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
