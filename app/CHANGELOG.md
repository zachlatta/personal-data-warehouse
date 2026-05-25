# Changelog

## Unreleased

- New: HTTP API at `/api/tools` (list) and `POST /api/tools/{name}` (invoke). Every MCP tool is exposed; same input/output, `{data: ...}` / `{error: {...}}` envelope. Static-bearer auth via `PDW_SECRET_TOKEN`. The query cache is shared with MCP — a `query_id` minted on one surface is fetchable on the other.
- New: `PDW_SECRET_TOKEN` is the preferred env var for the shared secret. `MCP_SECRET_TOKEN` is still read as a fallback for one release.
- Internal: MCP and HTTP both consume a shared `tool.Registry`. No MCP-visible behavior change: tool names, descriptions, input schemas, and call results are byte-identical (regression-tested via golden snapshot).
- Breaking: redesigned `query` to return cached `query_id` cursors plus preview/truncation metadata. Substring-based SQL pagination for long fields is no longer supported; callers should use `get_field`, `get_rows`, and `grep_rows`.
