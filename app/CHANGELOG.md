# Changelog

## Unreleased

- Breaking: `get_object` now returns a signed, time-limited `download_url` (plus `expires_at`) instead of inline `content_base64`/`content_omitted`. The link is served by a new unauthenticated-but-HMAC-verified `GET /objects/{storage_file_id}?exp=...&sig=...` endpoint, so it can be opened straight from a chat or browser. Link lifetime is `PDW_OBJECT_URL_TTL` (default 1h); `PDW_OBJECT_STORE_MAX_OBJECT_BYTES` now caps what the endpoint will serve (default raised 5MB → 100MB).
- New: `pdw download <storage_file_id> [--output PATH]` fetches a stored blob (Gmail attachment, Apple Notes/Messages attachment, Voice Memo audio, ...) to a local file via the signed link, verifying the content SHA-256 when the server reports one.
- Breaking: every client must now identify itself with a name that gets logged on every request, so it's possible to tell connectors apart (claude vs. codex vs. hermes, etc.).
  - HTTP API: `Authorization: Bearer <client_name>:<PDW_SECRET_TOKEN>`. A bare `Bearer <token>` is rejected with 401.
  - MCP OAuth: the authorize page has a new required `Client name` field alongside the secret. The name is embedded in the issued access/refresh tokens; existing tokens minted before this change are rejected and must be re-authorized.
  - The name appears as `client=<name>` on the per-request log line, on MCP tool-call logs, and on API tool-call logs.
- New: HTTP API at `/api/tools` (list) and `POST /api/tools/{name}` (invoke). Every MCP tool is exposed; same input/output, `{data: ...}` / `{error: {...}}` envelope. Static-bearer auth via `PDW_SECRET_TOKEN`. The query cache is shared with MCP — a `query_id` minted on one surface is fetchable on the other.
- New: `PDW_SECRET_TOKEN` is the preferred env var for the shared secret. `MCP_SECRET_TOKEN` is still read as a fallback for one release.
- Internal: MCP and HTTP both consume a shared `tool.Registry`. No MCP-visible behavior change: tool names, descriptions, input schemas, and call results are byte-identical (regression-tested via golden snapshot).
- Breaking: redesigned `query` to return cached `query_id` cursors plus preview/truncation metadata. Substring-based SQL pagination for long fields is no longer supported; callers should use `get_field`, `get_rows`, and `grep_rows`.
