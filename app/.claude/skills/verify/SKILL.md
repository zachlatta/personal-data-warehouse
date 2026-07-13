---
name: verify
description: Build, launch, and drive the pdw MCP/API app locally to verify changes end to end at its HTTP surface (OAuth, /mcp, /api/tools, discovery endpoints).
---

# Verifying the pdw app locally

## Build and launch

The app needs a reachable Postgres (it pings at startup) and a 32+ char secret.
A local dev database works; the repo's gitignored `.env` in the non-worktree
checkout has a `POSTGRES_DATABASE_URL`. All tools are read-only, so pointing at
a real database is safe.

```bash
cd app && go build -o /tmp/pdw-mcp ./cmd/pdw-mcp
PDW_SECRET_TOKEN='verify-secret-token-with-plenty-of-entropy-0123456789' \
POSTGRES_DATABASE_URL="$URL" MCP_ADDR=':8899' /tmp/pdw-mcp
```

`GET /` returns a plaintext banner with the Git SHA — use it to confirm which
build is serving (also works against prod to confirm a deploy landed).

## Mint an OAuth bearer token (what the Claude connector does)

1. `POST /oauth/register` with `{"redirect_uris":["https://claude.ai/api/mcp/auth_callback"]}` → `client_id`
2. `POST /oauth/authorize` form fields: `client_id`, `redirect_uri`,
   `response_type=code`, `code_challenge` (S256 of a verifier),
   `code_challenge_method=S256`, `secret_token=<PDW_SECRET_TOKEN>`,
   `client_name=<anything>` → 302; `code` is in the redirect URL query
3. `POST /oauth/token` with `grant_type=authorization_code`, `client_id`,
   `code`, `redirect_uri`, `code_verifier` → `access_token` + `refresh_token`

## Drive /mcp

POST JSON-RPC with headers `Authorization: Bearer <token>`,
`Content-Type: application/json`, and
`Accept: application/json, text/event-stream` (both media types required or
the handler 400s). The handler is stateless: `tools/list` and `tools/call`
work without a prior `initialize`, and any `Mcp-Session-Id` value is accepted
— send a bogus one to verify session-reuse resilience. `GET /mcp` is 405 by
design (no SSE offered).

```bash
curl -s $H -X POST :8899/mcp -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
curl -s $H -X POST :8899/mcp -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query","arguments":{"queries":[{"question":"ping","sql":"SELECT 1 AS n"}]}}}'
```

The `query` tool also accepts `queries` as a JSON-encoded *string* (claude.ai
serializes it that way); exercise both encodings.

## Flows worth driving after auth/MCP changes

- All discovery variants must return 200 unauthenticated: root, path-inserted
  (`/.well-known/<doc>/mcp`), and path-appended (`/mcp/.well-known/<doc>`)
  forms of `oauth-authorization-server`, `oauth-protected-resource`, and
  `openid-configuration`. Claude's 401 recovery probes these and gives up on
  the connector if any 404.
- Garbage bearer → 401 with a `WWW-Authenticate` header carrying
  `resource_metadata` and `scope="query"`.
- `grant_type=refresh_token` at the token endpoint issues a fresh access token
  that works on `/mcp`.
