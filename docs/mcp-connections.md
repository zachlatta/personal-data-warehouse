# MCP gateway connections

The PDW web app's `/connections` page manages remote MCP connections. Each has an
immutable name and HTTPS endpoint, access policy, encrypted OAuth/bearer credentials,
and a discovered tool snapshot. Tools appear on both `/mcp` and `/api/tools` (therefore
`pdw list`, `pdw describe`, and `pdw call`) as `<connection>__<tool>`.

## Operator setup

The existing application database and `PDW_SECRET_TOKEN` are required. Set `MCP_BASE_URL`
to the external HTTPS origin, including when a reverse proxy terminates TLS. Upstream
OAuth callbacks use `/connections/oauth/callback`. No additional service or deployment
is required. The Go app provisions its table on first use; Python's health provisioning
also creates it so the warehouse inventory stays complete.

Configuration is in the catalogued private MCP connection table, denied to the query role.
The payload is AES-GCM encrypted with a domain-separated key derived from
`PDW_SECRET_TOKEN`, with the connection name as associated data. Back up that secret
separately from the database. **Rotating it requires reconnecting the upstream accounts**;
old encrypted rows must be removed by an administrator before they can be recreated.
Never print their payloads, tokens, authorization codes, or secrets. The proxy logs no
upstream result bodies, and the existing HTTP logger records paths without query strings.

The admin API requires the existing named static bearer, never a downstream OAuth token.
A static-bearer holder is already a PDW administrator. Client names on static-bearer calls
are self-selected; the allowlist is not a security boundary against other administrators.
OAuth clients' names are bound into signed PDW access tokens.

## Authentication and network boundary

OAuth discovery verifies the protected resource and issuer, requires PKCE S256, and uses
public-client dynamic registration or entered pre-registration credentials. One-time
state is bound to an HttpOnly, Secure, SameSite=Lax browser cookie, expires in ten minutes,
and is consumed on exchange. Tokens and pending state survive app restarts. Refresh and
connection updates serialize through a Postgres row lock, including across app processes.
PDW access tokens are never forwarded upstream; each upstream uses only its own token.

Outbound HTTP requires HTTPS, rejects redirects, disables ambient HTTP proxies, validates
all DNS answers and dials a validated public address directly. This applies to MCP,
metadata, registration and token requests. Loopback, private, link-local, CGNAT and reserved
addresses are rejected. The gateway cannot be used to reach the host's internal services.
Bearer credentials are attached only to the exact configured MCP endpoint.

## Scope and failure behavior

This is a tool gateway, not a sync source. No proxied content lands on the timeline.
Sharing includes write tools, with the upstream provider's permissions and review semantics;
PDW's own mutation-review flow does not intercept them. Tools are hidden until explicitly
shared with named clients or all authenticated PDW clients. Disable/remove recheck at call
time, including for stale client tool lists. Calls already executing cannot be undone.

Discovery is explicit (after auth or Refresh tools), bounded to 1,000 tools per connection.
Calls and discovery have a 60-second budget, responses are capped at 32 MiB, and calls
are not automatically retried.
Results preserve MCP content blocks, structured content and `isError`. An ambiguous failed
write must be checked upstream, not blindly repeated. The original tool set survives a
failed refresh. A failed connection-store read leaves built-in PDW tools available.

The first version supports remote Streamable HTTP tools, not local subprocesses, legacy
SSE transports, resources/prompts, sampling/elicitation, subscriptions or upstream state
shared between tool calls. Each tool call uses a fresh upstream session. Clients that
cache tool lists need to reconnect after adding or changing connections.

Reference: [MCP authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization).
