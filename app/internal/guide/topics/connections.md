# Connected MCP servers

PDW is also a gateway to remote MCP servers connected by its owner. These are live
upstream calls, not warehouse ingestion. Skills, Tasks and another data warehouse can
be connected without duplicating their implementation or copying their databases.

## Discover and use

Tools are named `<connection>__<upstream_tool>`. The prefix prevents collisions with
PDW's own tools and with other connected servers.
{{if .CLI}}
Use `pdw list` to find a tool, then `pdw describe <tool>` **before the first call**.
Use the exact argument names and types from that schema, not guesses. Describe every
unfamiliar tool before launching parallel calls. Invoke with `pdw call <tool> --data '<json>'`.
For input from a file, use `pdw call <tool> < input.json`; for a pipe, omit `--data`.
`--data` accepts inline JSON only, not `@file` or `@-`.

The default `--output json` preserves the full result, including `content`,
`structuredContent`, and `isError`. `--output text` prints text content blocks in order,
without parsing JSON inside them, and warns when it omits non-text blocks.
`--output structured` prints only `structuredContent`; it fails clearly if that field is
absent rather than guessing from text. Formatting happens after execution: do not repeat a write
just to change output format. Formatting failures preserve the raw result on stderr.
An upstream `isError: true` exits nonzero and preserves the full error result on stdout
in JSON mode, or stderr in the projection modes. Read its diagnostic and check
`pdw describe <tool>` before correcting arguments; do not blindly retry.
{{else}}
Read the tool's input definition **before the first call**. Use its exact argument names
and types, and check every unfamiliar definition before launching parallel calls.
Some clients cache definitions; reconnect PDW in that client if newly connected tools
have not appeared.
{{end}}
The upstream input schema, result content, structured result and error flag are retained.
A tool error is not an empty successful result.

**Writes execute with the owner's upstream account.** They do not go through PDW's
mutation review unless the upstream tool implements its own review. Follow the upstream
service's rules and get the user's authorization before consequential writes. Calls are
not automatically retried after failures; a timed-out write may already have happened.

## Connect and authenticate in the web app

Open `/connections` in the PDW web app. Add a name and the remote HTTPS MCP endpoint.
Click **Authenticate** to authorize through the upstream provider in the browser. OAuth
uses discovery, PKCE and dynamic registration; a provider that requires pre-registration
needs a client ID (and, if required, client secret) in the optional fields. Register the
callback as the PDW app's HTTPS origin followed by `/connections/oauth/callback`.

Alternatively enter a bearer token, then **Refresh tools**. Unauthenticated servers also
use Refresh tools. Credentials remain on the PDW server, not in downstream MCP clients.
OAuth refresh tokens are stored and used automatically. Re-authenticate when access is
revoked or a provider cannot refresh it.

Sharing is explicit: choose PDW client names or all authenticated clients (including future
clients). No selection means the connection is not shared. Disable hides its tools and
blocks new calls. Refresh replaces the discovered set without restarting PDW. Remove
clears stored credentials and tools; revoke the grant at the upstream provider separately.
Holders of the PDW app secret are administrators and can manage connections or choose
client names; a client-name restriction is not isolation from those administrators.

## Limits and troubleshooting

This version proxies tools over remote Streamable HTTP. Local stdio, legacy SSE endpoints,
private-network addresses, resources, prompts, sampling and elicitation are not supported.
Each call opens an upstream session; providers must not require session-local state across
separate tool calls. Use the provider's Streamable HTTP endpoint, not its website URL.

Connection status and tool counts are on `/connections`. Other PDW tools remain available
when an upstream fails. A failed discovery retains the previous tool set and reports the
failure. The tools are a discovery snapshot: click Refresh tools after upstream changes.
