# Access tokens and `cli_authorize`

PDW's HTTP surfaces (`/api/tools`, the web app, the iOS app, push, mutation review,
connections) authenticate with a static bearer, `Authorization: Bearer <client>:<token>`.
Until 2026-10 the only token was `PDW_SECRET_TOKEN`, the one secret that also signs
uploads, OAuth codes and MCP access tokens, and encrypts `private.mcp_connections`. Every
client held the master key, and there was no way to give a sandbox (Claude Cowork, a
fresh VM) a CLI without handing it that key forever.

## The model

`private.app_access_tokens` holds any number of **issued tokens** beside the master
secret. A row stores only the SHA-256 of the token's secret; the plaintext is shown once,
when the token is issued.

| credential | spelled | scope | lifetime |
| --- | --- | --- | --- |
| master secret | `PDW_SECRET_TOKEN` | `root`: everything, including token administration and ingest signing | until rotated in the deployment env |
| issued token | `pdw_<id>_<secret>` | `full`: every bearer surface except token administration | dies after `idle_timeout` unused (default 30 days), optionally a hard `expires_at` |
| CLI token from `cli_authorize` | `pdw_<id>_<secret>` | `agent`: `/api/tools` only (the CLI's entire surface) | hard expiry, default 24 h, max 7 days |
| bootstrap code | `pdwb_<id>_<secret>` | none: redeemable once for one `agent` token | 15 minutes, single use |

Answers to the open questions in the original tasks:

- **Idle window:** 30 days for an issued token by default (`--idle` overrides it). Every
  authenticated request refreshes `last_used_at`. Writes are throttled to one a minute
  per token, so the idle clock is accurate to that minute.
- **Does use extend a token forever?** For an issued token, yes, unless it was created
  with a hard `--ttl`. A `cli_authorize` token has a hard expiry and no idle extension:
  24 hours means 24 hours however busy the sandbox is.
- **Issue, list, revoke:** `pdw token create|list|revoke` on a CLI logged in with the
  master secret, over the root-only `/api/auth/tokens` API. An issued token cannot mint
  another token, so a leaked one cannot extend itself. Revoking a token takes effect on
  its next request.
- **Migrating existing clients:** nothing breaks. The master secret is still accepted
  everywhere, because it must exist anyway: it is the HMAC key for ingest uploads and
  OAuth. A client moves off it by running `pdw token create --name <client>` and then
  `pdw login` with the printed token (or pasting that token into the iOS app or web
  page). Uploaders that sign ingest bodies (`pdw ingest`, `pdw heartbeat`, the
  `publish-session` commands) keep the master secret, because an upload signature is an
  HMAC over that key.
- **Command or link?** A command. The `cli_authorize` MCP tool returns
  `curl -fsSL <app>/cli/bootstrap.sh | sh -s -- <code>`. The script holds no secret.
  It installs `pdw` from the GitHub release into `~/.local/bin` if there is none, then
  runs `pdw login --bootstrap <code>`. That command POSTs the code to
  `/api/cli/bootstrap`, which redeems it inside one transaction (row lock, hash check,
  `redeemed_at` stamped) and returns the `agent` token. The CLI saves the token with its
  expiry.
- **Single use:** the redemption marks the code row under `SELECT ... FOR UPDATE`. A
  second redemption, an expired code, or a revoked code returns 401 and issues nothing.

## Why `agent` is narrower than `full`

`cli_authorize` is meant to be called by an agent, for an agent. Without a narrower
scope, the `agent` token could do two things the issuing MCP session cannot:

- approve its own `propose_mutation` through the review API;
- mint a longer-lived token.

So an `agent` token reaches only `/api/tools`. That is every warehouse tool, including
`propose_mutation` and proxied connections, and it is everything `pdw` itself calls.
Any other path returns 403 naming the scope.

The tool's result carries the bootstrap code, so the MCP result log line for
`cli_authorize` is redacted (`server.mcpToolHooks`). The code is short-lived, but Loki
keeps logs far longer than 15 minutes.

## Where it lives

- `app/internal/auth`: `Principal`, the scope check, and the static-bearer middleware.
  The master secret is checked first, then `TokenAuthenticator`.
- `app/internal/accesstokens`: the Postgres and in-memory stores, the token API and the
  redemption handlers, and the bootstrap script.
- `app/internal/server/tools_cli_authorize.go`: the MCP-only tool.
- `app/cmd/pdw-cli/token.go`: `pdw token`. `auth.go` holds `pdw login --bootstrap`.
- The Python twin of the DDL is `ensure_app_access_token_tables` in `postgres.py`, so a
  fresh warehouse inventory matches the catalog.

## Follow-ups not built here

- OAuth MCP connector tokens are still stateless HMAC tokens (24 h access, 365-day
  refresh). Revoking one connector means rotating the master secret. Moving refresh
  tokens into `app_access_tokens` would make connectors listable and revocable too.
- A sandbox whose egress allowlist blocks `github.com` cannot install the binary. The
  app could serve the release asset itself, from `/cli/download/<os>/<arch>`.
