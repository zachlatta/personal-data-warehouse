# Access tokens and `cli_authorize`

PDW's HTTP surfaces (`/api/tools`, the web app, the phone app, push, mutation review,
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
  the root of the OAuth signing keys. A client moves off it by running `pdw token create --name <client>` and then
  `pdw login` with the printed token (or pasting that token into the phone app or web
  page). Uploaders that sign ingest bodies (`pdw ingest`, `pdw heartbeat`, the
  `publish-session` commands) keep the master secret, because an upload signature is an
  HMAC over that key.
- **Command or link?** A command. The `cli_authorize` MCP tool returns
  `curl -fsSL <app>/cli/bootstrap.sh | sh -s -- <code>`. The script holds no secret.
  It installs `pdw` into `~/.local/bin` if there is none, by piping the app's own
  `/cli/install.sh`, which fetches the GitHub release through the app (see below), then
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
- `app/internal/clirelease`: `/cli/install.sh` and the release proxy behind it.
  `/cli/release/latest` is GitHub's latest-release JSON (cached 5 minutes, served stale
  if GitHub refuses) with every download URL rewritten to
  `/cli/release/download/<tag>/<asset>`, which streams the asset from GitHub. Only a
  `pdw-cli/v*` tag's platform tarballs and `SHA256SUMS` pass, so it is not an open
  proxy. It exists because a Claude Cowork sandbox got 403 from `api.github.com` on
  2026-10-09: GitHub refuses many datacenter egress IPs, and the app's is not one.
  `pdw update` and the background auto-update read the same proxy
  (`app/internal/selfupdate`), so nothing the CLI does reaches GitHub.
- `app/internal/server/tools_cli_authorize.go`: the MCP-only tool.
- `app/cmd/pdw-cli/token.go`: `pdw token`. `auth.go` holds `pdw login --bootstrap`.
- The Python twin of the DDL is `ensure_app_access_token_tables` in `postgres.py`, so a
  fresh warehouse inventory matches the catalog.

## MCP connector grants

Each OAuth MCP connector login (Claude, ChatGPT, any client that goes through
`/oauth/authorize`) is a row in `private.app_access_tokens` with kind `oauth` and scope
`query`, created when the authorization code is exchanged. Its access tokens (24 h) and
refresh tokens (365 days) name the row as `gid`, and both are checked against it:

- `pdw token list` shows each connector's grant with its client name and last use;
  `pdw token revoke <id>` turns that connector off. Its next MCP request is a 401 and its
  next refresh `invalid_grant`, so it has to authorize again. Other connectors and the
  master secret are untouched.
- A grant lapses after **90 days** with no MCP request and no refresh
  (`accesstokens.OAuthGrantIdleTimeout`).
- A good answer is cached in the app for a minute; a revoke clears it.
- When the grant store cannot answer (database down), `/mcp` returns a 5xx and
  `/oauth/token` returns 503 `temporarily_unavailable`, never 401 or `invalid_grant`:
  a connector told its token is invalid drops its login, and one told to retry keeps it.
- Tokens issued before grants existed carry no `gid`. Until
  `auth.LegacyOAuthTokenCutoff` (2026-11-28) such a refresh token still works and its
  first use moves the connector onto a grant whose id is a keyed hash of that token, so
  a retry lands on the same row and revoking it also stops the old token. Each
  refresh token minted before grants moves onto its own grant, so one connector can
  show more than one row. After the
  cutoff they are refused, and a connector unused for the whole 30 days authorizes once
  more.

The code is `app/internal/auth/service.go` (the OAuth handlers and `VerifyBearer`) and
`app/internal/accesstokens/oauth_grants.go` (the store side).

## Keys and rotating the master secret

`PDW_SECRET_TOKEN` is the root. `auth.Keyring` derives a separate subkey per purpose
from it with HKDF-SHA256 (OAuth tokens and client ids, legacy grant ids, object
download links) and a short key id, also HKDF-derived, that names the secret without
revealing it. OAuth values are `<key id>.<body>.<sig>` and a download link's `sig` is
`<key id>.<mac>`, so the verifier knows which key to use. Values from before key ids
(`<body>.<sig>`, and a `sig` without a key id) still verify against the raw secret.

Three uses stay on the raw secret by design: upload signatures (every uploader holds the
secret and signs with it, in Go and in `ingest_client.py`; the `object-upload` prefix
keeps those MACs apart), the static bearer, and the consent page.
`private.mcp_connections` keeps its own derived AES key.

`PDW_SECRET_TOKEN_PREVIOUS` opens a rotation window. While it is set, the previous
secret still verifies everywhere the current one does (OAuth tokens and client ids,
download links, uploads, the static bearer, sealed connections), except the consent
page, which takes only the current secret. New signatures always use the current key,
and every use of the previous secret as a bearer is logged with the client name. To
rotate:

1. Generate a new secret (at least 32 characters). In the deployment env, set
   `PDW_SECRET_TOKEN_PREVIOUS` to the old value and `PDW_SECRET_TOKEN` to the new one,
   then deploy. At boot the app re-encrypts `private.mcp_connections` under the new
   secret.
2. Update every client that holds the secret: `pdw login` on each machine, the uploader
   `.env` files (`bin/sync-mac-device-runtime`), the phone app, the web pages, and the
   Dagster env. Watch the logs for `previous master secret used`, which names the
   clients still on the old one.
3. Leave the window open long enough for idle connectors to refresh: each refresh
   re-signs their tokens with the new key. A connector that does not refresh during the
   window authorizes again.
4. Unset `PDW_SECRET_TOKEN_PREVIOUS` and deploy. Anything still signed only with the old
   secret stops verifying.
