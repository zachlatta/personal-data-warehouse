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
| issued token | `pdw_<id>_<secret>` | `full`: every bearer surface except token administration and mutation review decisions | dies after `idle_timeout` unused (default 30 days), optionally a hard `expires_at` |
| reviewer token (the phone app) | `pdw_<id>_<secret>` | `reviewer`: what `full` reaches, plus approving, denying, editing and undoing mutation requests | as `full` |
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

## The upload-only key: `PDW_INGEST_SECRET`

Uploads are authenticated by an HMAC over the endpoint, the body's sha256 and an expiry,
never by a bearer. Until 2026-11 the only key was the master secret, so every uploader host
held a credential that could also approve mutations and mint tokens. `PDW_INGEST_SECRET`
(at least 32 characters, different from the master secret) is a second key the app accepts
**for upload signatures only** (`auth.Service.SetIngestSecret`): as a bearer it is refused,
so a host that holds it can upload and nothing else. Uploaders sign with it ahead of the
master secret (`ingestclient.ResolveConfig`, `ingest_client.py`), and the master secret keeps
verifying uploads while hosts move over. An uploader host's env file holds
`PDW_INGEST_SECRET` and no `PDW_SECRET_TOKEN`.

## Who may decide a mutation request

Approve, deny, supersede, undo, cancel-by-reviewer and every edit route under
`/api/mutations/requests/<id>/…` require a principal that `CanReview`: the master secret
(the web app) or a `reviewer` token (the phone app). A `full` token, the kind a host CLI
holds, gets 403 there, as do `agent` and feed tokens: an agent proposes with
`propose_mutation` and withdraws with `withdraw_mutation`, and never decides. Reading a
request (`GET`) stays open to `full`. Issue the phone's token with
`pdw token create --name <phone> --scope reviewer`; never give a host a reviewer token, and never the master secret (hosts sign uploads with `PDW_INGEST_SECRET`).
The test that holds this is `TestOnlyAReviewerPrincipalReachesAReviewRoute`
(`app/internal/mutations/api_test.go`): add a new review route to its list.

## Workflow identities and `requested_by_token_id`

The `<client>` half of `Bearer <client>:<token>` is chosen by the caller, so it says
nothing about who is calling. Every mutation request therefore stores
`requested_by_token_id` beside `requested_by`: the issued token's id, or for an MCP
connector its grant id (both rows of `private.app_access_tokens`); empty for the master
secret. A mutation policy grant binds to that id, never to the name.

A scheduled workflow that proposes mutations gets its own token:
`pdw token create --name wf-<name> --scope agent --expires 2027-04-15`. An agent-scope
token issued this way **must** carry a hard expiry, at most 183 days away
(`MaxWorkflowTokenLifetime`), because a policy grant names that one token id for a season
and cannot follow weekly renewals the way a `cli_authorize` token is renewed. Keep it
readable only by the workflow (its own Unix user, mode `0600`), and design the grant so a
stolen token is an acceptable outcome.

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

## External feeds

The web app's **External feeds** tab (`/external-feeds`) issues dedicated keys,
shows creation, optional absolute expiry, last use and lifetime request counts,
and revokes keys. Administration uses the same root-only `/api/auth/tokens`
API as `pdw token`. Keys are shown once and stored only as SHA-256 hashes in
`private.app_access_tokens`; no new credential table or upstream MCP connection
is created. The name “External feeds” distinguishes these outgoing read interfaces
from **Connections**, which proxies upstream MCP tools.

The first scope is `feed:voice-memos`. It permits only
`GET /api/external-feeds/voice-memos`, with
`Authorization: Bearer <client>:<key>`. A feed key cannot query SQL, use MCP,
read other PDW endpoints or administer keys. Other credentials cannot read the
feed; issue a dedicated key. Feed keys have no idle expiry; an optional
`expires_at` (RFC3339, in the future) is a hard deadline. Revocation and expiry
are checked on every request, including conditional requests.

The response is `{feed: "feed:voice-memos", recordings: [...], next_cursor: "...", has_more: false}`.
Each recording includes `source`, `account`, `recording_id`, `content_sha256`,
`recorded_at`, duration, title, summary, transcript, participants/action-items/evidence
JSON text, calendar result and transcription/enrichment timestamps. Storage URLs
and audio credentials are excluded. Identity is the source/account/recording-id
triple; use `enriched_at` to detect revised enrichment.

Start with a GET without `since`. After processing a response, persist its opaque
`next_cursor` and pass it as `?since=<next_cursor>`. Drain pages immediately while
`has_more` is true; otherwise poll once per second with that cursor. Each response
has at most 200 recordings in arrival order. Keep the cursor on a 304 or error;
only advance after processing a successful response. Retain the ETag for the
same request URL and send `If-None-Match`; unchanged results return 304.

`ops.external_feed_items` stores only the current ready version of each item.
Refreshes read the unified mart, remove ineligible items, and allocate new arrival
numbers from `ops.external_feed_seq` when a memo becomes ready or its payload
changes. A transaction advisory lock covers allocation through commit, so a reader
cannot advance past an uncommitted arrival. Late/backdated enrichments and arrivals
between pages receive later numbers and cannot be skipped by cursor paging.
The indexed `(feed, sync_version)` read is bounded to 201 rows (one lookahead);
the entire refresh/read has a two-second deadline. The 48-hour window remains
based on recording time. A consumer offline beyond that window cannot recover
expired recordings from this endpoint.

Each refresh materializes the mart query once and filters changed payloads before
calling `nextval` or upserting. An unchanged refresh neither allocates sequence
values nor locks/writes item tuples; only the separate per-key usage counter is
written. The sequence's catalog-qualified name is bound as a regclass parameter,
not placed inside a quoted relation marker.

Voice enrichment must postdate its latest transcription (`enriched_at >= transcribed_at`).
A re-transcribed memo disappears until re-enrichment finishes. Calendar matching
is part of enrichment; unmatched outcomes qualify. Session joining does not hold
recordings back, so a stopped-and-restarted memo may arrive as separate parts.
There are no deletion notices or tombstones when a memo disappears or ages out.
A memo that re-qualifies after re-transcription and re-enrichment is delivered
again at a new cursor position. Consumers should upsert/deduplicate by the stable
`(source, account, recording_id)` identity, rather than treating each delivery as
a different recording.

The registry in `app/internal/externalfeeds/registry.go` supplies scope, endpoint,
display name, source query and handler. Authentication, server mounting and the
web UI consume it; the UI discovers definitions at `GET /api/external-feeds`.
Voice memos are the only registered feed.

Usage counts every authorized GET, including 304 and failed feed reads, and stamps
last use on each request. Per-poll writes change only unindexed `request_count`
and `last_used_at`, allowing HOT updates rather than churning the `updated_at`
index. Counters are exact and constant space per key; there is no per-poll log.
If usage cannot be recorded, the feed returns 503 rather than serving an untracked request.
