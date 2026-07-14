# personal_data_warehouse

This project syncs Gmail mailbox data, Google Calendar events, Slack data, WHOOP health data, Apple Notes, Apple Messages, Voice Memos, and Plaid-backed personal finance data into Postgres through Dagster.

Current ingestion path:

- Gmail mailboxes are synced with the Gmail API using per-account OAuth tokens.
- The first run does a full mailbox sync.
- Later runs are incremental via Gmail `history.list`.
- If Gmail expires the saved history cursor, the sync falls back to a full resync for that mailbox.
- Google Calendar events use the same per-account Google OAuth tokens.
- Calendar first runs do a full event sync, then later runs use Google Calendar `syncToken`.
- If Calendar expires the saved sync token, the sync falls back to a full resync for that calendar.
- Google Contacts syncs intentionally saved contacts for configured Google accounts through the
  People API. It mirrors source cards with raw JSON payloads, sync tokens, and tombstones; it does
  not import Gmail autocomplete/Other Contacts or infer people across sources.
- Voice Memos use a two-stage path: a local macOS uploader posts audio files and JSON metadata
  to the app's `/ingest/...` endpoints; the app writes Google Drive inbox objects, then Dagster
  ingests those metadata into Postgres.
- Apple Notes use the same two-stage local Mac path: a LaunchAgent snapshots the local Notes
  SQLite store, posts immutable note revision payloads to the app, and Dagster ingests the
  app-written Drive objects.
- Apple Messages use the same local Mac app-ingest path: a LaunchAgent snapshots `chat.db`,
  posts compressed message batches plus bounded attachment backfill objects to the app, and
  Dagster ingests them into normalized Postgres tables.
- WHOOP syncs read-only health data through the WHOOP v2 OAuth API: profile, body measurements,
  cycles, recovery, sleep, and workouts land in source-owned `whoop.*` Postgres tables.
- Plaid uses an interactive local Link flow to authorize personal institutions. Access tokens stay
  in the private Postgres schema; a Dagster asset incrementally syncs account/balance,
  transaction, investment, and liability data into the source-owned `plaid` schema.

## CLI: `pdw`

`pdw` is the command-line client for the warehouse's HTTP API. Install
the latest release with:

```bash
curl -fsSL https://raw.githubusercontent.com/zachlatta/personal-data-warehouse/main/app/install.sh | sh
```

The script detects your OS/architecture, downloads the matching binary from
the latest GitHub release, verifies it against `SHA256SUMS`, and installs it
as `pdw` in `/usr/local/bin` (or `~/.local/bin` if that's not writable). If the
install directory isn't on `PATH`, it appends an `export` line to your
shell's rc file (`~/.zshrc`, `~/.bashrc`/`~/.bash_profile`,
`~/.config/fish/config.fish`, or `~/.profile`) — re-runs are idempotent
thanks to a marker comment. Override with `PDW_INSTALL_DIR=/some/dir`,
pin a version with `PDW_VERSION=pdw-cli/v0.1.0`, or point at a fork
with `PDW_REPO=owner/repo`. (The legacy `PDW_CLI_*` names still work, and the
GitHub release/tag artifacts keep the `pdw-cli` name so binaries installed
before the rename can still self-update.)

After installing:

```bash
pdw login    # interactive: stores API URL + token in ~/.config/pdw/config.json
pdw list     # confirms the connection works
pdw update   # later: self-updates to the newest release
```

See [`app/README.md`](app/README.md) for the full command reference.

## Dependency Management

This repo uses [`uv`](https://docs.astral.sh/uv/).

Install dependencies:

```bash
uv sync
```

Run commands through the managed environment:

```bash
uv run pytest
uv run dg dev
```

## Required Environment

Add these to `.env`:

```bash
POSTGRES_DATABASE_URL=...
GMAIL_ACCOUNTS=zach@zachlatta.com,zach@hackclub.com
```

Optional:

```bash
GMAIL_DOMAIN_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON_B64=...
GMAIL_DOMAIN_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON_B64=...
GMAIL_ZACH_ZACHLATTA_COM_TOKEN_JSON_B64=...
GMAIL_ZACH_HACKCLUB_COM_TOKEN_JSON_B64=...
GOOGLE_ZACH_ZACHLATTA_COM_TOKEN_JSON_B64=...
GOOGLE_ZACH_HACKCLUB_COM_TOKEN_JSON_B64=...
GMAIL_PAGE_SIZE=500
GMAIL_INCLUDE_SPAM_TRASH=true
GMAIL_ATTACHMENT_MAX_BYTES=26214400
GMAIL_ATTACHMENT_TEXT_MAX_CHARS=1000000
GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE=100
CALENDAR_ACCOUNTS=zach@hackclub.com
CALENDAR_ZACH_HACKCLUB_COM_CALENDAR_IDS=primary
CALENDAR_PAGE_SIZE=2500
CALENDAR_EXPANDED_SYNC_LOOKBACK_DAYS=365
CALENDAR_EXPANDED_SYNC_LOOKAHEAD_DAYS=365
CALENDAR_EXPANDED_SYNC_INTERVAL_MINUTES=60
CONTACT_GOOGLE_ACCOUNTS=you@work.example,you@personal.example
CONTACT_PAGE_SIZE=1000
CONTACT_FORCE_FULL_SYNC=false
VOICE_MEMOS_ACCOUNT=you@example.com
PDW_API_URL=https://your-public-pdw-app
PDW_SECRET_TOKEN=<same-secret-as-pdw-app>
VOICE_MEMOS_EXTENSIONS=.m4a,.qta
APPLE_MESSAGES_ACCOUNT=you@example.com
APPLE_MESSAGES_STORE_PATH=~/Library/Messages/chat.db
APPLE_MESSAGES_ATTACHMENT_BYTES_PER_RUN=536870912
APPLE_MESSAGES_ATTACHMENT_COUNT_PER_RUN=200
APPLE_MESSAGES_UPLOAD_WORKERS=4
WHOOP_ACCOUNT=you@example.com
WHOOP_CLIENT_ID=...
WHOOP_CLIENT_SECRET=...
WHOOP_REDIRECT_URI=http://localhost:8080/callback
WHOOP_TOKEN_JSON_B64=...
WHOOP_PAGE_SIZE=25
WHOOP_INCREMENTAL_LOOKBACK_DAYS=14
WHOOP_MAX_RATE_LIMIT_SLEEP_SECONDS=120
WHOOP_FULL_SYNC_START=2022-01-01T00:00:00Z
```

Notes:

- OAuth client secrets are only needed when running the browser auth flow.
- Use a separate OAuth app per email domain: `GMAIL_DOMAIN_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON_B64` for `hackclub.com`, `GMAIL_DOMAIN_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON_B64` for `zachlatta.com`, and so on. `GOOGLE_DOMAIN_<DOMAIN_SLUG>_OAUTH_CLIENT_SECRETS_JSON_B64` is also supported.
- The legacy global `GMAIL_OAUTH_CLIENT_SECRETS_JSON_B64` fallback is only used when the configured Google accounts span one email domain.
- Sync runtime requires one `GOOGLE_<ACCOUNT_SLUG>_TOKEN_JSON_B64` or legacy `GMAIL_<ACCOUNT_SLUG>_TOKEN_JSON_B64` value per Google account.
- Google OAuth client secrets and account tokens are env-only; the app does not read or write Google secrets from the filesystem.
- Calendar sync defaults to the accounts in `GMAIL_ACCOUNTS` and the `primary` calendar unless `CALENDAR_ACCOUNTS` or `CALENDAR_<ACCOUNT_SLUG>_CALENDAR_IDS` are set.
- Calendar sync also expands recurring event instances in a rolling window so generated occurrences can be matched directly. The expanded pass runs hourly by default, and immediately after full syncs or recurrence-related changes. Tune it with `CALENDAR_EXPANDED_SYNC_LOOKBACK_DAYS`, `CALENDAR_EXPANDED_SYNC_LOOKAHEAD_DAYS`, and `CALENDAR_EXPANDED_SYNC_INTERVAL_MINUTES`.
- Contacts sync uses `CONTACT_GOOGLE_ACCOUNTS` and intentionally saved Google Contacts only. It does
  not sync Gmail autocomplete/Other Contacts or Workspace directory contacts in v1.
- WHOOP OAuth client secrets and the bootstrap token are env-only. `WHOOP_TOKEN_JSON_B64` is
  generated by `personal-data-warehouse-whoop-auth --write-env`; do not commit it. Runtime token
  rotations are stored only in the warehouse's private schema, as described below.

## Mutation Approval UI

The Go MCP server can expose Gmail, Calendar, and Contacts mutation proposal tools plus a browser review UI for
pending requests. Set a password before starting it:

```bash
PDW_MUTATION_UI_PASSWORD=...
```

Approval URLs returned by MCP tools point at `/mutation-review/requests/<request_id>` and redirect
to `/mutation-review/login` until you sign in. The UI uses an HttpOnly session cookie plus per-form
CSRF tokens for approve and deny actions. The old per-action `PDW_MUTATION_REVIEW_PIN` flow is no
longer used.

Optional UI settings:

```bash
PDW_MUTATION_UI_SESSION_SECRET=<high-entropy-secret>
PDW_MUTATION_UI_SESSION_TTL_SECONDS=43200
```

If `PDW_MUTATION_UI_SESSION_SECRET` is omitted, the process generates an ephemeral signing secret
at startup, which logs out existing browser sessions on restart.

## Gmail Auth

Authorize each mailbox once before running the sync:

```bash
uv run personal-data-warehouse-gmail-auth --email zach@zachlatta.com
uv run personal-data-warehouse-gmail-auth --email zach@hackclub.com
```

Or authorize every configured mailbox:

```bash
uv run personal-data-warehouse-gmail-auth --all
```

This uses browser-based OAuth for both Gmail readonly and Calendar readonly scopes, then prints
`GOOGLE_<ACCOUNT_SLUG>_TOKEN_JSON_B64=...` and legacy `GMAIL_<ACCOUNT_SLUG>_TOKEN_JSON_B64=...`
lines to add to `.env` or Coolify. `personal-data-warehouse-google-auth` is an alias for
the same auth flow.

Add each domain's OAuth app client secrets before authorizing mailboxes in that domain:

```bash
GMAIL_DOMAIN_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON_B64=...
uv run personal-data-warehouse-google-auth --email zach@zachlatta.com --write-env
```

If Contacts are configured, the auth flow also requests Google Contacts readonly access. Google
Drive access is needed only for server-side object-store readers/writers such as the app ingest
service, Drive ingest assets, Gmail attachment blob storage, and Alice archival; local device
uploaders do not hold Drive credentials.

## Google Contacts Sync

Configure the Google accounts whose intentionally saved contacts should be mirrored:

```bash
CONTACT_GOOGLE_ACCOUNTS=you@work.example,you@personal.example
CONTACT_PAGE_SIZE=1000
CONTACT_FORCE_FULL_SYNC=false
```

Re-authorize each account after adding contacts config so the token includes Contacts readonly:

```bash
uv run personal-data-warehouse-google-auth --email you@work.example --write-env
uv run personal-data-warehouse-google-auth --email you@personal.example --write-env
```

Run the sync manually:

```bash
uv run personal-data-warehouse-contacts-sync
```

Dagster exposes `google_contacts_sync` in the `contacts` group, scheduled hourly by
`contacts_sync_hourly`. The sync writes `contact_cards` and `contact_sync_state`, with
`clean_contacts` as the current non-deleted view.

## WHOOP Sync

WHOOP sync is a read-only OAuth integration against the WHOOP v2 API. It writes the main
health data collections into source-owned tables:

- `whoop.profiles` (`whoop_profiles` logical name): basic profile (`read:profile`)
- `whoop.body_measurements` (`whoop_body_measurements`): height/weight/max HR (`read:body_measurement`)
- `whoop.cycles` (`whoop_cycles`): physiological cycles and strain (`read:cycles`)
- `whoop.recoveries` (`whoop_recoveries`): recovery scores and HRV/RHR/SpO2/skin temp (`read:recovery`)
- `whoop.sleeps` (`whoop_sleeps`): sleeps/naps, stages, sleep need, respiratory rate (`read:sleep`)
- `whoop.workouts` (`whoop_workouts`): workouts, sport, strain, HR, distance, zones (`read:workout`)
- `whoop.sync_state` (`whoop_sync_state`): per-collection sync status and watermarks

Cycles, recovery scores, sleeps, and workouts are also projected into the warehouse's unified
`timeline.events` surface through dedicated WHOOP timeline adapters; profile/body snapshots remain
source entities rather than repeated timeline events.

Create a WHOOP app in the WHOOP Developer Dashboard, add the redirect URI you will use locally
(default `http://localhost:8080/callback`), and request these scopes:

```text
offline read:profile read:body_measurement read:cycles read:recovery read:sleep read:workout
```

Configure client credentials, then authorize once in a browser:

```bash
WHOOP_ACCOUNT=you@example.com
WHOOP_CLIENT_ID=...
WHOOP_CLIENT_SECRET=...
WHOOP_REDIRECT_URI=http://localhost:8080/callback
uv run personal-data-warehouse-whoop-auth --write-env
```

When the browser runs on another computer, use manual callback mode. After WHOOP redirects to
localhost, the page may fail to load; paste its complete address-bar URL into the prompt:

```bash
uv run personal-data-warehouse-whoop-auth --manual --no-browser --write-env
```

The auth command writes `WHOOP_TOKEN_JSON_B64` to `.env`. That value contains OAuth tokens and
must stay out of git, logs, and screenshots. The env token bootstraps the first warehouse run.
WHOOP rotates and invalidates both access and refresh tokens on refresh, so the sync persists each
rotated token under `private.whoop_oauth_tokens`; subsequent runs load that private row instead of
reusing the stale bootstrap token. Dagster's WHOOP advisory lock prevents concurrent scheduled
refreshes from racing the single-use refresh token. To verify credentials without syncing, run:

```bash
uv run personal-data-warehouse-whoop-sync --validate
```

A redacted record of the user-assisted live v2 endpoint verification performed during integration
is checked in at [`docs/verification/whoop-live-api-2026-07-11.json`](docs/verification/whoop-live-api-2026-07-11.json).
It retains only endpoint status, record counts, and field names—never response values or OAuth
credentials.

Run a sync manually:

```bash
uv run personal-data-warehouse-whoop-sync
```

Dagster exposes `whoop_sync` in the `whoop` group, scheduled every five minutes by
`whoop_sync_every_five_minutes`. The schedule defaults to running only when the account, client
credentials, and token are present at code-location load; otherwise it stays safely stopped.
`WHOOP_ENABLED=0` disables the asset. `WHOOP_PAGE_SIZE` defaults
to 25, WHOOP's current max collection page size. On a first run, collection endpoints are called
without a `start` filter unless `WHOOP_FULL_SYNC_START` is set (for example
`2022-01-01T00:00:00Z`). Later runs use each collection's stored watermark minus
`WHOOP_INCREMENTAL_LOOKBACK_DAYS` (default 14) as the `start` window, making normal incremental
runs idempotent while still overlapping recent WHOOP score updates. Use `WHOOP_FORCE_FULL_SYNC=1`
for an explicit backfill.

SQL starting points:

```sql
SELECT start_at, strain, average_heart_rate, max_heart_rate
FROM whoop.cycles ORDER BY start_at DESC LIMIT 30;

SELECT start_at, end_at, sleep_performance_percentage, respiratory_rate
FROM whoop.sleeps WHERE nap = 0 ORDER BY start_at DESC LIMIT 30;

SELECT start_at, sport_name, strain, distance_meter
FROM whoop.workouts ORDER BY start_at DESC LIMIT 30;

SELECT updated_at, recovery_score, resting_heart_rate, hrv_rmssd_milli
FROM whoop.recoveries ORDER BY updated_at DESC LIMIT 30;
```

WHOOP API notes followed by the implementation: base URL `https://api.prod.whoop.com/developer`,
OAuth auth URL `https://api.prod.whoop.com/oauth/oauth2/auth`, token URL
`https://api.prod.whoop.com/oauth/oauth2/token`, paginated collection responses use `records` +
`next_token` and the next request sends `nextToken`, and default WHOOP app rate limits are 100
requests/minute and 10,000/day with `429` plus rate-limit reset headers. Short rate limits are
retried in place (bounded by `WHOOP_MAX_RATE_LIMIT_SLEEP_SECONDS`, default 120) so a multi-page
first backfill can progress without restarting from page one; long/daily limits fail the run for
operator visibility instead of sleeping a Dagster worker for hours.

## Google Drive Source Sync

Mirrors your Google Drive *files* (metadata + extracted text) into the warehouse so they
surface through the unified `search_text()` function. This is Drive **as a data source**, distinct
from the Drive folders the storage backends above use as a transport/inbox — those transport
folders are excluded automatically so the warehouse never re-ingests its own staging blobs.

```bash
# Enabled by default for every GMAIL_ACCOUNTS mailbox; set to 0 to disable.
# GOOGLE_DRIVE_SOURCE_ENABLED=0
# Or list accounts explicitly instead of using every Gmail account.
# GOOGLE_DRIVE_SOURCE_ACCOUNTS=you@work.example,you@personal.example

# Additional folder IDs to exclude (unioned with the auto-detected transport folders),
# and optional path-prefix globs against the resolved "/My Drive/..." path.
# GOOGLE_DRIVE_EXCLUDE_FOLDER_IDS=0AExampleFolderId,0AAnotherFolderId
# GOOGLE_DRIVE_EXCLUDE_PATH_GLOBS=/Backups/*,/Archive/*

# Corpora (default: owned "My Drive" only). Opt into the noisier corpora as needed.
# GOOGLE_DRIVE_SOURCE_INCLUDE_SHARED_WITH_ME=0
# GOOGLE_DRIVE_SOURCE_INCLUDE_SHARED_DRIVES=0

# Tuning.
# GOOGLE_DRIVE_SOURCE_TEXT_MAX_CHARS=5000000
# GOOGLE_DRIVE_SOURCE_EXTRACT_MAX_BYTES=26214400
# GOOGLE_DRIVE_SOURCE_BINARY_EXTRACTION=0   # PDF/Office (Phase 3, not yet implemented)
# GOOGLE_DRIVE_SOURCE_FILES_PER_RUN=5000
```

Re-authorize each account so its token carries the full Drive scope (existing Drive-transport
accounts already have it):

```bash
uv run personal-data-warehouse-google-auth --email you@work.example --write-env
```

Dagster exposes `google_drive_source_sync` in the `google_drive` group, scheduled every 30
minutes by `google_drive_source_sync_every_thirty_minutes`. The first run does a full
`files.list` crawl and records a Drive Changes `startPageToken`; later runs read
`changes.list` incrementally. Text comes from Google-native exports (Docs→text, Sheets→CSV,
Slides→text) and already-text files (`.md`, `.txt`, `.json`, source code, ...). The sync writes
`google_drive_files`, `google_drive_file_texts`, and `google_drive_sync_state`.

File bytes are **not** copied into the warehouse: rows carry `storage_backend='google_drive_source'`
and `storage_file_id=<Drive file id>`. The Go app's `get_object` MCP tool mints an account-aware,
time-limited `download_url` (`/objects/{id}?account=...&exp=...&sig=...`) that streams the file
live from the owning account's Drive. The Go app resolves each account's token from the same
`GOOGLE_<SLUG>_TOKEN_JSON[_B64]` / `GMAIL_<SLUG>_...` env vars the sync uses.

## Running The Sync

Start Dagster:

```bash
uv run dg dev
```

Then materialize the `gmail_mailbox_sync` asset from the Dagster UI.
Materialize `calendar_event_sync` to sync Google Calendar events. Calendar sync uses Google's incremental cursor for ordinary changes and a rolling expanded-instance window for recurring event occurrences.

The Docker/Coolify deployment also includes an enabled Dagster schedule,
`gmail_mailbox_sync_every_fifteen_minutes`, which runs the Gmail sync every 15 minutes.
A full mailbox sync takes several minutes, so this cadence leaves an idle gap between runs
instead of running back-to-back continuously (which kept the host under sustained load).
Gmail syncs use a nonblocking lock so a scheduled tick skips if another sync is still running.
When `DAGSTER_POSTGRES_URL` or `DATABASE_URL` is set, the lock uses a Postgres advisory lock;
otherwise it falls back to a local process lock.

Each Gmail asset run also drains up to `GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE`
already-synced attachment-candidate messages per mailbox. This backfills attachment
text without advancing Gmail history cursors. Set it to `0` to disable this pass.
Image attachments and image-only PDFs that deterministic extraction cannot read
are enriched asynchronously by the `gmail_attachment_enrichment` asset, which runs
each attachment through the sandboxed agent container (the same Codex/Claude CLI
runner used for Voice Memos enrichment). The agent views the image and returns
structured metadata (summary, exact visible text, entities, search keywords) that
is upserted into the shared `file_attachment_enrichments` table under the
`agent_<provider>` identity. An hourly schedule plus a backlog sensor drain candidates in batches of
`GMAIL_ATTACHMENT_ENRICHMENT_BATCH_SIZE` (default 25); attachments whose agent
runs keep erroring are retried up to `GMAIL_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS`
(default 3) times. Enrichment reads attachment bytes from the Gmail attachment
object store, so blob storage (below) must be configured.

Gmail sync can also upload each attachment's raw bytes to Google Drive alongside
the extracted text. By default attachments land in the same object store as the
other Drive pipelines: the folder and account fall back to
`VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID` and `VOICE_MEMOS_ACCOUNT`, so storage is on
whenever that shared folder is configured. Set `GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID`
to store them in a different folder, or leave both unset to disable blob storage.
Blobs are content-addressed by SHA-256 (so identical attachments are stored once,
and they coexist with other pipelines in the shared folder via distinct `pdw_source`
and key prefixes). The resulting Drive pointer is recorded in the `storage_backend`,
`storage_key`, `storage_file_id`, and `storage_url` columns of `gmail_attachments`.
Uploads use a single Drive account for every mailbox (`GMAIL_ATTACHMENT_GOOGLE_DRIVE_ACCOUNT`,
falling back to `VOICE_MEMOS_ACCOUNT`) so attachments from mailboxes whose own OAuth
project lacks the Drive API still reach the shared store.

Slack sync splits freshness, coverage, and metadata into separate schedules. The
`slack_workspace_sync_every_five_minutes` schedule keeps recent messages fresh every five minutes.
`slack_workspace_coverage_sync_every_seven_minutes` backfills incomplete cached conversations
with smaller batches, and `slack_workspace_metadata_sync_every_fifteen_minutes` refreshes one
capped page of active conversation metadata at a time. `slack_workspace_user_sync_hourly`
refreshes the full Slack user list outside the more frequent metadata path. It runs at minute 11
and retries lock contention, so it does not silently report success while another Slack stage holds
the shared sync lock.
`slack_workspace_thread_sync_every_five_minutes` backfills `conversations.replies` for known
thread parents. It defaults to one thread per run and only revisits completed threads when the
parent's `latest_reply_ts` has advanced beyond the stored thread cursor, so it remains gentle on
Slack's thread API rate limits. Threads that already produced Slack API errors are skipped by the
scheduled pass so one inaccessible thread does not block the backlog. Configure it with
`SLACK_ASSET_THREAD_LIMIT`, `SLACK_ASSET_THREAD_SINCE_DAYS`, and `SLACK_ASSET_THREAD_ORDER`.
`slack_workspace_read_state_sync_every_five_minutes` refreshes `conversations.info` for a small
set of recently active account-relevant conversations, including DMs, group DMs, and member
channels, staggered two minutes after the thread schedule. This updates user-specific fields such
as `last_read` for deriving unread state. The every-minute freshness sync also piggybacks the same
read-state refresh while it already holds the Slack lock, so current-account state does not depend
on a separate read-state run winning the scheduler race. Configure it with
`SLACK_ASSET_READ_STATE_LIMIT`.
Freshness stages also poll capped sets of cached conversations per type, ordered by recent
activity, so each scheduled run remains bounded.
All Slack schedules share a nonblocking Slack lock, so most scheduled ticks skip if another Slack
sync stage is still running. The hourly Slack user sync is stricter: lock contention raises and
lets Dagster retry because a skipped user refresh is otherwise easy to miss.
Calendar sync runs through `calendar_event_sync_every_five_minutes` with its own nonblocking lock.

## Plaid Finance Sync

Plaid raw data is source-owned: physical relations live under `plaid.*`, not a generic
`finance` source schema. Stable finance-domain read views live under `marts.finance_*`.
Access tokens are isolated in `private.plaid_item_tokens`; normal read-only schema discovery and
query surfaces do not expose the private schema. Plaid's legacy public key is not used by the
modern Link-token API.

Configure the `PLAID_*` variables below and ensure `POSTGRES_DATABASE_URL` points at the target
warehouse:

```bash
PLAID_ACCOUNT=you@example.com
PLAID_CLIENT_ID=...
PLAID_SECRET=...
PLAID_ENV=production                 # sandbox, development, or production
PLAID_PRODUCTS=transactions,investments,liabilities
PLAID_COUNTRY_CODES=US
PLAID_TRANSACTIONS_LOOKBACK_DAYS=730 # Plaid's maximum transaction history window
PDW_QUERY_POSTGRES_ROLE=pdw_query      # NOLOGIN role assumed by user-authored SQL
# PLAID_REDIRECT_URI=https://registered.example/plaid/oauth-return
```

Keep the client secret in uncommitted environment/config only. Link one institution at a time:

```bash
pdw ingest plaid link
# If running directly from a checkout before updating the pdw binary:
uv run python -m personal_data_warehouse_plaid.cli link
```

The command creates a short-lived Link token, opens a localhost page, and prompts through Plaid
Link/OAuth/MFA. The browser returns only the one-time public token to localhost; the CLI exchanges
it server-side and writes the resulting access token directly to the private Postgres table. User
cancellation and Plaid errors stop the local server and return an actionable CLI error. OAuth
redirects resume the same Link token via Plaid's `receivedRedirectUri`; configure a fixed
`--host`/`--port` whose callback URL matches the redirect registered in the Plaid dashboard (or
route the registered HTTPS URL to it). Never paste access/public tokens into logs, issues, or
committed files. Repeat `link` for each personal institution, then sync all linked items:

When Transactions is enabled, Link requests `PLAID_TRANSACTIONS_LOOKBACK_DAYS` of history. The
setting defaults to Plaid's maximum of 730 days and also controls the Investments transaction
query window. Plaid fixes the Transactions history grant when an Item is first initialized, so
changing this setting cannot expand an existing Item: remove the old Item through Plaid and run
the Link flow again. The institution may return less than the requested maximum.

```bash
pdw ingest plaid sync
# Direct checkout equivalent:
uv run python -m personal_data_warehouse_plaid.cli sync
```

The `plaid_finance_sync` Dagster asset performs the same sync. Its enabled
`plaid_finance_sync_every_thirty_minutes` schedule uses a nonblocking Postgres advisory lock and
runs every 30 minutes. Production must provide `PLAID_ACCOUNT`, `PLAID_CLIENT_ID`, `PLAID_SECRET`,
`PLAID_ENV`, and `POSTGRES_DATABASE_URL` to the Dagster process. Set `PLAID_REDIRECT_URI` to an
HTTPS URI registered in the Plaid dashboard before linking OAuth institutions that require a
redirect URI. Optional tuning: `PLAID_CLIENT_NAME`, `PLAID_PRODUCTS`, `PLAID_COUNTRY_CODES`,
`PLAID_LANGUAGE`, `PLAID_WEBHOOK`, `PLAID_REQUEST_TIMEOUT_SECONDS`, and
`PLAID_TRANSACTIONS_LOOKBACK_DAYS`. Account, holding, and liability endpoints are authoritative
snapshots: accounts absent from a later snapshot are tombstoned, while absent holdings and
liabilities are removed. Product failures are written to `plaid.sync_state` with a redacted error
before the overall run fails, so an old `ok` cannot mask a broken credential/product.

Warehouse initialization provisions the NOLOGIN role named by `PDW_QUERY_POSTGRES_ROLE` (default
`pdw_query`), grants it read access only to queryable schemas, and revokes `private` from both that
role and `PUBLIC`. The Go MCP/HTTP SQL runner and Python `PostgresReadOnlyRunner` assume this role
for every user-authored query; startup/query execution therefore fails closed if the restricted
role has not been provisioned. Sync and credential writers continue to use the owner connection
without assuming the restricted role.

Generate the private per-institution/per-account linking report after each linking or verification
session. It contains status/presence flags and last-pull timestamps but no IDs, masks, balances,
transaction details, holdings values, or tokens; `*.private.md` is gitignored:

```bash
uv run python scripts/plaid_linking_report.py
# writes reports/plaid-linking-report.private.md with mode 0600
```

Safe ad-hoc verification queries should avoid selecting masks, balances, transaction details,
holdings, raw JSON, or private tokens. Useful aggregate/status surfaces are:

```sql
SELECT product, status, last_synced_at FROM plaid.sync_state ORDER BY product;
SELECT count(*) FROM marts.finance_accounts;
SELECT count(*) FROM marts.finance_transactions;
SELECT count(*) FROM marts.finance_investment_holdings;
SELECT count(*) FROM marts.finance_investment_transactions;
SELECT count(*) FROM marts.finance_liabilities;
```

## Deployment Metadata

The Docker images expose the deployed Git commit through `PDW_GIT_SHA`. Both the Dagster service
and the MCP/API service also accept common platform names such as `SOURCE_COMMIT`, `GIT_SHA`,
`GIT_COMMIT`, `COMMIT_SHA`, and `COOLIFY_GIT_COMMIT`; the Dockerfiles accept the same names as
build args and bake the selected value into the image. Startup logs and the app root endpoint print
the resolved SHA, and Slack Dagster materializations include it as `git_sha` metadata.

For Coolify Dockerfile apps, enable the Advanced setting that includes `SOURCE_COMMIT` in Docker
build arguments. Coolify excludes it by default to preserve build cache reuse.

## Docker / Coolify

This repo includes a `Dockerfile` that runs Dagster on port `3000` with `uv`.
The Docker image uses Postgres-backed Dagster storage via `docker/dagster.yaml`.

For Coolify:

1. Create a new Dockerfile-based application from this repo.
2. Attach or create a Coolify Postgres database.
3. Set the exposed port to `3000`.
4. Add the runtime env vars:

```bash
DAGSTER_POSTGRES_URL=postgresql://...
POSTGRES_DATABASE_URL=postgresql://...
GMAIL_ACCOUNTS=zach@hackclub.com
GOOGLE_ZACH_HACKCLUB_COM_TOKEN_JSON_B64=...
GMAIL_PAGE_SIZE=500
GMAIL_INCLUDE_SPAM_TRASH=true
GMAIL_ATTACHMENT_MAX_BYTES=26214400
GMAIL_ATTACHMENT_TEXT_MAX_CHARS=1000000
GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE=100
CALENDAR_ACCOUNTS=zach@hackclub.com
CALENDAR_ZACH_HACKCLUB_COM_CALENDAR_IDS=primary
CALENDAR_PAGE_SIZE=2500
```

If Coolify exposes the database as `DATABASE_URL`, the container entrypoint maps it to `DAGSTER_POSTGRES_URL` automatically. It also normalizes `postgres://...` to `postgresql://...` because Dagster's SQLAlchemy storage expects the `postgresql` dialect name.

5. Deploy, open the app URL, and materialize `gmail_mailbox_sync` in Dagster.

The token JSON generated by `personal-data-warehouse-gmail-auth` or
`personal-data-warehouse-google-auth` already includes the OAuth client details needed for refresh.

For a Coolify scheduled task instead of clicking the Dagster UI, run the existing Gmail asset:

```bash
uv run python -c "from dagster import materialize; from personal_data_warehouse.defs.gmail_sync import gmail_mailbox_sync; raise SystemExit(0 if materialize([gmail_mailbox_sync]).success else 1)"
```

Calendar asset:

```bash
uv run python -c "from dagster import materialize; from personal_data_warehouse.defs.calendar_sync import calendar_event_sync; raise SystemExit(0 if materialize([calendar_event_sync]).success else 1)"
```

## Voice Memos Sync

Voice Memos are split across three responsibilities so the Mac never talks to the warehouse or
Google Drive directly:

1. A local macOS CLI scans Apple's Voice Memos recordings directory and uploads new `.m4a`
   and `.qta` audio payloads to the app's semantic ingest endpoints, alongside one JSON metadata
   payload per audio file.
2. The app verifies the upload signature and writes byte-identical Google Drive inbox objects.
3. The remote Dagster asset reads inbox metadata, writes metadata rows to Postgres, then promotes
   the audio and JSON objects into the library prefix.

Configure the local Mac uploader with the app URL and signing secret by running
`pdw login` once, or by setting the same env vars pdw uses:

```bash
VOICE_MEMOS_ACCOUNT=you@example.com
PDW_API_URL=https://your-public-pdw-app
PDW_SECRET_TOKEN=<same-secret-as-pdw-app>
VOICE_MEMOS_EXTENSIONS=.m4a,.qta
```

The legacy `MCP_BASE_URL` / `MCP_SECRET_TOKEN` names are still accepted as
aliases for `PDW_API_URL` / `PDW_SECRET_TOKEN`.

Configure the app/Dagster object store with the shared Drive folder and re-authorize that Google
account so the token includes Drive scope:

```bash
PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
uv run personal-data-warehouse-google-auth --email you@example.com --write-env
```

Run the local Mac uploader:

```bash
pdw ingest voice-memos
```

The uploader defaults to a lightweight incremental mode for scheduled background runs. Incremental mode keeps local
state in `~/Library/Application Support/personal-data-warehouse/voice-memos-upload-state.json`;
unchanged recordings that already uploaded both audio and metadata are skipped before hashing or
network calls. Use full mode for periodic repair/backfill:

```bash
pdw ingest voice-memos --mode full
```

On Zach's MacBook Pro, the local uploader is managed by a per-user macOS LaunchAgent
instead of cron. The checked-in plist template lives at
`ops/launchd/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist` and runs
`bin/voice-memos-upload-launchd` every five minutes. The wrapper records each run, exit
code, duration, and heartbeat under `~/Library/Logs/personal-data-warehouse/`.

Install or refresh the LaunchAgent:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
```

Run it immediately:

```bash
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
```

Monitor it:

```bash
bin/voice-memos-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
tail -80 ~/Library/Logs/personal-data-warehouse/voice-memos-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/voice-memos-upload.heartbeat
```

If LaunchAgent runs fail with `PermissionError: [Errno 1] Operation not permitted` for
`~/Library/Group Containers/group.com.apple.VoiceMemos.shared/Recordings`, macOS Full Disk Access
is blocking the background process. Grant Full Disk Access in System Settings > Privacy & Security
> Full Disk Access to the executable chain used by the job, especially `/bin/zsh`,
`/opt/homebrew/bin/uv`, `/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`, and
its current real path `/Users/zrl/.local/share/uv/python/cpython-3.12.12-macos-aarch64-none/bin/python3.12`.
After changing privacy permissions, rerun:

```bash
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
bin/voice-memos-upload-status
```

The Mac uploader skips expected network no-op cases with exit code `0`: no default route,
blocked tethered/mobile/in-flight network, failed Google Drive preflight, or transient Drive
timeouts. Blocked Wi-Fi SSID and hardware-port regexes can be customized with
`VOICE_MEMOS_UPLOAD_BLOCKED_SSID_PATTERNS` and
`VOICE_MEMOS_UPLOAD_BLOCKED_HARDWARE_PORT_PATTERNS` as comma-separated regex lists.
On recent macOS releases, SSID visibility is protected by Location Services. The uploader tries
CoreWLAN first, then `networksetup`, `ipconfig`, and `system_profiler`, but a CLI may still see an
unavailable or redacted SSID until the launching app/process has location permission. Check what the
uploader can see with:

```bash
pdw ingest voice-memos --network-diagnostics
```

If it reports `SSID unavailable`, grant Location Services permission to the app launching the
command, such as Terminal or iTerm, in System Settings > Privacy & Security > Location Services.
Classic `cron` may not have a grantable GUI app identity on current macOS; in that case use the
default fail-open unknown-SSID behavior plus Drive preflight, or run the job from a user LaunchAgent
associated with a location-authorized app. Set `VOICE_MEMOS_UPLOAD_REQUIRE_WIFI_SSID=true` only if
unavailable SSID should fail closed.

The configured Drive folder is treated as the object-storage root. The uploader creates real
subfolders under that root, using colocated inbox object keys such as
`apple-voice-memos/inbox/YYYY/MM/YYYY-MM-DD-<sha256>.qta` and
`apple-voice-memos/inbox/YYYY/MM/YYYY-MM-DD-<sha256>.json`. The JSON body is storage-location-free:
it stores recording metadata, not Drive paths or future S3 keys. Dagster derives provider locations
from the backend context and stores them in warehouse columns. Drive `appProperties` are used only
for short indexing fields such as `pdw_stage`, `pdw_kind`, and content hashes. Dagster only scans
`pdw_stage=inbox`, then promotes processed files to `apple-voice-memos/library/YYYY/MM/`.
A future S3 backend can keep the same metadata format and swap only the object-store implementation.

## Apple Notes Sync

Apple Notes follows the Voice Memos split so the Mac never talks to the warehouse or Google Drive
directly:

1. A local macOS CLI snapshots `NoteStore.sqlite`, extracts notes, folders, readable HTML/text, and
   locally available attachments, then posts immutable revision payloads to the app.
2. The app verifies the upload signature and writes the Google Drive inbox objects.
3. A Dagster asset reads inbox metadata, writes latest-note, revision-history, and attachment rows
   to Postgres, then promotes the Drive objects into the library prefix.

Configure the local Mac uploader with the app URL and signing secret by running
`pdw login` once, or by setting the same env vars pdw uses:

```bash
APPLE_NOTES_ACCOUNT=you@example.com
PDW_API_URL=https://your-public-pdw-app
PDW_SECRET_TOKEN=<same-secret-as-pdw-app>
APPLE_NOTES_STORE_PATH=~/Library/Group\ Containers/group.com.apple.notes/NoteStore.sqlite
```

Configure the app/Dagster object store with the shared Drive folder. If
`APPLE_NOTES_GOOGLE_DRIVE_FOLDER_ID` is omitted, the reader falls back to
`VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID`. Re-authorize that Google account after adding Apple Notes
reader config so the token includes Drive scope:

```bash
PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
APPLE_NOTES_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
uv run personal-data-warehouse-google-auth --email you@example.com --write-env
```

Run the local Mac uploader:

```bash
pdw ingest apple-notes
```

Incremental mode keeps local state in
`~/Library/Application Support/personal-data-warehouse/apple-notes-upload-state.json`. If a note's
fingerprint is unchanged, the uploader skips Drive work. If the title, body, folder, timestamps, or
attachment manifest changes, it uploads a new immutable revision under:

```text
apple-notes/inbox/YYYY/MM/<note-id>/<revision-id>.json
apple-notes/inbox/YYYY/MM/<note-id>/<revision-id>.html
apple-notes/inbox/YYYY/MM/<note-id>/<revision-id>/attachments/<attachment-id>-<sha256>.<ext>
```

Deleted notes become tombstone revisions and mark the latest `apple_notes` row as deleted. The
revision history remains in `apple_note_revisions`.

On Zach's MacBook Pro, the local uploader is managed by a per-user macOS LaunchAgent. The checked-in
plist template lives at
`ops/launchd/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist` and runs
`bin/apple-notes-upload-launchd` every five minutes. The wrapper records each run, exit code,
duration, and heartbeat under `~/Library/Logs/personal-data-warehouse/`.

Install or refresh the LaunchAgent:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload
```

Run it immediately:

```bash
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload
```

Monitor it:

```bash
bin/apple-notes-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload
tail -80 ~/Library/Logs/personal-data-warehouse/apple-notes-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/apple-notes-upload.heartbeat
```

If LaunchAgent runs fail with `PermissionError` or SQLite `authorization denied` for
`~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite`, macOS Full Disk Access is
blocking the background process. Grant Full Disk Access in System Settings > Privacy & Security
> Full Disk Access to `/bin/zsh`, `/opt/homebrew/bin/uv`,
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`, and its current real path
`/Users/zrl/.local/share/uv/python/cpython-3.12.12-macos-aarch64-none/bin/python3.12`, then
kickstart the LaunchAgent again.

The Apple Notes uploader uses the same network guard as Voice Memos. Apple Notes-specific
`APPLE_NOTES_UPLOAD_BLOCKED_SSID_PATTERNS`,
`APPLE_NOTES_UPLOAD_BLOCKED_HARDWARE_PORT_PATTERNS`, and
`APPLE_NOTES_UPLOAD_REQUIRE_WIFI_SSID` override the Voice Memos defaults when set.

## Apple Messages Sync

Apple Messages follows the same local-Apple-data path as Apple Notes: the Mac never talks directly
to Postgres or Google Drive. The local uploader snapshots `~/Library/Messages/chat.db`, streams
iMessage, SMS, RCS, chat, handle, join, deletion, and attachment metadata into compressed JSONL
batches, and posts those batches to the app. Attachment binaries are posted separately in bounded
backfill runs so message text can land quickly while large media catches up.

Configure the local Mac uploader with the app URL and signing secret by running
`pdw login` once, or by setting the same env vars pdw uses:

```bash
APPLE_MESSAGES_ACCOUNT=you@example.com
PDW_API_URL=https://your-public-pdw-app
PDW_SECRET_TOKEN=<same-secret-as-pdw-app>
APPLE_MESSAGES_STORE_PATH=~/Library/Messages/chat.db
APPLE_MESSAGES_ATTACHMENT_BYTES_PER_RUN=536870912
APPLE_MESSAGES_ATTACHMENT_COUNT_PER_RUN=200
APPLE_MESSAGES_UPLOAD_WORKERS=4
```

Configure the app/Dagster object store with the shared Drive folder. If
`APPLE_MESSAGES_GOOGLE_DRIVE_FOLDER_ID` is omitted, the reader falls back to
`APPLE_NOTES_GOOGLE_DRIVE_FOLDER_ID`, then `VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID`. Re-authorize that
Google account after adding Apple Messages reader config so the token includes Drive scope:

```bash
PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
APPLE_MESSAGES_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
uv run personal-data-warehouse-google-auth --email you@example.com --write-env
```

Run the local Mac uploader:

```bash
pdw ingest apple-messages
```

Incremental mode keeps local state in
`~/Library/Application Support/personal-data-warehouse/apple-messages-upload-state.sqlite`. The app
writes message, chat, handle, join, deletion, and attachment metadata batches under:

```text
apple-messages/inbox/batches/YYYY/MM/<exported-at>-<batch-sha256>.jsonl.gz
```

The app writes attachment binaries under:

```text
apple-messages/inbox/attachments/YYYY/MM/<message-date>-<attachment-guid>-<content-sha256>.<ext>
```

Dagster exposes `apple_messages_drive_ingest` in the `apple_messages` group. The enabled Drive inbox
sensor checks for new batches every minute, writes `apple_messages`, `apple_message_chats`,
`apple_message_handles`, `apple_message_chat_handles`, `apple_message_chat_messages`, and
`apple_message_attachments`, then promotes Drive objects into the `apple-messages/library/...`
prefix.

On Zach's MacBook Pro, the local uploader is managed by a per-user macOS LaunchAgent. The checked-in
plist template lives at
`ops/launchd/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist` and runs
`bin/apple-messages-upload-launchd` every five minutes.

Install or refresh the LaunchAgent:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload
```

Monitor it:

```bash
bin/apple-messages-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload
tail -80 ~/Library/Logs/personal-data-warehouse/apple-messages-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/apple-messages-upload.heartbeat
```

If LaunchAgent runs fail with `PermissionError` or SQLite `authorization denied` for
`~/Library/Messages/chat.db`, macOS Full Disk Access is blocking the background process. Grant Full
Disk Access in System Settings > Privacy & Security > Full Disk Access to `/bin/zsh`,
`/opt/homebrew/bin/uv`, `/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`, and
its current real path
`/Users/zrl/.local/share/uv/python/cpython-3.12.12-macos-aarch64-none/bin/python3.12`, then
kickstart the LaunchAgent again.

### Alice App Voice Recordings

Alice App voice recordings are archived into the same Google Drive object-storage root using the
Voice Memos sidecar shape. The importer writes a raw recording file plus one storage-location-free
JSON metadata file with the same dated content-hash basename when audio is available:

```text
alice-voice-recordings/library/YYYY/MM/YYYY-MM-DD-<sha256>.<ext>
alice-voice-recordings/library/YYYY/MM/YYYY-MM-DD-<sha256>.json
```

Alice is treated as an archive source named `alice_voice_recordings`, so the importer preserves
whatever the API still exposes and tags Drive object metadata with that source name. Older Alice
items may no longer have a raw media file; those still get a durable metadata sidecar:

```text
alice-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<alice-id>.json
```

If Alice returns a recording webpage rather than raw media, the page is archived too, because it may
contain the only remaining transcript/source artifact:

```text
alice-voice-recordings/library/YYYY/MM/YYYY-MM-DD-<sha256-or-alice-id>.html
```

Alice transcript emails from Gmail are also treated as recovery artifacts. This covers recordings
that Alice no longer exposes through the API, including auto-deleted recordings where Gmail may be
the only remaining source. The Gmail recovery asset writes the email body, transcript attachments,
and any attached audio beside the recording metadata:

```text
alice-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<guid>-<title>.email.md
alice-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<guid>-<title>.email.json
alice-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<guid>-<title>.transcript.txt
alice-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<guid>-<title>.transcript-formatted.docx
alice-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<guid>-<title>.audio.<ext>
```

Configure Alice API credentials and the Drive object-storage root:

```bash
ALICE_VOICE_RECORDINGS_ACCOUNT=you@example.com
ALICE_API_KEY_ID=<alice-api-key-id>
ALICE_API_SECRET_KEY=<alice-api-secret-key>
ALICE_VOICE_RECORDINGS_GOOGLE_DRIVE_ACCOUNT=you@example.com
ALICE_VOICE_RECORDINGS_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
```

If `ALICE_VOICE_RECORDINGS_GOOGLE_DRIVE_ACCOUNT` or
`ALICE_VOICE_RECORDINGS_GOOGLE_DRIVE_FOLDER_ID` are omitted, the importer falls back to the
Voice Memos Drive account and folder settings. The Google account must have a stored token with
Drive scope, using the same auth flow as Voice Memos:

```bash
uv run personal-data-warehouse-google-auth --email you@example.com --write-env
```

Dagster exposes `alice_voice_recordings_import` and `alice_voice_recordings_gmail_recovery` in the
`alice_voice_recordings` group, with `alice_voice_recordings_import_job` scheduled daily at
04:17 UTC. This is the scheduler of record for regular Alice archival. To run the API import
manually:

```bash
uv run python -c "from dagster import materialize; from personal_data_warehouse.defs.alice_voice_recordings import alice_voice_recordings_import; raise SystemExit(0 if materialize([alice_voice_recordings_import]).success else 1)"
```

To run the Gmail recovery pass manually:

```bash
uv run python -c "from dagster import materialize; from personal_data_warehouse.defs.alice_voice_recordings import alice_voice_recordings_gmail_recovery; raise SystemExit(0 if materialize([alice_voice_recordings_gmail_recovery]).success else 1)"
```

Dagster uses enabled sensors for the Voice Memos pipeline. The Drive inbox sensor checks for new
Google Drive metadata every minute and launches ingest when it finds work. Backlog sensors then
launch transcription and enrichment when Postgres has unprocessed recordings or transcripts.
An hourly enrichment schedule remains enabled as a repair pass. For a Coolify scheduled task
instead of the Dagster UI:

```bash
uv run python -c "from dagster import materialize; from personal_data_warehouse.defs.apple_voice_memos_drive_ingest import apple_voice_memos_drive_ingest; raise SystemExit(0 if materialize([apple_voice_memos_drive_ingest]).success else 1)"
```

Transcription is a separate Dagster asset. Configure AssemblyAI:

```bash
ASSEMBLYAI_API_KEY=...
VOICE_MEMOS_TRANSCRIPTION_PROVIDER=assemblyai
VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE=3
AGENT_PROVIDER=codex
AGENT_MODEL=gpt-5.6-sol
AGENT_REASONING_EFFORT=medium
VOICE_MEMOS_ENRICHMENT_LOOKBACK_WEEKS=12
VOICE_MEMOS_ENRICHMENT_BATCH_SIZE=0
```

Run one transcription batch:

```bash
uv run python -c "from dagster import materialize; from personal_data_warehouse.defs.apple_voice_memos_transcription import apple_voice_memos_transcription; raise SystemExit(0 if materialize([apple_voice_memos_transcription]).success else 1)"
```

The transcription asset stores the raw AssemblyAI response JSON in Postgres and also stores
normalized diarized segments for search. AssemblyAI is configured with Universal-3 Pro plus
domain keyterms for Hack Club and common project/product terms.

Transcript enrichment is a separate agent-backed Dagster asset. It reads completed transcription
runs, asks a one-off subscription-authenticated Codex or Claude Code container to produce structured
JSON, and stores a canonical transcript, calendar match, participants, summary, action items,
evidence, and raw structured result JSON. The agent container receives only the
per-run prompt, schema, and deterministic local CLI helpers, not production API keys.

By default enrichment processes all completed transcripts from the last twelve weeks that do not
already have the current enrichment prompt version. Set `VOICE_MEMOS_ENRICHMENT_BATCH_SIZE` to a
positive number to cap a run; the default `0` means no cap. If a recording does not match a calendar
event, the enrichment still produces a title, recording-based time range, summary, action items,
and transcript. For long recordings, the model returns metadata and speaker/term evidence,
then the pipeline assembles the detailed speaker-labeled transcript locally from diarized segments
to avoid large model responses timing out.

Agent enrichment completion is keyed by provider and prompt version, not model. Changing
`AGENT_MODEL` affects new work without reprocessing the historical corpus; intentionally
re-enriching existing content requires a prompt-version change.

Run enrichment:

```bash
uv run python -c "from dagster import materialize; from personal_data_warehouse.defs.apple_voice_memos_enrichment import apple_voice_memos_enrichment; raise SystemExit(0 if materialize([apple_voice_memos_enrichment]).success else 1)"
```

### Containerized Agent Enrichment

Voice Memos enrichment runs through a one-off Codex or Claude Code container by default, using your
logged-in CLI subscription instead of API keys. Dagster owns the Docker socket and spawns the agent
container; the agent container does not receive the Docker socket, Dagster env, Postgres URL,
Google tokens, Slack tokens, or API keys.

The agent resource and auth/bootstrap command build the agent image on demand. They derive a tag
like `personal-data-warehouse-agent:<hash>` from the agent Dockerfile and entrypoint, check whether
that image already exists locally, and build it only when missing. Do not set `AGENT_DOCKER_IMAGE`;
the hash-derived tag is the only supported image path.

For Coolify, add two persistent Docker volumes and mount them into the Dagster app:

```text
pdw-agent-auth -> /agent-auth
pdw-agent-runs -> /agent-runs
```

`pdw-agent-auth` stores persistent Codex/Claude CLI login state. `pdw-agent-runs` is the shared
handoff volume for per-run prompts, schemas, local helper tools, final JSON/message files, and other
agent run artifacts. The run volume is not used for subscription auth.

Also mount the host Docker socket into the Dagster app only:

```text
/var/run/docker.sock -> /var/run/docker.sock
```

Set the Dagster app env:

Codex defaults to `gpt-5.6-sol` with `medium` reasoning. The explicit values below document the
production choice and can be changed when a run needs a different model or reasoning effort.

```bash
AGENT_PROVIDER=codex
AGENT_MODEL=gpt-5.6-sol
AGENT_REASONING_EFFORT=medium
AGENT_AUTH_VOLUME=pdw-agent-auth
AGENT_RUNS_VOLUME=pdw-agent-runs
AGENT_RUNS_DIR=/agent-runs
AGENT_DOCKER_NETWORK=coolify
```

The read-only Postgres tool is exposed through a short-lived proxy owned by the Dagster process.
The agent container receives only a proxy URL and per-run bearer token, not `POSTGRES_DATABASE_URL`. Defaults
work on Docker Desktop/OrbStack via `host.docker.internal`. In Coolify, set `AGENT_DOCKER_NETWORK`
to the app network, usually `coolify`; when the network is not `bridge`, the proxy host defaults to
the Dagster container hostname. Override the host only if Docker DNS cannot resolve that hostname:

```bash
AGENT_DOCKER_NETWORK=coolify
AGENT_TOOL_PROXY_BIND_HOST=0.0.0.0
AGENT_TOOL_PROXY_PUBLIC_HOST=<optional-dagster-container-hostname-or-ip>
```

Log in from the Coolify terminal after the Dagster app can use Docker:

```bash
uv run personal-data-warehouse-agent-auth login codex
uv run personal-data-warehouse-agent-auth status codex
```

The first command also ensures the agent image exists on the Docker host. Normal agent runs perform
the same check, so a fresh deploy can build the new hash-derived image before launching the one-off
agent container.

For Claude later:

```bash
AGENT_PROVIDER=claude
AGENT_MODEL=<claude-model-name>
uv run personal-data-warehouse-agent-auth login claude
uv run personal-data-warehouse-agent-auth status claude
```

Agent run metadata, streamed CLI events, and detected tool-call events are stored in Postgres
tables `agent_runs`, `agent_run_events`, and `agent_run_tool_calls`. The final Voice Memos result
still lands in `apple_voice_memos_enrichments`.

The agent integration is exposed to Dagster as an `AgentResource`. The core Docker runner remains
plain Python for testing, while assets receive the resource and use it to start one-off containers.
The Voice Memos enrichment asset consumes this resource by default; other assets can reuse the same
resource later.

Each run also gets a per-run `tools/` directory mounted into the agent container. The runner exports
absolute helper paths because Codex/Claude shell environments may not preserve `PATH` changes.
Current built-ins:

```bash
"$PDW_TOOL_HELP"
"$PDW_VALIDATE_JSON" candidate.json "$AGENT_SCHEMA_PATH"
"$PDW_POSTGRES_SCHEMA"
"$PDW_POSTGRES_QUERY" "SELECT summary, attendees_json FROM calendar_events LIMIT 5"
```

`$PDW_POSTGRES_SCHEMA` and `$PDW_POSTGRES_QUERY` route through the per-run read-only proxy. SQL
is locally restricted to read-only statement types.
Secret-backed tools should only be added deliberately, with the understanding that any credential or
capability available to a CLI is also available to arbitrary Bash inside the agent container.

Written tests that do not call live agents run with the normal suite:

```bash
uv run pytest tests/test_agent_runner.py tests/test_apple_voice_memos_enrichment_defs.py tests/test_postgres_warehouse.py
```

Live Docker/subscription smoke tests are opt-in because they require Docker and a logged-in
subscription auth volume:

```bash
uv run personal-data-warehouse-agent-auth login codex

RUN_LIVE_AGENT_TESTS=1 \
AGENT_PROVIDER=codex \
AGENT_MODEL=gpt-5.6-sol \
AGENT_REASONING_EFFORT=medium \
uv run pytest tests/test_agent_runner_live.py -q
```

The live tests verify the image has both CLIs and no Docker socket, start real one-off agent
containers through the subscription login, and check that the built-in Postgres CLI can reach a
host-owned read-only proxy without receiving the raw Postgres URL.

## Warehouse Tables

The sync creates and maintains:

- `gmail_messages`: latest known state for each Gmail message, keyed by `(account, message_id)`
- `gmail_attachments`: latest known state and extracted text for each Gmail attachment
- `gmail_attachment_backfill_state`: per-message marker for attachment backfill progress
- `gmail_sync_state`: per-mailbox sync cursor and last run status
- `calendar_events`: latest known state for each calendar event
- `calendar_sync_state`: per-account/calendar sync token and last run status
- `whoop_profiles`, `whoop_body_measurements`, `whoop_cycles`, `whoop_recoveries`,
  `whoop_sleeps`, `whoop_workouts`: read-only WHOOP v2 profile/health/activity data
- `whoop_sync_state`: per-WHOOP-collection sync window and last run status
- `private.whoop_oauth_tokens`: private runtime copy of the current rotated WHOOP OAuth token
- `apple_voice_memos_files`: latest known metadata for Voice Memos audio files uploaded through Drive
- `apple_voice_memos_transcription_runs`: raw transcription provider results and run state
- `apple_voice_memos_transcript_segments`: normalized diarized transcript segments
- `apple_voice_memos_enrichments`: canonical transcript, calendar match, participants, summary, action items, and evidence
- `apple_notes`: latest known state for each Apple Note
- `apple_note_revisions`: every observed Apple Note revision and tombstone
- `apple_note_attachments`: attachment metadata and storage pointers per note revision
- `apple_messages`: latest known state for each Apple Messages row, including decoded body text,
  service, tapback/reply/edit metadata, and tombstones
- `apple_message_chats`, `apple_message_handles`, `apple_message_chat_handles`,
  `apple_message_chat_messages`: normalized conversation, participant, and membership tables
- `apple_message_attachments`: attachment metadata and staged Drive storage pointers
- `agent_runs`, `agent_run_events`, `agent_run_tool_calls`: containerized Codex/Claude run audit logs
- `slack_account_identities`: authenticated Slack user identity for each synced Slack account/team
- `plaid.items`, `plaid.accounts`, `plaid.transactions`, `plaid.investment_securities`,
  `plaid.investment_holdings`, `plaid.investment_transactions`, and `plaid.liabilities`: raw,
  source-owned Plaid metadata and financial records
- `plaid.sync_state`: per-item/product cursor, status, and last successful sync timestamp
- `private.plaid_item_tokens`: private Plaid access-token storage; excluded from normal read surfaces
- `marts.finance_accounts`, `marts.finance_transactions`, `marts.finance_investment_holdings`,
  `marts.finance_investment_transactions`, and `marts.finance_liabilities`: stable finance-domain
  views over the Plaid source tables

The warehouse also creates clean views for current inbox and transcript state:

- `clean_gmail_inbox`: Gmail inbox threads with latest-message summary fields and
  `thread_messages_json` carrying full thread context oldest-to-newest
- `clean_slack_inbox`: Slack DMs, mentions, participating threads, and channel unread items for
  the authenticated Slack user when read state is known
- `clean_calendar_with_transcripts`: calendar events joined to matched Voice Memo transcript enrichments
- `clean_transcripts_no_calendar_match`: completed Voice Memo transcript enrichments without a calendar match

`gmail_messages` stores:

- mailbox/account
- Gmail message and thread IDs
- Gmail history ID
- internal timestamp
- labels
- deletion tombstones
- common headers like `Subject`, `From`, `To`, and `Message-ID`
- extracted `text/plain` and `text/html` bodies
- the raw Gmail message payload as JSON

Attachments are stored in `gmail_attachments`, keyed by `(account, message_id, part_id, filename)`.
The sync stores attachment metadata, content hashes, text extraction status, and extracted text for searchable formats.
Supported text extraction includes plain text-like files, HTML, PDF, ZIP contents, and Office Open XML files such as `.docx`, `.pptx`, and `.xlsx`.
Agent vision enrichment stores successful output in the shared `file_attachment_enrichments`
table with `text_extraction_status = 'agent_ok'` (or `agent_not_useful` for blank/decorative
images), alongside the provider, model, exact prompt, prompt hash, prompt version, source
extraction status, elapsed time, and processing timestamp. Rows produced by the removed inline
Ollama fallback (`ai_ok`, `ai_ocr_only`, ...) remain for search but are no longer written.
Attachments that cannot be extracted still get metadata rows with `text_extraction_status`.
`file_attachment_enrichments` is the renamed, source-agnostic successor to
`gmail_attachment_enrichments`: the same agent-vision pipeline now also enriches downloaded
WhatsApp image/PDF media (the `whatsapp_media_enrichment` asset), keyed by `content_sha256`
under each source's own `task_type`/`prompt_version`. See `file_attachment_enrichment.py`.

## Verification

Run tests:

```bash
uv run pytest
```
