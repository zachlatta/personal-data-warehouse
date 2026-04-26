# personal_data_warehouse

This project syncs Gmail mailbox data and Google Calendar events into ClickHouse through Dagster.

Current ingestion path:

- Gmail mailboxes are synced with the Gmail API using per-account OAuth tokens.
- The first run does a full mailbox sync.
- Later runs are incremental via Gmail `history.list`.
- If Gmail expires the saved history cursor, the sync falls back to a full resync for that mailbox.
- Google Calendar events use the same per-account Google OAuth tokens.
- Calendar first runs do a full event sync, then later runs use Google Calendar `syncToken`.
- If Calendar expires the saved sync token, the sync falls back to a full resync for that calendar.

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
CLICKHOUSE_URL=...
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
```

Notes:

- OAuth client secrets are only needed when running the browser auth flow.
- Use a separate OAuth app per email domain: `GMAIL_DOMAIN_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON_B64` for `hackclub.com`, `GMAIL_DOMAIN_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON_B64` for `zachlatta.com`, and so on. `GOOGLE_DOMAIN_<DOMAIN_SLUG>_OAUTH_CLIENT_SECRETS_JSON_B64` is also supported.
- The legacy global `GMAIL_OAUTH_CLIENT_SECRETS_JSON_B64` fallback is only used when the configured Google accounts span one email domain.
- Sync runtime requires one `GOOGLE_<ACCOUNT_SLUG>_TOKEN_JSON_B64` or legacy `GMAIL_<ACCOUNT_SLUG>_TOKEN_JSON_B64` value per Google account.
- Google OAuth client secrets and account tokens are env-only; the app does not read or write Google secrets from the filesystem.
- Calendar sync defaults to the accounts in `GMAIL_ACCOUNTS` and the `primary` calendar unless `CALENDAR_ACCOUNTS` or `CALENDAR_<ACCOUNT_SLUG>_CALENDAR_IDS` are set.

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

## Running The Sync

Start Dagster:

```bash
uv run dg dev
```

Then materialize the `gmail_mailbox_sync` asset from the Dagster UI.
Materialize `calendar_event_sync` to sync Google Calendar events.

The Docker/Coolify deployment also includes an enabled Dagster schedule,
`gmail_mailbox_sync_every_minute`, which runs the Gmail sync every minute.
Gmail syncs use a nonblocking lock so a scheduled tick skips if another sync is still running.
When `DAGSTER_POSTGRES_URL` or `DATABASE_URL` is set, the lock uses a Postgres advisory lock;
otherwise it falls back to a local process lock.

Each Gmail asset run also drains up to `GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE`
already-synced attachment-candidate messages per mailbox. This backfills attachment
text without advancing Gmail history cursors. Set it to `0` to disable this pass.
By default, Gmail sync calls an Ollama-compatible vision model for otherwise
unextractable image attachments and image-only PDFs. Set
`GMAIL_ATTACHMENT_AI_FALLBACK_ENABLED=false` to disable it.
The fallback is intended for slow background enrichment; deterministic extraction
still runs first. Configure it with `GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL`,
`GMAIL_ATTACHMENT_AI_FALLBACK_MODEL`, `GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS`,
`GMAIL_ATTACHMENT_AI_FALLBACK_PDF_MAX_PAGES`, and
`GMAIL_ATTACHMENT_AI_FALLBACK_PULL_MODEL`. A reusable Dagster `OllamaResource`
verifies the model before each Gmail run and pulls it when missing unless model
pulls are disabled.

Slack sync splits freshness, coverage, and metadata into separate schedules. The
`slack_workspace_sync_every_minute` schedule keeps recent messages fresh every minute.
`slack_workspace_coverage_sync_every_seven_minutes` backfills incomplete cached conversations
with smaller batches, and `slack_workspace_metadata_sync_every_fifteen_minutes` refreshes one
capped page of active conversation metadata at a time. `slack_workspace_user_sync_hourly`
refreshes the full Slack user list outside the more frequent metadata path.
`slack_workspace_thread_sync_every_five_minutes` backfills `conversations.replies` for known
thread parents. It defaults to one thread per run and only revisits completed threads when the
parent's `latest_reply_ts` has advanced beyond the stored thread cursor, so it remains gentle on
Slack's thread API rate limits. Threads that already produced Slack API errors are skipped by the
scheduled pass so one inaccessible thread does not block the backlog. Configure it with
`SLACK_ASSET_THREAD_LIMIT`, `SLACK_ASSET_THREAD_SINCE_DAYS`, and `SLACK_ASSET_THREAD_ORDER`.
Freshness stages also poll capped sets of cached conversations per type, ordered by recent
activity, so each scheduled run remains bounded.
All Slack schedules share a nonblocking Slack lock, so a scheduled tick skips if another Slack
sync stage is still running.
Calendar sync runs through `calendar_event_sync_every_minute` with its own nonblocking lock.

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
CLICKHOUSE_URL=...
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

## ClickHouse Tables

The sync creates and maintains:

- `gmail_messages`: latest known state for each Gmail message, keyed by `(account, message_id)`
- `gmail_attachments`: latest known state and extracted text for each Gmail attachment
- `gmail_attachment_backfill_state`: per-message marker for attachment backfill progress
- `gmail_sync_state`: per-mailbox sync cursor and last run status
- `calendar_events`: latest known state for each calendar event
- `calendar_sync_state`: per-account/calendar sync token and last run status

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
When AI fallback is enabled, successful model output is stored with `text_extraction_status = 'ai_ok'`
or `ai_truncated`, alongside the provider, model, base URL, exact prompt, prompt hash,
prompt version, source extraction status, elapsed time, and processing timestamp.
Attachments that cannot be extracted still get metadata rows with `text_extraction_status`.

Use `gmail_attachment_search` to search attachment text with surrounding message context:

```sql
SELECT
  internal_date,
  subject,
  from_address,
  filename,
  text
FROM gmail_attachment_search
WHERE positionCaseInsensitive(text, 'search terms') > 0
ORDER BY internal_date DESC
LIMIT 20
```

Use `calendar_event_search` to search current calendar events:

```sql
SELECT
  start_at,
  summary,
  location,
  description
FROM calendar_event_search
WHERE positionCaseInsensitive(summary, 'search terms') > 0
   OR positionCaseInsensitive(description, 'search terms') > 0
ORDER BY start_at DESC
LIMIT 20
```

## Verification

Run tests:

```bash
uv run pytest
```
