# personal_data_warehouse

This project syncs Gmail mailbox data, Google Calendar events, Slack data, and Voice Memos into Postgres through Dagster.

Current ingestion path:

- Gmail mailboxes are synced with the Gmail API using per-account OAuth tokens.
- The first run does a full mailbox sync.
- Later runs are incremental via Gmail `history.list`.
- If Gmail expires the saved history cursor, the sync falls back to a full resync for that mailbox.
- Google Calendar events use the same per-account Google OAuth tokens.
- Calendar first runs do a full event sync, then later runs use Google Calendar `syncToken`.
- If Calendar expires the saved sync token, the sync falls back to a full resync for that calendar.
- Voice Memos use a two-stage path: a local macOS uploader writes audio files and JSON metadata
  to Google Drive, then a Dagster asset ingests those metadata into Postgres.
- Finance accounts use Plaid Link for authorization, then sync accounts, transactions,
  investment holdings/transactions, and liabilities into Postgres.

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
VOICE_MEMOS_ACCOUNT=you@example.com
VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
VOICE_MEMOS_STORAGE_BACKEND=google_drive
VOICE_MEMOS_EXTENSIONS=.m4a,.qta
PLAID_CLIENT_ID=<plaid-client-id>
PLAID_SECRET=<plaid-secret>
PLAID_ENV=sandbox
PLAID_ITEMS=capital_one,robinhood,mortgage
PLAID_CAPITAL_ONE_ACCESS_TOKEN=<plaid-access-token>
PLAID_ROBINHOOD_ACCESS_TOKEN=<plaid-access-token>
PLAID_MORTGAGE_ACCESS_TOKEN=<plaid-access-token>
```

Notes:

- OAuth client secrets are only needed when running the browser auth flow.
- Use a separate OAuth app per email domain: `GMAIL_DOMAIN_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON_B64` for `hackclub.com`, `GMAIL_DOMAIN_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON_B64` for `zachlatta.com`, and so on. `GOOGLE_DOMAIN_<DOMAIN_SLUG>_OAUTH_CLIENT_SECRETS_JSON_B64` is also supported.
- The legacy global `GMAIL_OAUTH_CLIENT_SECRETS_JSON_B64` fallback is only used when the configured Google accounts span one email domain.
- Sync runtime requires one `GOOGLE_<ACCOUNT_SLUG>_TOKEN_JSON_B64` or legacy `GMAIL_<ACCOUNT_SLUG>_TOKEN_JSON_B64` value per Google account.
- Google OAuth client secrets and account tokens are env-only; the app does not read or write Google secrets from the filesystem.
- Calendar sync defaults to the accounts in `GMAIL_ACCOUNTS` and the `primary` calendar unless `CALENDAR_ACCOUNTS` or `CALENDAR_<ACCOUNT_SLUG>_CALENDAR_IDS` are set.
- Calendar sync also expands recurring event instances in a rolling window so generated occurrences can be matched directly. The expanded pass runs hourly by default, and immediately after full syncs or recurrence-related changes. Tune it with `CALENDAR_EXPANDED_SYNC_LOOKBACK_DAYS`, `CALENDAR_EXPANDED_SYNC_LOOKAHEAD_DAYS`, and `CALENDAR_EXPANDED_SYNC_INTERVAL_MINUTES`.

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

If Voice Memos are configured, the auth flow also requests Google Drive access so the local
uploader and Drive ingest asset can use the same account token.

## Running The Sync

Start Dagster:

```bash
uv run dg dev
```

Then materialize the `gmail_mailbox_sync` asset from the Dagster UI.
Materialize `calendar_event_sync` to sync Google Calendar events. Calendar sync uses Google's incremental cursor for ordinary changes and a rolling expanded-instance window for recurring event occurrences.

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
pulls are disabled. The default model is `qwen3-vl:2b`, chosen as the smallest
Qwen vision model that keeps background CPU fallback practical while deterministic
OCR supplies additional search text. When the resource starts Ollama itself, it sets
`GGML_METAL_TENSOR_DISABLE=1` so Apple Silicon keeps Metal acceleration while
avoiding the current Metal cooperative-tensor crash seen with some vision models.
The Docker image also includes Tesseract so fallback rows can append a short
deterministic OCR section when it recovers text the vision model misses.

Slack sync splits freshness, coverage, and metadata into separate schedules. The
`slack_workspace_sync_every_five_minutes` schedule keeps recent messages fresh every five minutes.
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
`slack_workspace_read_state_sync_every_five_minutes` refreshes `conversations.info` for a small
set of recently active account-relevant conversations, including DMs, group DMs, and member
channels, staggered two minutes after the thread schedule. This updates user-specific fields such
as `last_read` for deriving unread state. The every-minute freshness sync also piggybacks the same
read-state refresh while it already holds the Slack lock, so current-account state does not depend
on a separate read-state run winning the scheduler race. Configure it with
`SLACK_ASSET_READ_STATE_LIMIT`.
Freshness stages also poll capped sets of cached conversations per type, ordered by recent
activity, so each scheduled run remains bounded.
All Slack schedules share a nonblocking Slack lock, so a scheduled tick skips if another Slack
sync stage is still running.
Calendar sync runs through `calendar_event_sync_every_minute` with its own nonblocking lock.

## Finance Sync

Finance sync uses Plaid as the first provider. Plaid access tokens stay in `.env`; Postgres stores
the item/account identifiers, normalized query fields, and raw provider JSON, but not the access
tokens themselves.

Configure Plaid API credentials:

```bash
PLAID_CLIENT_ID=...
PLAID_SECRET=...
PLAID_ENV=development
PLAID_PRODUCTS=transactions
PLAID_ADDITIONAL_CONSENTED_PRODUCTS=investments,liabilities
```

Authorize each institution login through the local Plaid Link helper:

```bash
uv run personal-data-warehouse-plaid-auth --item capital_one --products transactions --write-env
uv run personal-data-warehouse-plaid-auth --item mortgage --products liabilities --additional-consented-products transactions --write-env
uv run personal-data-warehouse-plaid-auth --item robinhood --products investments --additional-consented-products transactions --write-env
uv run personal-data-warehouse-plaid-auth --item fidelity --products investments --additional-consented-products transactions --write-env
```

If a Plaid OAuth institution requires an explicit redirect URI, register it in the Plaid Dashboard
and pass it with `--redirect-uri` or set `PLAID_REDIRECT_URI`. Desktop web OAuth can often complete
without a redirect URI, but production OAuth access depends on Plaid account approval and institution
registration.

Run the sync manually:

```bash
uv run personal-data-warehouse-finance-sync
```

Dagster exposes `finance_account_sync` in the `finance` group, scheduled hourly by
`finance_account_sync_hourly`. The sync records product-specific errors in `finance_sync_state`
without failing the whole item, so a Capital One transaction sync can still succeed even if
investment or liability data is unavailable for that item. Fidelity connectivity depends on the
account and Plaid's current institution support; if Plaid cannot connect it, the warehouse schema is
ready for a future Akoya, Finicity, or export-file provider.

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

Voice Memos are split across two processes so the Mac never talks to the warehouse directly:

1. A local macOS CLI scans Apple's Voice Memos recordings directory and uploads new `.m4a`
   and `.qta` audio files to the Google Drive inbox, alongside one JSON metadata file per audio file.
2. The remote Dagster asset reads inbox metadata, writes metadata rows to Postgres, then promotes
   the audio and JSON objects into the library prefix.

Configure the shared Drive folder:

```bash
VOICE_MEMOS_ACCOUNT=you@example.com
VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID=<drive-folder-id>
VOICE_MEMOS_STORAGE_BACKEND=google_drive
VOICE_MEMOS_EXTENSIONS=.m4a,.qta
```

Re-authorize the Google account after adding Voice Memos config so the token includes Drive scope:

```bash
uv run personal-data-warehouse-google-auth --email you@example.com --write-env
```

Run the local Mac uploader:

```bash
uv run personal-data-warehouse-apple-voice-memos-upload
```

The uploader defaults to a lightweight incremental mode for scheduled background runs. Incremental mode keeps local
state in `~/Library/Application Support/personal-data-warehouse/voice-memos-upload-state.json`;
unchanged recordings that already uploaded both audio and metadata are skipped before hashing,
network checks, OAuth refresh, or Drive API calls. Use full mode for periodic repair/backfill:

```bash
uv run personal-data-warehouse-apple-voice-memos-upload --mode full
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
uv run personal-data-warehouse-apple-voice-memos-upload --network-diagnostics
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

### Alice App Voice Recordings

Alice App voice recordings are archived into the same Google Drive object-storage root using the
Voice Memos sidecar shape. The importer writes a raw recording file plus one storage-location-free
JSON metadata file with the same dated content-hash basename when audio is available:

```text
alice-app-voice-recordings/library/YYYY/MM/YYYY-MM-DD-<sha256>.<ext>
alice-app-voice-recordings/library/YYYY/MM/YYYY-MM-DD-<sha256>.json
```

Alice is treated as an archive source, so the importer preserves whatever the API still exposes.
Older Alice items may no longer have a raw media file; those still get a durable metadata sidecar:

```text
alice-app-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<alice-id>.json
```

If Alice returns a recording webpage rather than raw media, the page is archived too, because it may
contain the only remaining transcript/source artifact:

```text
alice-app-voice-recordings/library/YYYY/MM/YYYY-MM-DD-<sha256-or-alice-id>.html
```

Alice transcript emails from Gmail are also treated as recovery artifacts. This covers recordings
that Alice no longer exposes through the API, including auto-deleted recordings where Gmail may be
the only remaining source. The Gmail recovery asset writes the email body, transcript attachments,
and any attached audio beside the recording metadata:

```text
alice-app-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<guid>-<title>.email.md
alice-app-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<guid>-<title>.email.json
alice-app-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<guid>-<title>.transcript.txt
alice-app-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<guid>-<title>.transcript-formatted.docx
alice-app-voice-recordings/library/YYYY/MM/YYYY-MM-DD-alice-<guid>-<title>.audio.<ext>
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
AGENT_MODEL=gpt-5.3-codex
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

```bash
AGENT_PROVIDER=codex
AGENT_MODEL=gpt-5.3-codex
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
AGENT_MODEL=gpt-5.3-codex \
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
- `apple_voice_memos_files`: latest known metadata for Voice Memos audio files uploaded through Drive
- `apple_voice_memos_transcription_runs`: raw transcription provider results and run state
- `apple_voice_memos_transcript_segments`: normalized diarized transcript segments
- `apple_voice_memos_enrichments`: canonical transcript, calendar match, participants, summary, action items, and evidence
- `agent_runs`, `agent_run_events`, `agent_run_tool_calls`: containerized Codex/Claude run audit logs
- `slack_account_identities`: authenticated Slack user identity for each synced Slack account/team
- `finance_items`: Plaid item metadata for each linked institution login, without access tokens
- `finance_accounts`: latest known account profile and balance fields
- `finance_transactions`: Plaid transaction sync adds, modifications, and removal tombstones
- `finance_investment_holdings`: latest known investment positions
- `finance_investment_securities`: securities referenced by holdings and investment transactions
- `finance_investment_transactions`: investment transaction history over the configured lookback window
- `finance_liabilities`: mortgage, credit, and student-loan liability details
- `finance_sync_state`: per-item/product cursors and last run status

The warehouse also creates clean views for current inbox and transcript state:

- `clean_gmail_inbox`: Gmail inbox threads with latest-message summary fields and
  `thread_messages_json` carrying full thread context oldest-to-newest
- `clean_slack_inbox`: Slack DMs, mentions, participating threads, and channel unread items for
  the authenticated Slack user when read state is known
- `clean_finance_accounts`, `clean_finance_transactions`, and `clean_finance_holdings`: queryable
  current finance account, transaction, and holding views
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
When AI fallback is enabled, successful model output is stored with `text_extraction_status = 'ai_ok'`
or `ai_truncated`, alongside the provider, model, base URL, exact prompt, prompt hash,
prompt version, source extraction status, elapsed time, and processing timestamp.
Attachments that cannot be extracted still get metadata rows with `text_extraction_status`.

## Postgres Migration

Runtime code uses `POSTGRES_DATABASE_URL`. ClickHouse is still supported as a read-only migration
source so historical deployments can be moved over without preserving duplicate physical row
versions.

To migrate and verify ClickHouse `FINAL` state into Postgres:

```bash
CLICKHOUSE_URL=... POSTGRES_DATABASE_URL=... \
uv run personal-data-warehouse-postgres-migration migrate-and-verify
```

For a dry run, point `--schema` at a temporary Postgres schema first. Verification compares row
counts and canonical row hashes table-by-table. Leave ClickHouse read-only for rollback/archive
until the migrated Postgres warehouse has been running cleanly.

## Verification

Run tests:

```bash
uv run pytest
```
