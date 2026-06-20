# Agent Notes

Development practices:

* We use TDD for this repo and follow good code practices
* When asked to refactor or change existing code flows, please plan to completely replace the old legacy flow with the new requested flow - including ripping out any and all legacy code

## Commit and Push Safety

Before committing or pushing, review the complete staged diff line by line for secrets,
credentials, tokens, private URLs, personal data, generated artifacts, and anything else
that should not be public. If there is even a smidgen of doubt about whether a change is
safe to commit or push, stop and check with Zach before proceeding. Never
include other people's names in code, even if their names are public.

Always assume other agents may be running in the same worktree. Before committing, carefully
verify the staged changes and commit only the changes made in the current session unless Zach
explicitly instructs otherwise.

## Deployment / Production

This app runs in production as a Coolify app on `rotom`, Zach's personal Coolify server (Linux,
on the Tailscale tailnet). You can `ssh rotom` to inspect its running config directly.

Coolify management tooling lives in the `sysadmin` repo at `~/dev/zachlatta/sysadmin`:

- On `crobat` you can obtain a Coolify API key from that repo to drive the Coolify API. See its
  `README.md` and the `rotom/` notes folder for details.
- The same repo holds the Loki log wrapper used to read production logs — see the
  [Production Logs](#production-logs) section below.

To investigate the production Dagster deployment directly, connect to its Postgres. The
production Dagster Postgres URL is **not** present in this worktree: `.env` is gitignored and
only exists in the parent (non-worktree) checkout. Read `PROD_DAGSTER_URL` from the parent
repo's env file at `~/dev/zachlatta/personal-data-warehouse/.env`. In production, Dagster reads
the same connection string from `DAGSTER_POSTGRES_URL` (see `docker/dagster.yaml`).

## Production Logs

Production runs as a Coolify app on the `rotom` server. The best way to read
its logs is the Loki wrapper in the `sysadmin` repo:
`~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs`.

That script talks to Loki over Tailscale, so it only works from a machine on
the tailnet. Zach's dev machines `crobat` and `porygon` are both on the tailnet
and have access. Before assuming you can use it, confirm you are actually on
`crobat` or `porygon` by running `hostname` (or `scutil --get LocalHostName`)
and checking the output equals one of those. If you are anywhere else, stop and
ask Zach instead of guessing.

Once you have confirmed you are on `crobat` or `porygon`, useful starting points:

```bash
# Recent app/container logs for the Coolify deployment.
~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs \
  --format-logs --since 1h '{job="coolify",server="rotom"}'

# Filter to a specific container by resource UUID (see warning below).
~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs \
  --format-logs --since 1h \
  '{job="coolify",server="rotom"} | json | container_name =~ "(?i).*<resource-uuid>.*"'

# Host-level system logs for rotom itself.
~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs \
  --format-logs --since 1h '{job="machine",server="rotom"}'
```

### Pin to the right deployment before reading logs

The `rotom` Coolify server hosts many apps, several with confusingly similar
names. A loose name filter like `container_name =~ ".*dagster.*"` can silently
match more than one and return logs for the wrong app. Don't filter by guessed
names — first ask the Coolify API for the deployment's exact resource UUID, then
filter on that. Coolify names each container `<resource-uuid>-<deploy-timestamp>`,
so the UUID is an unambiguous key.

The Coolify API URL and key live in the `sysadmin` repo's gitignored `.env`
(`~/dev/zachlatta/sysadmin/.env`) as `COOLIFY_URL` and `COOLIFY_API_KEY`:

```bash
set -a && source ~/dev/zachlatta/sysadmin/.env && set +a
curl -fsS -H "Authorization: Bearer $COOLIFY_API_KEY" \
  "$COOLIFY_URL/api/v1/applications" \
  | jq -r '.[] | "\(.uuid)\t\(.name)\t\(.fqdn // "-")"'
```

Find the UUID for the exact app name you want, plug it into the
`container_name` filter above, then sanity-check the output: every line's
`coolify[...]` tag should share one `<resource-uuid>-<deploy-timestamp>` prefix.
More than one prefix means the filter is still too broad.

See `~/dev/zachlatta/sysadmin/README.md` and the script's `--help` for the
full set of selectors and flags.

## Local Voice Memos Upload Scheduler

This Mac is intended to run the local Voice Memos uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.voice-memos-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist`
- Wrapper script: `bin/voice-memos-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest voice-memos --mode incremental` (the wrapper runs the pdw CLI, which execs `uv run python -m personal_data_warehouse_voice_memos.cli`)
- Main run log: `~/Library/Logs/personal-data-warehouse/voice-memos-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/voice-memos-upload.heartbeat`
- Status helper: `bin/voice-memos-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/voice-memos-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
tail -80 ~/Library/Logs/personal-data-warehouse/voice-memos-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/voice-memos-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
```

Do not replace this with cron unless there is a specific reason. On current macOS, LaunchAgents
behave better for user-session jobs and are easier to inspect with `launchctl`.

If the run log shows `PermissionError: [Errno 1] Operation not permitted` for
`~/Library/Group Containers/group.com.apple.VoiceMemos.shared/Recordings`, the LaunchAgent is
loaded correctly but macOS Full Disk Access is blocking the background process. Grant Full Disk
Access to the executable chain used by the job, especially `/bin/zsh`, the `pdw` binary (`~/.local/bin/pdw`), `/opt/homebrew/bin/uv`, and
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`. Its current real path is
`/Users/zrl/.local/share/uv/python/cpython-3.12.12-macos-aarch64-none/bin/python3.12`. Then
kickstart the LaunchAgent again.

## Local Apple Notes Upload Scheduler

This Mac is intended to run the local Apple Notes uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.apple-notes-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist`
- Wrapper script: `bin/apple-notes-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest apple-notes --mode incremental` (the wrapper runs the pdw CLI, which execs `uv run python -m personal_data_warehouse_apple_notes.cli`)
- Main run log: `~/Library/Logs/personal-data-warehouse/apple-notes-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/apple-notes-upload.heartbeat`
- Status helper: `bin/apple-notes-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/apple-notes-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload
tail -80 ~/Library/Logs/personal-data-warehouse/apple-notes-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/apple-notes-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-notes-upload
```

If the run log shows `PermissionError` or SQLite `authorization denied` for
`~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite`, the LaunchAgent is loaded
correctly but macOS Full Disk Access is blocking the background process. Grant Full Disk Access to
the executable chain used by the job, especially `/bin/zsh`, the `pdw` binary (`~/.local/bin/pdw`), `/opt/homebrew/bin/uv`, and
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`. Its current real path is
`/Users/zrl/.local/share/uv/python/cpython-3.12.12-macos-aarch64-none/bin/python3.12`. Then
kickstart the LaunchAgent again.

## Local Apple Messages Upload Scheduler

This Mac is intended to run the local Apple Messages uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.apple-messages-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist`
- Wrapper script: `bin/apple-messages-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest apple-messages --mode incremental` (the wrapper runs the pdw CLI, which execs `uv run python -m personal_data_warehouse_apple_messages.cli`)
- Main run log: `~/Library/Logs/personal-data-warehouse/apple-messages-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/apple-messages-upload.heartbeat`
- Status helper: `bin/apple-messages-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/apple-messages-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload
tail -80 ~/Library/Logs/personal-data-warehouse/apple-messages-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/apple-messages-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-messages-upload
```

If the run log shows `PermissionError` or SQLite `authorization denied` for
`~/Library/Messages/chat.db`, the LaunchAgent is loaded correctly but macOS Full Disk Access is
blocking the background process. Grant Full Disk Access to the executable chain used by the job,
especially `/bin/zsh`, the `pdw` binary (`~/.local/bin/pdw`), `/opt/homebrew/bin/uv`, and
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`. Its current real path is
`/Users/zrl/.local/share/uv/python/cpython-3.12.12-macos-aarch64-none/bin/python3.12`. Then
kickstart the LaunchAgent again.

Apple Messages SQL starting points are `apple_messages`, `apple_message_chats`,
`apple_message_handles`, `apple_message_chat_handles`, `apple_message_chat_messages`, and
`apple_message_attachments`.

## Local Agent Sessions Upload Scheduler

Captures AI agent CLI session transcripts (Claude Code + Codex + OpenClaw) so every device's
sessions are queryable in the warehouse. The append-only transcripts are tailed and shipped,
line by line, through the same Drive-inbox pipeline as Apple Messages/WhatsApp.

> The macOS LaunchAgent below runs on Zach's Macs (crobat, porygon) for Claude Code/Codex. The
> **openclaw VM** runs the same uploader for OpenClaw sessions via a systemd user timer — see
> [OpenClaw Agent Sessions](#openclaw-agent-sessions-openclaw-vm) below.

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.agent-sessions-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.agent-sessions-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.agent-sessions-upload.plist`
- Wrapper script: `bin/agent-sessions-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest agent-sessions --mode incremental` (the wrapper runs the pdw CLI, which execs `uv run python -m personal_data_warehouse_agent_sessions.cli`)
- Main run log: `~/Library/Logs/personal-data-warehouse/agent-sessions-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/agent-sessions-upload.heartbeat`
- Status helper: `bin/agent-sessions-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/agent-sessions-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.agent-sessions-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.agent-sessions-upload
tail -80 ~/Library/Logs/personal-data-warehouse/agent-sessions-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/agent-sessions-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.agent-sessions-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.agent-sessions-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.agent-sessions-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.agent-sessions-upload
```

The uploader reads `~/.claude/projects/**/*.jsonl`, `~/.codex/sessions/**/rollout-*.jsonl`, and
`~/.openclaw/agents/main/sessions/<sessionId>.jsonl` (override with
`AGENT_SESSIONS_CLAUDE_PROJECTS_DIR` / `AGENT_SESSIONS_CODEX_SESSIONS_DIR` /
`AGENT_SESSIONS_OPENCLAW_SESSIONS_DIR`; set one to empty to disable that tool on a host). Each
tool's directory that doesn't exist on a given machine is simply skipped, so the same uploader
binary works everywhere. The OpenClaw scan ignores the `<sessionId>.trajectory.jsonl` runtime
trace and the `.json` sidecars next to each transcript. It tracks a byte offset per file,
coalesces new lines across files into full-size gzipped JSONL batches, and posts them through the
app's ingest endpoint (see below), which writes them into the `agent-sessions/inbox/` Drive
folder. The `--limit` flag bounds a run (useful for a first backfill). In Dagster, the
`agent_sessions_drive_inbox_sensor` + `agent_sessions_drive_ingest` asset consume the batches.

## Client uploads via the app (the only write path)

Every uploader (agent-sessions, voice-memos, apple-notes, apple-messages, and the in-process
whatsapp client) writes through the app — there is no longer a direct-to-Drive write path in the
clients. Each device POSTs domain payloads to the app's semantic ingestion endpoints
(`POST /ingest/<source>/<type>`, e.g. `/ingest/agent-sessions/batch`,
`/ingest/apple-messages/batch` + `/attachment`, `/ingest/whatsapp/batch` + `/media`,
`/ingest/voice-memos/audio` + `/metadata`, `/ingest/apple-notes/body` + `/attachment` +
`/revision`). The app owns the Drive credential, folder ids, object keys, `kind` values, and
`pdw_*` tags; the device holds none of that. The app writes byte-identical Drive objects, so the
Dagster `*_drive_ingest` readers are unchanged.

Every uploader therefore needs the warehouse URL and the app secret token. The canonical source
is pdw's own config: because the uploaders run via `pdw ingest <source>`, the pdw CLI resolves the
URL + token the way it does for every other command (`pdw login`, then `PDW_API_URL` /
`PDW_SECRET_TOKEN`) and passes them down — so a single `pdw login` configures uploads too, with no
separate ingest URL to manage. The client reads `PDW_API_URL` (legacy alias: `MCP_BASE_URL`) for
the URL and `PDW_SECRET_TOKEN` (legacy alias: `MCP_SECRET_TOKEN`) for the signing key. Without any
of them the uploader fails fast. On the app side, ingestion turns on automatically when the object
store is configured; per-source folders default to `PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID` and
can be overridden with `PDW_INGEST_<SOURCE>_FOLDER_ID` (e.g.
`PDW_INGEST_AGENT_SESSIONS_FOLDER_ID`). Uploads are authenticated with the same HMAC scheme as
signed download links, bound to the endpoint and the body's sha256, and the app dedups by stable
content sha. `<SOURCE>_STORAGE_BACKEND` / `<SOURCE>_GOOGLE_DRIVE_FOLDER_ID` now only provision the
Dagster reader's Drive access (the reader still reads Drive directly); they no longer affect how
clients write.

Agent sessions SQL starting points are the `agent_session_events` table (one row per transcript
line; `source` is `claude_code`, `codex`, or `openclaw`, `device` tags the machine) and the
`clean_agent_sessions` view (per-session roll-up: counts, token sums, title, cwd/git, first
prompt). Free-text content is also in the unified `searchable_text` view under
`source = 'agent_session'`. (Not to be confused with `agent_runs`/`agent_run_events`, which log
the warehouse's own internal enrichment agent.)

### OpenClaw Agent Sessions (openclaw VM)

OpenClaw runs on the `openclaw` Ubuntu VM (libvirt/KVM guest on `rotom`; reach it with
`ssh openclaw`, or `ssh -J rotom openclaw` when direct TCP is wedged — pings work but SSH can
time out, a known rotom-side issue). It writes one JSONL transcript per session under
`~/.openclaw/agents/main/sessions/`. Because the VM is Linux (no launchd), the uploader runs as
a **systemd user timer** (zrl has `Linger=yes`, so user units run without an active login).

- Checkout: `~/dev/zachlatta/personal-data-warehouse` (clone of `main` via a read-only GitHub
  deploy key; `core.sshCommand` points at `~/.ssh/pdw_deploy_key`); runs via `uv`
  (`~/.local/bin/uv`).
- Env: `~/dev/zachlatta/personal-data-warehouse/.env` holds the **app-ingest** config (the VM has
  no Drive credential): `PDW_API_URL` (the app, `https://data-warehouse-mcp.zachlatta.com`),
  `PDW_SECRET_TOKEN` (= the app's `PDW_SECRET_TOKEN`/`MCP_SECRET_TOKEN`),
  `AGENT_SESSIONS_STORAGE_BACKEND=http_app`, and `AGENT_SESSIONS_ACCOUNT=zach@zachlatta.com`
  (tags the envelope `account` + keys the upload-offset state DB — keep it stable).
  `AGENT_SESSIONS_CLAUDE_PROJECTS_DIR=`/`AGENT_SESSIONS_CODEX_SESSIONS_DIR=` are blanked so the
  VM uploads only OpenClaw sessions. `device` auto-resolves to the hostname `openclaw`. Uploads
  POST to the app, which writes the batch into the Drive inbox the Dagster ingest reads — see
  [Client uploads via the app](#client-uploads-via-the-app-the-only-write-path).
- Systemd unit: `personal-data-warehouse-agent-sessions-upload.{service,timer}` (user scope).
- Checked-in templates: `ops/systemd/personal-data-warehouse-agent-sessions-upload.{service,timer}`.
- Wrapper: `bin/agent-sessions-upload-systemd`; status helper: `bin/agent-sessions-upload-status-systemd`.
- Run cadence: every 300s (`OnUnitActiveSec=300s`, `Persistent=true`), mirroring the macOS cadence.
- Run log: `~/.local/state/personal-data-warehouse/agent-sessions-upload.run.log`;
  heartbeat: `~/.local/state/personal-data-warehouse/agent-sessions-upload.heartbeat`.

Inspect or repair it (from `ssh -J rotom openclaw`):

```bash
~/dev/zachlatta/personal-data-warehouse/bin/agent-sessions-upload-status-systemd
systemctl --user list-timers personal-data-warehouse-agent-sessions-upload.timer --all
systemctl --user start personal-data-warehouse-agent-sessions-upload.service   # run once now
journalctl --user -u personal-data-warehouse-agent-sessions-upload.service -n 80 --no-pager
tail -80 ~/.local/state/personal-data-warehouse/agent-sessions-upload.run.log
```

Install / reinstall the units after editing the templates:

```bash
mkdir -p ~/.config/systemd/user
cp ops/systemd/personal-data-warehouse-agent-sessions-upload.* ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now personal-data-warehouse-agent-sessions-upload.timer
```

To pull new code: `cd ~/dev/zachlatta/personal-data-warehouse && git pull && uv sync`. Because
uploads go through the app, end-to-end also depends on the **app** (`/ingest/agent-sessions/batch`)
and the **prod Dagster** reader both running `main` (the app writes the object tags the Dagster
reader expects, and the reader carries `openclaw_event_row`). Land/deploy code on both before
relying on the timer.

## WhatsApp Client (linked device)

WhatsApp syncs through a real WhatsApp Web multidevice client (neonize, Python bindings over
whatsmeow), not a local-store scanner. The client registers as a linked device on Zach's
WhatsApp account, holds a persistent connection, and receives live messages plus history sync.

It runs in-process with the production Dagster deployment; no separate image or service:

- Asset/job: `whatsapp_client` / `whatsapp_client_job` (`src/personal_data_warehouse/defs/whatsapp_client.py`)
- The client runs in bounded windows (`WHATSAPP_CLIENT_RUN_SECONDS`, default 10800s) so it
  never trips Dagster run monitoring (`max_runtime_seconds: 14400`); the
  `whatsapp_client_keepalive_sensor` relaunches it whenever no run is active. WhatsApp queues
  messages for offline linked devices, so the seconds between windows lose nothing.
- A Postgres advisory lock prevents two concurrent connections on one session, which would
  corrupt the device state.
- Records flow exactly like Apple Messages: the client batches JSONL.gz envelopes to
  `whatsapp/inbox/batches/` and media blobs to `whatsapp/inbox/media/` in Google Drive; the
  `whatsapp_drive_inbox_sensor` + `whatsapp_drive_ingest` asset consume and promote them.
- Session state is canonical in Postgres table `whatsapp_client_sessions` as a bytea SQLite
  snapshot keyed by `WHATSAPP_ACCOUNT` + `WHATSAPP_SESSION_KEY` (default `default`). neonize
  still requires a SQLite filename at runtime, so `WHATSAPP_SESSION_PATH` is only a disposable
  cache path restored from Postgres before each run and snapshotted back after pairing,
  connect, contact dumps, flushes, and shutdown.

Enabling and pairing (first time):

1. The client is enabled by default once WhatsApp is configured; leave
   `WHATSAPP_CLIENT_ENABLED` unset or set it to `1`. Set it to `0` only when the client needs
   to be paused. `WHATSAPP_SESSION_PATH` may be left as the default runtime cache path; it does
   not need a persistent volume. Optionally set `WHATSAPP_PAIR_PHONE=<E.164 number without +>`
   to pair with an 8-character code instead of a QR.
2. Wait for the keepalive sensor to launch `whatsapp_client_job` (or launch it from the
   Dagster UI) and open the run logs.
3. Scan the QR printed in the logs (WhatsApp > Settings > Linked Devices > Link a Device), or
   enter the logged pairing code. After pairing, the client snapshots the session into Postgres
   and history sync chunks arrive automatically.

Other env vars: `WHATSAPP_ACCOUNT`, `WHATSAPP_GOOGLE_DRIVE_FOLDER_ID` (both fall back to the
Apple Messages values), `WHATSAPP_SESSION_KEY`, `WHATSAPP_CLIENT_ID` (normally leave unset),
`WHATSAPP_FLUSH_INTERVAL_SECONDS`, `WHATSAPP_MEDIA_BYTES_PER_FLUSH`,
`WHATSAPP_MEDIA_COUNT_PER_FLUSH`, `WHATSAPP_DOWNLOAD_HISTORY_MEDIA` (default **on**: all
attachments — live and history-sync — are downloaded to object storage. Set it to `0` as an
escape hatch if the history-media backfill causes load/ban-risk issues; WhatsApp often will not
serve very old media, so expect some `whatsapp_media_items.is_missing = true` rows for old
messages even with it on).

For local pairing/debugging there is a CLI: `uv run personal-data-warehouse-whatsapp-client`
(requires `brew install libmagic` on macOS). It requires `POSTGRES_DATABASE_URL` because
Postgres is the session source of truth. `--session-file` only selects the runtime cache file.

Caveats: unofficial clients violate WhatsApp ToS and carry a small account-ban risk. neonize is
pinned to 0.3.17.post0 because newer releases need protobuf>=7, which dagster pins below; its
Go shared library is pre-fetched in the Dockerfile (`import neonize.client` at build).

WhatsApp SQL starting points are `whatsapp_messages`, `whatsapp_chats`,
`whatsapp_chat_participants` (group rosters: one row per member with admin flags),
`whatsapp_contacts`, and `whatsapp_media_items`. Group subjects and rosters are populated by
a once-per-run-window `get_joined_groups()` dump in the client (history sync never carries
them); `whatsapp_chats.name` is preserved against later empty-name history rows.

Downloaded WhatsApp image (and document-PDF) media is enriched the same way Gmail attachments
are: the `whatsapp_media_enrichment` asset (`defs/whatsapp_media_enrichment.py`) scans
`whatsapp_media_items` for stored blobs (`is_missing = 0`), runs each through the agent-container
vision pipeline, and upserts the structured text into the shared `file_attachment_enrichments`
table — the renamed, source-agnostic successor to `gmail_attachment_enrichments` that both Gmail
and WhatsApp write to (keyed by `content_sha256` + `ai_provider`/`ai_model`/`ai_prompt_version`,
each source under its own `task_type`/`prompt_version`). The runner, image prep, agent prompt,
and candidate query all live in `file_attachment_enrichment.py`; each source is a
`FileEnrichmentSource` descriptor. That enrichment text is surfaced by `search_text()` under
`source = 'whatsapp_media'`, `subsource = 'content'`.

## Shared file-attachment enrichment

`gmail_attachment_enrichments` was renamed to `file_attachment_enrichments` and generalized into
a single source-agnostic enrichment pipeline (`file_attachment_enrichment.py`). To add a new
attachment source, define a `FileEnrichmentSource` (its table, sha/filename/mime/size/order
columns, a `stored_predicate`, and whether PDFs need a prior deterministic-extraction step), then
wire a Dagster asset/sensor that runs `FileAttachmentEnrichmentRunner` with that source — see
`defs/gmail_attachment_enrichment.py` and `defs/whatsapp_media_enrichment.py`. The table rename
migrates in place via `ensure_*` (`ALTER TABLE IF EXISTS … RENAME`), preserving existing rows.
