# Agent Notes

Development practices:

* We use TDD for this repo and follow good code practices
* When asked to refactor or change existing code flows, please plan to completely replace the old legacy flow with the new requested flow - including ripping out any and all legacy code
* When querying the database, you can use the pdw CLI

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
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`. The python lives under a
versioned uv directory, so its real path **drifts on every uv python patch bump** (e.g.
`cpython-3.12.12-…` → `cpython-3.12.13-…`), silently breaking the previously-granted FDA. Don't
hardcode it — derive the current target with
`uv run python -c 'import sys,os;print(os.path.realpath(sys.executable))'`, grant FDA to that, then
kickstart the LaunchAgent again. Because the path changes under you, re-check it whenever an
uploader starts failing with a permission error after working fine before.

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
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`. The python lives under a
versioned uv directory, so its real path **drifts on every uv python patch bump** (e.g.
`cpython-3.12.12-…` → `cpython-3.12.13-…`), silently breaking the previously-granted FDA. Don't
hardcode it — derive the current target with
`uv run python -c 'import sys,os;print(os.path.realpath(sys.executable))'`, grant FDA to that, then
kickstart the LaunchAgent again. Because the path changes under you, re-check it whenever an
uploader starts failing with a permission error after working fine before.

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
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`. The python lives under a
versioned uv directory, so its real path **drifts on every uv python patch bump** (e.g.
`cpython-3.12.12-…` → `cpython-3.12.13-…`), silently breaking the previously-granted FDA. Don't
hardcode it — derive the current target with
`uv run python -c 'import sys,os;print(os.path.realpath(sys.executable))'`, grant FDA to that, then
kickstart the LaunchAgent again. Because the path changes under you, re-check it whenever an
uploader starts failing with a permission error after working fine before.

Apple Messages SQL starting points are `apple_messages`, `apple_message_chats`,
`apple_message_handles`, `apple_message_chat_handles`, `apple_message_chat_messages`, and
`apple_message_attachments`.

## Local Apple Photos Upload Scheduler

This Mac is intended to run the local Apple Photos uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.photos-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.photos-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.photos-upload.plist`
- Wrapper script: `bin/photos-upload-launchd`
- Run cadence: every 1800 seconds with `RunAtLoad`
- Command: `pdw ingest apple-photos --mode incremental` (the wrapper runs the pdw CLI, which execs `uv run python -m personal_data_warehouse_photos.cli`)
- Main run log: `~/Library/Logs/personal-data-warehouse/photos-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/photos-upload.heartbeat`
- Status helper: `bin/photos-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/photos-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.photos-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.photos-upload
tail -80 ~/Library/Logs/personal-data-warehouse/photos-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/photos-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.photos-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.photos-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.photos-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.photos-upload
```

If the run log shows `PermissionError` for `~/Pictures/Photos Library.photoslibrary`, the
LaunchAgent is loaded correctly but macOS Full Disk Access is blocking the background process —
the same FDA/uv-python-path-drift story as the other uploaders above.

The uploader snapshots `Photos.sqlite` (never reads the live DB), uploads each present original
(plus a Live Photo's `<uuid>_3.mov` motion sibling, under the still's ZUUID with
`role=live_video`) through `POST /ingest/photos/file` + `/ingest/photos/metadata`, and reports
**coverage** every run: in an "Optimize Mac Storage" library most originals are cloud-only
placeholders, so expect `missing=` counts — those defer and re-check every run, never error.
Files above the route's upload ceiling defer likewise; edited renditions are not uploaded yet
(originals only; the run log counts assets with adjustments). First backfill ramp:
`pdw ingest apple-photos --mode full --limit N`.

Serverside, `photos_drive_inbox_sensor` + `photos_drive_ingest` consume the inbox into
`apple_photos.files`; the `photo_identity` asset dedups renditions into logical photos
(`photos.assets` + `photos.asset_files` link/audit rows, 256-bit dhash fingerprints in
`enrichment.media_fingerprints`, 1280px JPEG thumbnails in Drive); `photo_enrichment` runs the
vision agent once per logical photo over `marts.photo_canonical_renditions`; the `photo`
timeline adapter emits one event per photo with the AI caption in `search_text`.

Photos SQL starting points are `apple_photos.files` (raw renditions), `photos.assets` (one row
per deduplicated logical photo), `photos.asset_files` (identity links + `match_method`/
`match_score` dedup audit), `marts.photos` (assets + caption + rendition counts),
`marts.photo_files` (all renditions across sources), and timeline `source = 'photos'`. Free-text
search: `search.search_text()` with `sources => ARRAY['photo']`.

### Adding a photo source (google_photos Takeout import, manual imports, ...)

The photos pipeline is multi-source by construction; Apple Photos is just the first source.
`PHOTO_SOURCE_RELATIONS` in `src/personal_data_warehouse/relations.py` is THE extension point —
it drives Drive-ingest routing, the identity runner's scan, and the `marts.photo_files` union.
To add a source:

1. **Raw table**: add `<source>` to `SOURCE_RAW_SCHEMAS`, a `("<source>_files", "<source>",
   "files")` relation row, and a `TableSpec(PHOTO_SOURCE_FILE_COLUMNS, ...)` in `postgres.py`
   (same shared column list and provenance primary key as `apple_photos_files`), then add the
   table to `_PHOTO_TABLES` and `TIMELINE_TABLE_COVERAGE` (a `detail` of `photo_assets`).
2. **Registry**: one entry in `PHOTO_SOURCE_RELATIONS` (`"<source>": "<source>_files"`). Unknown
   sources fail loud at ingest — register before uploading.
3. **Uploader**: post the shared envelope (`personal_data_warehouse_photos/envelope.py`,
   `source="<source>"`, native id + role per file, raw record under a source-named key like
   `takeout_sidecar`) to the existing `/ingest/photos/file` + `/ingest/photos/metadata`
   endpoints via `IngestClient.upload_photo_file`/`upload_photo_metadata`. Live/motion
   components upload under the same native id with `role=live_video`; edited outputs use
   `role=edited`.
4. **Precedence**: slot the source into `PHOTO_SOURCE_PRECEDENCE`
   (`src/personal_data_warehouse/photo_identity.py`) so canonical-field resolution knows who
   wins when renditions disagree.
5. Nothing else: identity/dedup (incl. the burst guard and cross-source perceptual merge),
   thumbnails, enrichment, timeline, and search all follow automatically from the registry.

## Local Agent Sessions Upload Scheduler

Captures AI agent CLI session transcripts (Claude Code + Codex + OpenClaw + pi) so every device's
sessions are queryable in the warehouse. The append-only transcripts are tailed and shipped,
line by line, through the same Drive-inbox pipeline as Apple Messages/WhatsApp.

> The macOS LaunchAgent below runs on Zach's Macs (crobat, porygon) for Claude Code/Codex/pi. The
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

The uploader reads `~/.claude/projects/**/*.jsonl`, `~/.codex/sessions/**/rollout-*.jsonl`,
`~/.openclaw/agents/main/sessions/<sessionId>.jsonl`, and
`~/.pi/agent/sessions/**/*.jsonl` (override with `AGENT_SESSIONS_CLAUDE_PROJECTS_DIR` /
`AGENT_SESSIONS_CODEX_SESSIONS_DIR` / `AGENT_SESSIONS_OPENCLAW_SESSIONS_DIR` /
`AGENT_SESSIONS_PI_SESSIONS_DIR`; set one to empty to disable that tool on a host). Each
tool's directory that doesn't exist on a given machine is simply skipped, so the same uploader
binary works everywhere. The OpenClaw scan ignores the `<sessionId>.trajectory.jsonl` runtime
trace and the `.json` sidecars next to each transcript. It tracks a byte offset per file,
coalesces new lines across files into full-size gzipped JSONL batches, and posts them through the
app's ingest endpoint (see below), which writes them into the `agent-sessions/inbox/` Drive
folder. The `--limit` flag bounds a run (useful for a first backfill). In Dagster, the
`agent_sessions_drive_inbox_sensor` + `agent_sessions_drive_ingest` asset consume the batches.

## Client uploads via the app (the write path for remote devices)

Every *remote-device* uploader (agent-sessions, voice-memos, apple-notes, apple-messages) writes
through the app — those devices are untrusted and must not hold the Drive credential. Each device
POSTs domain payloads to the app's semantic ingestion endpoints (`POST /ingest/<source>/<type>`,
e.g. `/ingest/agent-sessions/batch`, `/ingest/apple-messages/batch` + `/attachment`,
`/ingest/voice-memos/audio` + `/metadata`, `/ingest/apple-notes/body` + `/attachment` +
`/revision`). The app owns the Drive credential, folder ids, object keys, `kind` values, and
`pdw_*` tags; the device holds none of that. The app writes byte-identical Drive objects, so the
Dagster `*_drive_ingest` readers are unchanged.

**Exception — the in-process WhatsApp client writes directly to Drive.** It runs *inside* the
trusted prod Dagster deployment (co-located with the app on rotom) and already holds the full
Drive read+write credential the readers use, so the app indirection buys nothing and only re-adds
a Cloudflare 100 MiB body cap on its large media (WhatsApp videos). It builds the same Drive
`ObjectStore` the `whatsapp_drive_ingest` reader builds and writes `whatsapp/inbox/batches/` +
`whatsapp/inbox/media/` objects itself (byte/tag-identical to what the app would have written),
deduping by content sha. There is **no** `/ingest/whatsapp/*` endpoint — see
[WhatsApp Client](#whatsapp-client-linked-device). (Note `claude_desktop_client` also runs in
Dagster but still posts to the shared `/ingest/agent-sessions/batch`: small payloads, no cap
problem.)

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

### Large uploads and the Cloudflare 100 MiB cap

The public app hostnames are fronted by **Cloudflare**, which hard-caps request bodies at **100
MiB** on non-Enterprise plans (it answers `413 Payload Too Large` before the request reaches the
app, whose own cap is `PDW_INGEST_MAX_OBJECT_BYTES`, default 512 MiB). Voice memos in particular
routinely exceed 100 MiB, so a client posting to the Cloudflare URL silently fails on big files —
and because a per-file failure used to re-raise, a single oversized memo wedged the whole run.

The upload client (`ingest_client.py`, shared by every uploader) handles this two ways:

- **Prefer a Tailscale-direct origin.** When `PDW_INGEST_TAILSCALE_HOST` names a tailnet node
  (e.g. `rotom`, the Coolify host) — or `PDW_INGEST_DIRECT_URL` gives an explicit base — the client
  resolves that node's current tailnet IPv4 via the `tailscale` CLI and, if it answers `/healthz`
  as the app, sends uploads straight there over plain HTTP (Tailscale/WireGuard is the transport
  encryption) with the public `Host:` header so Traefik still routes to the app. That bypasses
  Cloudflare entirely and lifts the ceiling to the app's 512 MiB cap. Off-tailnet (probe fails) it
  transparently falls back to the public `PDW_API_URL`. These are set in the gitignored repo `.env`
  on the tailnet machines, so the committed repo stays generic. `PDW_TAILSCALE_BIN` overrides the
  CLI path.
- **Defer what the route still can't carry.** `IngestClient.effective_max_upload_bytes` reports the
  real ceiling for the chosen route (512 MiB direct, else min(app cap, 100 MiB)). The voice-memos
  runner defers any recording above it (like its partial/age deferrals) instead of 413-ing and
  wedging — so e.g. a lone 588 MiB memo is skipped while every other memo uploads.

Agent-session SQL starting points are the source-owned raw event tables
`claude_code.events`, `codex.events`, `openclaw.events`, `pi.events`, `claude_desktop.events`, and
`chatgpt.events` (one row per transcript/conversation line; `device` tags the machine where
applicable). Cross-source querying uses `marts.ai_conversation_events`, and per-session roll-ups
(counts, token sums, title, cwd/git, first prompt) use `marts.ai_conversation_sessions`. Free-text
content is available through `search.search_text()` with `source = 'agent_session'`. (Not to be
confused with `ai_processing.agent_runs` / `ai_processing.agent_run_events`, which log the
warehouse's own internal enrichment agent.)

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

## Claude Desktop Sessions (claude.ai)

Captures normal Claude conversations from the Claude Desktop app so they're queryable in
the warehouse alongside the agent-CLI sources. They land in the source-owned
`claude_desktop.events` raw table and are also exposed through `marts.ai_conversation_events`,
`marts.ai_conversation_sessions`, and `search.search_text()`, normalized by
`claude_desktop_event_row` in `agent_sessions_drive_ingest.py`.

Unlike Claude Code/Codex/OpenClaw, **the desktop app keeps no transcripts on disk** - it is a
claude.ai wrapper; conversations live server-side. So this source is **authed clientside, polled
serverside**:

- **Clientside auth (native Go in the `pdw` CLI - all local-machine logic lives in the CLI, not
  Python):** `pdw ingest claude-desktop` decrypts the desktop app's `sessionKey` cookie (Chromium
  cookie store + macOS Keychain AES key) and pushes the session credential
  (`account`/`session_key`/`org_id`) to the app's HMAC-signed `/ingest/claude-desktop/credential`
  endpoint. Implementation: `app/cmd/pdw-cli/claudedesktop.go` (Keychain via `security`, cookie DB
  via the macOS-bundled `sqlite3`, AES/PBKDF2 from the Go stdlib). `--dry-run` prints what would be
  pushed without contacting the app.
- **App credential endpoint (Go):** `app/internal/server/credential_ingest.go` verifies the same
  object-upload HMAC as the other ingest endpoints and upserts the credential into the
  `private.claude_desktop_credentials` Postgres table (keyed by account). Registered in `NewMux`
  whenever `POSTGRES_DATABASE_URL` is set.
- **Serverside poller (Dagster):** `defs/claude_desktop_client.py` - the `claude_desktop_client`
  asset + `claude_desktop_client_keepalive_sensor` (5-min cadence) read the credential from
  Postgres and poll the claude.ai API (`personal_data_warehouse_claude_desktop/{api,sync,state}.py`).
  The `sessionKey` alone authenticates the API, so it works from prod's IP - no Cloudflare cookies
  needed. It fetches conversations changed since the per-conversation `updated_at` cursor
  (`claude_desktop.conversation_state`, Postgres-durable) and ships one `conversation` header line +
  one `message` line per turn through the SAME `/ingest/agent-sessions/batch` path as the other
  agent sources. Re-shipping a whole conversation when it gains a turn is cheap (warehouse dedupes
  by `(source, session_id, event_uuid)` into `claude_desktop.events`).

The `sessionKey` rotates ~monthly; the desktop app refreshes it, and the clientside LaunchAgent
re-pushes it hourly so the server's copy stays fresh.

Env: `CLAUDE_DESKTOP_ACCOUNT` (keys the credential + cursor; falls back to
`AGENT_SESSIONS_ACCOUNT`/`APPLE_MESSAGES_ACCOUNT`/`VOICE_MEMOS_ACCOUNT`/`GMAIL_ACCOUNTS[0]` - must
match between the clientside push and the serverside poller), `CLAUDE_DESKTOP_ENABLED` (default on;
set `0` to pause the poller), `CLAUDE_DESKTOP_ORG_ID` (override the org from the cookie),
`CLAUDE_DESKTOP_BASE_URL` (default `https://claude.ai`), and clientside-only
`CLAUDE_DESKTOP_COOKIES_PATH` / `CLAUDE_DESKTOP_KEYCHAIN_SERVICE` / `CLAUDE_DESKTOP_KEYCHAIN_ACCOUNT`.

> End-to-end depends on the **app** (`/ingest/claude-desktop/credential` + `/ingest/agent-sessions/batch`)
> and **prod Dagster** (the poller + the `claude_desktop_event_row` reader) both running `main`. Land
> and deploy code on both before relying on the LaunchAgent. The serverside poller is unofficial-API
> access to claude.ai; treat it like the WhatsApp linked-device client (small ToS/account risk).

### Local Claude Desktop Auth Scheduler

The Mac with the Claude Desktop app pushes the credential through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.claude-desktop-auth`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.claude-desktop-auth.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.claude-desktop-auth.plist`
- Wrapper script: `bin/claude-desktop-auth-launchd` (sources the repo `.env`, then runs
  `pdw ingest claude-desktop`; the Go command does not load `.env` itself)
- Run cadence: every 3600 seconds with `RunAtLoad`
- Main run log: `~/Library/Logs/personal-data-warehouse/claude-desktop-auth.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/claude-desktop-auth.heartbeat`
- Status helper: `bin/claude-desktop-auth-status`

Inspect or repair it:

```bash
bin/claude-desktop-auth-status
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.claude-desktop-auth
tail -80 ~/Library/Logs/personal-data-warehouse/claude-desktop-auth.run.log
pdw ingest claude-desktop --dry-run   # verify cookie decryption without pushing
```

Install / reinstall the plist after editing the template:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.claude-desktop-auth.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.claude-desktop-auth 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.claude-desktop-auth.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.claude-desktop-auth
```

If `pdw ingest claude-desktop` fails reading the Keychain or cookie store, macOS Full Disk Access
is likely blocking the background process from
`~/Library/Application Support/Claude/Cookies` or the `Claude Safe Storage` Keychain item. Grant
Full Disk Access to `/bin/zsh` and the `pdw` binary (`~/.local/bin/pdw`), then kickstart again.

Claude Desktop SQL starting points are `claude_desktop.events` for raw rows and
`marts.ai_conversation_events` / `marts.ai_conversation_sessions` filtered to
`source = 'claude_desktop'` for unified querying (one `meta` row per conversation carrying the
title/model, then `user`/`assistant` rows per turn; `session_id` is the claude.ai conversation
uuid). Free-text is in `search.search_text()` under `source = 'agent_session'`.

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
- Records land in the same Drive layout as Apple Messages, but the WhatsApp client writes them
  **directly** (it holds the Drive credential), not through the app's ingest endpoints: it builds
  the same `ObjectStore` the reader uses (`google_drive_spec(..., source="whatsapp")`) and writes
  JSONL.gz envelope batches to `whatsapp/inbox/batches/` and media blobs to `whatsapp/inbox/media/`
  itself (kind `whatsapp_export_batch` / `whatsapp_media_item`, deduped by content sha). Because
  the write skips Cloudflare, large media (videos) over the 100 MiB public-body cap upload fine.
  The `whatsapp_drive_inbox_sensor` + `whatsapp_drive_ingest` asset consume and promote them
  unchanged. The batch/media object keys + `pdw_*` tags live in
  `src/personal_data_warehouse_whatsapp/batcher.py`.
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
`FileEnrichmentSource` descriptor. That enrichment text is folded into the parent WhatsApp
message's timeline search document and surfaced by `search_text()` under `source = 'whatsapp'`.

## ChatGPT (consumer) - server-side backend poll

Normal ChatGPT conversations (the consumer product, not the API) land in the source-owned
`chatgpt.events` raw table (`source = 'chatgpt'`), alongside the other AI conversation sources,
and roll up through `marts.ai_conversation_sessions` with free-text in `search.search_text()`
(`subsource = 'chatgpt'`).

Why this one is different: the **ChatGPT desktop app** (`~/Library/Application Support/com.openai.chat`)
stores conversations **encrypted** (`conversations-v3-*/*.data`), and both the decryption key and
the app's auth token live in the macOS **data-protection keychain** under OpenAI's team access
group (`2DC432GLL2.com.openai.chat`). That is an `errSecMissingEntitlement` wall: a code-signing
check on the calling binary, not a user-consent gate, so no local helper can read them. We
therefore do **not** read the desktop app. Instead the warehouse polls ChatGPT's backend API
**server-side** using a chatgpt.com **web session** captured from a browser.

Two pieces:

- **Client-side setup (manual, interactive): `pdw chatgpt publish-session`.** Reads the
  chatgpt.com session cookie from a local Chrome-family browser (Chrome/Brave/Edge/Arc; auto-detected
  or `--browser`), decrypting it with the browser's *legacy*, consent-readable "<Browser> Safe
  Storage" keychain item (a one-time "allow" prompt); see `chatgpt_cookies.py`. It validates the
  session against `/api/auth/session`, then POSTs the full cookie header (HMAC-signed, like every
  other ingest) to the app endpoint `POST /ingest/chatgpt/session`, which upserts it into Postgres
  `private.chatgpt_sessions` (`app/internal/chatgptsession`). The cookie never goes to Drive. Re-run
  this whenever the server reports the session expired. Flags: `--account` (defaults through the
  same account fallback), `--session-key`, `--dry-run`.
- **Server-side poll (Dagster): `chatgpt_backend_ingest` asset + `chatgpt_backend_ingest_sensor`.**
  The sensor fires every `CHATGPT_POLL_INTERVAL_SECONDS` (default 300) once a session is published
  (it *skips* with a "run publish-session" reason before first setup, so a missing session never
  floods failures). The asset reads the stored session, exchanges it for a short-lived `accessToken`
  (`chatgpt_backend.py`), walks `backend-api/conversations` newest-first, fetches each conversation
  whose `update_time` is newer than the per-conversation watermark in `chatgpt.conversation_sync`,
  and normalizes the message tree via `chatgpt_conversation_to_event_rows`
  (`agent_sessions_drive_ingest.py`; depth-first `seq`, `tool`/`tool_use` detection, `model_slug`,
  reasoning -> `thinking`). Re-ingest is idempotent in `chatgpt.events` (PK
  `source,session_id,event_uuid`).

**Fail-loud / self-heal:** when the session is rejected (logout/expiry), the backend client raises
`ChatGPTAuthError`, the asset re-raises it with *"run `pdw chatgpt publish-session`"* and the run
goes **red** in monitoring; never a silent skip. The fix is one local re-run of publish-session.

Prod config (Coolify, on the **Dagster** deployment): ChatGPT polling is enabled by default once
an account label is available (`CHATGPT_ACCOUNT`, falling back to the agent-sessions/gmail
account); `CHATGPT_CLIENT_ENABLED=0` pauses it. Optional:
`CHATGPT_POLL_INTERVAL_SECONDS`, `CHATGPT_PAGE_SIZE`, `CHATGPT_MAX_CONVERSATIONS_PER_RUN` (bound a
first backfill), `CHATGPT_SESSION_KEY`, `CHATGPT_BASE_URL`. The **app** auto-exposes
`/ingest/chatgpt/session` whenever it has Postgres; no extra config. This is an unofficial API
(same ToS/ban-risk class as the WhatsApp client); it reads only the configured account. ChatGPT SQL
starting points: `chatgpt.events` plus `marts.ai_conversation_events` /
`marts.ai_conversation_sessions` filtered to `source = 'chatgpt'`, and `private.chatgpt_sessions`
(credential) / `chatgpt.conversation_sync` (per-conversation watermark).

## Plaid Finance

Personal financial data is linked through Plaid and stored in the source-owned `plaid` schema.
Raw/query tables are `plaid.items`, `plaid.accounts`, `plaid.transactions`,
`plaid.investment_securities`, `plaid.investment_holdings`, `plaid.investment_transactions`,
`plaid.liabilities`, and `plaid.sync_state`. Finance-domain read views are
`marts.finance_accounts`, `marts.finance_transactions`, `marts.finance_investment_holdings`,
`marts.finance_investment_transactions`, and `marts.finance_liabilities`. Access tokens are
isolated in `private.plaid_item_tokens`. Warehouse initialization provisions the NOLOGIN
`PDW_QUERY_POSTGRES_ROLE` (default `pdw_query`), revokes `private` from it/`PUBLIC`, and both Go and
Python read-only query runners assume that role for every user-authored query; never bypass this
boundary or expose the token table through normal query surfaces.

Configure `PLAID_ACCOUNT`, `PLAID_CLIENT_ID`, `PLAID_SECRET`, and `PLAID_ENV` on the machine doing
interactive linking and in the production Dagster deployment. `pdw ingest plaid link` opens the
localhost Plaid Link flow and persists the exchanged token; repeat it once per institution.
`pdw ingest plaid sync` performs an immediate pull. Production uses the `plaid_finance_sync` asset
and `plaid_finance_sync_every_thirty_minutes` schedule. Account, holding, and liability responses
are authoritative snapshots: reconcile missing accounts/holdings/liabilities rather than leaving
stale current rows. Product errors must persist a redacted failed `plaid.sync_state` row before the
run fails. Optional products default to read-only `transactions,investments,liabilities`; no
payment/money-movement Plaid products are requested.
New Links request `PLAID_TRANSACTIONS_LOOKBACK_DAYS` of Transactions history, defaulting to Plaid's
730-day maximum; the same setting controls the Investments transaction query window. Transactions
is the required Link product, while configured Investments and Liabilities are additional
consented products so partial-product institutions remain linkable. Sync marks products absent
from an Item's Plaid product metadata as `unsupported` without failing supported products. Plaid
cannot expand an existing Item's Transactions history grant, so Items created with a shorter
window must be removed and linked again. Preserve and verify warehouse history during that
migration before deleting rows belonging to the old Item.
Run `uv run python scripts/plaid_linking_report.py` after linking/live verification to refresh the
mode-0600, gitignored `reports/plaid-linking-report.private.md` artifact with every institution and
anonymous account status plus last-pull evidence. See the README's **Plaid Finance Sync** section
for all settings and safe aggregate verification queries.

## Shared file-attachment enrichment

`gmail_attachment_enrichments` was renamed to `file_attachment_enrichments` and generalized into
a single source-agnostic enrichment pipeline (`file_attachment_enrichment.py`). To add a new
attachment source, define a `FileEnrichmentSource` (its table, sha/filename/mime/size/order
columns, a `stored_predicate`, and whether PDFs need a prior deterministic-extraction step), then
wire a Dagster asset/sensor that runs `FileAttachmentEnrichmentRunner` with that source — see
`defs/gmail_attachment_enrichment.py` and `defs/whatsapp_media_enrichment.py`. The table rename
migrates in place via `ensure_*` (`ALTER TABLE IF EXISTS … RENAME`), preserving existing rows.
