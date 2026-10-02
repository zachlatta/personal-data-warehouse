# Agent sessions and AI conversations

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

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
- Command: `pdw ingest agent-sessions --mode incremental` (native Go in the signed pdw binary; the wrapper runs nothing else — no uv, no Python)
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
trace and the `.json` sidecars next to each transcript.

**OpenClaw 2026.9 stopped writing `<sessionId>.jsonl` at all.** It imported every transcript
into the agent's SQLite store (`~/.openclaw/agents/main/agent/openclaw-agent.sqlite`, table
`transcript_events`: one row per event, `event_json` byte-for-byte the old JSONL line) and
left only `.deleted`/`.reset`/`.migrated` archives in the sessions directory. From 2026-09-09
to 09-20 the uploader therefore ran green every five minutes reporting `Discovered 0 agent
session transcript file(s)`, its heartbeat read `ok`, and no OpenClaw session reached the
warehouse -- a healthy uploader over a source that had moved is exactly what the run
heartbeat cannot see. The uploader now also reads that store (read-only, live, no snapshot:
it is >100 MB), with a per-session `seq` cursor kept in the same state file. The store path
defaults to `../agent/openclaw-agent.sqlite` beside the sessions dir, so blanking
`AGENT_SESSIONS_OPENCLAW_SESSIONS_DIR` disables both; `AGENT_SESSIONS_OPENCLAW_STORE_PATH`
overrides it. **The rewrite generation is part of the cursor**
(`transcript_rewrite_watermarks.generation`): a compaction or rewind reuses `seq` numbers for
different events, so a bare `seq` cursor would skip them; a new generation re-ships the
session and ingest dedupes by the event's own `id`. A host still on the JSONL layout has no
store file and is unaffected. The uploader tracks a byte offset per file,
coalesces new lines across files into full-size gzipped JSONL batches, and posts them through the
app's ingest endpoint (see below), which writes them into the `agent-sessions/inbox/` Drive
folder. The `--limit` flag bounds a run (useful for a first backfill). In Dagster, the
`agent_sessions_drive_inbox_sensor` + `agent_sessions_drive_ingest` asset consume the batches.

## Client uploads via the app (the write path for remote devices)

Every *remote-device* uploader (agent-sessions, voice-memos, apple-notes, apple-messages) writes
through the app — those devices are untrusted and must not hold the Drive credential. Each device
POSTs domain payloads to the app's semantic ingestion endpoints (`POST /ingest/<source>/<type>`,
e.g. `/ingest/agent-sessions/batch`, `/ingest/apple-messages/batch` + `/attachment`,
`/ingest/voice-memos/audio/resumable` + `/metadata`, `/ingest/apple-notes/body` + `/attachment` +
`/revision`). The app owns the Drive credential, folder ids, object keys, `kind` values, and
`pdw_*` tags; the device holds none of that. The app writes byte-identical Drive objects, so the
Dagster `*_drive_ingest` readers are unchanged.

**Exception — the in-process WhatsApp client writes directly to Drive.** It runs *inside* the
trusted prod Dagster deployment (co-located with the app on mew-coolify) and already holds the full
Drive read+write credential the readers use, so the app indirection buys nothing and only re-adds
a Cloudflare 100 MiB body cap on its large media (WhatsApp videos). It builds the same Drive
`ObjectStore` the `whatsapp_drive_ingest` reader builds and writes `whatsapp/inbox/batches/` +
`whatsapp/inbox/media/` objects itself (byte/tag-identical to what the app would have written),
deduping by content sha. There is **no** `/ingest/whatsapp/*` endpoint — see
[WhatsApp Client](other-sources.md#whatsapp-client-linked-device). (Note `claude_desktop_client` also runs in
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

The upload client (`app/internal/ingestclient`, shared by every `pdw ingest` uploader; the
Python `ingest_client.py` is now only the server-side sliver the Claude Desktop poller uses to
post agent-session batches from Dagster) handles this two ways:

- **Prefer a Tailscale-direct origin.** When `PDW_INGEST_TAILSCALE_HOST` names a tailnet node
  (e.g. `mew-coolify`, the host the app runs on) — or `PDW_INGEST_DIRECT_URL` gives an explicit base — the client
  resolves that node's current tailnet IPv4 via the `tailscale` CLI and, if it answers `/healthz`
  as the app, sends uploads straight there over plain HTTP (Tailscale/WireGuard is the transport
  encryption) with the public `Host:` header so Traefik still routes to the app. That bypasses
  Cloudflare entirely and lifts the ceiling to the app's 512 MiB cap. Off-tailnet (probe fails) it
  transparently falls back to the public `PDW_API_URL`. These are set in the gitignored repo `.env`
  on the tailnet machines, so the committed repo stays generic. `PDW_TAILSCALE_BIN` overrides the
  CLI path.
- **Defer what the route still can't carry.** `Client.EffectiveMaxUploadBytes` reports the
  real ceiling for the chosen route (512 MiB direct, else min(app cap, 100 MiB)); the Muse
  uploader defers a blob above it instead of 413-ing and wedging.
- **Photos and voice-memo audio do not use a body route at all.** Their signed start request
  (`/ingest/photos/file/resumable`, `/ingest/voice-memos/audio/resumable`) is tiny, and the
  file's bytes go straight into an app-created Drive resumable session in 16 MiB chunks. This
  works through the public app URL and has no per-file ceiling; Drive's acknowledged range
  handles lost responses and its final sha256 + size gate the metadata upload.

**The Tailscale-direct route has a TIME ceiling, not just a size one: Traefik's 60-second
entrypoint `readTimeout`.** Coolify's proxy on mew-coolify is Traefik v3 with no
`respondingTimeouts` set, and Traefik v3 defaults `readTimeout` to 60s, so any request body
that takes longer than a minute to send is cut: the app logs `status=400 duration=1m0.2s`
(the body read failed) and the client receives `499` or `504`. At the ~6-10 MiB/s porygon
reaches over Tailscale that is roughly 350-600 MiB, which is why a 371 MiB and a 430 MiB memo
each needed 60-150 failed runs to land in August/September 2026 (a run succeeded only when
the link happened to be fast), and why a 625 MiB, 2 h 42 m recording on 2026-09-30 failed
every run until voice-memo audio moved to the resumable session. The client's own timeout
scales with size (~1 MiB/s floor), so a 499 there is the proxy, not the client. The
remaining raw-body endpoints (Apple Messages / Notes attachments, manual-finance documents,
Muse files) are still bounded by it; a file that needs more than a minute belongs on the
resumable path, not on a longer proxy timeout.

Agent-session SQL starting points are the source-owned raw event tables
`base_claude_code.events`, `base_codex.events`, `base_openclaw.events`, `base_pi.events`, `base_claude_desktop.events`, and
`base_chatgpt.events` (one row per transcript/conversation line; `device` tags the machine where
applicable). Cross-source querying uses `marts_ai_conversations.events`, and per-session roll-ups
(counts, token sums, title, cwd/git, first prompt) use `marts_ai_conversations.sessions`. Free-text
content is available through `timeline.search_text()` with `source = 'agent_session'`. (Not to be
confused with `ops.ai_processing_agent_runs` / `ops.ai_processing_agent_run_events`, which log the
warehouse's own internal enrichment agent.)

### Codex tool calls: one custom tool, many inner calls

**Codex runs nearly every tool through a custom tool, and until 2026-10-02 PDW stored those
rows as noise.** A custom tool takes free-form text instead of JSON arguments: `exec`, whose
input is a small JS program that may make several inner calls (`tools.exec_command`,
`tools.mcp__skills__skill_read`, `tools.web__run`, `tools.apply_patch`, ...), and
`apply_patch`. The normalizer handled only `function_call`/`function_call_output`, so every
`custom_tool_call` and `custom_tool_call_output` row landed as `role = 'meta'` with an
empty `tool_name`, `tool_input_json` and `tool_result_json`. Measured on production that
morning: 75,541 calls and 75,690 outputs against 82,323 function calls over the whole
history, and from July on the custom shape was most of Codex's tool use (22,482 calls in
the week of 2026-08-31). Every "pair tool calls with results" query — the guide's own
advice — saw almost none of it, and a skills-usage audit had to parse `raw_json`. The
hosted `web_search_call`, `tool_search_call`/`tool_search_output` and
`image_generation_call` items had the same gap.

What the normalizer writes now (`_apply_codex_response_item`):

- **The call** is `role = 'assistant'`, `subtype = 'tool_use'`, `tool_name` = the custom
  tool's own name (`exec`, `apply_patch`), `turn_id` = the call id. `tool_input_json` is
  `{"input": <verbatim text>}` plus, for `exec`, `tools` (distinct inner tool names in
  first-call order) and `commands` (every `cmd` string literal, decoded; a command built
  from a variable is only in `input`), and for `apply_patch`, `files`. `tool_name` stays the
  custom tool rather than one inner tool because a script routinely calls several; the
  inner names are an array lookup away.
- **The output** is `role = 'tool'`, `subtype = 'tool_result'`, same `turn_id`, `text` = the
  joined output text, `tool_result_json` = `{"output", "exit_codes", "truncated"}`.
  `exit_codes` are read only from an `exec_command` result chunk's header
  (`{"chunk_id":…,"exit_code":N,…,"output":…}`), so an `exit_code` printed inside some
  command's own output never counts.
- `web_search`, `tool_search` and `image_generation` are named tool rows; a generated
  image's base64 stays in `raw_json` only.
- Tool rows carry no turn text, so the timeline's `agent_session_turn` adapter (user and
  assistant rows **with** text) and search are unchanged.

Codex also writes an `event_msg` / `item_completed` row per inner call since 2026-08-31
(`CommandExecution` with `command`, `aggregated_output`, `exit_code`; `McpToolCall`;
`FileChange`). Those stay `meta` with the detail in `raw_json`: they have no call id linking
them to their `exec`, they do not exist before 2026-08-31, and naming them as tool rows too
would count every command twice. `marts_ops.agent_usage` reads them for exactly that reason
and skips the `exec` call they follow.

**History was rewritten from each row's own `raw_json`, not re-ingested from Drive.** Every
row stores its source line and `codex_event_row` is a pure function of (line, session,
seq), so `AgentSessionsDriveIngestRunner` re-runs the normalizer over stored rows whose
`subtype` is still one of `LEGACY_CODEX_TOOL_SUBTYPES` and upserts them through the normal
write path, newest first, same primary key. The table is the cursor: a rewritten row no
longer carries a legacy subtype (the normalizer never writes one), so each ingest run
resumes where the last stopped, and `codex_events_legacy_tool_rows_idx` — partial over
exactly those subtypes — makes the probe an index scan that is empty once converged. It is
capped at `AGENT_SESSIONS_CODEX_RENORMALIZE_ROWS_PER_RUN` (5,000) rows a run because an
output row is ~16 KB of `raw_json` and the rewrite adds the output to `text` and
`tool_result_json` as every other provider's tool rows do: ~150k rows is ~1 GB on disk,
and spreading it over ~30 runs keeps its WAL far under what the archiver ships. A rewritten
row carries the run's `ingested_at`, so the `agent_session` timeline adapter re-reads the
sessions it touched (their `assistant_events` count grows). `base_codex.events` rows still
in a legacy subtype after convergence are ones whose `raw_json` would not parse; the run
logs them and stops rather than refetching them forever.

### OpenClaw Agent Sessions (openclaw VM)

OpenClaw runs on the `openclaw` Ubuntu VM (libvirt/KVM guest on `rotom`; reach it with
`ssh openclaw`, or `ssh -J rotom openclaw` when direct TCP is wedged — pings work but SSH can
time out, a known rotom-side issue). It writes one JSONL transcript per session under
`~/.openclaw/agents/main/sessions/` (through 2026.8; since 2026.9 in the agent's SQLite
store, which the same uploader reads -- see above). Because the VM is Linux (no launchd), the uploader runs as
a **systemd user timer** (zrl has `Linger=yes`, so user units run without an active login).

- Checkout: `~/dev/zachlatta/personal-data-warehouse` (clone of `main` via a read-only GitHub
  deploy key; `core.sshCommand` points at `~/.ssh/pdw_deploy_key`). The checkout supplies only
  the wrapper, the shared lib and `.env`; the uploader itself is the native Go
  `pdw ingest agent-sessions`, so the VM needs a Linux release of the pdw binary at
  `~/.local/bin/pdw` (or `PDW_BIN`) and no `uv`/Python at all.
- Env: `~/dev/zachlatta/personal-data-warehouse/.env` holds the **app-ingest** config (the VM has
  no Drive credential): `PDW_API_URL` (the app, `https://data-warehouse-mcp.zachlatta.com`),
  `PDW_SECRET_TOKEN` (= the app's `PDW_SECRET_TOKEN`/`MCP_SECRET_TOKEN`),
  `AGENT_SESSIONS_STORAGE_BACKEND=http_app`, and `AGENT_SESSIONS_ACCOUNT=zach@zachlatta.com`
  (tags the envelope `account` + keys the upload-offset state DB — keep it stable).
  `AGENT_SESSIONS_CLAUDE_PROJECTS_DIR=`/`AGENT_SESSIONS_CODEX_SESSIONS_DIR=` are blanked so the
  VM uploads only OpenClaw sessions. `device` auto-resolves to the hostname `openclaw`. Uploads
  POST to the app, which writes the batch into the Drive inbox the Dagster ingest reads — see
  [Client uploads via the app](#client-uploads-via-the-app-the-write-path-for-remote-devices).
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

To pull new code: `cd ~/dev/zachlatta/personal-data-warehouse && git pull` for the wrapper and
`pdw update` for the uploader (there is no Python environment to sync any more). Because
uploads go through the app, end-to-end also depends on the **app** (`/ingest/agent-sessions/batch`)
and the **prod Dagster** reader both running `main` (the app writes the object tags the Dagster
reader expects, and the reader carries `openclaw_event_row`). Land/deploy code on both before
relying on the timer.

## Muse (Meta's hosted personal agent)

Muse is Meta's hosted personal agent; each user gets an agent on its own VM. Everything
it keeps on that VM's disk lands in PDW: **its chat transcripts** as the seventh
agent-session source (`base_muse.events`, source `muse`, read through
`marts_ai_conversations.events` like every other provider) and **its persistent
workspace** as `base_muse.files` — `MEMORY.md` and `memory/` (what it believes about Zach
and the people around him), goal pages, feed research, podcasts, deliverables and what
Zach attached in a chat — one row per path at its latest content (`content_text` inline
for text, `storage_file_id` for a binary's bytes through `get_object`, `is_deleted = 1`
for a path that is gone). Timeline adapter `muse_file`, search scope `muse_file`.

**The VM shapes the transport.** It accepts no inbound connection, loses every process
(and everything outside `/home/hatch`, including apt packages and `/usr/local`) on
restart, and reaches the internet only through Meta's egress proxy. So the uploader runs
**on** the VM — `pdw ingest muse` (native Go, `app/internal/uploaders/muse`), with its
state in `/home/hatch/.local/state/pdw` because that is what survives — and it is
scheduled by a **Muse hook**, not cron: a hook is a Bash script the runtime itself polls,
and one that ends with `silent` never wakes the model. A Muse cron job would have been a
model turn every five minutes, each writing a new transcript for the next run to ingest.
The script is `ops/muse/pdw-ingest-hook.sh`; `ops/muse/README.md` has the install steps.
It posts `pdw heartbeat --pipeline muse`, which is the `muse` row's run signal.

**Muse writes its own loops on the user channel, so `role` is rebuilt, not copied.**
Of 485 transcripts in its first two days, 313 were self-improvement runs, 54 hourly
feed runs and ~70 subagents — every one opened by a `role: user` message Zach never
typed. Each transcript line carries the Muse `source` that wrote it; only
`source = 'runtime'` in a non-subagent session is Zach typing, and everything else is
`role = 'system'` with the loop in `subtype`. Which session is a subagent is only said
by its opening message (`[Subagent Context] … Requester agent id: <id>`), so the
uploader reads that once per transcript (plus the model from `sessions.json`) and ships
it on every line; the warehouse sets `is_sidechain = 1` and `parent_uuid` from it. With
that, the existing agent-session rules classify Muse with no Muse-specific priority SQL:
chats are `self`, loops and subagents `background`. The one Muse-specific piece is the
session row's **title**: a loop has no typed prompt and Muse writes no session title, so on
2026-09-29 730 of 745 Muse session rows on the timeline were blank. The `agent_session`
adapter now names a Muse session with neither for its loop (`MUSE_LOOP_SESSION_TITLES`,
keyed by `entrypoint`: `Muse self-improvement run`, `Muse hourly feed run`,
`Muse subagent`, ...); it never feeds the priority rules. Muse transcripts carry no token
usage at all, so `input_tokens`/`output_tokens` are 0 by fact, not by a mapping gap. Reasoning items stay in `raw_json`
only, as for every provider; Meta's own `muse.db` tool withholds them, but they are in
the transcript files on Zach's VM.

What PDW does **not** get: Muse's Postgres (feed, goals, ideas, device and health tables)
is reachable only through the agent's own read-only `muse.db` tool, not from a shell, and
its phone data (contacts, calendar, health, media library) was empty for this account on
2026-09-28. Its connectors read other systems PDW already syncs.

## Claude Desktop Sessions (claude.ai)

Captures normal Claude conversations from the Claude Desktop app so they're queryable in
the warehouse alongside the agent-CLI sources. They land in the source-owned
`base_claude_desktop.events` raw table and are also exposed through `marts_ai_conversations.events`,
`marts_ai_conversations.sessions`, and `timeline.search_text()`, normalized by
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
  (`ops.claude_desktop_conversation_state`, Postgres-durable) and ships one `conversation` header line +
  one `message` line per turn through the SAME `/ingest/agent-sessions/batch` path as the other
  agent sources. Re-shipping a whole conversation when it gains a turn is cheap (warehouse dedupes
  by `(source, session_id, event_uuid)` into `base_claude_desktop.events`).

The `sessionKey` rotates ~monthly; the desktop app refreshes it, and the clientside LaunchAgent
re-pushes it hourly so the server's copy stays fresh.

**The poller's verdict is on the credential row, and it is the only heartbeat that means
anything.** Until 2026-09-10 the row's `updated_at` was `pipeline_health`'s run heartbeat,
and the hourly push kept it fresh — so the poller failed **3,450 times** between 08-29 and
09-10 (`claude.ai returned 403`, a dead desktop login) with one identical red Dagster run
every five minutes while `marts_ops.pipeline_health` read `ok` and only
`marts_ops.mart_view_health` noticed, eleven days later, that `marts_ai_conversations` had
gone quiet. A 401/403 is now `ClaudeAiAuthError`; the poller records it on
`private.claude_desktop_credentials` (`status = action_required`, `error`,
`rejected_session_sha256`, `rejected_at` — columns `ensure_claude_desktop_tables` adds
beside the Go pusher's DDL), `pipeline_health` reads that status, and the keepalive sensor
sits out the exact rejected key for an hour per probe, resuming the moment a different key
is pushed. The repair is a human one: open the Claude Desktop app on the Mac, sign in, and
the hourly `claude-desktop-auth` LaunchAgent pushes the new key — a push that *succeeds*
with the old cookie is not evidence the cookie works, which is exactly what the 12 days of
green looked like.

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
pdw's grant survives self-updates only because release binaries are signed with the stable
identity — see
[pdw CLI Full Disk Access vs self-updates](local-uploaders.md#pdw-cli-full-disk-access-vs-self-updates-macos);
if it breaks, make sure a signed release build is installed (`pdw update --force`).

### Why the ChatGPT session expires, and the hourly auth LaunchAgent

**The ChatGPT credential dies exactly 10 days after capture, by construction.** The
`accessToken` is an RS256 JWT with `exp - iat = 864000s`, and ChatGPT mints it **only during
a browser sign-in**. Replaying the captured cookie at `/api/auth/session` returns *that same
cached token forever* - measured 2026-08-22, twelve days after capture and two days after the
token's own expiry, chatgpt.com still returned the token issued at capture time, even when the
rotated `Set-Cookie` was honored and replayed across consecutive calls. Nothing server-side
renews it. The 2026-08 cycle is the shape to recognize: published 08-10 11:17:30, 286 green
polls/day for ten days, first failure 08-20 12:20 - one sensor tick after the JWT expired.
Do not go looking for rate limits, IP binding, or a flaky cookie; check `token_expires_at`
in `private.chatgpt_sessions` first.

Two mechanisms follow from that:

- **`chatgpt-auth`, an hourly LaunchAgent** (below) re-publishes the browser's session, so the
  server's copy is never staler than the browser's. **Chrome renews the token by itself** as
  long as it stays running and signed in - porygon's Chrome re-minted at 2026-08-20 11:36:10,
  nineteen minutes after the previous token expired at 11:16:47, with nobody at the keyboard.
  Hourly re-publishing plus a permanently-running Chrome is therefore hands-off. What it cannot
  survive is Chrome being quit or signed out for ten days: the agent will then faithfully
  republish a token that is already dead.
- **An early warning instead of a silent stop.** Every successful poll writes the token's real
  expiry to `private.chatgpt_sessions.token_expires_at`; within two days of it the credential
  reads `action_required` on `/pipelines` **while polling continues** (it never sets
  `expired_at`, which is the separate "rejected, stop polling" mark). `publish-session` prints
  the same warning to stderr, so it lands in the agent's run log and in
  `bin/chatgpt-auth-status`.

**chatgpt.com is behind a Cloudflare managed challenge.** Plain `requests`/`urllib` gets a 403
with `cf-mitigated: challenge`; the identical cookie under `curl_cffi` Chrome impersonation gets
a 200 (measured from the prod host, 2026-08-22). `ChatGPTBackendClient` therefore defaults to a
`curl_cffi` session, exactly like `personal_data_warehouse_claude_desktop.api`. Never "simplify"
it back to `requests` - the symptom is an opaque `ChatGPTAuthError: session expired`, because a
403 is classified as an auth failure.

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.chatgpt-auth`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.chatgpt-auth.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.chatgpt-auth.plist`
- Wrapper script: `bin/chatgpt-auth-launchd` (sources the repo `.env`, then runs
  `pdw chatgpt publish-session --non-interactive`)
- Run cadence: every 3600 seconds with `RunAtLoad`
- Main run log: `~/Library/Logs/personal-data-warehouse/chatgpt-auth.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/chatgpt-auth.heartbeat`
- Status helper: `bin/chatgpt-auth-status`

**It runs on porygon**, whose Chrome holds the chatgpt.com login and stays running around the
clock - which is exactly what keeps the token renewing. A laptop that sleeps or quits Chrome is
a worse host, even though `publish-session` works there too (crobat's Chrome `Profile 1` also
has the login). Whichever Mac hosts it, that Mac's Chrome must stay running and signed in.

```bash
bin/chatgpt-auth-status
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.chatgpt-auth
tail -80 ~/Library/Logs/personal-data-warehouse/chatgpt-auth.run.log
pdw chatgpt publish-session --dry-run   # verify cookie decryption without publishing
```

Install / reinstall the plist after editing the template:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.chatgpt-auth.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.chatgpt-auth 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.chatgpt-auth.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.chatgpt-auth
```

The agent reads the browser's "Chrome Safe Storage" keychain item through `/usr/bin/security`,
so it needs the login keychain auto-unlocked by loginwindow **and** an ACL that says **Always
Allow** (plain "Allow" is one-shot and the next tick fails). Grant it once by running
`pdw chatgpt publish-session` from a GUI-attached terminal on that Mac and clicking Always
Allow; on porygon this grant is already in place, and the sibling `claude-desktop-auth` agent
proves the launchd session can read the keychain there. When it later fails, decode
`security`'s exit status before theorising - 36 is
"cannot prompt" and says nothing, 51 on *every* item means the login keychain password
diverged from the account password. This is a keychain problem, not TCC: TCC fails on a file
path with `Operation not permitted`.

Claude Desktop SQL starting points are `base_claude_desktop.events` for raw rows and
`marts_ai_conversations.events` / `marts_ai_conversations.sessions` filtered to
`source = 'claude_desktop'` for unified querying (one `meta` row per conversation carrying the
title/model, then `user`/`assistant` rows per turn; `session_id` is the claude.ai conversation
uuid). Free-text is in `timeline.search_text()` under `source = 'agent_session'`.

## ChatGPT (consumer) - server-side backend poll

Normal ChatGPT conversations (the consumer product, not the API) land in the source-owned
`base_chatgpt.events` raw table (`source = 'chatgpt'`), alongside the other AI conversation sources,
and roll up through `marts_ai_conversations.sessions` with free-text in `timeline.search_text()`
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
  Storage" keychain item (a one-time "allow" prompt); see `app/internal/browsersessions/chatgpt`
  (native Go — the Python `chatgpt_cookies.py` is gone). It validates the
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
  whose `update_time` is newer than the per-conversation watermark in `ops.chatgpt_conversation_sync`,
  and normalizes the message tree via `chatgpt_conversation_to_event_rows`
  (`agent_sessions_drive_ingest.py`; depth-first `seq`, `tool`/`tool_use` detection, `model_slug`,
  reasoning -> `thinking`). Re-ingest is idempotent in `base_chatgpt.events` (PK
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
starting points: `base_chatgpt.events` plus `marts_ai_conversations.events` /
`marts_ai_conversations.sessions` filtered to `source = 'chatgpt'`, and `private.chatgpt_sessions`
(credential) / `ops.chatgpt_conversation_sync` (per-conversation watermark).
