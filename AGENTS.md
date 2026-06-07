# Agent Notes

## Commit and Push Safety

Before committing or pushing, review the complete staged diff line by line for secrets,
credentials, tokens, private URLs, personal data, generated artifacts, and anything else
that should not be public. If there is even a smidgen of doubt about whether a change is
safe to commit or push, stop and check with Zach before proceeding.

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
the tailnet. This dev machine, `crobat`, is on the tailnet and has access.
Before assuming you can use it, confirm you are actually on `crobat` by
running `hostname` (or `scutil --get LocalHostName`) and checking the output
equals `crobat`. If you are anywhere else, stop and ask Zach instead of
guessing.

Once you have confirmed you are on `crobat`, useful starting points:

```bash
# Recent app/container logs for the Coolify deployment.
~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs \
  --format-logs --since 1h '{job="coolify",server="rotom"}'

# Filter to a specific container, e.g. the Dagster app.
~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs \
  --format-logs --since 1h \
  '{job="coolify",server="rotom"} | json | container_name =~ "(?i).*dagster.*"'

# Host-level system logs for rotom itself.
~/dev/zachlatta/sysadmin/scripts/coolify-and-server-loki-logs \
  --format-logs --since 1h '{job="machine",server="rotom"}'
```

See `~/dev/zachlatta/sysadmin/README.md` and the script's `--help` for the
full set of selectors and flags.

## Local Voice Memos Upload Scheduler

This Mac is intended to run the local Voice Memos uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.voice-memos-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist`
- Wrapper script: `bin/voice-memos-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `/opt/homebrew/bin/uv run personal-data-warehouse-voice-memos-upload --mode incremental`
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
Access to the executable chain used by the job, especially `/bin/zsh`, `/opt/homebrew/bin/uv`, and
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
- Command: `/opt/homebrew/bin/uv run personal-data-warehouse-apple-notes-upload --mode incremental`
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
the executable chain used by the job, especially `/bin/zsh`, `/opt/homebrew/bin/uv`, and
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
- Command: `/opt/homebrew/bin/uv run personal-data-warehouse-apple-messages-upload --mode incremental`
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
especially `/bin/zsh`, `/opt/homebrew/bin/uv`, and
`/Users/zrl/dev/zachlatta/personal-data-warehouse/.venv/bin/python3`. Its current real path is
`/Users/zrl/.local/share/uv/python/cpython-3.12.12-macos-aarch64-none/bin/python3.12`. Then
kickstart the LaunchAgent again.

Apple Messages SQL starting points are `apple_messages`, `apple_message_chats`,
`apple_message_handles`, `apple_message_chat_handles`, `apple_message_chat_messages`, and
`apple_message_attachments`.
