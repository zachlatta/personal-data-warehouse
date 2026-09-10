# Ingest: local uploaders and credential publishers

These commands run on the machine that holds the data or the login, and they **write to
the warehouse**. Never run one speculatively, and never from a machine that does not
have the source (the Apple sources need macOS with Full Disk Access granted to the
uploader's exec chain).

## `pdw ingest <source>`

| source | what it ships |
| --- | --- |
| `apple-messages`, `apple-notes`, `apple-contacts`, `apple-photos`, `voice-memos` | this Mac's Messages, Notes, Contacts, Photos and Voice Memos; normally run by a LaunchAgent every 5–30 minutes |
| `agent-sessions` | Claude Code, Codex, pi and OpenClaw transcripts from this machine |
| `claude-desktop` | this Mac's Claude Desktop session credential, so the server can poll claude.ai conversations |
| `manual-finance <files-or-dir>` | bank, brokerage and mortgage statements, valuation screenshots, fund documents; the folder name `<institution>-<name>-<mask>/` IS the account. `--evidence-only` for tax returns and supporting records, which are searchable but never booked into the ledger |
| `plaid link|items|update <item>|unlink <item>|sync` | link a genuinely new institution, list Items, repair an existing Item's consent, deliberately retire a duplicate, pull now |

Flags after the source are forwarded to the uploader (`--mode incremental|full`,
`--limit N`, `--dry-run` where supported); `pdw ingest <source> --help` lists them. The
uploader posts through the app's signed ingest endpoints and dedups by content sha, so a
re-run is cheap; upload success means inbox delivery, not that the row is queryable yet —
a Dagster reader promotes it within minutes.

**Plaid repair, not re-link.** `pdw ingest plaid link` is for a new institution only. To
repair an existing Item use `pdw ingest plaid items` to find it, then
`pdw ingest plaid update <item-id>`; a fresh link used as a repair can mint a second live
Item that double-counts net worth. Retire a confirmed duplicate with
`pdw ingest plaid unlink <item-id> --dry-run`, then without the flag.

## Credential publishers

| command | what it does |
| --- | --- |
| `pdw slack publish-session` | captures the Slack desktop app's client session so the sync can ask Slack what changed in one request instead of polling ~950 conversations |
| `pdw chatgpt publish-session` | captures the chatgpt.com browser session for the server-side ChatGPT poller; the token lives ~10 days and only a running, signed-in browser renews it |
| `pdw whoop publish-session` | captures the app.whoop.com browser session for the private WHOOP source; self-renews while sync runs |
| `pdw ingest claude-desktop` | pushes the Claude Desktop session credential |

Each reads a browser or app keychain item, so it must run from a **GUI terminal** on the
Mac that holds the login (SSH cannot reach the keychain), and the keychain prompt must
be answered **Always Allow** — a one-shot "Allow" makes the next scheduled run fail.
`--dry-run` verifies the capture without publishing. When a scheduled publisher fails,
`security` timing out means a keychain prompt nobody can answer (Mac locked, or a
one-shot grant), not a hung binary.

## Setup

- `pdw login` writes `~/.config/pdw/config.json` (0600); `PDW_API_URL` and
  `PDW_SECRET_TOKEN` override it; `pdw config show` prints the resolution with the token
  redacted.
- The binary self-updates in the background at most every five minutes. Leave that on.
  `pdw version` and `pdw update --check` are the first thing to run when the CLI behaves
  oddly.
- Uploaders write through the app, not to storage directly, so a device never holds the
  storage credential. Large files route over Tailscale directly to the app host when
  `PDW_INGEST_TAILSCALE_HOST` names it, lifting the public edge's 100 MiB body cap.
