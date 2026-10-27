# Local Mac uploaders and their permissions

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

## Where the Mac jobs run from (2026-10-24)

Development moved to mew-dev; porygon keeps no checkout. Each LaunchAgent in `ops/launchd/`
runs its wrapper from the **device runtime** `~/.local/share/personal-data-warehouse` on the
Mac: `bin/` (wrappers, status helpers, `_pdw-upload-lib.sh`) and a 0600 `.env` holding only
device-job settings: accounts, `PDW_INGEST_*`, `VOICE_MEMOS_*`, `SCREEN_TIMELAPSE_SOURCES`, and
`POSTGRES_DATABASE_URL` (the resident mutation workers exit 2 without it). No OAuth tokens or
API keys: those live in the dev `.env` on mew-dev and in production. Wrappers find the runtime from their own path, so
`PDW_INGEST_PROJECT_DIR` is the runtime dir. After changing a wrapper or plist, push it and
run `bin/sync-mac-device-runtime porygon` from a mew-dev checkout: it copies `bin/` and
reloads each installed LaunchAgent whose plist changed. It never writes `.env` and never
installs a new LaunchAgent. Status helpers run there too, e.g.
`ssh porygon '~/.local/share/personal-data-warehouse/bin/apple-messages-upload-status'`.

## pdw CLI Full Disk Access vs self-updates (macOS)

macOS TCC keys a Full Disk Access grant to the binary's code-signing designated
requirement. Unsigned darwin binaries only carry the Go linker's ad-hoc signature, whose
requirement is the cdhash of that exact build — so every pdw self-update used to silently
invalidate pdw's FDA grant (System Settings still showed the toggle on). Fixed by signing
release binaries with a stable identity **in the release workflow**:

- **The `pdw-cli-release.yml` build job signs both darwin binaries** with a pinned,
  sha256-verified `rcodesign` (signs Mach-O from plain PEM files on the Linux runner — no
  macOS runner, keychain, or trust settings involved), using the self-signed 100-year
  `pdw-codesign` certificate from the repo Actions secrets `PDW_CODESIGN_KEY` /
  `PDW_CODESIGN_CERT`, under the stable identifier `com.zachlatta.pdw`. The designated
  requirement — `identifier "com.zachlatta.pdw" and certificate root = H"<cert hash>"` — is
  therefore identical for every release, so TCC grants survive self-updates. Signing runs
  before packaging so `SHA256SUMS` covers the signed bytes; a release build with missing
  secrets **fails loudly** (only unreleased fork-PR dry-runs may skip signing), and
  `selfupdate/workflow_test.go` pins the whole contract.
- **Per-Mac setup is just the grant itself**: install a released binary (`pdw update
  --force` or a release tarball), then toggle pdw on once in System Settings → Privacy &
  Security → Full Disk Access. Done forever on that Mac. Granted on porygon 2026-07-14.
- **If pdw's FDA breaks anyway**, check `codesign -d --verbose=2 ~/.local/bin/pdw`: it must
  show `Identifier=com.zachlatta.pdw` and `Authority=pdw-codesign`. `Signature=adhoc` means
  a local `go build` or pre-signing binary is installed — replace it with a release
  (`pdw update --force`); the existing grant starts matching again with no new GUI toggle.
- **The signing identity must never be regenerated casually**: a new certificate is a new
  requirement, which means a new manual FDA toggle on every Mac that granted against it.
  The canonical copy lives in the GitHub Actions secrets; the original key/cert (plus the
  rcodesign used to mint them) are kept as a local backup in `~/.config/pdw/codesign/` on
  mew-dev (moved from porygon 2026-10-24). If the key is ever lost, generate a new one (openssl self-signed cert with the
  `codeSigning` EKU), update both secrets, and expect one re-toggle per Mac.

This covers every grant now, because every device-side job runs through the one signed
binary: the uploaders (`pdw ingest <source>`), the Notes/Contacts mutation workers
(`pdw mutations <provider>`), the browser-session publishers (`pdw slack|chatgpt|whoop|hn
publish-session`) and the run heartbeat (`pdw heartbeat`). The LaunchAgents used to keep
pdw OUT of their exec chain and run `uv run python` directly, precisely because an unsigned
self-update revoked the grant; with the stable signing identity that reason is gone, and so
are uv and Python from the chain (`tests/test_device_wrappers.py` pins it). The one-time cost
per Mac is re-issuing each grant to `~/.local/bin/pdw`: Full Disk Access (the uploaders),
Automation → Notes and Automation → Contacts (the mutation workers), Photos (the photos
uploader, alongside the separate PDW Photos Exporter helper grant), and the "Safe Storage"
keychain ACLs (the session publishers, Always Allow). The uv-python path-drift gotcha that
used to break these grants on every `uv` patch bump no longer applies.

## Local Apple Notes Upload Scheduler

This Mac is intended to run the local Apple Notes uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.apple-notes-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.apple-notes-upload.plist`
- Wrapper script: `bin/apple-notes-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest apple-notes --mode incremental` (native Go in the signed pdw binary; the wrapper runs nothing else — no uv, no Python)
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

The uploader only sees this Mac's local NoteStore, and macOS only pulls Notes iCloud changes
while Notes.app is running — with the app quit, the store silently freezes and the uploader
reports healthy `selected=0` runs while edits made on other devices never arrive. Each run
therefore ensures Notes.app is running (launched hidden via `open -g -j -a Notes`; see
`app/internal/uploaders/applenotes`). Set `APPLE_NOTES_OPEN_NOTES_APP=0` to disable. If apple_notes data looks
stale despite healthy runs, check the `NoteStore.sqlite-wal` mtime — days old means iCloud
delivery is stalled, not the uploader.

If the run log shows `PermissionError` or SQLite `authorization denied` for
`~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite`, the LaunchAgent is loaded
correctly but macOS Full Disk Access is blocking the background process. Grant Full Disk
Access to the executable chain used by the job: `/bin/zsh` and the `pdw` binary
(`~/.local/bin/pdw`). Nothing else is in the chain any more — no `uv`, no venv python, and
therefore no uv-python path drift to re-check. The grant survives `pdw update` because release
binaries are signed with a stable identity; if it breaks anyway, `codesign -d --verbose=2
~/.local/bin/pdw` must show `Identifier=com.zachlatta.pdw` (a local `go build` is ad-hoc signed
and does NOT inherit the grant — install a release with `pdw update --force`). Then kickstart
the LaunchAgent again.

## Local Apple Messages Upload Scheduler

This Mac is intended to run the local Apple Messages uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.apple-messages-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.apple-messages-upload.plist`
- Wrapper script: `bin/apple-messages-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest apple-messages --mode incremental` (native Go in the signed pdw binary; the wrapper runs nothing else — no uv, no Python)
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
blocking the background process. Grant Full Disk
Access to the executable chain used by the job: `/bin/zsh` and the `pdw` binary
(`~/.local/bin/pdw`). Nothing else is in the chain any more — no `uv`, no venv python, and
therefore no uv-python path drift to re-check. The grant survives `pdw update` because release
binaries are signed with a stable identity; if it breaks anyway, `codesign -d --verbose=2
~/.local/bin/pdw` must show `Identifier=com.zachlatta.pdw` (a local `go build` is ad-hoc signed
and does NOT inherit the grant — install a release with `pdw update --force`). Then kickstart
the LaunchAgent again.

### This Mac's chat.db is not the phone's

PDW sees iMessage through the uploading Mac's `chat.db`, and that Mac is its own iMessage
device: what it holds and what the iPhone shows can differ. Until 2026-10-05 the Mac received
nothing live and took every message from Messages in iCloud in one daily batch (about 19:15Z;
`ingested_at − message_at` was 8–24 h, and every row had `ck_sync_state = 1`). From 19:14Z
that day it receives iMessages directly, minutes after they are sent, and **does not upload
them to iCloud** until its own background sync runs (all 69 live-received rows in the first
16 h read `ck_sync_state = 0` with no `ck_record_id`; opening Messages.app or restarting
`imagent` does not force it). So a message that reached the Mac but not the phone — sent
while the phone was in the air, in the reported case — shows up in PDW and in PDW's push
alert but not in iMessage on the phone, and the phone cannot backfill it from iCloud. Read
the CloudKit state from the raw row:

```sql
SELECT message_rowid, message_at, is_from_me,
       raw_metadata_json::jsonb->'raw'->>'ck_sync_state' AS ck_sync_state
FROM base_apple_messages.messages
WHERE account = '<account>' AND message_at > now() - interval '2 days'
ORDER BY message_rowid DESC LIMIT 50;
```

`ck_sync_state = 0` means "only on this Mac so far", not "missing from the phone": the phone
may well have received the same message directly. PDW cannot repair delivery to the phone;
the remedy to try is Messages ▸ Settings ▸ iMessage ▸ Sync Now on the Mac (a GUI session),
which should put the Mac's copy in iCloud for the phone to pull (untested as of 2026-10-06).

Apple Messages SQL starting points are `base_apple_messages.messages`, `base_apple_messages.chats`,
`base_apple_messages.handles`, `base_apple_messages.chat_handles`,
`base_apple_messages.chat_messages`, and `base_apple_messages.attachments`, with the resolved
read view at `marts_messages.apple_messages`.

## Local Apple Contacts Upload Scheduler

This Mac is intended to run the local Apple Contacts uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.apple-contacts-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-contacts-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.apple-contacts-upload.plist`
- Wrapper script: `bin/apple-contacts-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest apple-contacts --mode incremental` (native Go in the signed pdw binary; the wrapper runs nothing else — no uv, no Python)
- Main run log: `~/Library/Logs/personal-data-warehouse/apple-contacts-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/apple-contacts-upload.heartbeat`
- Status helper: `bin/apple-contacts-upload-status`

Use these commands when inspecting or repairing it:

```bash
bin/apple-contacts-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-contacts-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-contacts-upload
tail -80 ~/Library/Logs/personal-data-warehouse/apple-contacts-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/apple-contacts-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.apple-contacts-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-contacts-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.apple-contacts-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.apple-contacts-upload
```

The uploader snapshots every `AddressBook-v22.abcddb` under
`~/Library/Application Support/AddressBook`, including local and account/iCloud stores. It sends
changed cards and tombstones through the app's `/ingest/apple-contacts/batch` endpoint. Dagster's
`apple_contacts_drive_inbox_sensor` consumes them into `base_apple_contacts.cards`; `marts_contacts.contacts`
unions active Apple and Google cards and `marts_contacts.contact_points` provides normalized phones/emails
for identity joins. `marts_messages.apple_messages` uses those points to resolve Messages senders.

If the run log shows `PermissionError` or SQLite `authorization denied` for an Address Book
store, grant Full Disk Access to `/bin/zsh` and the signed `pdw` binary (`~/.local/bin/pdw`),
then kickstart the LaunchAgent.

## Local Apple Photos Upload Scheduler

This Mac is intended to run the local Apple Photos uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.photos-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.photos-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.photos-upload.plist`
- Wrapper script: `bin/photos-upload-launchd`
- Run cadence: every 1800 seconds with `RunAtLoad`
- Command: `pdw ingest apple-photos --mode incremental --limit 100` (override the bounded-run default with `PHOTOS_UPLOAD_LIMIT`; native Go in the signed pdw binary. The wrapper used to run uv DIRECTLY because unsigned pdw self-updates invalidated the TCC grants attributed to it; release binaries are signed with a stable identity now, so pdw is the whole chain and the Full Disk Access grant for the `Photos.sqlite` snapshot moves to `~/.local/bin/pdw`, once)
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

The uploader also needs the macOS Photos privacy grant used by PhotoKit. PhotoKit runs in the
hidden native app `~/Library/Application Support/personal-data-warehouse/photos-helper/PDW Photos
Exporter.app`; every helper call goes through LaunchServices so TCC consistently attributes the
grant to `com.zachlatta.pdw.photos-exporter`. Do not replace this with a loose executable: macOS
attributes a command-line PhotoKit request to its responsible parent (Ghostty interactively,
launchd when scheduled), producing a grant that works in only one context. **A scheduled export
never raises the consent prompt**: request access with `pdw ingest apple-photos --authorize`
from a GUI session on that Mac, which must show **PDW Photos Exporter** as the requester. Grant
**Full Access** (Selected Photos is insufficient), then kickstart the LaunchAgent. The helper is
rebuilt only when its checked-in Swift source or privacy plist changes; because it is ad-hoc
signed, such a change resets the grant (the designated requirement is the build's cdhash) and
needs `--authorize` again. Until 2026-09-20 the scheduled export prompted by itself, and on a
headless Mac the rebuild from the Go port parked every export on a dialog nobody could click
until the 3600s per-file timeout — 26 timeouts, six-hour runs, and a `failing` row that took a
day to read — while `sample tccd` sat in `CFUserNotificationReceiveResponse`. Now the export
checks the status and exits with `Photos library access is not determined …`, the runner stops
the batch on that error (a `PhotosAccessError`, never backed off per file) so the run is red
within seconds, and a helper that outlives the export deadline is killed through the pid it
writes to `--pid-path` instead of being orphaned. If a run reports that
Photos access was denied or limited, repair PDW Photos Exporter in System Settings → Privacy &
Security → Photos. LaunchServices invocation is asynchronous and the uploader waits on the app's
redirected result files; do not use `open -W`, which races short-lived helper instances and can
fail with `initial call to kevent() failed: No such process` even when the app launched.

The uploader snapshots `Photos.sqlite` (never reads the live DB) for metadata and candidate
selection, but deliberately never reads `Photos Library.photoslibrary/originals` for media:
under Optimize Mac Storage that tree is only an incomplete cache. Every selected resource is
exported through PhotoKit with iCloud network access enabled, so Photos downloads the complete
original before upload. Scanner selection is limited to `ZBUNDLESCOPE = 0`: nonzero bundle scopes
are transient syndicated/shared records that Photos stores in `ZASSET` but does not expose as
user-library `PHAsset`s. The native helper fetches with `includeAllBurstAssets` +
`includeHiddenAssets`: burst-stack members (`ZVISIBILITYSTATE = 2`) and hidden assets are ordinary
rows in `Photos.sqlite` but are invisible to a default PhotoKit fetch, so without those flags they
are permanently "not available through PhotoKit". Photo and video assets request PhotoKit's
original resource type; Live Photos also request the original paired-video resource under the
still's ZUUID with `role=live_video`. A missing asset, failed iCloud download, empty export, or
size mismatch is a loud run failure that retries later—never a successful local-only coverage
count. Repeated
failures on one file back off exponentially (30 min doubling to 7 days) and are dropped from
selection while backed off, so a file PhotoKit will never export cannot consume the run's
`--limit` slots; after 5 attempts it also stops failing the run and is reported as
`deferred=`/`failed=` in the summary instead. That demotion requires an upload to have succeeded
since the streak began, so a real outage (revoked Photos access, dead network) stays loudly red
rather than going quietly green. `--retry-failed` clears every backoff for an immediate retry.
Complete bytes then go through
`POST /ingest/photos/file/resumable` + `/ingest/photos/metadata`. The app creates a
scoped Google Drive resumable session after its normal content-sha dedup check; the uploader
streams the export to Drive in 16 MiB chunks, resumes from Drive's acknowledged byte after
timeouts, and verifies Drive's final sha256 + size before uploading the envelope. Photo files
therefore have no app/Cloudflare body ceiling and never permanently defer for size. Edited
renditions are not uploaded yet (originals only; the run log counts assets with adjustments).
The scheduled 100-resource limit bounds disk/network work and still walks the backlog because
incremental state selection happens before the limit. For a manual backfill batch, use
`pdw ingest apple-photos --mode incremental --limit N`; `full` is only for intentionally
re-exporting already-complete resources.

Serverside, `photos_drive_inbox_sensor` + `photos_drive_ingest` consume the inbox into
`base_apple_photos.files`; the `photo_identity` asset dedups renditions into logical photos
(`derived_photos.assets` + `derived_photos.asset_files` link/audit rows, 256-bit dhash fingerprints in
`derived_enrichment.media_fingerprints`, 1280px JPEG thumbnails in Drive); `photo_enrichment` runs the
vision agent once per logical photo over `marts_photos.canonical_renditions`; the `photo`
timeline adapter emits one event per photo with the AI caption in `search_text`.

Photos SQL starting points are `base_apple_photos.files` (raw renditions), `derived_photos.assets` (one row
per deduplicated logical photo), `derived_photos.asset_files` (identity links + `match_method`/
`match_score` dedup audit), `marts_photos.photos` (assets + caption + rendition counts),
`marts_photos.files` (all renditions across sources), and timeline `source = 'photos'`. Free-text
search: `timeline.search_text()` with `sources => ARRAY['photo']`.

### Adding a photo source (google_photos Takeout import, manual imports, ...)

The photos pipeline is multi-source by construction; Apple Photos is just the first source.
`PHOTO_SOURCE_RELATIONS` in `src/personal_data_warehouse/relations.py` is THE extension point —
it drives Drive-ingest routing, the identity runner's scan, and the `marts_photos.files` union.
To add a source:

1. **Raw table**: add `<source>` to `SOURCE_RAW_SCHEMAS`, a `("<source>_files", "<source>",
   "files")` relation row, and a `TableSpec(PHOTO_SOURCE_FILE_COLUMNS, ...)` in `postgres.py`
   (same shared column list and provenance primary key as `apple_photos_files`), then add the
   table to `_PHOTO_TABLES` and `TIMELINE_TABLE_COVERAGE` (a `detail` of `photo_assets`).
2. **Registry**: one entry in `PHOTO_SOURCE_RELATIONS` (`"<source>": "<source>_files"`). Unknown
   sources fail loud at ingest — register before uploading.
3. **Uploader**: post the shared envelope (`app/internal/uploaders/photos/envelope.go`,
   `source="<source>"`, native id + role per file, raw record under a source-named key like
   `takeout_sidecar`) to `/ingest/photos/file/resumable` + `/ingest/photos/metadata` via
   `ingestclient.Client.UploadPhotoFile`/`UploadPhotoMetadata`. Live/motion
   components upload under the same native id with `role=live_video`; edited outputs use
   `role=edited`.
4. **Precedence**: slot the source into `PHOTO_SOURCE_PRECEDENCE`
   (`src/personal_data_warehouse/photo_identity.py`) so canonical-field resolution knows who
   wins when renditions disagree.
5. Nothing else: identity/dedup (incl. the burst guard and cross-source perceptual merge),
   thumbnails, enrichment, timeline, and search all follow automatically from the registry.

## Local Browser History Upload Scheduler

The Mac that Zach browses on runs the browser-history uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.browser-history-upload`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.browser-history-upload.plist`
- Wrapper script: `bin/browser-history-upload-launchd`; status helper `bin/browser-history-upload-status`
- Run cadence: every 900 seconds with `RunAtLoad`
- Command: `pdw ingest browser-history --mode incremental` (native Go,
  `app/internal/uploaders/browserhistory`)
- Logs: `~/Library/Logs/personal-data-warehouse/browser-history-upload.run.log` and `.heartbeat`;
  the run heartbeat posts as pipeline `browser_history`

It reads every profile of Chrome (`~/Library/Application Support/Google/Chrome/<profile>/History`),
Brave (`…/BraveSoftware/Brave-Browser/<profile>/History`) and Safari (`~/Library/Safari/History.db`
plus Safari 17 profiles under `Profiles/<uuid>/History.db`). Each database is **copied** with its
`-wal`/`-journal` sidecars to a temp directory before it is opened: Chromium holds `History` under
an exclusive lock while running, and nothing may write to a live browser store. Times convert from
Chromium's microseconds since 1601 and Safari's seconds since 2001. The state file keeps one
upload mark per (browser, profile) — the largest visit id shipped — and both browsers'
visit ids are AUTOINCREMENT, so every newer visit (including ones sync delivered from another
device) is above it; a database whose ids fall below the mark was recreated and is re-read
from the start (the warehouse upserts by `(account, device, browser, profile, visit_id)`).
History deleted in the browser is **not** deleted from the warehouse. A first run backfills
everything the browsers still hold (Chrome keeps ~90 days, Safari ~a year).

**Search terms.** Chromium's `keyword_search_terms` (omnibox searches) wins; otherwise the
query of a known search engine's results URL (Google, Bing, DuckDuckGo, Brave Search, Kagi,
Yahoo, Ecosia) fills `search_term`, which is how Safari searches are captured.

**Timeline volume.** Every visit lands in `base_browser_history.visits`, but a visit per
timeline row would put redirect hops, subframe loads, reloads and tab-switch revisits into the
BM25 index and the embedding queue. The `browser_visit` adapter therefore emits only page views:
not a redirect hop (`is_redirect = 0`), not `auto_subframe`/`manual_subframe`/`reload`, and not
a revisit of the same `url_id` in the same profile within 30 minutes of a previous visit.
`browser_search` emits the same filter for visits with a `search_term`, titled
`Searched: <term>`. Both are `self` (Zach did it). `context` is the profile's UTC day, so
`timeline.context()` on a hit shows what else was read around it.

**Full Disk Access.** Chrome and Brave need none; Safari's `History.db` does. If the run log
shows `Could not open …/Library/Safari/History.db … Grant Full Disk Access`, the Chromium
profiles were still uploaded and the run exits 1 (so `/pipelines` reads `failing`): grant Full
Disk Access to `/bin/zsh` and `~/.local/bin/pdw` (System Settings → Privacy & Security →
Full Disk Access → `+`, ⌘⇧G to type the path), then
`launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.browser-history-upload`.

Install or reinstall:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.browser-history-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.browser-history-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.browser-history-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.browser-history-upload
```

SQL starting points: `base_browser_history.visits`; timeline `source = 'browser_history'`
(`kind` `visit` or `search`); search scope `sources => ARRAY['browser_history']`.

## Local Screen Timelapse Upload Scheduler

The screen timelapse is a periodic image of a device's whole screen, about once a minute while
it is unlocked and in use, plus the accessibility tree of the same moment when the capture loop
could read one. The **capture** belongs to the sysadmin repository
(`scripts/periodic-screenshot-*` for the Mac, `crobat/phone-capture/` for the iPhone,
`scripts/pixel-screen-timelapse-*` for the Android phone); this repository only **uploads**
what those loops wrote:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.screen-timelapse-upload`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.screen-timelapse-upload.plist`
- Wrapper script: `bin/screen-timelapse-upload-launchd`; status helper `bin/screen-timelapse-upload-status`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest screen-timelapse` (native Go, `app/internal/uploaders/screentimelapse`)
- Logs: `~/Library/Logs/personal-data-warehouse/screen-timelapse-upload.run.log` and `.heartbeat`;
  the run heartbeat posts as pipeline `screen_timelapse`

**Which folder is which device.** `SCREEN_TIMELAPSE_SOURCES` in the repo `.env` is a list of
`device=folder` pairs; the device is the screen's, never the uploading host's, because the Mac
that images the phone uploads the phone's frames too. On crobat:
`SCREEN_TIMELAPSE_SOURCES=crobat=~/Pictures/Screenshots/periodic,joltik=~/Pictures/PhoneScreenshots/joltik`.
On porygon (it imaged the iPhone on the office network, and pulls the Android phone's frames):
`joltik=~/Pictures/PhoneScreenshots/joltik,pichu=~/Pictures/PhoneScreenshots/pichu`.
Unset, it is `<short hostname>=~/Pictures/Screenshots/periodic`.

**What a file is.** The name is all the metadata (`app/internal/timelapsefiles`):
`periodic-screenshot-<stamp>-display-<n>.heic` (a pre-2026-05-23 single-display frame has no
`-display-<n>`), `periodic-screenshot-<stamp>.ax.json.gz`, `phone-screenshot-<stamp>.png` and
`phone-ax-<stamp>.json` for the iPhone, and `phone-screenshot-<stamp>.avif` and
`phone-ax-<stamp>.xml.gz` (gzipped `uiautomator dump`) for the Android phone. The stamp is the
capture host's local wall time; the uploader resolves it in the host's zone, and in the repeated
fall-back hour picks the instant nearest the file's mtime (birth time reads 1969 on these files).
A stamp may end in its UTC offset (`2026-10-23_21-09-00-0400`), and then it names exactly that
instant whatever the uploader's zone: the Android phone writes one because porygon uploads its
frames, and the phone travels while porygon does not. The app refuses a `captured_at` that
disagrees with an offset stamp. A screenshot and the tree of
its moment share the stamp, so they share `(device, captured_at)`, which is the whole link
between the two tables. Anything else in the folder (`.tmp`, a not-yet-gzipped `.ax.json`) is
ignored, and a file younger than two minutes waits for the next run.

**One request per file, and no inbox.** Each file goes to `/ingest/screen-timelapse/file`
exactly as written, except for the card guard below. The app stores the bytes under `screen-timelapse/library/<device>/<UTC day>/` and writes
the file's row before it answers, so nothing has to promote it; it also provisions the two
tables itself (identical to the Python `TableSpec`, pinned by
`tests/test_screen_timelapse_store.py`). Dedup is the row: a resend of a moment whose row has
the same sha is answered from Postgres without touching Drive, and a new file is stored with the
Drive per-upload dedup search skipped. A store that succeeds with a failed row write answers an
error and the file is sent again (worst case an orphaned Drive object, never a row without
bytes).

**A moment goes as a whole, after its tree is checked.** The uploader groups files by moment
(device plus the stamp's instant: the tree is display 0, screenshots 1..n) and judges a moment
only once its tree has settled. The Mac gzips its tree in place *after* renaming the image into
place, so a screenshot can settle first; it is held (`held=` in the run line) until the tree
settles, or for ten minutes past the settle age when the moment has no tree, and is then sent
unchecked. Before anything of the moment is sent, the tree is scanned for payment card numbers
(13-19 digits, unbroken or grouped 4-4-4-4 / 4-6-5 / 4-4-4-4-3 / 4-3-3-3, passing Luhn with a
Visa, Mastercard, Amex or Discover prefix valid for the length; a run right after `/` is a URL
id, not a card). On a hit:

- the tree is sent with each number replaced by `[redacted card number]`;
- a screenshot is boxed only when the hit can be placed on it with certainty: it is matched to
  a screen **by pixel size** (the Mac's tree `displays` in points at 1x/2x/3x, the Android
  dump's top-level bounds at 1x), never by `display-N` order, and every image of the moment
  must fit exactly one screen. It is sent with solid red over the number and the card's other
  fields near it (expiry, CVC, cardholder, billing address), redrawn in its own format (PNG in
  Go, HEIC and AVIF through macOS `sips`);
- every other screenshot of the moment is **withheld**: never sent, recorded in the ledger as
  `withheld: card number in accessibility tree` at its size and mtime (`withheld=` in the run
  line). The tree only sees the frontmost app, and on 2026-10-21 the full card was on the
  display the tree did not cover. If two displays share a size, a hit has no frame (the
  iPhone's trees) or the tree cannot be read, every screenshot of the moment is withheld.

The log names each redacted or withheld path and the reason, never the digits. A withheld file
that is later edited (blacked out by hand) is sent as it then is, without a new scan: the edit is
the review. A redaction failure (`sips` missing, a frame that will not decode) fails the run
before any of the moment is sent. The guard covers new uploads only; copies already in the
warehouse are removed with the `screen_timelapse.purge_moment` mutation.

**Ledger and backlog.** The state file remembers each acknowledged file's path, size and mtime;
a changed file is sent again and replaces its row. A run sends at most 300 files, newest first, cut between moments,
so a first run's backlog (months of frames) drains behind the live minute over successive runs.
A run stops at its first failed upload and exits 1, so `/pipelines` reads `failing`. The usual
network guard applies (`SCREEN_TIMELAPSE_UPLOAD_*`, falling back to the voice-memos settings):
nothing uploads while tethered to a phone or on in-flight Wi-Fi.

`pdw ingest screen-timelapse --dry-run` lists what would be sent and records nothing.

Install or reinstall:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.screen-timelapse-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.screen-timelapse-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.screen-timelapse-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.screen-timelapse-upload
```

**The Android phone (`pichu`) captures and uploads itself.** A root Magisk module on the phone
writes a PNG and the tree about once a minute while the screen is on and past the keyguard.
Until 2026-10-27 `porygon`'s `com.zrl.pixel-screen-timelapse-pull` copied them over Tailscale
SSH only while the phone's default network was Wi-Fi, which left 822 frames waiting on the
phone for 39 hours while on cellular (2026-10-25..27) and left `screen_timelapse` green on
`/pipelines` because `crobat` kept uploading. Now `pdw ingest screen-timelapse` runs on the
phone every five minutes (`ops/android/pdw-screen-timelapse/service.sh` →
`bin/screen-timelapse-upload-android`, heartbeat `screen_timelapse` on `pichu`): after the card
guard scans the `.xml.gz` tree and boxes out any payment card numbers in red on the PNG,
`--compress-png` re-encodes each PNG in memory to full-resolution (`2410×1080`) lossy WebP
(`--quality 75`, `--max-edge 0` by default) through Android's native Skia encoder
(`app/internal/uploaders/screentimelapse/webp_android.jar`, built reproducibly by
`ops/android/pdw-screen-timelapse/build.sh` from `PdwPngCompress.java`, with a pure-Go JPEG
fallback off-Android), cutting an average 340 KB PNG to ~37 KB (9.2× smaller, ~10–18 MB/day,
~0.3% battery/day), uploads it with the `.webp` filename and `image/webp` MIME type directly to
`POST /ingest/screen-timelapse`, and deletes the local PNG and tree once acknowledged
(`--delete-after-upload`; a screenshot withheld by the card guard is never sent or deleted).
Pass `--wifi-only` (or `SCREEN_TIMELAPSE_WIFI_ONLY=1` in the phone's `.env`) to defer uploads
on cellular (`dumpsys connectivity` active default network). Setup, switches and rollback:
`pichu/2026-10-23-screen-timelapse.md` in the sysadmin repository.

**The tree is also text in the warehouse.** The app writes each tree's text, decompressed, into
`base_screen_timelapse.accessibility_trees.content_text` in the same request (NUL and invalid
UTF-8 replaced, capped at 8 MiB; an undecodable gzip leaves it empty and the bytes stay the
record). Trees stored before 2026-10-24 are filled from the object store by the enrichment
asset below, 500 a run, newest first.

**What was on the screen.** The Dagster asset `screen_timelapse_enrichment`
(`src/personal_data_warehouse/screen_timelapse_enrichment.py`; hourly at :17 plus a backlog
sensor every 2 minutes, 30 agent runs a run) has an agent read each frame captured **or
ingested** in the last `SCREEN_TIMELAPSE_ENRICHMENT_LOOKBACK_HOURS` (24 by default, newest
first — `captured_at >= since OR ingested_at >= since`, so frames delayed on a phone past 24h
are still described when they land) and write one row per frame and prompt version to
`derived_screen_timelapse.descriptions`: `screen_state` (`active`, `locked`, `off`), `app`,
`window_title`, `summary`, `screen_text`, and `evidence`, which says whether the answer came
from the accessibility tree, the screenshot, or both. The agent reads the tree first and opens
the screenshot only when the tree does not say what is on screen: on the first samples a Mac
tree named the DataGrip query and its error, while the Pixel's tree of an open Superhuman email
held the buttons and the sender but none of the email. A frame without a tree whose bytes match
a frame already read reuses that answer (`is_reused`), so an asleep iPhone's identical black
frame costs one run, not one a minute. A tree that lands after its frame was read, or a new
`SCREEN_DESCRIPTION_PROMPT_VERSION`, reads the frame again (re-encoding an already-described
frame's image bytes does not); the older answer stays in the table and in the mart until the
new one completes. Model and effort: the fleet model at `low` effort unless
`SCREEN_TIMELAPSE_ENRICHMENT_MODEL` / `SCREEN_TIMELAPSE_ENRICHMENT_REASONING_EFFORT` say
otherwise; `SCREEN_TIMELAPSE_ENRICHMENT_ENABLED=0` pauses it. To backfill history, raise the
lookback.

SQL starting points: `marts_screen_timelapse.active_screens` (one row per frame an agent judged
in use, with `storage_file_id` for `get_object` and `accessibility_tree_storage_file_id`),
`base_screen_timelapse.screenshots` and `base_screen_timelapse.accessibility_trees` (join on
`device, captured_at`); timeline `source = 'screen_timelapse'`, kind `screen_capture`, one
`noise` event per moment (display 1) with no search text yet: the descriptions do not reach the
timeline.
