# Local Mac uploaders and their permissions

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

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
  porygon. If the key is ever lost, generate a new one (openssl self-signed cert with the
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
