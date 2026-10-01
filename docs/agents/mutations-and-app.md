# Reviewed writes, the iOS app and push

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

## iOS app and push notifications

`mobile/` is an Expo app over the app's own HTTP API — the timeline, the mutation
review queue, and push notifications; see `mobile/README.md`. Three server pieces
back it, all behind the static bearer the CLI uses:

- `GET|POST /api/mutations/requests…` (`app/internal/mutations/api.go`) is THE review
  surface: list/get, approve, deny, drop one email, edit an email before approval
  (`…/mutations/<id>/update-email`), and mark a dead request superseded
  (`…/supersede`, offered when `can_supersede`). Every `gmail.send_email` mutation
  carries an `email` view-model (delivery mode, variants with the selected one marked,
  each body split into editable part / signature / quoted thread, the reply thread) so
  no client re-derives it. Each part is carried as HTML for the web's contenteditable
  and as plain text (`editor_text`, `signature_text`, `quoted_text`) for the phone's
  TextInput; both clients reassemble editor + signature + quote in that order, which
  is the seam the server splits on next time. The actor is `app:<client_name>`. It executes nothing.
  **Agents get one control over a request after proposing it, and it is not editing.** Since
  2026-09-24 `withdraw_mutation` (reason required) moves a *pending* request to `withdrawn`, and
  `propose_mutation` with `replaces_request_id` + `replaces_reason` withdraws the request it
  corrects in the same transaction as the corrected proposal, linking the two both ways
  (`superseded_by` / `replaces`). The design was chosen over agent-editable proposals on the
  production record: every agent-side correction in 255 requests was a whole-request
  replacement or a proposal gone moot (eleven inbox-cleanup v1 requests hand-denied with a typed
  "superseded by v2", six 5,000-thread batches denied "outdated" after nine days, "already
  sent", "did it manually", "too late"), while the one same-request tweak that recurs — a
  footer, a recipient — is what the reviewer's own edit-before-approve already does (42
  `mutation_edited` events). An in-place agent edit would also put a payload under the feet of
  a reviewer reading it; a replacement is a new row the reviewer reads fresh, and the old row is
  never rewritten, so what a human approves is exactly what the agent proposed. The
  duplicate-send hazard it closes is the 2026-08-14 calendar batch: the agent proposed a
  replacement and wrote "do not approve the earlier proposal", the earlier one had already been
  approved, both ran, and a third request deleted the duplicates. Now a replacement of an
  approved, executing, or finished request is refused with its status and nothing is created;
  the race between an approval and a withdrawal is the same `FOR UPDATE` row lock, so exactly
  one wins; and once a reviewer has edited a request its `revision` moves and the agent must
  pass the revision it read (`replaces_revision` / `expected_revision`) or be refused, so a
  version a human is still changing is never taken back blind. `withdrawn` is terminal and
  distinct from `rejected` (a reviewer's decision, kept as that record); the actor is the
  bearer's client name (`requested_by` / `withdrawn_by`, the `pdw login` client or the
  connector's `client_name`, "mcp" when unknown), the reason lives in `error` beside a denial's,
  and the request event ledger carries `withdrawn` / `replaces` / `superseded` with actor and
  revision. The three columns (`replaces_request_id`, `withdrawn_by`, `withdrawn_at`) are declared
  in both ensure paths and pinned by tests on each side; the worker never claims a withdrawn row.
  **The browser UI is a client of this same API**: `/mutation-review`, `/timeline` and
  `/search` are one static single-page app (`app/internal/webapp`, ES modules embedded in
  the binary, no build step, no CDN) that renders nothing server-side and authenticates
  with `PDW_SECRET_TOKEN` exactly like the phone (`Bearer web:<token>`). The old
  server-rendered review pages and their password cookie are gone; a still-set
  `PDW_MUTATION_UI_*` variable is reported as deprecated at startup.
- **A batch is reviewed as the thing it is about, not as its payload.** A request is
  n mutations, and n is routinely in the hundreds — 43 Gmail threads, 277 Slack
  conversations — so a review that renders each mutation's JSON is a review that gets
  approved unread. The preview the API already returns is what makes that avoidable, and
  each surface renders it in the shape of its source:
  - **Gmail thread batches** (`gmail.archive_threads` / `unarchive_threads` /
    `modify_thread_labels`) read as an inbox: sender, subject, snippet, time, one row per
    thread, grouped by the day it last moved (the reader's LOCAL day — keying on the UTC
    prefix of the timestamp splits one evening into two sections and labels both the
    same), with chips for unread / automated / kept and a filter box past eight threads.
    Opening a row shows the messages themselves. **The sender's display name is not a
    column**: `base_gmail.messages` stores only the bare address, and the review of an
    archive batch is a column of `no-reply@t.<brand>.com` without it, so the preview
    query lifts the `From` header out of `payload_json`
    (`gmailHeaderDisplayName`, bounded to the threads already joined) and both clients
    prefer it over the address-derived guess. Measured 2026-09-01 on the 43-thread
    production batch, with the columns actually materialized rather than counted (a
    `count(*)` wrapper lets the planner delete the expression and report it as free):
    0.22s -> 0.30s for the whole preview query.
  - **Slack mark-read batches** carry the actor's profile picture (Slack keeps it in the
    user payload, not a column: `base_slack.users.raw_json -> profile -> image_192`) and
    a permalink on every message and every row, because the honest answer to "mark this
    read?" is often "let me reply first". A face identifies a DM row; a channel keeps its
    `#`/`◈` glyph, which is what tells public from private at a glance.
  - **The Slack conversation context is snapshotted at PROPOSAL time and never re-read,
    so anything derived from it has to be resolved on READ or it never reaches a request
    already in the queue.** Only the Gmail previews are re-enriched in `GetRequest`;
    `enrichSlackMarkReadPreviews` runs once, in `CreateRequest`, because re-reading a
    277-conversation batch is ~1,100 queries on one page load. Faces and links shipped
    into the creation path first and were invisible in production on the pending
    277-conversation request — the only tell was that `team_domain` was absent, since
    every other key had been stored hours earlier. `hydrateSlackMarkReadPreviewLinks`
    now resolves both on read from **two** bounded lookups whatever the batch size (575
    distinct speakers in that request; ~0.5s for the avatar query on production), and
    overwrites rather than fills, because an avatar URL goes stale the moment somebody
    changes their picture. The rule it leaves behind: anything cheap and derivable
    belongs on the read path, and only the expensive conversation walk belongs in the
    snapshot.
  - **One mutation can be dropped from a batch without denying it** —
    `POST …/mutations/<id>/remove`, which is operation-agnostic and has always been; the
    phone offers it per thread as "Keep this in the inbox", and the approve button then
    counts what will still run rather than the request's original size.
  - **The phone reviews a queue, and every word of a decision names its effect.**
    Measured from a screen recording on 2026-09-29: three one-email requests took ~10s
    each, ~3s of it tapping back, waiting on the list and re-opening, and the confirm
    read "1 mutation will run upstream" on all three. Now a decision opens the next
    pending request (`mobile/src/lib/review-queue.ts`, title "3 to review", Skip in the
    header, the next one prefetched) and leaves a one-line note ("Sent · …"); and the
    buttons, confirm and note come from `requestDecision` in `mutation-review.ts` —
    "Send" / "Don't send" and "Send to front@…?" for one email, "Archive" / "Keep in
    inbox" for one thread. An email reply shows the message it answers first, with
    its quoted history folded.
  - **An email's paragraphs survive a phone edit, in Gmail's own shape.** Gmail's
    composer writes one `<div>` per line and `<div><br></div>` per blank line (checked
    against handwritten sent mail, 2026-09-29). Until then `editor_text` came from
    `htmlFragmentText`, which drops blank lines, and the phone rebuilt an edit as one
    `<div>` joined by `<br>`: two replies edited on the phone that day went out as one
    run-on block while every unedited `<p>`-paragraphed proposal was fine.
    `htmlEmailText` (Go, mirrored in `mutation-review.ts`) keeps the paragraphs, and
    `emailPlainTextToHTML` on both sides writes the Gmail shape;
    `TestEmailTextSurvivesARoundTripThroughHTML` pins the round trip.
  - **The editor is open, the keyboard is not.** In the same recording the keyboard
    rose unasked in five of nine emails: a touch that stops a moving page lands
    natively on the text view. `scroll-lock.ts` makes the inputs unfocusable while the
    page moves and for 350ms after it rests; a tap on still text still puts the cursor
    where it lands. Edits on screen are saved on the way to sending ("Save & send"),
    because approval runs the stored version, never the screen's — the old "save your
    edits first" refusal cost five taps and a hunt under the keyboard for Save.
  - **The Slack URL shape lives in `app/internal/deeplink`**, used by both
    `timeline_links.go` and the mutation preview, so a permalink cannot drift between
    them. The part that drifts silently is the thread query string: without
    `?thread_ts=…&cid=…` Slack opens the channel at the parent and the reply is nowhere
    on screen.
  - **Calendar create-event reviews are calendars, not payloads.** `GetRequest`
    resolves the proposal in its named IANA timezone and date-bounds one read of
    every synced `base_google_calendar.events` calendar for that account. The
    phone places the proposal and existing timed/all-day events on one day grid,
    flags opaque overlaps (excluding cancelled, transparent, and owner-declined
    events), and renders the complete proposed invite. This enrichment stays on
    the READ path so an old or pending request sees calendar changes that landed
    after proposal; lookup failure is non-fatal and must render as availability
    unknown, never as a clear slot. All-day overlap uses `start_date`/`end_date`,
    not the UTC-midnight `start_at` values, which otherwise leak tomorrow into
    today west of UTC. The read also applies the calendar adapter's bytewise
    latest-row de-duplication and draws expanded recurring instances instead of
    placing the series master on top of its first occurrence.
- `POST /api/push/register` stores the device's Expo push token in
  `private.push_devices` (`app/internal/push`); `POST /api/push/test` sends to every
  active device and returns the fan-out report.
- **A timeline alert is stacked on the phone by the conversation, not by the source.**
  iOS groups notifications by `thread_id`, and until 2026-09-26 every timeline alert
  carried `timeline:<source>`, so every Slack channel, DM and group DM piled into one
  unreviewable stack. `notificationThreadID` (`timeline_notifications.go`) now keys a
  Slack alert by `team_id:conversation_id`, Gmail by the thread (the sender when there is
  none), iMessage and WhatsApp by the chat, Drive by the file, and everything else by the
  row's `context` stream, falling back to the bare source only when there is nothing
  finer. The Gmail thread reaches the outbox because the capture trigger
  (`notifications.py`) copies `thread_id` beside `thread_ts` and `chat_id` into the
  payload's `metadata`; a key the trigger does not copy cannot group anything.
- A request landing in `pending_review` fires `mutations.Config.RequestCreated`, which
  the server wires to the push notifier. Delivery is asynchronous and bounded; a
  `DeviceNotRegistered` ticket flips that row to `disabled` with the reason, so an
  unreachable phone is a fact in the table rather than a quietly shrinking fan-out.
  **The alert carries the request** (`data.request`, the same JSON `GET
  …/requests/<id>` returns), so the phone renders the review the moment the alert
  is tapped and on no network at all; the phone's `mutation-cache.ts` is seeded from
  the alert and every API read, and the screens paint from it before they fetch.
  APNs caps a payload at 4 KB (`push.MaxMessageBytes`, budgeted with
  `push.MessageSize`), so a request that does not fit ships its header with
  `partial: true` and the phone loads the mutations; one whose header alone does
  not fit ships only `request_id`, as before.
- **The request list carries no `context` or `result`.** Both are review detail —
  an agent's whole proposal snapshot, a status per mutation — and neither client's
  list rendered them, yet on 2026-09-22 they were 95% of a 1.3 MB / 1.5 s
  `GET /api/mutations/requests?limit=200` (the same 1.3 MB at `limit=50`, because
  a handful of observed requests carried the bulk). The list query does not read
  the columns at all; `GET …/requests/<id>` still returns both. The web review
  keeps an in-tab `RequestCache` so the list and a request already read paint from
  memory while the fetch confirms them, and the shell `modulepreload`s the module
  graph so the SPA's imports are one round trip rather than one per depth.

**Every timeline row carries its deep link, and the detail carries the conversation.**
`GET /api/timeline`, `/api/timeline/item` and `/api/timeline/item/context` attach
`open: {url, label, app_url?}` to each row (`app/internal/server/timeline_links.go`),
computed from `adapter` + `source_pk` + `metadata` alone so the list pays no extra query:
a Slack permalink (`<domain>.slack.com/archives/<conv>/p<ts>`, with `thread_ts`/`cid` for
a reply, domain from `base_slack.teams`), Gmail as Superhuman's `/<account>/thread/<thread_id>`,
Calendar's base64 `eid`, Drive `open?id=`, Google Contacts, ChatGPT/claude.ai conversations,
`wa.me` for a 1:1 WhatsApp chat, `imessage:`/`sms:` for a 1:1 iMessage chat, and the
Notes `showNote?identifier=` scheme. `app_url` is the native scheme a phone tries first
and falls back from — the iOS app never calls `canOpenURL`, which would need every scheme
declared in the native Info.plist. A source with no honest URL (health, photos, voice memos,
a group chat, a CLI agent transcript) gets **no** link rather than a guessed one.
`/api/timeline/item` also inlines `context` — `timeline.context(ref, 15, 15)`, the same
function agents call — with `is_anchor` on the row asked about; both UIs render it as a
scrollable transcript with earlier/later controls up to the function's 50-a-side cap.

Push is delivered through the Expo push service (`exp.host`), not APNs directly.
`PDW_EXPO_ACCESS_TOKEN` is optional on the app deployment. A simulator cannot
receive push; the app reports `unsupported` there and everything else still works.

**Notifications are rich, and the server is where their UX is iterated.**
`push.Notification` (`app/internal/push/notifier.go`) carries `subtitle`, an https
`image_url` (or `image_storage_file_id`, signed into an `/objects/` link), a `category`
for action buttons, `route`, `thread_id`, `collapse_id`, `interruption_level`, `badge`
and `sound`, and `Validate()` refuses what iOS or Expo would otherwise drop silently — an
unknown category shows no buttons, an `http` image is blocked by App Transport Security
in the extension. Three senders share it: the `notify` tool (`pdw call notify --data
'{…}'`, also on MCP), `POST /api/push/send` (same JSON, for curl), and the mutation hook,
whose alert is now `mutation_review` with Approve / Deny / Review buttons that act without
opening the app. Categories live in `app/internal/push/categories.go` and are published
at `GET /api/push/categories`; the app registers them on launch, so a new button is a Go
edit — only handling a *new action id* needs `mobile/src/lib/push.ts`. Images on iOS need
the Notification Service Extension target (`mobile/targets/notification-service`, added
at prebuild by `@bacons/apple-targets`), which reads Expo's `body._richContent.image`
from the payload; a build without it shows the same push as text, and changing it is a
native build, not an OTA update. Settings → "Send test push" sends an image + subtitle +
Open button, which is how a phone proves the extension is installed.

## Writing to Apple Notes (apple_notes mutations)

Apple Notes is the first **write-back** source that reviewed mutations reach. Everything
else `propose_mutation` supports — Gmail, Calendar, Contacts — has a server API the cloud
worker can call. Notes has none: iCloud publishes no write endpoint, and the desktop app
keeps note bodies as gzipped protobuf inside a Core Data store whose decryption key and
auth token sit behind OpenAI-style team-scoped entitlements. **The only supported way to
change a note is to ask Notes.app itself, on a Mac that is signed in.** So the proposal
and review halves live with every other mutation type, and the executor is local.

```
agent  → propose_mutation apple_notes.create_note / apple_notes.update_note
       → ops.upstream_mutation_operations, status pending_review
human  → /mutation-review approves it
Mac    → the apple-notes uploader run claims provider apple_notes and applies it
         through Notes.app over AppleScript
Mac    → the same run's upload stage ships the changed note back
warehouse → base_apple_notes.notes, then timeline.events
```

The round trip closes inside one uploader run because mutations are applied **before** the
scan, not after: an approved edit reaches the warehouse in that cycle instead of waiting
five minutes for the next one.

### The two operations

- `apple_notes.create_note` — `folder` (default `PDW Agent`, created if missing), optional
  `name`, required `body`.
- `apple_notes.update_note` — `note_id` plus any of `name`, `body`, `append_body`.

**`body` and `append_body` are not interchangeable and the difference is destructive.**
`body` replaces the entire note; `append_body` leaves it alone and adds to the end. They
are rejected together at proposal time. Prefer `append_body`: the executor cannot tell an
intentional rewrite from a stale read, so it records the pre-edit body in the mutation's
`result_json.previous_body` — that is a recovery path, not a guard.

**A note has two identifiers and they look nothing alike.** Notes' AppleScript `id` is
`x-coredata://<store-uuid>/ICNote/p<Z_PK>`; `base_apple_notes.notes.note_id` is the store's
ZIDENTIFIER, a bare UUID. An agent can only discover the second one, so the executor
accepts either and resolves a UUID through a snapshot of the local store. Requiring the
Core Data form would have meant the one id a proposal can find is the one the executor
rejects.

**Title is the first line, not the `name` property.** Notes recomputes a note's name from
its body, so setting `name` alone does not survive; the executor promotes it into a leading
heading and, on update, rewrites the existing first line.

### Why the cloud worker must not claim these

`LOCAL_ONLY_MUTATION_PROVIDERS` in `defs/upstream_mutations.py` excludes `apple_notes` from
both the sensor's count and the worker's claim. Without that exclusion the cloud worker
claims the row, fails it as unknown-provider, and bumps `attempt_count` every ten seconds
while the Mac that could have applied it never sees an approved row. Any future source
whose upstream is a local app belongs in that tuple **and** needs a local worker, or its
rows sit approved forever with nothing reporting that they are stuck.

A create is also never reclaimed from a stale `executing` claim: replaying it makes a
second note. Only `apple_notes.update_note` is in the reclaim set.

### macOS Automation permission — the part that will break

The executor sends AppleEvents, so the calling chain needs **Automation → Notes**, which is
separate from the Full Disk Access the uploaders already hold. Three things learned the
hard way on porygon, 2026-08-24:

- **An unanswered prompt wedges the whole machine's AppleEvents, and it does not look like
  a permission problem.** `tccd` blocks in `CFUserNotificationReceiveResponse` waiting for a
  click nobody will make on an unattended Mac, and every AppleEvent to a *TCC-protected*
  app (Notes, Contacts, Calendar, Reminders) then hangs to `-1712 AppleEvent timed out` —
  while unprotected apps (TextEdit, Music) answer instantly, which is what makes it read as
  "Notes is broken" rather than "consent is pending". Restarting Notes does not clear it;
  the pending dialog does. Diagnose with
  `sample $(pgrep -f 'tccd$') 1 -mayDie -f /tmp/t.txt` and grep for
  `CFUserNotificationReceiveResponse`.
- **A grant row with `flags = 1` is a pending prompt, not an allow.** Flipping `auth_value`
  to 2 while leaving `flags = 1` re-prompts forever. A working row reads
  `auth_value = 2, auth_reason = 3, flags = NULL`. The user TCC database is readable and
  writable only from a chain that already holds Full Disk Access — in practice a LaunchAgent,
  not an SSH shell — and `tccd` caches, so a change needs `kill -9 $(pgrep -f 'tccd$')`.
- **TCC attributes the event to the `pdw` binary.** The chain is
  launchd → `/bin/zsh` → `pdw` → `osascript`, so the Automation → Notes grant lands on
  `~/.local/bin/pdw`. It used to land on `uv` at its versioned Cellar path
  (`/opt/homebrew/Cellar/uv/<version>/bin/uv`), which **drifted on every uv upgrade** and made
  a working executor start returning `blocked_missing_credentials` after a `brew upgrade` with
  no code change; release binaries are signed with a stable identity now, so the grant
  survives `pdw update`, and the move to the Go worker needs the grant re-issued ONCE per
  Mac. The executor classifies `-1743` as `blocked_missing_credentials` rather than a failure
  precisely so this reads as "a human must re-grant on that Mac".

The resident worker is `pdw mutations apple-notes` (`app/internal/mutationworkers/applenotes`,
run by the `apple-notes-mutation-worker` LaunchAgent through `bin/apple-notes-mutation-worker-launchd`);
`--once` applies one batch and exits, which is what the uploader's pre-scan pass does.
`APPLE_NOTES_MUTATIONS_ENABLED=0` pauses the local worker without touching the uploader.
`pdw ingest apple-notes --mutations-only` applies approved mutations and skips the upload;
`--no-mutations` does the reverse.

SQL starting points: `base_apple_notes.notes` for the resulting note, and
`ops.upstream_mutation_operations` for the mutation's own status, result and error.

## Writing to Apple Contacts (apple_contacts mutations)

Apple (iCloud) Contacts is the second write-back source, in exactly the Apple Notes shape:
iCloud publishes no write API, so the proposal and review halves live with every other
mutation type (`app/internal/mutations/apple_contacts.go`) and the executor is local —
`app/internal/mutationworkers/applecontacts` (native Go in the pdw binary) driven by
Contacts.app over AppleScript, claimed by `pdw mutations apple-contacts` (a resident
LaunchAgent, `com.zachlatta.personal-data-warehouse.apple-contacts-mutation-worker`, through
`bin/apple-contacts-mutation-worker-launchd`, plus the five-minute uploader as fallback, which
applies approved rows **before** it scans so the changed card reaches
`base_apple_contacts.cards` in the same cycle). `apple_contacts` sits in
`LOCAL_ONLY_MUTATION_PROVIDERS` (`defs/upstream_mutations.py`) so the cloud worker never
claims it. The AppleScript plumbing both executors share (string quoting, `osascript`
execution, the `-1743` → `blocked_missing_credentials` classification) lives in
`app/internal/mutationworkers/applescript`.

Three operations, all keyed by `base_apple_contacts.cards.card_id` — which is also Contacts'
own AppleScript `id` (`<UUID>:ABPerson`), so unlike Notes there is no second identifier to
resolve:

- `apple_contacts.create_contact` — `contact` with `given_name`/`family_name`/`organization`
  (at least one), `middle_name`, `nickname`, `job_title`, `department`, `note`, and
  `emails`/`phones`/`urls` as `[{label, value}]`.
- `apple_contacts.update_contact` — `card_id` + `contact` (scalars SET, list values ADDED
  when the card lacks them, `note` replaces, `append_note` appends) + `remove`
  (`emails`/`phones`/`urls` values to delete). Additive by default: nothing leaves the card
  unless named in `remove`, and `result_json.previous_card` records the pre-edit card.
- `apple_contacts.merge_contacts` — `keep_card_id` + `merge_card_ids` + optional `contact`
  overrides. Unions every list field, fills the kept card's empty scalars from the merged
  ones, applies the overrides, then **deletes the merged cards**; `result_json.previous_cards`
  holds all of them. Only `update_contact` is reclaimed from a stale `executing` claim — a
  replayed create duplicates and a replayed merge deletes cards that are already gone.

**A card an earlier merge deleted is redirected, not failed.** Two requests proposed from
one warehouse snapshot can name the same card — one `merge_contacts` folds it away, the
other `update_contact` adds an email to it. The merge ran first on 2026-09-13 and
Contacts.app answered `-1728` for the update one minute later, which read
`failed_terminal` on a 168-mutation request whose other 167 rows succeeded. The executor
now asks the mutation ledger (`queue.PostgresStore.AppleContactsMergedCardTarget`: the newest SUCCEEDED
`merge_contacts` whose `merge_card_ids` names the card, chains followed) and applies the
update to the surviving card, recording `redirected_from` in `result_json`; a merge whose
target is already folded into the same kept card skips it (`already_merged_card_ids`), and
one folded into a *different* card stays terminal, because that is a real conflict. A card
nobody merged is still simply missing.

Unknown `contact` keys are rejected at proposal time rather than dropped, because a
misspelled `email` for `emails` would otherwise become an approved mutation that changes
nothing and reports success. `GetRequest` hydrates every named card's current row into
`preview.contact.cards` (kept card first for a merge, `missing: true` for a card the
warehouse no longer holds) so the reviewer compares against what the card holds today.

The Automation → Contacts TCC grant is separate from the Notes grant and from Full Disk
Access, attributed to the same `launchd → /bin/zsh → pdw → osascript` chain, and needs the
same one-time re-grant to the signed `pdw` binary documented for Notes above (it was granted
to uv's Cellar path when this shipped on porygon, 2026-09-12, and drifted on every uv upgrade
until the worker moved into pdw). `APPLE_CONTACTS_MUTATIONS_ENABLED=0`
pauses the local worker; `pdw ingest apple-contacts --mutations-only` / `--no-mutations`
split the two stages. The app needs `APPLE_CONTACTS_ACCOUNTS` (falls back to
`GMAIL_ACCOUNTS`) to accept proposals for the account.
