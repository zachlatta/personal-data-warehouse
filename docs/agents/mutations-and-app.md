# Reviewed writes, the phone app and push

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

## Phone app (Android and iOS) and push notifications

`mobile/` is an Expo app over the app's own HTTP API — the timeline, the mutation
review queue, and push notifications; see `mobile/README.md`. Three server pieces
back it, all behind the static bearer the CLI uses:

- `GET|POST /api/mutations/requests…` (`app/internal/mutations/api.go`) is THE review
  surface. Every `POST` there decides or edits a request and needs a reviewer principal:
  the master secret (web) or a `reviewer`-scope token (phone); a host's `full` token gets
  403 (see [access tokens](access-tokens.md#who-may-decide-a-mutation-request)). It offers list/get, approve, deny, drop one email, edit an email before approval
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
  **The browser UI is a client of this same API**: `/mutation-review`, `/timeline`, `/feed` and
  `/search` are one static single-page app (`app/internal/webapp`, ES modules embedded in
  the binary, no build step, no CDN) that renders nothing server-side and authenticates
  with `PDW_SECRET_TOKEN` exactly like the phone (`Bearer web:<token>`; `/feed` names its client `crobat-web`, see
  [the Feed doc](../feed.md)). The old
  server-rendered review pages and their password cookie are gone; a still-set
  `PDW_MUTATION_UI_*` variable is reported as deprecated at startup.
- **A forward is a `gmail.send_email`, not a type of its own.** `message.forward_message_id`
  (the original's `base_gmail.messages.message_id`) makes a send a forward the way
  `reply_to_thread_id` makes it a reply, so variants, drafts, the signature and
  edit-before-approve all apply unchanged; the two are refused together, and the variants
  of a forward must all forward one message. At proposal time
  (`app/internal/mutations/gmail_forward.go`) the store reads the original from the
  warehouse — and refuses the proposal when it is not there, because an empty forward is
  a proposal to fix, not one to review — then writes it in exactly as Gmail's Forward
  composes it: `Fwd: <subject>` when the agent gave none, the note, the signature, then
  the `---------- Forwarded message ---------` block (whose `gmail_quote` container is
  what the view splits off as the folded quote), with `In-Reply-To`/`References` set to
  the original. The original's **files** are not snapshotted: they can be 25 MB and the
  sent message is immutable, so the executor (`gmail_mutations.py`) re-reads its raw
  MIME from Gmail (`messages.get format=raw`, so the send asks for `gmail.modify` beside
  `gmail.compose`) and re-attaches every file part as-is, inline `Content-ID` images
  included; the review's `email.forward` lists them so the reviewer sees what goes out.
  The forward is filed in the original's thread, as Gmail files its own.
  `tests/test_email_attachments_e2e.py` drives the whole path: real proposal, phone-shaped
  edit and approval through the API, real worker, loopback Gmail.
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
  - **One mutation can be dropped from a batch without denying it, and put back** —
    `POST …/mutations/<id>/remove`, which is operation-agnostic and has always been, and
    since 2026-10-11 `POST …/mutations/<id>/restore` (`RestoreMutation`), which returns a
    mutation to `pending_review` only when it was dropped during review (status
    `rejected`, error `removed during review`) and its request is still pending; a denial
    or a failure shares the status but not the error, and is never resurrected. Both bump
    the request's revision and write a `mutation_removed` / `mutation_restored` event.
    The phone makes it the row's own control: "✓ Archive" ⇄ "Keep in inbox", one tap each
    way, applied optimistically, with the approve count following. In the 2026-10-11
    screen recording keeping one thread cost an expand, a scroll past its body and a
    confirm, and a mistaken keep could not be undone.
  - **An email in a review is rendered, not printed.** The thread preview already
    carried each message's `body_html` (capped at 200,000 characters); the phone showed
    `body_text`, which for bulk mail is the HTML source or empty, so the same recording
    was a scroll through MSO conditionals, CSS and analytics JavaScript. The phone now
    renders the HTML part in a WebView with the mail's scripts stripped and forbidden by
    CSP, links opened outside the review (`mobile/src/lib/email-html.ts`).
  - **The phone reviews a queue, and every word of a decision names its effect.**
    Measured from a screen recording on 2026-09-29: three one-email requests took ~10s
    each, ~3s of it tapping back, waiting on the list and re-opening, and the confirm
    read "1 mutation will run upstream" on all three. Now a decision opens the next
    pending request (`mobile/src/lib/review-queue.ts`, title "3 to review", Skip in the
    header, the next one prefetched); and the buttons and note come from
    `requestDecision` in `mutation-review.ts` — "Send" / "Don't send" and "Sent to
    front@…" for one email, "Archive" / "Keep in inbox" for one thread.
  - **A decision is undoable for ten seconds instead of confirmed.** The confirm
    dialog was a second tap on every request and was read once, then tapped through.
    Now Approve/Deny moves on at once and the decision is held in the phone's memory
    (`mobile/src/lib/undo-decision.ts`) behind an Undo bar that names the recipient;
    the API call goes out when the window closes, when the next request is decided,
    or when the app is backgrounded. The server is never asked to un-send anything:
    a held decision has not been sent, and one lost to a killed app leaves the
    request pending, which is the safe way to fail. A held or in-flight request is
    left out of the queue and the list, since the server still says pending. An email reply shows the message it answers first, with
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
and falls back from — the phone app never calls `canOpenURL`, which on iOS would need
every scheme declared in the native Info.plist. A source with no honest URL (health, photos, voice memos,
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
opening the app on iOS (Android draws no buttons; a tap opens the review, see below). Categories live in `app/internal/push/categories.go` and are published
at `GET /api/push/categories`; the app registers them on launch, so a new button is a Go
edit — only handling a *new action id* needs `mobile/src/lib/push.ts`. Images on iOS need
the Notification Service Extension target (`mobile/targets/notification-service`, added
at prebuild by `@bacons/apple-targets`), which reads Expo's `body._richContent.image`
from the payload; a build without it shows the same push as text, and changing it is a
native build, not an OTA update. Settings → "Send test push" sends an image + subtitle +
Open button, which is how a phone proves the extension is installed. On Android the native
service below downloads the image and a test push has no button.

**On Android every alert is a data-only push the app draws itself.** Since 2026-10-28, a timeline alert from `android_messages`, `apple_messages`, `whatsapp` or `slack`
carries `Alert.Android` (`AndroidStyle`: sender, conversation id and title, group, an
`avatar_url` from a non-placeholder contact photo or the Slack profile, the event id), and
`Transport.Send` (`app/internal/notifications/sender.go`) picks the wire shape per device from
the delivery endpoint, which the fan-out fills from `push_devices.platform` and
`push_devices.capabilities`: an Android device that registered `android_native_alerts` gets
a data-only Expo message (no title, so FCM hands it to the app instead of drawing it in the
"Miscellaneous" fallback channel), everything else gets the ordinary alert on the `default`
channel. Since 2026-11-06 a non-chat alert goes the same way from both Expo senders
(`expoMessage`, and `Notification.message` behind the `notify` tool, `POST /api/push/send`, the
mutation hook and the test push): `Message.AndroidDataOnly` moves its title, subtitle, body,
image, collapse id and silence under `data.android_alert`, and `push.DrawsAlertsNatively` is the
one gate. `android_native_alerts` replaced `android_messaging_style`, whose builds draw chats only
and show nothing for a data-only non-chat push; the old stored value normalizes away, so such a
build gets ordinary alerts for everything until it registers again. Android alerts have no action
buttons: a tap opens the route (an Approve with no running JS would wait silently for the next
launch), and `collapse_id` now really replaces an alert there, which Expo's Android path never did.
`capabilities` is declared in both ensure paths (`store.go`, `postgres.py`) with an
`ADD COLUMN IF NOT EXISTS`, and every registration replaces it. The avatar lookup runs in the
worker's `Render` under a one-second bound (`withAvatar`); a slow or failed lookup means an
initial, never a late or lost alert. The phone side is `mobile/plugins/with-messaging-notifications.js`;
see `mobile/README.md`.

- **PDW does not repeat what another app on the phone already showed (2026-11-11).** An
  Android delivery to a device whose phone is known waits 90 s from the event
  (`PhoneWitnessHold`) and is then skipped as
  `already_notified` when `base_android_notifications.events` holds another app's
  notification for the same event (posted up to ten minutes after it): a MessagingStyle
  message at the event's millisecond from the same sender or with the same opening text, or
  the alert's title plus at least 10 characters of its text's opening. Only the recipient's
  own phone counts: the app stamps each alert with `pdw.delivery_id`, and the listener row
  carrying it names the phone. An unknown phone or a failed check sends. iOS still gets it.
  See
  [docs/notifications.md](../notifications.md#already-shown-on-the-android-phone).
- **Android's 50-notification package cap and burst pacing (2026-11-05).** Joining the 861
  `provider_accepted` Android chat pushes (`857` through `2026-11-06 02:05Z`) and 715 non-chat
  pushes from `2026-10-28` to `2026-11-06` against `base_android_notifications.events` showed that
  66 (`64` of `857`) delayed chat pushes arrived when `pichu` reconnected from offline or
  powered-off windows (up to 10h45m lag on Oct 29 and 1h45m on Oct 30), while 69 chat alerts (71
  when disambiguating 2 repeated-body messages on Oct 29 within 5s of `accepted_at`) and 234
  non-chat alerts never appeared on the phone (including 2 unexplained non-chat drops on
  `2026-11-02 20:01..20:05Z` when ~5 alerts were active and the app was not in the foreground,
  which shade-cap eviction does not explain). Six blackout windows accounted for 64 of the 69
  missing chat alerts (62 of 71 exact) and 215 of the 234 missing non-chat alerts because non-chat
  `FCM-Notification:<tag>` pushes filled Android's `MAX_PACKAGE_NOTIFICATIONS = 50` per-app limit
  (confirmed by two `event_type = 'active'` snapshots at exactly 50 and six 50-notification removal
  events that immediately restored delivery); the remaining 5–9 chat drops occurred in sub-second
  burst tails before `system_server` committed the previous `notify()`. `PdwNotifications` now:
  - Evicts the oldest active non-summary notifications across all tags (every non-chat tag first,
    `pdw-alert` and anything else, then oldest `pdw-chat` conversations other than the target
    conversation) down to `< 30`
    (`MAX_ACTIVE_NOTIFICATIONS = 30`) and polls up to 250ms for async cancel completion before
    posting.
  - Synchronizes `post()` and `postPlain()` on the push service's thread, caches contact avatars in
    an `LruCache<String, Bitmap>(32)`, paces `notify()` at 200ms (`MIN_NOTIFY_INTERVAL_MS = 200L`),
    and prefers its own last post over the shade's copy for 5 seconds (`RECENT_POST_TTL_MS = 5000L`), after
    which a conversation missing from the shade counts as dismissed and starts fresh.
  - Stores `pdw.event_ids` via `putString` as a newline-delimited string so
    `SpoolListenerService.java` serializes every stacked `event_id` into
    `raw_json->'extras'->>'pdw.event_ids'` instead of `"<ArrayList>"`.
  - `marts_ops.notification_health` (and `/pipelines`) reconciles 24h `provider_accepted` Android
    deliveries (settled `> 15m` and older than `LEAST(max(synced_at), max(event_at)) - 2m` on
    `base_android_notifications.events`) against `[accepted_at - 10s, accepted_at + 12h)` phone
    events (`android_accepted_24h`, `android_posted_24h`, `android_missing_24h`), turning
    `attention` when accepted pushes never appear on the phone.
  - **Eviction runs for every alert** since 2026-11-06, when non-chat alerts became data-only and
    `PdwNotifications.postAlert` started drawing them under the tag `pdw-alert` on the `default`
    channel. A build without `android_native_alerts` still has FCM drawing its non-chat alerts, so
    a run of them can reach 50 there; the health view reports what that drops.
  - **Rolling a phone back needs a launch.** After reinstalling an older APK (as root on the phone,
    `pm install -r -d <apk>`; a refused downgrade means rebuilding that commit with a higher version
    code, never an uninstall, which loses the app's data), open the app and confirm
    `private.push_devices.capabilities` no longer holds `android_native_alerts`. Until that launch
    the server keeps sending data-only alerts, which the older build drops without a sign.

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

**The worker launches Contacts.app and waits for its address book before writing.** From
2026-09-21 to 2026-10-08, osascript's implicit launch of Contacts kept answering
`Application isn't running` (`-600`), even though `open -g -j -a Contacts` started the same
app at once on porygon. Two bugs turned that outage into lost writes. First, the resident
drain loop re-claimed a `failed_retryable` row the instant it failed. That came to about
one attempt a second: 1.28M failures over the period, and 82,828 attempts on one create.
Second, when Contacts finally answered, the first script reached it while it was still
starting. A write to a card that existed got `Can't get person id` (`-1728`), and a create
got `Can't make class person` (`-2710`). Both were classed `failed_terminal`, so the request
failed with its Google half applied and its Apple half not. Now:

- Before every mutation, the executor runs `open -g -j -b com.apple.AddressBook`, which
  needs no Automation grant.
- It then asks for `count of people`, and treats a launch failure or a count of zero as
  `failed_retryable`.
- `-609` and `-2710` are retryable in `applescript.Classify`.
- A `failed_retryable` row is claimable again only after `queue.RetryBackoff(attempt_count)`:
  10 s, doubling per attempt, capped at 15 min.

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

## Sending text messages (android_messages.send_message)

`android_messages.send_message` sends one text as Zach from Google Messages on his Pixel
(device `pichu`; he switched from the iPhone on 2026-10-23, and `apple_messages.send_message`
is retired: the proposal API refuses it and the Mac worker fails any old approved row
terminal with nothing sent). SMS and RCS have no server API, so the proposal, review and edit
halves live with every other type (`app/internal/mutations/android_messages.go`) and the
executor is local, in two halves (`app/internal/mutationworkers/androidmessages`):

- **porygon:** `pdw mutations android-messages`, the resident queue worker (LaunchAgent
  `com.zachlatta.personal-data-warehouse.android-messages-mutation-worker`, through
  `bin/android-messages-mutation-worker-launchd`). For each approved row it runs the phone
  half over the root SSH login (`ssh pichu`; override `ANDROID_MESSAGES_SSH_HOST`) with a
  JSON request on stdin. `android_messages` is in `LOCAL_ONLY_MUTATION_PROVIDERS`, so the
  cloud worker never claims it.
- **the Pixel:** `pdw mutations android-messages-device send`, run by the pdw binary of the
  `pdw-android-messages` Magisk module (`ANDROID_MESSAGES_DEVICE_PDW`, default
  `/data/adb/modules/pdw-android-messages/bin/pdw`, which self-updates from releases). It
  starts Messages' own no-UI send service as root: `am startservice -a
  android.intent.action.RESPOND_VIA_MESSAGE -d smsto:<number> --es android.intent.extra.TEXT
  <text> -n com.google.android.apps.messaging/.shared.NoConfirmationSmsSendService` (the
  dialer's "reply with a message" path). No screen, works while the phone is locked, and
  Messages picks RCS or SMS for the conversation itself. Tested 2026-10-24 to Zach's own
  number: RCS, delivered in about 3 s. `... android-messages-device find --recipient
  --text --since` is a read-only check.

- **One approval sends one message.** Before sending, the phone reads `bugle_db`
  (read-only, never when its `-wal`/`-shm` are absent) for an outgoing message
  (`message_status` < 100) with the same trimmed text in the one-to-one conversation with
  the number (`conversations.participant_normalized_destination`, or its only participant)
  since `approved_at` minus a minute; a hit is `succeeded` with `already_sent: true`. After
  sending it polls up to 30 s: status 1/2 is sent/delivered (`verified: true`), 8/9 failed
  (`failed_terminal`: the failed copy sits in the conversation), still sending is
  `succeeded`, `verified: false` (Messages keeps retrying itself). An unreachable phone or a
  dropped SSH session is `failed_retryable`, safe because of the check. Nothing is
  reclaimed from a stale `executing` claim.
- **Edit before sending.** `POST .../mutations/<id>/update-text-message {text}` (web review
  textarea; phone app `TextMessageCard`) replaces the words of a pending text, bumps the
  revision and marks `preview.text_message.edited`. The number is not editable. The phone
  app saves an unsaved edit before Send.
- **Review.** `hydrateAndroidMessagesPreviews` names the contact card(s) for the number and
  shows the six newest messages with it from `base_android_messages` (1:1 `chat_key`) and
  `base_apple_messages` (handle), warning on a stranger or a first text.
- **`ANDROID_MESSAGES_SEND_ALLOWED_RECIPIENTS`** (porygon env or repo `.env`) restricts sends
  for testing; `ANDROID_MESSAGES_MUTATIONS_ENABLED=0` pauses the worker. The app needs
  `ANDROID_MESSAGES_ACCOUNTS` (falls back to the Apple Messages setting, then
  `GMAIL_ACCOUNTS`); the account is `base_android_messages.messages.account`.

## Marking texts read (apple_messages.mark_read)

`apple_messages.mark_read` marks one one-to-one conversation read on the Mac that syncs
Apple Messages, through the same `pdw mutations apple-messages` worker
(`app/internal/mutationworkers/applemessages/markread.go`; proposal and review half in
`app/internal/mutations/apple_messages_read.go`).

- **Why it drives the window.** Messages' AppleScript dictionary has no read state, and
  the private IMCore call that does (`markAllMessagesAsRead`) is refused to unentitled
  processes while SIP is on (porygon: SIP enabled). Writing `chat.db` directly would not
  reach imagent or iCloud. So the executor does what a person does: `open sms:<chat>`
  shows the conversation, Messages marks it read and syncs that through iCloud, and the
  executor closes the window again (only if Messages had none open before), because a
  conversation left on screen keeps marking new texts read.
- **It needs the screen unlocked.** Tested on porygon 2026-10-11: under the lock screen,
  `open sms:72975` left the message unread. The executor reads
  `CGSSessionScreenIsLocked` from `ioreg -n Root -d1 -a` first and returns
  `failed_retryable` while locked, so approved rows wait (backoff capped at 15 min) and run
  after the next unlock.
- **Verified in chat.db.** Before opening, it counts incoming unread ordinary messages
  (`is_from_me = 0`, `is_read = 0`, `item_type = 0`, no tapbacks) in the style-45 chats with
  that `chat_identifier`; zero is `succeeded` with `already_read: true` and opens nothing.
  After opening it polls up to 20 s. All read: `verified: true`. Some read:
  `succeeded`, `verified: false`, `still_unread`. None: `failed_retryable`.
- **Review.** `hydrateAppleMessagesMarkReadPreviews` names the contact card(s) and lists up
  to 10 messages the warehouse shows unread in the conversation now, warning when there
  are none. Approving marks the whole conversation read, including texts that arrive
  after review, and senders with read receipts on see it.
- `chat` accepts an E.164 phone, an email (iMessage or a business-chat sender such as
  `google@rbm.goog`), or a 3 to 8 digit short code; `recipient` is accepted as an alias.
  Group chats (`chat123…`) are refused at proposal time.

## Gmail filters and labels (gmail.*_filter, gmail.*_label)

Six reviewed types let an agent set up rules rather than only move mail:
`gmail.create_filter`, `gmail.replace_filter`, `gmail.delete_filter`, `gmail.create_label`,
`gmail.update_label`, `gmail.delete_label`. Proposal, snapshot and review live in
`app/internal/mutations/gmail_settings.go` / `gmail_settings_store.go`; the executor is
`src/personal_data_warehouse/gmail_settings_mutations.py`, run by the cloud worker beside every
other Gmail mutation.

- **The review reads the warehouse, so the warehouse holds the mailbox's configuration.**
  The app server has no Google credentials, and nothing synced labels or filters before
  this. `gmail_settings_sync` (`gmail_settings_sync.py`, Dagster, every 30 minutes) snapshots
  both per account into `base_gmail.labels` and `base_gmail.filters` (full listing, two API
  calls, `gmail.readonly`; a row Gmail stops returning is kept with `is_deleted = 1`), with
  `ops.gmail_settings_sync_state` as its `gmail_settings` pipeline heartbeat. The worker
  re-runs it inline for an account right after one of these mutations succeeds, so the next
  review reads the new state, and the observer marks the mutation `observed` once the
  snapshot shows the change. They are `entity` tables, not timeline events: what changes them
  is the reviewed mutation, and that is on the timeline.
- **Proposal-time snapshot, execution-time re-check.** `delete_filter` / `replace_filter`
  copy the filter (Gmail's own criteria/action) into `payload.expected_filter`;
  `update_label` / `delete_label` resolve the label to `label_id` + `expected_name`. A
  proposal naming a filter or label the warehouse does not have, a system label, a rename onto
  an existing name, an existing label name to create, or a filter identical to an existing one
  is refused with the reason. After approval the executor re-reads Gmail and does nothing
  (`failed_terminal`) when the rule or label changed since review; already gone on a delete is
  `succeeded` with `already_deleted`.
- **Gmail has no filter update, so replace is create-then-delete.** The mailbox is never
  without the rule. If the delete fails after the create, the row is `failed_retryable` with
  `new_filter_id` in `result_json`, and the retry finds that filter (identical criteria and
  action) instead of creating a second one. `create_filter` does the same lookup, and treats
  Gmail's 400 "Filter already exists" as done.
- **Friendly actions, label names resolved at execution.** System labels are reachable only
  through booleans (`skip_inbox` = remove INBOX, `mark_read` = remove UNREAD, `star`,
  `mark_important` / `never_important`, `never_spam` = remove SPAM, `trash`, `category`), so
  there is one way to say each. User labels are names: strict in `add_labels` /
  `remove_labels` (resolved before anything is created), created only when listed in
  `create_and_add_labels`. Unknown keys anywhere are refused, because a misspelled field is an
  approved rule that does less than the reviewer read.
- **Forwarding is refused.** It needs a verified forwarding address and
  `gmail.settings.sharing`, and sending Zach's mail elsewhere is its own decision.
- **Order inside a request.** The claim now orders a request's mutations by `request_index`
  (they share `approved_at`/`created_at`, so it used to fall through to the random id, and
  `UPDATE … RETURNING` comes back in heap order regardless; the rows are re-sorted in Python),
  and the worker runs label/filter changes before the thread-label batches. So "create label
  Receipts, create a filter that adds it, move these 40 threads under it" in one request works.
- **Filters only touch new mail.** There is no apply-to-existing flag; the review shows how
  many threads the criteria matched in the last 90 days (substring match on from/to/subject,
  `has_attachment`, `size`, with up to five examples), and the agent proposes a
  `gmail.modify_thread_labels` batch in the same request when existing mail should move. The
  preview is computed on the read path and bounded (3 s, ten filters a page). Gmail search
  syntax (`query`, `negated_query`, operators in `from`/`subject`) is not translated: with
  other criteria the count is labelled an upper bound; with none it says "no preview". A
  failed lookup reads "unknown", never 0. Warnings: a `trash` action, a broad rule (≥100
  threads), deleting a filter, a label on messages or used by filters, nested labels a rename
  does not carry.
- **Scopes.** Label changes run under `gmail.modify`. Filter create/delete need
  `gmail.settings.basic`, which only filter operations request (`Settings.gmail_settings_scopes`).
  Requesting a scope the refresh token lacks fails the refresh, so adding it to the shared
  `gmail_mutation_scopes` would have blocked every archive and send until re-consent. A missing
  grant is `blocked_missing_credentials` with the fix in the error. The fix is
  `uv run personal-data-warehouse-gmail-auth --email <account> --production-scopes`, which
  asks for every scope production uses: a consent replaces the whole grant, and on 2026-07-15
  a re-consent without the write flags silently dropped them. Put the printed
  `GOOGLE_<SLUG>_TOKEN_JSON_B64` and `GMAIL_<SLUG>_TOKEN_JSON_B64` on the Dagster deployment.

## Printing (printer.print_document)

`printer.print_document` prints one PDF on the home Brother MFC-L2710DW, the
machine the scanner service also talks to. One-sided, black and white, US Letter (pages are
fitted to it).

- **Proposal** (`app/internal/mutations/printer.go`): `filename`, `content_type`
  (`application/pdf` only), `data_base64` (at most 20 MiB decoded, must start with `%PDF-`),
  `page_count` (1 to 200, what the agent counted) and optional `copies` (1 to 20, default 1).
  The account is always `home-brother` and may be omitted; unknown fields (`sides`, a URL)
  are refused. The bytes are stored inline like an email attachment, so what is approved is
  what prints; the payload also stores their `sha256` and size.
- **Review.** Review reads (`GetRequest`, so the web and phone review, the push alert and
  the proposal response) drop `data_base64` (`hydratePrintDocuments`); the generic card
  shows the payload and `preview.print_document`: file, pages, copies, sheets, sides, paper,
  size and sha256. There is no PDF viewer in the review yet.
- **Execution** (`app/internal/mutationworkers/printer`): `pdw mutations printer`, a
  resident queue worker in the `printer` service of the Coolify app
  `personal-data-warehouse-scanner` on mew-coolify (`app/scanner-compose.yaml` +
  `app/printer.Dockerfile`, host networking for the LAN leg, see
  [Scans](other-sources.md#scans-the-scanners-panel)). It needs `POSTGRES_DATABASE_URL`
  (the warehouse writer URL, in the Coolify environment) and `PRINTER_URI` (required,
  `ipp://<printer LAN address>/ipp/print`, set in Coolify like `SCANNER_HOST`; the worker
  refuses to start without it). `printer` is in `LOCAL_ONLY_MUTATION_PROVIDERS`.
  For each row it checks the bytes against the stored sha256, counts pages with `pdfinfo`
  (a different count fails terminal, nothing sent), renders 600 dpi 8-bit grey PWG raster
  with Ghostscript (`-sDEVICE=pwgraster`; the printer takes no PDF) and sends one IPP
  Print-Job named `pdw-<mutation id>` with `ipptool -t -L` (`print_job.test`, embedded). It
  then reads the job with Get-Job-Attributes (`job_status.test`) every 5 s for up to 5
  minutes: `completed` is `succeeded` (`verified: true`), `canceled`/`aborted` is
  `failed_terminal`, still going is `succeeded` with `verified: false`. No CUPS daemon, so
  no spool can hold or resend a job behind the worker.
- **One approval prints at most once.** Only a failure that proves nothing reached the
  printer is `failed_retryable`: ipptool's `Unable to connect`, or the printer answering
  `server-error-busy`/`server-error-service-unavailable` (an error answer creates no job).
  Any other IPP error is `failed_terminal`, nothing printed. A failure after the document
  may have been sent (connection drop, timeout) is `failed_terminal` "may have printed,
  check the printer" and is never sent again; the Brother keeps no job history to check
  (Get-Jobs `which-jobs=completed` answered empty, 2026-11-01). Nothing is reclaimed from a
  stale `executing` claim. To reprint, propose a new request with a different title or
  reason: an identical proposal returns the old request.
- **`PRINTER_MUTATIONS_ENABLED=0`** pauses printing: rows wait as `failed_retryable`.
- Tests: `app/internal/mutations/printer_test.go` (validation, preview, the read-time
  drop), `app/internal/mutationworkers/printer/executor_test.go` (every outcome, with
  ipptool output captured from ipptool 2.4.2), `postgres_integration_test.go`
  (`PDW_TEST_POSTGRES_URL`: proposal, approval, the claim, the result) and
  `realtools_test.go`, which runs the real pdfinfo, Ghostscript and ipptool against a fake
  printer named by `PDW_PRINTER_TEST_URI`. Run it in the printer image with
  `ippeveprinter` (cups-ipp-utils; it needs avahi and IPv6, and listens on localhost only,
  so share its network namespace); never point it at the Brother.

## Deleting a screen capture (screen_timelapse.purge_moment)

The one mutation type that deletes warehouse data rather than writing to an upstream
service. It exists because on 2026-10-25 a screen timelapse frame caught something that
must not be kept, and the only way to remove it was superuser SQL on the warehouse and a
one-off Drive script, with the agent deciding by itself whether the timeline event and
the readings had to go too. Scope is one captured
moment of one device; a general warehouse-row purge would be its own design.

- **Proposal** (`app/internal/mutations/screen_timelapse_purge.go`): `device`,
  `captured_at` (matched exactly), optional `display_index` (omitted = every display) and
  a required `reason`; the device is the account. `CreateRequest` reads the moment through
  the `@screen_timelapse_*` markers and refuses it when nothing matches (no screenshot and
  no tree, or a display the moment lacks). The preview carries storage ids, sizes and
  counts, the timeline event that goes and a `timeline_path` to it, and how many other
  frames have the same bytes; never tree text or readings, because every proposal is
  itself a `mutation_requests` timeline event.
- **Execution** (`src/personal_data_warehouse/screen_timelapse_purge.py`, the cloud
  worker): phase one is ONE transaction that deletes the screenshots (first, so their row
  locks are what a racing writer waits on), the moment's accessibility tree (one per
  moment, so it goes even when one display is named: it can hold what that display
  showed), the matching `derived_screen_timelapse.descriptions`, and, when display 1 goes,
  the `screen_capture` event (and any search chunks) from `timeline.events`, computed
  with the adapter's own `event_id_expression`. The same transaction writes the removed
  storage ids into the mutation's `result_json` (phase `rows_deleted`). Phase two runs
  after the commit: `delete_object` on each file, then `get_metadata` must raise
  `ObjectNotFoundError`. Any failure is `failed_retryable` with the pending objects kept
  in `result_json`; the next claim skips straight to them, and a Drive delete of a file
  already gone is a no-op, so a row never points at a deleted file and a retry never
  redoes the rows. That is also why the operation is in `RECLAIMABLE_IDEMPOTENT_OPERATIONS`.
  A file another row still names is left alone (`still_referenced`). The record holds ids
  and counts, never content, and `not_touched` says the device's own files and the
  pgBackRest backups on slowking are untouched.
- **Nothing writes it back.** The `screen_capture` adapter has no `prune_sql`, so the
  explicit event delete is the only way the event goes; the sync never re-creates it
  without a row. Phase one runs under the timeline sync's own advisory lock
  (`timeline_sync`, waited on for 30s, otherwise `failed_retryable`), so no sync run
  holding a pre-purge snapshot can upsert the event again; it lives on the lock database
  both workers already share for `upstream_mutation_worker`. The enrichment pass pages
  its candidates before reading them, so `insert_screen_timelapse_descriptions` writes a
  reading only while its screenshot row still exists with the bytes it read (`EXISTS …
  FOR KEY SHARE`, which waits on a purge in flight and then sees the row gone).
- **The uploader may bring a frame back, and that is intended.** It skips a file while its
  path, size and mtime match what the app acknowledged; a file whose size or mtime changed
  is sent again and ingested as a new row. That is how a redacted copy comes back in. A
  bare `touch` re-sends the original bytes too, so redact or delete the file on the device.
- Tests: `tests/test_screen_timelapse_purge.py` (the worker end to end, the retry, the
  race with enrichment on two connections, and the Go proposal half run against an
  isolated database through `TestScreenTimelapsePurgeProposalReviewApproval`), and
  `app/internal/mutations/screen_timelapse_purge_test.go` for validation.

## Mutation policies (auto-approval)

A **policy** is a named, versioned rule for one mutation type that only Zach can create,
widen or extend; a **grant** gives one authenticated token (one workflow's identity,
`requested_by_token_id`, see [access tokens](access-tokens.md#workflow-identities-and-requested_by_token_id))
the right to use it until the grant expires. PDW decides, inside the proposal's own
transaction (`PostgresStore.CreateRequest` → `evaluateRequestPolicy`, `app/internal/mutations/policy*.go`):

1. **Type rules** (`typeRules`, in code). A type is proposable and never auto-approved
   unless listed; only `resideo.set_thermostat` is auto-approvable today. No policy can
   approve a type the table does not mark auto-approvable.
2. **Deny rules** (`deny_json`: `below`, `above`, `in` per field). An enforced policy's deny
   rule refuses the proposal with the rule named; a dry-run one records `would_refuse`.
3. **Grant match.** The proposer's token holds an active grant (not revoked, not past its
   required `expires_at`), the policy is inside `valid_from`..`valid_until`, every field of
   `match_json` (`min`/`max`, `in`, `absent`) passes on the type's facts
   (`policyFactsFor`; for the thermostat `thermostatPolicyFacts`: `device_id`,
   `heat_setpoint`, `cool_setpoint`, `hold`), and every limit in `limits_json` passes:
   `max_per_day`, `max_per_hour`, `min_interval_minutes`, `input_max_age_minutes` (the
   thermostat poll the proposal was judged against) and `pause_after_manual_change_hours`
   (any setpoint change in `base_resideo.thermostat_readings` that no mutation this policy
   approved explains). Limits are counted under the policy's advisory lock, over the policy's
   `approved` **and** dry-run `would_approve` decisions: a dry run exercises the limits, and
   switching a policy to `enforce` never lets more changes through in the next hour or day
   than the limits allow.
4. **Otherwise review**, with the closest policy's reasons ("heat_setpoint 74 is above this
   policy's 72"). A policy approves **one change per request**: its limits count
   changes, so a batch of several changes, even one wholly inside the policy, goes to
   review whole.

The policy JSON is parsed strictly: an unknown rule, limit or field never matches, because
ignoring it would widen the policy. Policy rows are never updated; a change is a new
version, and every decision names `policy:<id>@<version>`.

Each decision is stored three ways: a row of `ops.mutation_policy_evaluations` (phase
`propose`), `context.policy` on the request (served as `request.policy`), and a
`policy_evaluated` request event with `actor_type = 'policy'`. A policy's `mode` is `off`,
`dry_run` or `enforce`; in `dry_run` the review shows a **"Would auto-approve"** badge (web
`policyBadge`, phone `policyBadge` in `mobile/src/lib/mutation-review.ts`) and the request
still waits for Zach. A request no policy could touch carries no note.

Tables (ops schema, app-only; Go `mutationPolicySchemaStatements`, Python twin in
`ensure_upstream_mutation_tables`, held equal by `tests/test_mutation_policies.py`):
`mutation_policies` (`id`, `version`, `operation`, `account`, `match_json`, `deny_json`,
`limits_json`, `mode`, `valid_from`, `valid_until`, who created and approved it),
`mutation_policy_grants`, `mutation_policy_evaluations`, `mutation_policy_settings` (the
kill switch).
