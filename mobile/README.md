# PDW phone app (Android and iOS)

An Expo (React Native) app over the warehouse's own HTTP API: the unified
timeline, the mutation review queue, and push notifications when a new
mutation request needs a decision. Android is the phone Zach carries since
2026-10-23; iOS still builds from the same code.

The app talks to the same Go app the `pdw` CLI does and authenticates the same
way — a static bearer `"<client_name>:<PDW_SECRET_TOKEN>"` — so signing in is
pasting the app URL and the secret token once. The token lives in the Android
Keystore or the iOS Keychain (`expo-secure-store`).

| screen | endpoint |
| --- | --- |
| Timeline (tiers default to `self`, `direct`, `cc`) | `GET /api/timeline`, `GET /api/timeline/item` |
| Mutations (needs review / past) | `GET /api/mutations/requests[?status=…]` |
| Review one request: approve, deny, skip an email (and take the skip back), edit an email | `GET /api/mutations/requests/{id}`, `POST …/approve`, `POST …/reject`, `POST …/mutations/{mid}/remove`, `POST …/mutations/{mid}/restore`, `POST …/mutations/{mid}/update-email` |
| Alerts: the global on/off switch for timeline notifications and the ledger of every one sent (delivered / opened / skipped / failed, paged by `next_cursor`) | `GET /api/notifications[?limit=&before=]`, `POST /api/notifications/settings` |
| Feed: full-width cards agents push with `pdw feed push`; every impression (dwell, visible fraction), open, read time, like and dismissal is logged (docs/feed.md) | `GET /api/feed`, `POST /api/feed/events` |
| Settings: push registration, test push, disconnect | `POST /api/push/register`, `POST /api/push/test` |

Push goes through the Expo push service: the app registers its
`ExponentPushToken[...]` with the server (`private.push_devices`), and the Go
app fans a notification out to every active device when a request lands in
`pending_review` (tapping it opens that request). A `DeviceNotRegistered`
ticket retires the device row rather than silently shrinking the fan-out.

## Reviewing a batch

A mutation request is n mutations and n is routinely in the hundreds, so the
review screen renders the batch as the thing it is about rather than as its
payload. The pure part of that lives in `src/lib/mutation-review.ts` and has
node tests (`npm test`, also run in CI before the OTA publish); the source-shaped
components live in `src/components/*-review.tsx`.

**Approve and Deny do not ask to confirm.** The screen moves straight on to the
next request waiting, and an Undo bar ("Sent to vendor@… · Reply to …",
counting down from 10s) sits above the action bar. Nothing reaches the server
until the window closes; Undo cancels it and reopens the request. Only the last
decision can be undone: deciding the next request sends the previous one at
once, and leaving the app sends a held decision immediately. The hold lives in
memory (`src/lib/undo-decision.ts`), so if the app dies inside the window the
request is still waiting for review rather than half-sent. A deny no longer
asks for a reason.

- **Gmail thread reviews** (archive / unarchive / relabel) read as an inbox:
  sender, subject, snippet, time, and proposed action, grouped by the day each
  thread last moved. This also applies to mixed Gmail/Slack requests; other
  actions appear below the inbox and approval still covers the whole request.
  Tap a thread to see its full subject and messages. The latest message opens
  in full; earlier messages and sender/recipient details expand independently.
  **A message renders its HTML part as the sender laid it out**
  (`src/lib/email-html.ts`, `src/components/email-html-body.tsx`): a WebView
  on white, scaled to the phone's width, as tall as the message up to 520pt
  with "Show full email" past that, never scrolling inside the list. The
  mail's own scripts never run: `<script>`, frames, forms, inline handlers
  and refreshes are stripped, and the document's Content-Security-Policy
  forbids scripts anyway; the only one that runs is the app's injected sizing
  script. Remote images load (plain-http ones are upgraded to https), and a
  tapped link opens in Safari, never inside the review. Until 2026-10-11 the
  phone printed the text part, which for bulk mail is the HTML source (MSO
  conditionals, CSS, analytics JavaScript) or empty. A text part that is
  really HTML renders as HTML too; a message with neither part says so, with
  Open in Superhuman. Filters cover unread / automated / skipped, with
  sender/subject search past eight threads.
  **Every row's action is a one-tap toggle**: "✓ Archive" while the thread is
  in the batch, "Keep in inbox" once it is out, and the approve button's count
  follows at once (`gmailThreadToggle`). Keeping is `POST …/mutations/{mid}/remove`;
  tapping again is `POST …/mutations/{mid}/restore`, which takes back only a
  drop made during review. One mutation that archives several threads asks
  before keeping them all. Missing previews never hide affected thread IDs
  from the list. Approval counts only mutations that will still run.
- **Email proposals** (`gmail.send_email`) read as the composer they are: To,
  Cc, Bcc, Subject and the body are editable, the signature and quoted thread
  sit below read-only exactly as they will be sent, a proposal with several
  versions is chosen from chips, and Send-on-approval / Save-as-draft is a
  toggle. The body is edited as plain text — the server's `email` view carries
  each part as text (`editor_text`, `signature_text`, `quoted_text`) beside the
  HTML the web editor uses — and `assembleEmailBody` reassembles the stored
  body in the order the server splits it again (editor, signature, quote).
  Save posts to `update-email` and reloads: approval sends what is stored,
  never what is on screen, so an unsaved edit is visibly unsaved.
  Tap an outgoing attachment to review it without leaving the proposal: images
  and text display natively, PDFs preview on iOS, and Share / Save opens other
  formats in an installed app or saves them to Files. Previewing never changes
  attachment bytes or approves the email. Each viewer uses an isolated temporary
  directory, deleted when it closes. HTML/SVG attachments are not rendered.
  This viewer adds native modules and needs a new preview build, not only an OTA
  update; the mobile workflow detects the fingerprint change and queues it.
- **Slack mark-read batches** show each speaker's profile picture and open that
  exact message in Slack on a tap, because the answer to "mark this read?" is
  often "let me reply first". Faces identify DM rows; channels keep their glyph.
- **Calendar create-event mutations** read as a day calendar, not a flattened Google
  payload. The event to add sits in the same time grid as every synced event on
  the covered day, real busy overlaps are called out, all-day events stay
  visible, and the full invite shows names, addresses, RSVP state, organizer,
  optional guests, notification policy, location, description, reminders, and
  the remaining technical payload on demand. Availability is hydrated when the
  request is opened, so a pending request reflects calendar changes that landed
  after it was proposed; a failed lookup says unavailable rather than "clear."
- The day grouping keys on the reader's LOCAL day. Keying on the timestamp's UTC
  prefix splits one evening across two sections and labels both of them the same.

## Rich notifications

Alerts can carry a subtitle, an image, action buttons, a thread, a collapse
id, an interruption level and a badge. The server builds them
(`app/internal/push.Notification`) and there are three ways to send one:

```bash
pdw call notify --data '{"title":"Wire due today","subtitle":"Invoice 4831","body":"Instructions in inbox.","image_url":"https://…/receipt.png","category":"link","route":"/timeline","interruption_level":"time-sensitive"}'
curl -X POST "$PDW_API_URL/api/push/send" -H "Authorization: Bearer cli:$PDW_SECRET_TOKEN" -d '{"title":"…"}'
# Settings → "Send test push" sends one with an image, subtitle and Open button.
```

Two pieces of the app make that work, and both are worth knowing when
something looks plain:

- **Images on iOS need the Notification Service Extension** (Android shows
  `richContent.image` itself), the
  `targets/notification-service` Swift target that `@bacons/apple-targets`
  adds at prebuild. iOS runs it for any push with `mutableContent`; it reads
  the Expo payload's `body._richContent.image`, downloads it and attaches it.
  Without the extension (Expo Go, or a build before it existed) the same push
  is delivered as text only. Changing the Swift or the target config needs a
  new native build (`eas build`), not an OTA update.
- **Action buttons are categories the server publishes.** On launch the app
  fetches `GET /api/push/categories` and registers each with
  `setNotificationCategoryAsync`, so adding a button is a Go edit
  (`app/internal/push/categories.go`). Handling a NEW action id is the one
  thing that still needs app code (`handleNotificationResponse` in
  `src/lib/push.ts`): `approve`/`deny` on a `mutation_review` alert call the
  review API in the background without opening the app; `open` (and a plain
  tap) opens `data.route`; a `reply` action's text arrives as `userText`.

## Android

The same screens, with four things that differ from iOS and are easy to
break from a Mac that only ever runs the iOS simulator:

- **Icons go through `<Icon>`** (`src/components/icon.tsx`), never
  `SymbolView` directly. `SymbolView` draws nothing on Android for a bare SF
  Symbol name, so every icon in the app was invisible there; `src/lib/icons.ts`
  maps each SF Symbol to the Material Symbol Android shows, and
  `icons.test.mjs` fails on a direct `expo-symbols` import or a Material name
  the font lacks. The Material font has no filled variants, so a filled state
  (a liked heart) reads through its colour.
- **A menu of more than three choices is `showActionSheet`**
  (`src/components/action-sheet.tsx`): `ActionSheetIOS` on iOS, a bottom
  sheet on Android, where `Alert` silently drops every button past the third.
- **iOS-only native props can crash Android outright.** `dataDetectorTypes="none"`
  on the email WebView aborted the whole app on Android (a Fabric
  `castValue` assertion) the moment a review showed an HTML email. Pass such
  props only on iOS (`emailWebViewPlatformProps`).
- **Push needs FCM.** Expo delivers to Android through Firebase Cloud
  Messaging: `google-services.json` from a Firebase project with the
  `com.zachlatta.pdw` Android app, named by `android.googleServicesFile` in
  `app.json`, and that project's FCM V1 service-account key uploaded with
  `eas credentials -p android`. The project is `pdw-zachlatta` (Firebase
  console, Zach's personal Google account); its `google-services.json` is
  committed here (client identifiers, not secrets). Without it the Android
  build has no Firebase app and `getExpoPushTokenAsync` fails with "Unable to
  get FirebaseMessaging instance"; it is native config, so it ships only in a
  new APK, not over the air. Every alert goes to the `default` channel the
  app creates at high importance (`ANDROID_CHANNEL_ID` here,
  `AndroidChannelID` in `app/internal/push`); without it an alert lands
  silently in Expo's default-importance fallback channel.

- **PDW web links open in the app** (Android App Links). The intent filter in
  `app.json` claims `https://personal-data-warehouse.zachlatta.com` under
  `/timeline`, `/mutation-review`, `/search` and `/notifications`;
  `src/app/+native-intent.tsx` maps each web path to its screen
  (`src/lib/app-links.ts`). Android trusts the claim only while the server's
  `/.well-known/assetlinks.json` lists the signing key's SHA-256
  (`app/internal/webapp/applinks.go`): a build signed with another key needs its
  digest added there. Check a phone with
  `adb shell pm get-app-links com.zachlatta.pdw` (`verified`).

- **The Feed is also the home screen's left page** on Zach's Pixel (swipe right
  on the first home page; it replaced Google Discover). `plugins/with-left-screen.js`
  adds `LeftScreenActivity`, which handles the Pixel Launcher's
  `com.android.launcher3.WINDOW_OVERLAY`; the launcher embeds it in its own task
  and runs the swipe in and out. It draws the `leftscreen` component
  (`src/features/feed/left-screen.tsx`, registered in `index.js`): the same
  `FeedScreen`, given its navigation by `feed-host.tsx` instead of expo-router.
  It must stay the only activity there, so it starts everything it opens in a
  new task (a card opens `pdw://feed/<id>` in the app). The launcher only asks
  this app because of a static overlay on the phone (sysadmin repo,
  `pichu/2026-10-25-pdw-left-screen.md`).

The preview profile builds an installable APK (`eas build -p android
--profile preview`); CI queues one whenever the Android fingerprint moves,
exactly as for iOS.

## Run it

```bash
cd mobile
npm install
npx expo start --ios          # Expo Go on the simulator; push is unsupported there
npx expo run:ios --device     # dev build on a phone (needs the Apple team in Xcode)
npx expo run:android          # dev build on an emulator or a USB/adb phone
npm run typecheck && npm run lint && npm test
```

An Android emulator on an Apple-silicon Mac: `brew install --cask zulu@17
android-commandlinetools`, then `sdkmanager "platform-tools" "emulator"
"platforms;android-36" "system-images;android-36;google_apis_playstore;arm64-v8a"`
and `avdmanager create avd`. With Metro on a fixed port, `adb reverse
tcp:<port> tcp:<port>` before every launch: the mapping is dropped whenever
the app process dies, and the dev client then reports a connection refused.

EAS project: `pdw` under the `zlatta` account (`extra.eas.projectId` in
`app.json`); bundle id `com.zachlatta.pdw`. For a real device build, run
`eas build --platform ios --profile development` after `eas credentials` has
the APNs key on file — the Expo push service needs it to reach APNs.

The server needs nothing to send: Expo accepts unauthenticated sends unless
the project turns on enhanced push security, in which case set
`PDW_EXPO_ACCESS_TOKEN` on the app deployment.
