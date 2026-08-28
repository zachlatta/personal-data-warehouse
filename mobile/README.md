# PDW iOS app

An Expo (React Native) app over the warehouse's own HTTP API: the unified
timeline, the mutation review queue, and push notifications when a new
mutation request needs a decision.

The app talks to the same Go app the `pdw` CLI does and authenticates the same
way — a static bearer `"<client_name>:<PDW_SECRET_TOKEN>"` — so signing in is
pasting the app URL and the secret token once. The token lives in the iOS
Keychain (`expo-secure-store`).

| screen | endpoint |
| --- | --- |
| Timeline (tiers default to `self`, `direct`, `cc`) | `GET /api/timeline`, `GET /api/timeline/item` |
| Mutations (needs review / past) | `GET /api/mutations/requests[?status=…]` |
| Review one request: approve, deny, skip an email | `GET /api/mutations/requests/{id}`, `POST …/approve`, `POST …/reject`, `POST …/mutations/{mid}/remove` |
| Settings: push registration, test push, disconnect | `POST /api/push/register`, `POST /api/push/test` |

Push goes through the Expo push service: the app registers its
`ExponentPushToken[...]` with the server (`private.push_devices`), and the Go
app fans a notification out to every active device when a request lands in
`pending_review` (tapping it opens that request). A `DeviceNotRegistered`
ticket retires the device row rather than silently shrinking the fan-out.

## Rich notifications

Alerts can carry a subtitle, an image, action buttons, a thread, a collapse
id, an interruption level and a badge. The server builds them
(`app/internal/push.Notification`) and there are three ways to send one:

```bash
pdw call notify --data '{"title":"Wire Mosaic","subtitle":"YC SAFEs","body":"Instructions in inbox.","image_url":"https://…/receipt.png","category":"link","route":"/timeline","interruption_level":"time-sensitive"}'
curl -X POST "$PDW_API_URL/api/push/send" -H "Authorization: Bearer cli:$PDW_SECRET_TOKEN" -d '{"title":"…"}'
# Settings → "Send test push" sends one with an image, subtitle and Open button.
```

Two pieces of the app make that work, and both are worth knowing when
something looks plain:

- **Images need the Notification Service Extension**, the
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

## Run it

```bash
cd mobile
npm install
npx expo start --ios          # Expo Go on the simulator; push is unsupported there
npx expo run:ios --device     # dev build on a phone (needs the Apple team in Xcode)
npm run typecheck && npm run lint
```

EAS project: `pdw` under the `zlatta` account (`extra.eas.projectId` in
`app.json`); bundle id `com.zachlatta.pdw`. For a real device build, run
`eas build --platform ios --profile development` after `eas credentials` has
the APNs key on file — the Expo push service needs it to reach APNs.

The server needs nothing to send: Expo accepts unauthenticated sends unless
the project turns on enhanced push security, in which case set
`PDW_EXPO_ACCESS_TOKEN` on the app deployment.
