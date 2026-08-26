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
