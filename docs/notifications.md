# Timeline notification experiment

PDW captures **every newly inserted `direct` timeline event** and delivers eligible items to its
registered iPhone and Web Push devices. It captured `cc` as well until 2026-09-20: in the week
the experiment was on, Hacker News thread chatter (then `cc`) produced 17,908 pushes, Slack `cc`
1,136 at a 2% open rate with 651 of them over an hour old when sent, against 171 Slack `direct`
at 7% and 100 iMessages at 18%. `cc` is for reading, not paging. A recurring calendar invite
pages once per series, not once per expanded instance (one weekly team sync produced 53 pushes).
/notifications (the **alerts** tab) is
the control panel and recent delivery/open ledger; the phone app's Alerts tab
has the same global on/off switch and pages through the whole ledger. /pipelines shows the live notification health verdict.

This is an experiment, not yet a lossless or instant replacement for native
notifications. Leave original notifications available until you have tried the
sources that matter. The implementation does not change other apps' OS settings.

## Try it

1. Open the PDW phone app, apply the latest update in Settings, and allow
   notifications. Use **Send test push** to verify that installation's push setup.
   That existing manual test is not part of the timeline ledger.
2. On each computer, open /notifications in your usual browser and choose
   **Enable this browser**. Grant the browser and OS notification permissions.
   The page need not remain open, but the OS/browser must permit background push.
3. Choose **Start experiment** once. It affects all registered devices. Existing
   history is not replayed; a new device does not receive old queued alerts.
4. Tap real alerts. Check the ledger's observed opens. Try Slack threads, mail,
   calendar, and your actual message conversations before muting native apps.
5. **Pause all timeline notifications** stops capture and cancels queued work.
   An in-flight or already accepted push cannot be retracted. Paused arrivals
   are not replayed on resume. **Disable this browser** affects only that device.

Notification previews can expose message content on the lock screen and transmit
that content to the configured push provider. Set OS preview visibility to suit
your privacy preferences. The experiment deliberately includes CC, which can be
a high-volume tier.

## What a tap can do

Both the phone and timeline use the same source-link builder, not competing URL
guesses. Navigation starts before telemetry networking.

| Source | Destination |
| --- | --- |
| Slack | Native workspace/channel/message link on iPhone, thread-aware web permalink on browsers |
| Gmail | Account-qualified web message link; no invented native Gmail message scheme |
| Calendar / Drive | Provider event/file URL; native handoff depends on the installed app and OS |
| Messages | Native one-to-one conversation when a usable phone/email handle exists |
| WhatsApp | Phone-number conversation when the JID contains a verified phone number |
| Unsupported/group/LID chat | Exact PDW timeline item, not a guessed person or unrelated conversation |

A conversation link is not an exact-message link. A successful URL launch is not
proof that the provider selected the expected message; test actual devices.
Browsers prefer HTTPS source links and otherwise open the exact PDW event.

iOS always retains **PDW's app name/icon in the notification header**. Source
artwork appears as a rich-content thumbnail through the existing notification
service extension. PDW cannot impersonate another app's notification identity.
Browsers likewise retain their own OS attribution. Icons are served by PDW;
source attribution/provenance is in the static notification-icons/README.txt.

## Already read or replied to

Immediately before **each device send and retry**, the worker checks current
synced source rows, not the captured timeline snapshot. A confirmed read or response
skips that delivery with status `suppressed` and `error = already_read` or
`already_replied` in marts_ops.notification_deliveries. These are intentional
skips, not failures. The web/mobile ledger shows separate read/reply skip counts.

- **Gmail:** no UNREAD label on that message, or a later SENT message in the same
  account/thread. Sending in another thread or account does not count.
- **Slack:** the conversation's read cursor covers a top-level message; a thread
  reply requires the parent thread's own explicit read cursor, never a channel
  cursor. A later message by the authenticated user in that exact thread counts
  as a response. In a one-to-one DM, a later top-level message also counts.
  Unrelated channel/group posts and other threads do not.
- **Messages:** an incoming message's is_read/date_read, or a later successfully
  sent message in the same one-to-one chat. A group response must explicitly
  quote the original message. Reactions and unsent messages do not count.
- **WhatsApp:** a later outgoing message in the same direct chat, or an explicit
  quoted response in a group. The synced data does not yet provide a verified
  local per-message read watermark, so read-only suppression is not claimed.
- Other sources and missing read/reply evidence still notify. Source query errors
  stop an unchecked send and surface through worker health.

A later direct-chat message is a conservative conversation-level response heuristic,
not proof that it answered each earlier question. Group suppression is narrower.
These checks use **synced evidence**: reading/replying in an app can still race its
next sync or the final provider request. Already accepted pushes cannot be recalled.
Notification taps alone are not proof that source content was read.

## Already shown on the Android phone

An **Android** delivery is also skipped, with `error = already_notified`, when another
app on **the recipient's own phone** already posted a notification for the same event
(`base_android_notifications.events`, from the phone's notification listener). iOS and
web deliveries are unaffected: the phone's evidence says nothing about them.

**Which phone.** The app stamps every alert it draws with its delivery id
(`pdw.delivery_id`, a string extra the listener uploads). A push device's phone is the
listener `account`/`device` that recorded PDW's own alert for an earlier delivery to that
push device, within the last seven days. A device with no such alert (an app build that
does not stamp, a phone without the listener, nothing shown lately) has no known phone and
gets every alert. Another phone's notifications never decide it. The stamp is native code
(`mobile/plugins/with-messaging-notifications.js`): it reaches a phone with a new native
build, not an OTA update, and until then that phone gets every alert.

**Which event.** It is the same event when either:

- the phone's notification is a MessagingStyle one carrying a message at the event's own
  millisecond (chat apps stamp each message with its send time) **and** that message's
  sender is the alert's actor or its text opens with the same 20 characters as the
  alert's snippet. The millisecond alone is not enough, since two apps' messages can
  share one; or
- it shows the alert's title (an email app shows the subject) and the first 30
  characters of the alert's text, of which there must be at least 10. A subject alone, or
  a subject with an empty or very short body, is not proof of the same email.

Any package but PDW's own counts, and group summaries do not. The notification must be
posted between two minutes before the event and ten minutes after it: a later one that
repeats the subject and body is another event, also when this alert is retried or
recovered after it appeared.

Which app showed it is deliberately not checked. The question is whether the phone
already told Zach, not which sources have an app: an app can hold back some of its
source's messages (Slack, for one, while the desktop is active, and per channel), so a
per-source list would either drop messages nobody else showed or keep paging duplicates.
The per-event check needs no edit when an app is installed, muted or removed.

**Fail open.** If the phone's notifications cannot be read, the worker logs a warning
and sends, and the delivery's `error` reads `phone_witness_unchecked` (an accepted
delivery, not a failure; Expo's receipt keeps the note). Each phone lookup (which phone a
device is, and whether that phone showed the event) has its own short deadline
(`DefaultWitnessTimeout`), so a slow lookup ends as an unchecked send instead of spending
the worker pass's deadline. The read/reply checks above, by contrast, stop the attempt.

**Query shape.** The phone lookup (`phoneOfDevice`) reads PDW's own recent rows once,
extracts each stamp once, and probes the delivery's primary key per stamp through
`LATERAL ... LIMIT 1`. Joining the listener rows to the delivery table directly let the
planner rescan the deliveries for every PDW row, parsing `raw_json` on each pair, which is
slowest exactly when nothing is stamped yet. Check a change here with
`EXPLAIN (ANALYZE, BUFFERS)` against production-sized tables, with the parameters hidden
from the planner (`(SELECT 'value'::text)`), as a prepared statement's generic plan sees
them.

**The hold.** So the other app's notification can be posted and uploaded before the
check runs, an Android delivery **to a device whose phone is known** is created with
`next_attempt_at` 90 s after the event
(`PhoneWitnessHold`), or after it landed when that was earlier or the event has no time (a
calendar invite carries the meeting's start). A delivery fanned out later than that is not
held, and neither is a device with no known phone (or whose lookup failed): there is no
evidence to wait for, so it is sent as soon as it is fanned out. The hold must stay under the two minutes after which
`marts_ops.notification_health` reads a pending delivery as stuck;
`test_the_phone_witness_hold_ends_before_health_calls_a_pending_delivery_stuck` pins it.
A witness that lands after the hold is missed, and the alert is sent.

## Capture, retry and evidence semantics

- An AFTER INSERT trigger captures the bounded source snapshot in the source
  transaction. A rollback produces no alert. Only `direct` inserts qualify:
  edits, enrichment, reclassification and update-only backfills never notify again.
  A genuinely new historical insertion **does** qualify while enabled.
- Installation starts paused. The state lock serializes capture/fanout with pause.
  Trigger installation uses a bounded DDL lock and does not re-lock the large
  timeline on every normal sync.
- The app worker drains every five seconds, atomically fixes the recipient set,
  and uses exclusive, expiring, fenced delivery leases. Six send attempts are
  allowed with bounded exponential delays. Invalid devices are retired.
- There is one outbox row per timeline key and one delivery row per device.
  Network ambiguity or a process crash can still cause an at-least-once resend.
  Stable collapse IDs/tags reduce duplicates; **exactly-once display is not
  promised**. Separate timeline rows for duplicate provider data remain separate.
- accepted_at means Expo/the Web Push service accepted the request. Expo
  receipts are checked after 15 minutes; provider_accepted means the provider
  accepted handoff, **not** that the screen displayed anything. Missing receipts
  become unknown after 23 hours. A final provider error is retained too.
- opened_at is the server's first observation of a tap, not a read receipt or
  the physical tap time. Offline uploads arrive later. Phone Keychain records
  have a replayable enqueue journal; browsers retain taps transactionally in
  IndexedDB. Phone uploads retry on PDW foreground; browsers retry on new pushes
  or focus/online events on the notification page. OS background limits still apply.
- Missing opens can mean ignored, dismissed, offline, an old app build, disabled
  background execution, lost local storage, or undelivered. Do not label them
  “ignored” without independent evidence.
- A delivery-specific HMAC capability authorizes only an idempotent open stamp.
  It cannot read content. Service workers never store the warehouse bearer.
  Rotating PDW_SECRET_TOKEN invalidates old pending open capabilities.
- Ledger history is retained, not automatically pruned. Device endpoints,
  subscriptions, capabilities and rendered private transport payloads are hidden
  from the read-only query role. Public marts expose safe outcome fields and
  translate every epoch timestamp to NULL.

## Measure and iterate

Use marts_ops.notifications, marts_ops.notification_deliveries, and
marts_ops.notification_health. Notification instrumentation is explicitly
classified as state, not another real-world timeline event; it must not create
a notification feedback loop.

Start with health:

~~~sql
SELECT * FROM marts_ops.notification_health;
~~~

`marts_ops.notification_health` also reconciles the last 24 hours of `provider_accepted`
Android pushes against `base_android_notifications.events` (`android_accepted_24h`,
`android_posted_24h`, `android_missing_24h`). To avoid false alarms while the phone is
offline or between upload batches, only deliveries accepted more than 15 minutes ago and
at least 2 minutes before `LEAST(max(synced_at), max(event_at))` on
`base_android_notifications.events` are evaluated, matching phone events in
`[accepted_at - 10s, accepted_at + 12h)` (bounded by `event_at >= now() - interval '24 hours 10 seconds'`).
Chat pushes (`payload ? 'android_style'`) match by `pdw.event_ids` (newline-delimited
string extra), top-level `text`, or `messages[]` element text; non-chat pushes match on
`channel_id = 'default'` by `title` and `text`. When `android_missing_24h > 0`, `status`
becomes `attention` and `error` reports how many accepted Android alerts never appeared on
the phone. (In the 2026-10-28..11-06 audit of 861 chat and 715 non-chat Android pushes,
66 delayed chat alerts arrived on reconnect, 69 view-matched / 71 exact-timestamp chat
alerts and 234 non-chat alerts never reached the phone—64/62 chat and 215 non-chat inside
six windows where `com.zachlatta.pdw` hit Android's 50-notification per-package limit,
5–9 chat alerts in sub-second bursts, and 2 unexplained non-chat drops on
2026-11-02 20:01..20:05Z when ~5 notifications were active in the shade.)

Compare device-delivery cohorts by source and tier, allowing a day for opens:

~~~sql
SELECT n.source, n.priority, d.transport,
       count(*) AS attempts_to_devices,
       count(*) FILTER (WHERE d.accepted_at IS NOT NULL) AS accepted,
       count(*) FILTER (WHERE d.opened_at IS NOT NULL) AS observed_opens,
       count(*) FILTER (WHERE d.status IN ('failed','unknown')) AS failed_or_unknown,
       round(100.0 * count(*) FILTER (WHERE d.opened_at IS NOT NULL)
             / NULLIF(count(*) FILTER (WHERE d.accepted_at IS NOT NULL), 0), 1)
         AS observed_open_percent_of_accepted
FROM marts_ops.notifications n
JOIN marts_ops.notification_deliveries d ON d.notification_id = n.id
WHERE n.created_at >= now() - interval '8 days'
  AND n.created_at < now() - interval '1 day'
GROUP BY n.source, n.priority, d.transport;
~~~

The denominator is per device, not per unique event or person. An observed open
can arrive even when a provider acknowledgement was lost; investigate rather
than silently clamping a rate.

Separate warehouse-to-push delay from the source's upstream sync delay:

~~~sql
SELECT n.source,
       percentile_cont(0.5) WITHIN GROUP
         (ORDER BY extract(epoch FROM d.accepted_at - n.landed_at)) AS push_p50_seconds,
       percentile_cont(0.95) WITHIN GROUP
         (ORDER BY extract(epoch FROM d.accepted_at - n.landed_at)) AS push_p95_seconds
FROM marts_ops.notifications n
JOIN marts_ops.notification_deliveries d ON d.notification_id = n.id
WHERE n.created_at >= now() - interval '1 day'
  AND d.accepted_at IS NOT NULL
GROUP BY n.source;
~~~

For message sources, compare landed_at - occurred_at separately. Calendar
occurred_at is the scheduled meeting time, not creation time, so that subtraction
does **not** measure calendar sync latency.

A pre-rollout stratified sample of 44 direct/cc rows across six sources on
2026-09-09 found 34 source links and 10 honest PDW fallbacks (three Messages
groups and seven WhatsApp group/LID chats). All 10 Slack samples had native
message links. The largest rendered preview was 1,160 UTF-8 bytes before the
transport envelope. This is a small quality sample, not a coverage guarantee.
Past-day message activity also showed minutes of upstream landing delay and
hundreds of Slack CC events; a five-second push worker cannot erase that delay
or make CC quiet.

## Operator setup and verification

Python owns DDL through ensure_timeline_tables; the catalog is the sole schema
authority. The Go app runs delivery alongside HTTP and shuts down with it.
Deploy both app and Dagster revisions, and verify the trigger and live heartbeat
rather than assuming a webhook completed.

Native delivery uses the existing Expo credentials/token registration. For Web
Push, set a persistent matching P-256 VAPID pair in the **app's runtime env**:

- PDW_WEB_PUSH_PUBLIC_KEY: base64url unpadded uncompressed P-256 public point
- PDW_WEB_PUSH_PRIVATE_KEY: base64url unpadded 32-byte private scalar

Do not rotate VAPID keys casually: existing subscriptions are bound to them and
must re-register. Missing keys disable web registration; partial or mismatched
keys fail startup. Never put keys in git, a browser, or a test fixture.

The authenticated API is /api/notifications (newest first, `limit` up to 500,
`before=<next_cursor>` to page; the cursor is the `(created_at, id)` keyset so a
burst created in one instant is neither skipped nor repeated), with POST
/settings, /web/register, and /web/disable. Only /opened is capability-authenticated
instead of bearer-authenticated. Request bodies are bounded and strict.
Subscription destinations are allowlisted push services, with redirects refused.

Before landing changes:

~~~sh
uv run pytest
(cd app && go test ./...)
(cd mobile && npx tsc --noEmit && npx expo lint && npm test)
~~~

Also inspect desktop and phone-width layouts and test actual delivery, the
notification image, cold-start/background taps, and offline opens on hardware.
Automated transport tests and source-link sampling cannot prove OS display.
