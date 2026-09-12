# Timeline notification experiment

PDW captures **every newly inserted direct or cc timeline event** and delivers eligible items to its
registered iPhone and Web Push devices. /notifications (the **alerts** tab) is
the control panel and recent delivery/open ledger; the iPhone app's Alerts tab
has the same global on/off switch and pages through the whole ledger. /pipelines shows the live notification health verdict.

This is an experiment, not yet a lossless or instant replacement for native
notifications. Leave original notifications available until you have tried the
sources that matter. The implementation does not change other apps' OS settings.

## Try it

1. Open the PDW iPhone app, apply the latest update in Settings, and allow
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

## Capture, retry and evidence semantics

- An AFTER INSERT trigger captures the bounded source snapshot in the source
  transaction. A rollback produces no alert. Only direct/cc inserts qualify:
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

For message sources, compare landed_at - event_ts separately. Calendar
event_ts is the scheduled meeting time, not creation time, so that subtraction
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
