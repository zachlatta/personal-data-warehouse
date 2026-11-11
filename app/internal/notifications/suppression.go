package notifications

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"time"
)

// Check the authoritative source immediately before EACH send, not the stale
// timeline snapshot. Missing evidence is not evidence of a read. Query errors
// stop this attempt; they must never turn into an unchecked send.
func (s *Service) suppressionReason(ctx context.Context, d Delivery) (string, error) {
	var source, raw string
	if err := s.DB.QueryRowContext(ctx, s.q(`SELECT COALESCE(payload->>'source',''),COALESCE(payload->>'source_pk','{}') FROM @notification_events WHERE id=$1`), d.NotificationID).Scan(&source, &raw); err != nil {
		return "", err
	}
	reason, err := s.sourceSuppression(ctx, source, raw)
	if reason != "" || err != nil || !isAndroid(d.Endpoint) {
		return reason, err
	}
	// The phone's evidence is an optimization, not a safety check: if it
	// cannot be read in time, send rather than hold the alert, and say so on
	// the delivery (errWitnessUnchecked). Each lookup has its own short
	// deadline, so a slow one cannot spend the worker pass's.
	phone, known, err := s.phoneOf(ctx, d.DeviceID)
	if err != nil {
		slog.Warn("phone lookup failed; sending", "notification_id", d.NotificationID, "error", err)
		return "", errWitnessUnchecked
	}
	if !known {
		return "", nil
	}
	wctx, cancel := context.WithTimeout(ctx, s.witnessTimeout())
	defer cancel()
	var shown bool
	if err = s.DB.QueryRowContext(wctx, s.q(phoneWitness), d.NotificationID, phone.account, phone.device).Scan(&shown); err != nil {
		slog.Warn("phone witness check failed; sending", "notification_id", d.NotificationID, "error", err)
		return "", errWitnessUnchecked
	}
	if shown {
		return "already_notified", nil
	}
	return "", nil
}

// DefaultWitnessTimeout bounds each phone lookup (which phone a device is,
// and whether that phone showed the event). Past it the alert is sent.
const DefaultWitnessTimeout = 3 * time.Second

func (s *Service) witnessTimeout() time.Duration {
	if s.WitnessTimeout > 0 {
		return s.WitnessTimeout
	}
	return DefaultWitnessTimeout
}

type listenerPhone struct{ account, device string }

// phoneOf is the listener phone a push device is, if known: see phoneOfDevice.
func (s *Service) phoneOf(ctx context.Context, deviceID string) (listenerPhone, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.witnessTimeout())
	defer cancel()
	var p listenerPhone
	err := s.DB.QueryRowContext(ctx, s.q(phoneOfDevice), deviceID).Scan(&p.account, &p.device)
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	return p, err == nil, err
}

// errWitnessUnchecked means the phone's notifications could not be read: the
// alert is sent anyway, and its delivery's error reads phone_witness_unchecked
// (an accepted delivery, not a failure) so the gap is visible in
// marts_ops.notification_deliveries and not only in the server log.
var errWitnessUnchecked = errors.New("phone_witness_unchecked")

// PhoneWitnessHold is how long an Android delivery waits, from the event (or
// from when it landed, if that was earlier or the event has no time), before
// it is sent, so the source's own app can post its notification and the
// phone's listener can upload it before the witness check runs. A delivery
// PDW fans out later than this is not held at all. It must stay under the
// pending threshold in marts_notification_health (notifications.py), which
// reads a delivery still pending after two minutes as stuck.
const PhoneWitnessHold = 90 * time.Second

// phoneWitnessAnchor is the moment the hold and the witness window count
// from: the event, or when it landed if the event is dated later (a calendar
// invite carries the meeting's start) or has no time (the epoch).
const phoneWitnessAnchor = `(CASE WHEN event_ts>'epoch'::timestamptz THEN least(event_ts,landed_at) ELSE landed_at END)`

// phoneWitnessWindow bounds how long after the event another app's
// notification still counts as being about it. A later notification that
// repeats the subject and body is a different event, also when this alert is
// retried or recovered after it appeared.
const phoneWitnessWindow = `interval '10 minutes'`

// phoneAssociationLookback is how far back PDW looks for its own alert on a
// phone to learn which phone a push device is. It bounds the scan for a
// device that has none; a phone that has shown no PDW alert for this long is
// unknown again until it shows one.
const phoneAssociationLookback = `interval '7 days'`

// phoneOfDevice is the listener (account, device) that most recently recorded
// PDW's own alert for a delivery to this push device: the app stamps each
// alert it draws with pdw.delivery_id. No row (a build that does not stamp,
// no listener, nothing shown lately) means the phone is unknown.
//
// The shape is for the planner. PDW's own recent rows are read once, their
// stamp extracted once (unstamped rows drop out there), and each stamp is a
// primary-key probe of the delivery that LATERAL ... LIMIT 1 keeps from being
// flattened into a join; joining the two tables directly let the planner
// rescan deliveries for every PDW row, with raw_json parsed on each pair.
const phoneOfDevice = `WITH stamped AS MATERIALIZED (
 SELECT e.account, e.device, e.event_at, e.raw_json->'extras'->>'pdw.delivery_id' AS delivery_id
 FROM @android_notification_events e
 WHERE e.package_name='com.zachlatta.pdw' AND e.event_type IN ('posted','active')
   AND e.event_at >= now() - ` + phoneAssociationLookback + `
)
SELECT s.account, s.device FROM stamped s
CROSS JOIN LATERAL (SELECT 1 FROM @notification_deliveries o WHERE o.id=s.delivery_id AND o.device_id=$1 LIMIT 1) mine
WHERE COALESCE(s.delivery_id,'')<>''
ORDER BY s.event_at DESC LIMIT 1`

// phoneWitness is true when another app on the recipient's own Android phone
// (never another phone) already posted a notification for this event, from
// base_android_notifications: any package
// but PDW's own, no group summaries, posted from two minutes before the event
// until phoneWitnessWindow after it. It is the same event when either
//
//   - the notification is a MessagingStyle one carrying a message at the
//     event's millisecond (chat apps stamp each message with its send time)
//     whose sender is the alert's actor or whose text opens like the alert's
//     snippet; the millisecond alone is not enough, since two apps' messages
//     can share one; or
//   - it shows the alert's title (an email's subject) and the alert's text
//     opening, at least 10 characters of it; a subject alone is not.
//
// Which app it was is deliberately not checked: what counts is that the phone
// already told Zach, so a source whose app does not notify (or is muted, or
// not installed) keeps paging, and a new app needs no list edit. Missing
// evidence (listener down, phone unknown) means the alert is sent.
const phoneWitness = `WITH n AS MATERIALIZED (
 SELECT event_ts, payload, ` + phoneWitnessAnchor + ` AS anchor,
   lower(btrim(COALESCE(payload->>'actor',''))) AS actor,
   btrim(COALESCE(payload->>'title','')) AS title,
   lower(left(btrim(regexp_replace(COALESCE(payload->>'snippet',''),'\s+',' ','g')),20)) AS body20,
   lower(left(btrim(regexp_replace(COALESCE(payload->>'snippet',''),'\s+',' ','g')),30)) AS body30
 FROM @notification_events WHERE id=$1)
SELECT EXISTS (
 SELECT 1 FROM n
 JOIN @android_notification_events e ON e.account=$2 AND e.device=$3
   AND e.event_at >= n.anchor - interval '2 minutes'
   AND e.event_at < n.anchor + ` + phoneWitnessWindow + `
 WHERE e.event_type IN ('posted','active') AND e.is_group_summary=0
   AND e.package_name NOT LIKE 'com.zachlatta.pdw%'
   AND (
     (n.event_ts>'epoch'::timestamptz AND jsonb_typeof(e.messages)='array' AND EXISTS (
       SELECT 1 FROM jsonb_array_elements(e.messages) m
       WHERE jsonb_typeof(m->'time_ms')='number'
         AND abs((m->>'time_ms')::numeric - extract(epoch FROM n.event_ts)*1000) < 2
         AND ((n.actor<>'' AND lower(btrim(COALESCE(m->>'sender','')))=n.actor)
           OR (n.body20<>'' AND lower(left(btrim(regexp_replace(COALESCE(m->>'text',''),'\s+',' ','g')),20))=n.body20))))
     OR (n.title<>'' AND length(n.body30)>=10
       AND strpos(lower(concat_ws(E'\n',e.title,e.text,e.big_text)), lower(n.title))>0
       AND strpos(lower(regexp_replace(concat_ws(' ',e.text,e.big_text),'\s+',' ','g')), n.body30)>0)
   ))`

// sourceSuppression is Zach's own read or reply in the source, which holds on
// every device.
func (s *Service) sourceSuppression(ctx context.Context, source, raw string) (string, error) {
	if source != "gmail" && source != "slack" && source != "apple_messages" && source != "whatsapp" {
		return "", nil
	}
	var pk map[string]string
	if err := json.Unmarshal([]byte(raw), &pk); err != nil {
		return "", err
	}
	var query string
	var args []any
	switch source {
	case "gmail":
		query = gmailSuppression
		args = []any{pk["account"], pk["message_id"]}
	case "slack":
		query = slackSuppression
		args = []any{pk["account"], pk["team_id"], pk["conversation_id"], pk["message_ts"]}
	case "apple_messages":
		query = messagesSuppression
		args = []any{pk["account"], pk["message_id"]}
	case "whatsapp":
		query = whatsappSuppression
		args = []any{pk["account"], pk["chat_id"], pk["message_id"]}
	default:
		return "", nil
	}
	for _, a := range args {
		if a == "" {
			return "", nil
		}
	}
	var reason string
	err := s.DB.QueryRowContext(ctx, s.q(query), args...).Scan(&reason)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return reason, err
}

const gmailSuppression = `SELECT CASE
 WHEN NOT ('UNREAD'=ANY(m.label_ids)) THEN 'already_read'
 WHEN m.thread_id<>'' AND EXISTS (
   SELECT 1 FROM @gmail_messages r WHERE r.account=m.account AND r.thread_id=m.thread_id
   AND r.internal_date>m.internal_date AND r.is_deleted=0 AND 'SENT'=ANY(r.label_ids)
 ) THEN 'already_replied' ELSE '' END
 FROM @gmail_messages m WHERE m.account=$1 AND m.message_id=$2`

// A channel read cursor does NOT prove a thread was read. For replies use only
// the parent message's explicit thread cursor. Posting elsewhere in a channel
// or another thread does not suppress this item.
const slackSuppression = `SELECT CASE
 WHEN (CASE WHEN watermark.value ~ '^[0-9]+([.][0-9]+)?$' THEN watermark.value::numeric ELSE 0 END)
   >= (CASE WHEN m.message_ts ~ '^[0-9]+([.][0-9]+)?$' THEN m.message_ts::numeric ELSE NULL END)
 THEN 'already_read'
 WHEN i.user_id<>'' AND EXISTS (
   SELECT 1 FROM @slack_messages r
   WHERE r.account=m.account AND r.team_id=m.team_id AND r.conversation_id=m.conversation_id
   AND r.user_id=i.user_id AND r.is_deleted=0 AND r.bot_id=''
   AND r.subtype IN ('','file_share','me_message','thread_broadcast')
   AND r.message_datetime>m.message_datetime
   AND (r.thread_ts=COALESCE(NULLIF(m.thread_ts,''),m.message_ts)
        OR (c.is_im=1 AND COALESCE(NULLIF(m.thread_ts,''),m.message_ts)=m.message_ts
            AND (r.thread_ts='' OR r.thread_ts=r.message_ts)))
 ) THEN 'already_replied' ELSE '' END
 FROM @slack_messages m
 LEFT JOIN @slack_conversations c ON c.account=m.account AND c.team_id=m.team_id AND c.conversation_id=m.conversation_id
 LEFT JOIN @slack_account_identities i ON i.account=m.account AND i.team_id=m.team_id
 LEFT JOIN @slack_messages p ON p.account=m.account AND p.team_id=m.team_id
   AND p.conversation_id=m.conversation_id AND p.message_ts=m.thread_ts
 CROSS JOIN LATERAL (SELECT CASE WHEN m.thread_ts<>'' AND m.thread_ts<>m.message_ts
   THEN NULLIF(p.raw_json,'')::jsonb->>'last_read'
   ELSE NULLIF(c.raw_json,'')::jsonb->>'last_read' END AS value) watermark
 WHERE m.account=$1 AND m.team_id=$2 AND m.conversation_id=$3 AND m.message_ts=$4`

// is_read on incoming messages is local read state; an outgoing date_read is
// someone else's receipt. A group response must explicitly quote this message.
const messagesSuppression = `SELECT CASE
 WHEN m.is_from_me=0 AND (m.is_read=1 OR m.date_read>'epoch'::timestamptz) THEN 'already_read'
 WHEN EXISTS (
   SELECT 1 FROM @apple_message_chat_messages cm
   JOIN @apple_message_chats c ON c.account=cm.account AND c.chat_id=cm.chat_id
   JOIN @apple_message_chat_messages rm ON rm.account=cm.account AND rm.chat_id=cm.chat_id
     AND rm.message_date>m.message_at
   JOIN @apple_messages r ON r.account=rm.account AND r.message_id=rm.message_id
   WHERE cm.account=m.account AND cm.message_id=m.message_id
     AND r.is_from_me=1 AND r.is_sent=1 AND r.is_deleted=0 AND r.message_at>m.message_at
     AND r.associated_message_type=0 AND r.is_system_message=0 AND r.is_service_message=0
     AND (c.style=45 OR r.reply_to_guid=m.message_id)
 ) THEN 'already_replied' ELSE '' END
 FROM @apple_messages m WHERE m.account=$1 AND m.message_id=$2`

// WhatsApp's synced corpus has no verified per-message local read watermark.
// Do not mistake an absent/default unread count or a recipient's receipt for
// the user's read. Direct-chat responses or explicit group quotes are evidence.
const whatsappSuppression = `SELECT CASE WHEN EXISTS (
 SELECT 1 FROM @whatsapp_messages r
 WHERE r.account=m.account AND r.chat_id=m.chat_id AND r.message_at>m.message_at
   AND r.is_from_me=1 AND r.is_deleted=0 AND r.message_kind IN ('text','image','video','voice','audio','document','sticker','contact','location','live_location','poll')
   AND (r.quoted_message_id=m.message_id
        OR m.chat_id LIKE '%@s.whatsapp.net' OR m.chat_id LIKE '%@lid')
 ) THEN 'already_replied' ELSE '' END
 FROM @whatsapp_messages m WHERE m.account=$1 AND m.chat_id=$2 AND m.message_id=$3`
