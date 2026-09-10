package notifications

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// Check the authoritative source immediately before EACH send, not the stale
// timeline snapshot. Missing evidence is not evidence of a read. Query errors
// stop this attempt; they must never turn into an unchecked send.
func (s *Service) suppressionReason(ctx context.Context, d Delivery) (string, error) {
	var source, raw string
	if err := s.DB.QueryRowContext(ctx, s.q(`SELECT payload->>'source',COALESCE(payload->>'source_pk','{}') FROM @notification_events WHERE id=$1`), d.NotificationID).Scan(&source, &raw); err != nil {
		return "", err
	}
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
