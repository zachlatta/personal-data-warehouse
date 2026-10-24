package androidmessages

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Google Messages' own database (bugle_db) is the only store on the phone
// that holds every text: Android's telephony provider (mmssms.db) has SMS and
// MMS but not RCS chats. Its schema is undocumented; the columns read here
// are the long-stable ones (AOSP Messaging's names) and a column a future
// version drops fails the run loudly instead of silently sending less.

const messagesQuery = `
SELECT
  m._id AS bugle_id,
  m.conversation_id AS conversation_id,
  m.sender_id AS sender_id,
  m.self_id AS self_id,
  COALESCE(m.sent_timestamp, 0) AS sent_timestamp_ms,
  COALESCE(m.received_timestamp, 0) AS received_timestamp_ms,
  COALESCE(m.message_protocol, -1) AS message_protocol,
  COALESCE(m.message_status, -1) AS message_status,
  COALESCE(m.seen, 0) AS seen,
  COALESCE(m.read, 0) AS read,
  COALESCE(m.sms_message_uri, '') AS telephony_uri,
  COALESCE(m.mms_subject, '') AS subject,
  COALESCE(CAST(m.rcs_message_id AS TEXT), '') AS rcs_message_id,
  COALESCE(m.is_hidden, 0) AS is_hidden,
  COALESCE(m.sms_error_code, 0) AS sms_error_code,
  COALESCE(c.name, '') AS conversation_name,
  COALESCE(c.participant_count, 0) AS participant_count,
  COALESCE(c.conv_type, 0) AS conversation_type,
  COALESCE(c.rcs_group_id, '') AS rcs_group_id,
  COALESCE(sp.normalized_destination, '') AS sender_address,
  COALESCE(sp.full_name, '') AS sender_name,
  COALESCE(me.normalized_destination, '') AS self_address,
  COALESCE(me.subscription_name, '') AS sim_name
FROM messages m
LEFT JOIN conversations c ON c._id = m.conversation_id
LEFT JOIN participants sp ON sp._id = m.sender_id
LEFT JOIN participants me ON me._id = m.self_id
WHERE m.received_timestamp >= ?1
  AND (m._id > ?2 OR m.received_timestamp >= ?3)
ORDER BY m._id`

const partsQuery = `
SELECT
  p.message_id AS message_id,
  p._id AS part_id,
  COALESCE(p.content_type, '') AS content_type,
  COALESCE(p.text, '') AS text,
  COALESCE(p.file_name, '') AS file_name,
  COALESCE(p.width, 0) AS width,
  COALESCE(p.height, 0) AS height,
  COALESCE(p.duration, 0) AS duration_ms,
  COALESCE(p.file_size_bytes, 0) AS file_size_bytes
FROM parts p
JOIN messages m ON m._id = p.message_id
WHERE m.received_timestamp >= ?1
  AND (m._id > ?2 OR m.received_timestamp >= ?3)
ORDER BY p.message_id, p._id`

const participantsQuery = `
SELECT
  cp.conversation_id AS conversation_id,
  COALESCE(p.normalized_destination, '') AS address,
  COALESCE(p.full_name, '') AS name
FROM conversation_participants cp
JOIN participants p ON p._id = cp.participant_id
WHERE cp.conversation_id IN (
  SELECT DISTINCT m.conversation_id FROM messages m
  WHERE m.received_timestamp >= ?1
    AND (m._id > ?2 OR m.received_timestamp >= ?3)
)
ORDER BY cp.conversation_id, p.normalized_destination`

// readMessages returns every message received at or after sinceMS that is
// either newer than afterID or received at or after recentMS (the window
// re-read each run so a status or read change is sent again), oldest id
// first, each with its parts and its conversation's other participants.
func readMessages(db *sql.DB, sinceMS, afterID, recentMS int64) ([]map[string]any, error) {
	rows, err := common.Query(db, messagesQuery, sinceMS, afterID, recentMS)
	if err != nil {
		return nil, fmt.Errorf("read messages: %w", err)
	}
	partRows, err := common.Query(db, partsQuery, sinceMS, afterID, recentMS)
	if err != nil {
		return nil, fmt.Errorf("read parts: %w", err)
	}
	participantRows, err := common.Query(db, participantsQuery, sinceMS, afterID, recentMS)
	if err != nil {
		return nil, fmt.Errorf("read participants: %w", err)
	}
	parts := map[int64][]map[string]any{}
	for _, row := range partRows {
		id := intOf(row.Values["message_id"])
		parts[id] = append(parts[id], map[string]any{
			"part_id":         intOf(row.Values["part_id"]),
			"content_type":    stringOf(row.Values["content_type"]),
			"text":            stringOf(row.Values["text"]),
			"file_name":       stringOf(row.Values["file_name"]),
			"width":           intOf(row.Values["width"]),
			"height":          intOf(row.Values["height"]),
			"duration_ms":     intOf(row.Values["duration_ms"]),
			"file_size_bytes": intOf(row.Values["file_size_bytes"]),
		})
	}
	participants := map[int64][]map[string]any{}
	for _, row := range participantRows {
		id := intOf(row.Values["conversation_id"])
		participants[id] = append(participants[id], map[string]any{
			"address": stringOf(row.Values["address"]),
			"name":    stringOf(row.Values["name"]),
		})
	}
	var out []map[string]any
	for _, row := range rows {
		message := map[string]any{}
		for _, column := range row.Columns {
			message[column] = row.Values[column]
		}
		id := intOf(row.Values["bugle_id"])
		conversation := intOf(row.Values["conversation_id"])
		message["parts"] = nonNil(parts[id])
		message["participants"] = nonNil(participants[conversation])
		message["chat_key"] = chatKey(stringOf(row.Values["rcs_group_id"]), participants[conversation])
		message["message_key"] = messageKey(message)
		out = append(out, message)
	}
	return out, nil
}

// messageKey is the message's identity in the warehouse. Google Messages'
// own row id restarts when its data is cleared (it then re-imports from the
// telephony store), so a message is keyed by what survives that: the RCS
// message id, else its row in Android's telephony store.
func messageKey(message map[string]any) string {
	if id := stringOf(message["rcs_message_id"]); id != "" {
		return "rcs:" + id
	}
	if uri := stringOf(message["telephony_uri"]); uri != "" {
		return uri
	}
	return "bugle:" + strconv.FormatInt(intOf(message["bugle_id"]), 10)
}

// chatKey names the conversation the same way across a Messages reset: an
// RCS group's id, else the other participants' addresses, sorted.
func chatKey(rcsGroupID string, participants []map[string]any) string {
	if rcsGroupID != "" {
		return "rcs-group:" + rcsGroupID
	}
	var addresses []string
	for _, p := range participants {
		if address := stringOf(p["address"]); address != "" {
			addresses = append(addresses, address)
		}
	}
	sort.Strings(addresses)
	return strings.Join(addresses, ",")
}

func nonNil(list []map[string]any) []map[string]any {
	if list == nil {
		return []map[string]any{}
	}
	return list
}

func intOf(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	default:
		return 0
	}
}

func stringOf(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}
