package androidmessages

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"unicode/utf8"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Messages keeps what happens to a message after it arrives in side tables
// keyed by the message's row: a reaction (itself a message row) names the
// row it reacts to, an edit names the row that replaced it, a reply the row
// it answers. Each is read for the same window of messages as the messages
// themselves and attached to its message under one name.

// windowIDs is the message rows a run reads (see messagesQuery).
const windowIDs = `SELECT m._id FROM messages m
  WHERE m.received_timestamp >= ?1 AND (m._id > ?2 OR m.received_timestamp >= ?3)`

// referencedKey is messageKey in SQL, for a message another row points to.
// That message can be older than --since (a reaction to a text from before
// the switch); only its key is read, never its content.
func referencedKey(alias string) string {
	return fmt.Sprintf(`CASE
    WHEN COALESCE(CAST(%[1]s.rcs_message_id AS TEXT), '') <> '' THEN 'rcs:' || CAST(%[1]s.rcs_message_id AS TEXT)
    WHEN COALESCE(%[1]s.sms_message_uri, '') <> '' THEN %[1]s.sms_message_uri
    WHEN %[1]s._id IS NOT NULL THEN 'bugle:' || %[1]s._id
    ELSE '' END`, alias)
}

var (
	reactionQuery = `
SELECT r.message_id AS bugle_id,
  COALESCE(r.reacted_message_id, 0) AS reacted_bugle_id,
  ` + referencedKey("t") + ` AS reacted_message_key,
  COALESCE(r.reaction, 0) AS reaction_type,
  COALESCE(hex(r.applied_reaction), '') AS applied_reaction,
  COALESCE(hex(r.animation_effect), '') AS animation_effect,
  COALESCE(hex(r.reactions_data), '') AS reactions_data
FROM message_reactions r
LEFT JOIN messages t ON t._id = r.reacted_message_id
WHERE r.message_id IN (` + windowIDs + `)`

	editQuery = `
SELECT e.message_id AS bugle_id,
  COALESCE(e.latest_message_id, 0) AS latest_bugle_id,
  ` + referencedKey("l") + ` AS latest_message_key,
  ` + referencedKey("o") + ` AS original_message_key,
  COALESCE(e.original_rcs_message_id, '') AS original_rcs_message_id,
  COALESCE(e.edited_at_timestamp_ms, 0) AS edited_at_ms,
  COALESCE(e.received_at_timestamp_ms, 0) AS received_at_ms
FROM message_edits e
LEFT JOIN messages l ON l._id = e.latest_message_id
LEFT JOIN messages o ON o._id = e.message_id
WHERE e.message_id IN (` + windowIDs + `) OR e.latest_message_id IN (` + windowIDs + `)`

	replyQuery = `
SELECT r.message_id AS bugle_id,
  COALESCE(r.replied_to_message_id, 0) AS replied_to_bugle_id,
  CASE WHEN r.replied_to_message_id IS NULL THEN '' ELSE ` + referencedKey("t") + ` END AS replied_to_message_key,
  COALESCE(r.replied_to_rcs_message_id, '') AS replied_to_rcs_message_id,
  COALESCE(r.replied_to_message_id_null_reason, 0) AS replied_to_null_reason,
  COALESCE(r.message_thread_id, '') AS thread_id
FROM message_replies r
LEFT JOIN messages t ON t._id = r.replied_to_message_id
WHERE r.message_id IN (` + windowIDs + `)`

	captionQuery = `
SELECT c.message_id AS bugle_id, c.caption AS caption
FROM message_captions c WHERE c.message_id IN (` + windowIDs + `)`

	starQuery = `
SELECT s.message_id AS bugle_id FROM message_star s WHERE s.message_id IN (` + windowIDs + `)`

	selfMentionQuery = `
SELECT s.message_id AS bugle_id FROM message_self_mention s WHERE s.message_id IN (` + windowIDs + `)`

	linkPreviewQuery = `
SELECT l.message_id AS bugle_id,
  COALESCE(l.trigger_url, '') AS url,
  COALESCE(l.link_title, '') AS title,
  COALESCE(l.link_description, '') AS description,
  COALESCE(l.link_image_url, '') AS image_url,
  COALESCE(l.link_domain, '') AS domain,
  COALESCE(l.link_canonical_url, '') AS canonical_url,
  COALESCE(l.favicon_url, '') AS favicon_url,
  COALESCE(l.link_preview_prevented, 0) AS prevented,
  COALESCE(l.link_preview_failed, 0) AS failed
FROM link_preview l WHERE l.message_id IN (` + windowIDs + `)`

	readReportQuery = `
SELECT r.message_id AS bugle_id,
  COALESCE(p.normalized_destination, '') AS address,
  COALESCE(p.full_name, '') AS name,
  COALESCE(r.receive_time, 0) AS delivered_at_ms,
  COALESCE(r.read_time, 0) AS read_at_ms
FROM read_reports r
LEFT JOIN participants p ON p._id = r.participant_id
WHERE r.message_id IN (` + windowIDs + `)
ORDER BY r.message_id, r._id`

	annotationQuery = `
SELECT a.message_id AS bugle_id,
  COALESCE(a.annotation_type, 0) AS annotation_type,
  COALESCE(hex(a.annotation_details), '') AS annotation_details
FROM messages_annotations a
WHERE a.message_id IN (` + windowIDs + `)
ORDER BY a.message_id, a._id`
)

// readExtras returns, per extra's name, its value for each message row that
// has one.
func readExtras(db *sql.DB, sinceMS, afterID, recentMS int64) (map[string]map[int64]any, error) {
	query := func(name, statement string) ([]common.Row, error) {
		rows, err := common.Query(db, statement, sinceMS, afterID, recentMS)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		return rows, nil
	}
	out := map[string]map[int64]any{}
	set := func(name string, id int64, value any) {
		if out[name] == nil {
			out[name] = map[int64]any{}
		}
		out[name][id] = value
	}
	add := func(name string, id int64, value map[string]any) {
		list, _ := outValue(out, name, id).([]map[string]any)
		set(name, id, append(list, value))
	}

	rows, err := query("reactions", reactionQuery)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		id := intOf(row.Values["bugle_id"])
		// A reaction row on a reaction message names its target; one with
		// reactions_data is the tally Messages keeps on the reacted-to
		// message. A row can be either.
		if target := intOf(row.Values["reacted_bugle_id"]); target != 0 {
			applied := decodeProtobufHex(stringOf(row.Values["applied_reaction"]))
			set("reaction", id, map[string]any{
				"reacted_bugle_id":    target,
				"reacted_message_key": stringOf(row.Values["reacted_message_key"]),
				"reaction_type":       intOf(row.Values["reaction_type"]),
				"emoji":               protobufString(applied, 1),
				"applied_reaction":    applied,
				"animation_effect":    decodeProtobufHex(stringOf(row.Values["animation_effect"])),
			})
		}
		if data := stringOf(row.Values["reactions_data"]); data != "" {
			set("reactions_data", id, decodeProtobufHex(data))
		}
	}

	rows, err = query("edits", editQuery)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		edit := map[string]any{
			"original_bugle_id":       intOf(row.Values["bugle_id"]),
			"original_message_key":    stringOf(row.Values["original_message_key"]),
			"latest_bugle_id":         intOf(row.Values["latest_bugle_id"]),
			"latest_message_key":      stringOf(row.Values["latest_message_key"]),
			"original_rcs_message_id": stringOf(row.Values["original_rcs_message_id"]),
			"edited_at_ms":            intOf(row.Values["edited_at_ms"]),
			"received_at_ms":          intOf(row.Values["received_at_ms"]),
		}
		// The edited message points at its replacement, and the replacement
		// row says which message it replaced.
		set("edit", intOf(row.Values["bugle_id"]), edit)
		if latest := intOf(row.Values["latest_bugle_id"]); latest != 0 && latest != intOf(row.Values["bugle_id"]) {
			set("edit_of", latest, edit)
		}
	}

	rows, err = query("replies", replyQuery)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		set("reply", intOf(row.Values["bugle_id"]), map[string]any{
			"replied_to_bugle_id":       intOf(row.Values["replied_to_bugle_id"]),
			"replied_to_message_key":    stringOf(row.Values["replied_to_message_key"]),
			"replied_to_rcs_message_id": stringOf(row.Values["replied_to_rcs_message_id"]),
			"replied_to_null_reason":    intOf(row.Values["replied_to_null_reason"]),
			"thread_id":                 stringOf(row.Values["thread_id"]),
		})
	}

	rows, err = query("captions", captionQuery)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		set("caption", intOf(row.Values["bugle_id"]), stringOf(row.Values["caption"]))
	}

	for name, statement := range map[string]string{"is_starred": starQuery, "is_self_mention": selfMentionQuery} {
		rows, err = query(name, statement)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			set(name, intOf(row.Values["bugle_id"]), int64(1))
		}
	}

	rows, err = query("link previews", linkPreviewQuery)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		preview := map[string]any{}
		for _, column := range row.Columns {
			if column != "bugle_id" {
				preview[column] = row.Values[column]
			}
		}
		set("link_preview", intOf(row.Values["bugle_id"]), preview)
	}

	rows, err = query("read reports", readReportQuery)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		add("read_reports", intOf(row.Values["bugle_id"]), map[string]any{
			"address":         stringOf(row.Values["address"]),
			"name":            stringOf(row.Values["name"]),
			"delivered_at_ms": intOf(row.Values["delivered_at_ms"]),
			"read_at_ms":      intOf(row.Values["read_at_ms"]),
		})
	}

	rows, err = query("annotations", annotationQuery)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		add("annotations", intOf(row.Values["bugle_id"]), map[string]any{
			"annotation_type": intOf(row.Values["annotation_type"]),
			"details":         decodeProtobufHex(stringOf(row.Values["annotation_details"])),
		})
	}
	return out, nil
}

func outValue(out map[string]map[int64]any, name string, id int64) any {
	if out[name] == nil {
		return nil
	}
	return out[name][id]
}

// decodeProtobufHex renders one of Messages' protobuf blobs (given as hex)
// for the warehouse: {"hex": the bytes, "fields": its fields by number}.
// The message types are Google's and undocumented, so the decoding is
// generic: a length-delimited field is shown as a nested message when it
// parses as one, else as text when it is valid UTF-8, else as hex. The
// bytes are always kept, so a wrong guess loses nothing.
func decodeProtobufHex(text string) any {
	if text == "" {
		return nil
	}
	raw, err := hex.DecodeString(text)
	if err != nil {
		return map[string]any{"hex": text}
	}
	out := map[string]any{"hex": text}
	if fields, ok := decodeProtobuf(raw, 0); ok {
		out["fields"] = fields
	}
	return out
}

// protobufString is field number's first value when it is text.
func protobufString(decoded any, number int) string {
	m, ok := decoded.(map[string]any)
	if !ok {
		return ""
	}
	fields, ok := m["fields"].(map[string]any)
	if !ok {
		return ""
	}
	values, _ := fields[fmt.Sprint(number)].([]any)
	for _, value := range values {
		if s, ok := value.(string); ok {
			return s
		}
	}
	return ""
}

// decodeProtobuf returns each field number's values ({"1": [...]}).
func decodeProtobuf(raw []byte, depth int) (map[string]any, bool) {
	if len(raw) == 0 || depth > 8 {
		return nil, false
	}
	fields := map[string]any{}
	push := func(number string, value any) {
		list, _ := fields[number].([]any)
		fields[number] = append(list, value)
	}
	for i := 0; i < len(raw); {
		tag, n := readVarint(raw[i:])
		if n == 0 || tag>>3 == 0 {
			return nil, false
		}
		i += n
		number := fmt.Sprint(tag >> 3)
		switch tag & 7 {
		case 0:
			value, n := readVarint(raw[i:])
			if n == 0 {
				return nil, false
			}
			i += n
			push(number, int64(value))
		case 1:
			if i+8 > len(raw) {
				return nil, false
			}
			push(number, hex.EncodeToString(raw[i:i+8]))
			i += 8
		case 5:
			if i+4 > len(raw) {
				return nil, false
			}
			push(number, hex.EncodeToString(raw[i:i+4]))
			i += 4
		case 2:
			length, n := readVarint(raw[i:])
			if n == 0 || uint64(len(raw)-i-n) < length {
				return nil, false
			}
			i += n
			chunk := raw[i : i+int(length)]
			i += int(length)
			if nested, ok := decodeProtobuf(chunk, depth+1); ok && !printableText(chunk) {
				push(number, nested)
			} else if utf8.Valid(chunk) {
				push(number, string(chunk))
			} else {
				push(number, hex.EncodeToString(chunk))
			}
		default:
			return nil, false
		}
	}
	return fields, true
}

// printableText is UTF-8 with no control characters: text, not a message
// that happens to parse.
func printableText(chunk []byte) bool {
	if !utf8.Valid(chunk) {
		return false
	}
	for _, r := range string(chunk) {
		if r < 0x20 && r != '\n' && r != '\t' && r != '\r' {
			return false
		}
	}
	return true
}

func readVarint(raw []byte) (uint64, int) {
	var value uint64
	for i := 0; i < len(raw) && i < 10; i++ {
		value |= uint64(raw[i]&0x7f) << (7 * i)
		if raw[i] < 0x80 {
			return value, i + 1
		}
	}
	return 0, 0
}
