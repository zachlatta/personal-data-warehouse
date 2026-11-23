package server

import (
	"context"
	"database/sql"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/notifications"
	"github.com/zachlatta/personal-data-warehouse/app/internal/push"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

var notificationSlackLink = regexp.MustCompile(`<[^<>|]+\|([^<>]+)>`)
var notificationSlackFormatting = regexp.MustCompile(`(?:\*|_|~)([^*_~]+)(?:\*|_|~)`)
var notificationMarkup = regexp.MustCompile(`(?i)</?(?:a|b|i|strong|em|p|br|div|span|html|body|table|tr|td|ul|ol|li)(?:\s+[^<>]*|\s*/?)>`)

func notificationText(s string, limit int) string {
	s = notificationSlackLink.ReplaceAllString(s, "$1")
	s = notificationMarkup.ReplaceAllString(s, "")
	s = strings.Join(strings.Fields(html.UnescapeString(s)), " ")
	r := []rune(s)
	if len(r) > limit {
		return string(r[:limit-1]) + "…"
	}
	return s
}

type notificationSourceMeta struct {
	label, icon, channel, color, category string
}

var notificationSources = map[string]notificationSourceMeta{
	"slack":            {label: "Slack", icon: "slack", channel: push.AndroidSlackChannelID, color: "#4A154B", category: "msg"},
	"gmail":            {label: "Gmail", icon: "gmail", channel: push.AndroidMailChannelID, color: "#D93025", category: "email"},
	"apple_messages":   {label: "Messages", icon: "messages", channel: push.AndroidMessagesChannelID, color: "#1A73E8", category: "msg"},
	"android_messages": {label: "Messages", icon: "messages", channel: push.AndroidMessagesChannelID, color: "#1A73E8", category: "msg"},
	"whatsapp":         {label: "WhatsApp", icon: "whatsapp", channel: push.AndroidWhatsAppChannelID, color: "#25D366", category: "msg"},
	"google_drive":     {label: "Google Drive", icon: "drive", channel: push.AndroidChannelID, color: "#0F9D58"},
	"calendar":         {label: "Google Calendar", icon: "calendar", channel: push.AndroidCalendarChannelID, color: "#039BE5", category: "event"},
	"scans":            {label: "Scanner", icon: "pdw", channel: push.AndroidChannelID, color: "#5F6368"},
	"github":           {label: "GitHub", icon: "pdw", channel: push.AndroidChannelID, color: "#24292F"},
}

// notificationThreadID is what the phone stacks alerts under. iOS groups by
// thread identifier, so "timeline:<source>" put every Slack channel, DM and
// group DM in one pile that could not be reviewed. The pile is the
// conversation (a Slack conversation, a Gmail thread, an iMessage or
// WhatsApp chat), the person when there is no conversation (a mail with no
// thread), or the thing itself (a Drive file); the row's `context` stream
// stands in for sources with no finer identity, and the bare source is the
// last resort rather than a guessed key.
func notificationThreadID(row map[string]any) string {
	source := linkString(row, "source")
	pk := decodeLinkJSON(row["source_pk"])
	meta := decodeLinkJSON(row["metadata"])
	account := linkString(pk, "account")
	key := ""
	switch source {
	case "slack":
		if team, conversation := linkString(pk, "team_id"), linkString(pk, "conversation_id"); team != "" && conversation != "" {
			key = team + ":" + conversation
		}
	case "gmail":
		if thread := linkString(meta, "thread_id"); thread != "" {
			key = account + ":" + thread
		} else if actor := strings.TrimSpace(linkString(row, "actor")); actor != "" {
			key = account + ":from:" + actor
		}
	case "apple_messages":
		if chat := linkString(meta, "chat_id"); chat != "" {
			key = account + ":" + chat
		}
	case "android_messages":
		if chat := linkString(meta, "chat_key"); chat != "" {
			key = account + ":" + chat
		}
	case "whatsapp":
		if chat := linkString(pk, "chat_id"); chat != "" {
			key = account + ":" + chat
		}
	case "google_drive":
		if file := linkString(pk, "file_id"); file != "" {
			key = account + ":" + file
		}
	}
	if key == "" {
		key = strings.TrimSpace(linkString(row, "context"))
	}
	if key == "" {
		return "timeline:" + source
	}
	return "timeline:" + source + ":" + key
}

// appleGroupContext names an unnamed iMessage group by its people. chat.db
// gives such a group only an opaque identifier ("0f6d2c1e9b8a…", "chat1234…"),
// which the timeline row carries as its context, so the alert said
// "Messages · 0f6d2c1e9b8a47d5a3c2e1f0b9d8c7a6" and nothing on the phone said
// which conversation to open. A named group, a 1:1 chat, any other source,
// and a group whose people cannot be looked up keep the context they had.
func appleGroupContext(row map[string]any, members func(account, chatID string) []string) string {
	context := linkString(row, "context")
	if linkString(row, "source") != "apple_messages" || members == nil {
		return context
	}
	chatID := linkString(decodeLinkJSON(row["metadata"]), "chat_id")
	parts := strings.SplitN(chatID, ";", 3)
	if len(parts) != 3 || parts[1] != "+" || (context != "" && context != parts[2]) {
		return context
	}
	names := members(linkString(decodeLinkJSON(row["source_pk"]), "account"), chatID)
	if len(names) == 0 {
		return context
	}
	const shown = 3
	if len(names) <= shown {
		return strings.Join(names, ", ")
	}
	return strings.Join(names[:shown], ", ") + " +" + strconv.Itoa(len(names)-shown)
}

// appleGroupMembersSQL lists who has spoken in a chat recently, earliest
// first, by the same contact-resolved sender name the timeline shows. Only
// the chat's newest 200 messages are read, through the chat_messages
// (account, chat_id, message_date) index.
const appleGroupMembersSQL = `
SELECT recent.sender_name
FROM (
    SELECT m.sender_name, m.message_at
    FROM @apple_message_chat_messages cm
    JOIN @clean_apple_messages m ON m.account = cm.account AND m.message_id = cm.message_id
    WHERE cm.account = $1 AND cm.chat_id = $2 AND m.is_from_me = 0 AND m.sender_name <> ''
    ORDER BY cm.message_date DESC
    LIMIT 200
) recent
GROUP BY recent.sender_name
ORDER BY min(recent.message_at)
LIMIT 20`

func renderTimelineNotification(row map[string]any, baseURL string, env timelineLinkEnv) notifications.Alert {
	if linkString(row, "adapter") == mutations.PolicyOutcomeAdapter {
		return renderPolicyOutcome(row)
	}
	source := linkString(row, "source")
	brand, ok := notificationSources[source]
	if !ok {
		brand = notificationSourceMeta{label: source, icon: "pdw", channel: push.AndroidChannelID}
	}
	actor := notificationText(linkString(row, "actor"), 60)
	title := notificationText(linkString(row, "title"), 90)
	headline := actor
	// A scan is Zach's own act: "me" would be the headline, so what was
	// scanned is ("Duplex color scan, 6 pages").
	if source == "scans" {
		headline = ""
	}
	if headline == "" {
		headline = title
	}
	if headline == "" {
		headline = "New timeline item"
	}
	subtitle := brand.label
	if title != "" && title != headline {
		subtitle += " · " + title
	} else if context := notificationText(linkString(row, "context"), 90); context != "" && context != headline {
		subtitle += " · " + context
	}
	body := notificationText(linkString(row, "snippet"), 280)
	if source == "slack" {
		body = notificationSlackFormatting.ReplaceAllString(body, "$1")
	}
	if body == "" && source == "scans" {
		body = "Open to see the PDF"
	}
	if body == "[unknown message]" {
		body = "Message preview unavailable — open to view"
	}
	a := notifications.Alert{Title: headline, Subtitle: notificationText(subtitle, 110), Body: body,
		Route: "/timeline/" + url.PathEscape(linkString(row, "adapter")) + "/" + url.PathEscape(linkString(row, "event_id")),
		Open:  timelineDeepLinkFor(row, env), ThreadID: notificationThreadID(row)}
	if strings.HasPrefix(baseURL, "https://") {
		a.Icon = strings.TrimRight(baseURL, "/") + "/app/notification-icons/" + brand.icon + ".png"
	}
	if notificationChatSources[source] {
		a.Android = androidChatStyle(row, brand, headline, a.ThreadID)
	} else {
		a.AndroidAlert = androidAlertStyle(row, brand, actor, title)
	}
	return a
}

// notificationChatSources are the sources whose alerts are a person saying
// something in a conversation, which Android draws as a chat notification
// (sender, avatar, stacked messages, the Conversations section) rather than
// as a generic alert.
var notificationChatSources = map[string]bool{"android_messages": true, "apple_messages": true, "whatsapp": true, "slack": true}

// androidChatStyle names the conversation the way the source's own app does:
// a 1:1 chat is the person, a group is its name (or its people, for an unnamed
// iMessage group, which appleGroupContext has already put in context).
func androidChatStyle(row map[string]any, brand notificationSourceMeta, sender, threadID string) *notifications.AndroidStyle {
	source := linkString(row, "source")
	pk := decodeLinkJSON(row["source_pk"])
	meta := decodeLinkJSON(row["metadata"])
	context := notificationText(linkString(row, "context"), 90)
	group := false
	title := context
	groupKey := "pdw:" + source
	switch source {
	case "android_messages":
		group = meta["group"] == true
		title = notificationText(linkString(meta, "chat_name"), 90)
		groupKey = "pdw:messages"
	case "apple_messages":
		group = strings.Contains(linkString(meta, "chat_id"), ";+;")
		groupKey = "pdw:messages"
	case "whatsapp":
		group = strings.HasSuffix(linkString(pk, "chat_id"), "@g.us")
	case "slack":
		// D… is a direct message; channels and group DMs are conversations
		// with a name of their own.
		conversation := linkString(pk, "conversation_id")
		group = conversation != "" && !strings.HasPrefix(conversation, "D")
	}
	style := &notifications.AndroidStyle{
		Source:         brand.label,
		SourceKey:      source,
		ChannelID:      brand.channel,
		Color:          brand.color,
		GroupKey:       groupKey,
		ConversationID: threadID,
		Group:          group,
		SenderName:     sender,
		SenderKey:      source + ":" + sender,
		EventID:        linkString(row, "event_id"),
	}
	if group {
		if title == "" {
			title = brand.label + " group"
		}
		style.ConversationTitle = title
	}
	return style
}

// androidAlertStyle builds native Android presentation for non-chat sources
// (Gmail, Calendar, Google Drive, Scans, GitHub): per-source channel, accent
// color, category, grouping, email subject, and sender monogram fallback.
func androidAlertStyle(row map[string]any, brand notificationSourceMeta, actor, title string) *notifications.AndroidAlertStyle {
	source := linkString(row, "source")
	style := &notifications.AndroidAlertStyle{
		Source:     brand.label,
		SourceKey:  source,
		ChannelID:  brand.channel,
		Color:      brand.color,
		Category:   brand.category,
		GroupKey:   "pdw:" + source,
		GroupTitle: brand.label,
	}
	if source == "gmail" && title != "" {
		style.Subject = title
	}
	// Sources with a human sender/organizer get a circular contact face or
	// colored monogram as the largeIcon, just like Gmail/Calendar on Android.
	if actor != "" && source != "scans" {
		style.AvatarName = actor
	}
	return style
}

// notificationMedia holds resolved authenticated avatar and/or image attachment
// URLs for one notification event.
type notificationMedia struct {
	AvatarURL      string
	AvatarCacheKey string
	ImageURL       string
	ImageMimeType  string
}

// mediaLookup finds an authenticated avatar URL and/or image attachment URL
// for an alert's event. It runs on the worker's claim of the outbox row, so
// withMedia bounds it.
type mediaLookup func(ctx context.Context, row map[string]any) (notificationMedia, error)

// withMedia attaches the sender's face and any inline image attachment to an
// alert. A failed, empty or slow lookup leaves the initial avatar and text-only
// body; it never delays the alert past the timeout and never drops it.
func withMedia(a notifications.Alert, row map[string]any, lookup mediaLookup, timeout time.Duration) notifications.Alert {
	if (a.Android == nil && a.AndroidAlert == nil) || lookup == nil {
		return a
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	found := make(chan notificationMedia, 1)
	go func() {
		media, err := lookup(ctx, row)
		if err != nil {
			media = notificationMedia{}
		}
		found <- media
	}()
	select {
	case media := <-found:
		avatarURL := ""
		if strings.HasPrefix(media.AvatarURL, "https://") && len(media.AvatarURL) <= 600 {
			avatarURL = media.AvatarURL
		}
		imageURL := ""
		if strings.HasPrefix(media.ImageURL, "https://") && len(media.ImageURL) <= 600 {
			imageURL = media.ImageURL
		}
		if a.Android != nil && (avatarURL != "" || imageURL != "") {
			style := *a.Android
			if avatarURL != "" {
				style.AvatarURL = avatarURL
				style.AvatarCacheKey = media.AvatarCacheKey
			}
			if imageURL != "" {
				style.ImageURL = imageURL
				style.ImageMimeType = media.ImageMimeType
			}
			a.Android = &style
		}
		if a.AndroidAlert != nil && (avatarURL != "" || imageURL != "") {
			style := *a.AndroidAlert
			if avatarURL != "" {
				style.AvatarURL = avatarURL
				style.AvatarCacheKey = media.AvatarCacheKey
			}
			if imageURL != "" {
				style.ImageURL = imageURL
			}
			a.AndroidAlert = &style
		}
	case <-ctx.Done():
	}
	return a
}

var googlePhotoSize = regexp.MustCompile(`=s\d+(-c)?$`)

// notificationMediaLookup is the production mediaLookup: resolves the sender's
// avatar across Slack, Google Contacts (by phone, email, or display name), and
// Slack-by-email (for Gmail/Calendar/iMessage), mints a short-lived signed
// /api/notifications/avatar link, and resolves any stored image attachment as
// a signed /objects/ link.
func notificationMediaLookup(db *sql.DB, baseURL string, signer *pdwauth.Service, now func() time.Time) mediaLookup {
	if now == nil {
		now = time.Now
	}
	signObj := func(fileID string) string {
		if !strings.HasPrefix(baseURL, "https://") || signer == nil || fileID == "" {
			return ""
		}
		exp := now().Add(notificationAvatarTTL)
		sig := signer.SignObjectDownload(fileID, "", exp)
		return fmt.Sprintf("%s/objects/%s?exp=%d&sig=%s", strings.TrimRight(baseURL, "/"), url.PathEscape(fileID), exp.Unix(), url.QueryEscape(sig))
	}
	return func(ctx context.Context, row map[string]any) (notificationMedia, error) {
		var out notificationMedia
		key, rawURL := resolveAvatarKeyForRow(ctx, db, row)
		if key != "" {
			if signed := signAvatarURL(baseURL, signer, now, key); signed != "" {
				out.AvatarURL = signed
				out.AvatarCacheKey = avatarCacheKey(key)
			} else if strings.HasPrefix(rawURL, "https://") {
				// Fallback when baseURL is non-https (dev/tests without signer).
				out.AvatarURL = rawURL
				out.AvatarCacheKey = avatarCacheKey(key)
			}
		}
		fileID, mimeType := resolveImageAttachmentForRow(ctx, db, row)
		if fileID != "" {
			if signed := signObj(fileID); signed != "" {
				out.ImageURL = signed
				out.ImageMimeType = mimeType
			}
		}
		return out, nil
	}
}

func resolveAvatarKeyForRow(ctx context.Context, db *sql.DB, row map[string]any) (key, rawURL string) {
	if db == nil {
		return "", ""
	}
	source := linkString(row, "source")
	pk := decodeLinkJSON(row["source_pk"])
	actor := strings.TrimSpace(linkString(row, "actor"))
	switch source {
	case "slack":
		var account, teamID, userID, imgURL string
		if db.QueryRowContext(ctx, warehouse.ExpandRelations(notificationSlackAvatarKeySQL),
			linkString(pk, "account"), linkString(pk, "team_id"), linkString(pk, "conversation_id"), linkString(pk, "message_ts")).
			Scan(&account, &teamID, &userID, &imgURL) == nil && imgURL != "" {
			return fmt.Sprintf("slack|%s|%s|%s", account, teamID, userID), imgURL
		}
		return "", ""
	case "android_messages":
		var address string
		_ = db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT sender_address FROM @android_messages WHERE account=$1 AND device=$2 AND message_key=$3`),
			linkString(pk, "account"), linkString(pk, "device"), linkString(pk, "message_key")).Scan(&address)
		return lookupPersonAvatarKey(ctx, db, notificationPhoneDigits(address), notificationEmail(address), actor)
	case "apple_messages":
		var address string
		_ = db.QueryRowContext(ctx, warehouse.ExpandRelations(`
SELECT COALESCE(NULLIF(h.address, ''), m.handle_id)
FROM @apple_messages m
LEFT JOIN @apple_message_handles h ON h.account = m.account AND h.handle_id = m.handle_id
WHERE m.account = $1 AND m.message_id = $2
LIMIT 1`),
			linkString(pk, "account"), linkString(pk, "message_id")).Scan(&address)
		return lookupPersonAvatarKey(ctx, db, notificationPhoneDigits(address), notificationEmail(address), actor)
	case "whatsapp":
		var senderJID string
		_ = db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT sender_jid FROM @whatsapp_messages WHERE account=$1 AND chat_id=$2 AND message_id=$3`),
			linkString(pk, "account"), linkString(pk, "chat_id"), linkString(pk, "message_id")).Scan(&senderJID)
		phone := ""
		if strings.HasSuffix(senderJID, "@s.whatsapp.net") {
			phone = notificationPhoneDigits(strings.SplitN(senderJID, "@", 2)[0])
		}
		return lookupPersonAvatarKey(ctx, db, phone, "", actor)
	case "gmail":
		var fromAddr string
		_ = db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT from_address FROM @gmail_messages WHERE account=$1 AND message_id=$2`),
			linkString(pk, "account"), linkString(pk, "message_id")).Scan(&fromAddr)
		email := notificationEmail(fromAddr)
		if email == "" {
			email = notificationEmail(actor)
		}
		return lookupPersonAvatarKey(ctx, db, "", email, actor)
	case "calendar", "google_drive":
		return lookupPersonAvatarKey(ctx, db, "", notificationEmail(actor), actor)
	default:
		return "", ""
	}
}

// lookupPersonAvatarKey finds a contact photo (by phone, email, or display
// name) or a Slack user avatar (by indexed email) and returns its opaque
// avatar key and upstream URL.
func lookupPersonAvatarKey(ctx context.Context, db *sql.DB, phone, email, displayName string) (key, rawURL string) {
	var cSource, cAccount, cKind, cBook, cCard, photoURL string
	if db.QueryRowContext(ctx, warehouse.ExpandRelations(notificationContactPhotoKeySQL), phone, email, displayName).
		Scan(&cSource, &cAccount, &cKind, &cBook, &cCard, &photoURL) == nil && photoURL != "" {
		return fmt.Sprintf("contact|%s|%s|%s|%s|%s", cSource, cAccount, cKind, cBook, cCard), googlePhotoSize.ReplaceAllString(photoURL, "=s256-c")
	}
	if email != "" {
		var sAccount, sTeam, sUser, imgURL string
		if db.QueryRowContext(ctx, warehouse.ExpandRelations(notificationSlackAvatarByEmailSQL), email).
			Scan(&sAccount, &sTeam, &sUser, &imgURL) == nil && imgURL != "" {
			return fmt.Sprintf("slack|%s|%s|%s", sAccount, sTeam, sUser), imgURL
		}
	}
	return "", ""
}

func resolveImageAttachmentForRow(ctx context.Context, db *sql.DB, row map[string]any) (fileID, mimeType string) {
	if db == nil {
		return "", ""
	}
	pk := decodeLinkJSON(row["source_pk"])
	switch linkString(row, "source") {
	case "android_messages":
		_ = db.QueryRowContext(ctx, warehouse.ExpandRelations(`
SELECT storage_file_id, content_type
FROM @android_message_attachments
WHERE account = $1 AND device = $2 AND message_key = $3
  AND content_type LIKE 'image/%' AND storage_file_id <> '' AND is_missing = 0
ORDER BY part_key
LIMIT 1`),
			linkString(pk, "account"), linkString(pk, "device"), linkString(pk, "message_key")).Scan(&fileID, &mimeType)
	case "apple_messages":
		_ = db.QueryRowContext(ctx, warehouse.ExpandRelations(`
SELECT storage_file_id, COALESCE(NULLIF(content_type, ''), mime_type)
FROM @apple_message_attachments
WHERE account = $1 AND message_id = $2
  AND (content_type LIKE 'image/%' OR mime_type LIKE 'image/%') AND storage_file_id <> '' AND is_missing = 0
ORDER BY attachment_id
LIMIT 1`),
			linkString(pk, "account"), linkString(pk, "message_id")).Scan(&fileID, &mimeType)
	case "whatsapp":
		_ = db.QueryRowContext(ctx, warehouse.ExpandRelations(`
SELECT storage_file_id, mime_type
FROM @whatsapp_media_items
WHERE account = $1 AND chat_id = $2 AND message_id = $3
  AND mime_type LIKE 'image/%' AND storage_file_id <> '' AND is_missing = 0
LIMIT 1`),
			linkString(pk, "account"), linkString(pk, "chat_id"), linkString(pk, "message_id")).Scan(&fileID, &mimeType)
	case "slack":
		_ = db.QueryRowContext(ctx, warehouse.ExpandRelations(`
SELECT file_id, mimetype
FROM @slack_files
WHERE account = $1 AND team_id = $2 AND conversation_id = $3 AND message_ts = $4
  AND mimetype LIKE 'image/%' AND is_deleted = 0 AND file_id <> ''
ORDER BY file_id
LIMIT 1`),
			linkString(pk, "account"), linkString(pk, "team_id"), linkString(pk, "conversation_id"), linkString(pk, "message_ts")).Scan(&fileID, &mimeType)
	}
	return fileID, mimeType
}

// notificationEmail extracts a normalized lowercase email address if s looks
// like one (or contains <email@domain>).
func notificationEmail(s string) string {
	s = strings.TrimSpace(s)
	if start := strings.LastIndexByte(s, '<'); start >= 0 {
		if end := strings.IndexByte(s[start:], '>'); end > 1 {
			s = strings.TrimSpace(s[start+1 : start+end])
		}
	}
	s = strings.ToLower(strings.Trim(s, "\"' "))
	at := strings.IndexByte(s, '@')
	if at <= 0 || at == len(s)-1 || strings.ContainsAny(s, " \t\r\n") || !strings.Contains(s[at+1:], ".") {
		return ""
	}
	return s
}

// notificationPhoneDigits matches marts_contacts.contact_points.normalized_value:
// digits only, with a leading country code (a bare 10-digit number is NANP).
func notificationPhoneDigits(address string) string {
	var b strings.Builder
	for _, r := range address {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	digits := b.String()
	if len(digits) == 10 {
		return "1" + digits
	}
	if len(digits) < 7 {
		return ""
	}
	return digits
}

const notificationSlackAvatarKeySQL = `
SELECT u.account, u.team_id, u.user_id,
       COALESCE(NULLIF(u.raw_json, '')::jsonb->'profile'->>'image_192', NULLIF(u.raw_json, '')::jsonb->'profile'->>'image_72', '')
FROM @slack_messages m
JOIN @slack_users u ON u.account = m.account AND u.team_id = m.team_id AND u.user_id = m.user_id
WHERE m.account = $1 AND m.team_id = $2 AND m.conversation_id = $3 AND m.message_ts = $4
LIMIT 1`

const notificationSlackAvatarByEmailSQL = `
SELECT u.account, u.team_id, u.user_id,
       COALESCE(NULLIF(u.raw_json, '')::jsonb->'profile'->>'image_192', NULLIF(u.raw_json, '')::jsonb->'profile'->>'image_72', '')
FROM @slack_users u
WHERE u.email <> '' AND lower(u.email) = lower($1)
  AND COALESCE(NULLIF(u.raw_json, '')::jsonb->'profile'->>'image_192', NULLIF(u.raw_json, '')::jsonb->'profile'->>'image_72', '') LIKE 'https://%'
ORDER BY u.is_deleted ASC, u.synced_at DESC
LIMIT 1`

const notificationContactPhotoKeySQL = `
SELECT source, account, source_kind, address_book_id, card_id, url FROM (
    SELECT c.source, c.account, c.source_kind, c.address_book_id, c.card_id, photo->>'url' AS url, 0 AS rank
    FROM @clean_contact_points cp
    JOIN @clean_contacts c ON c.source = cp.source AND c.account = cp.account AND c.card_id = cp.card_id
    CROSS JOIN LATERAL jsonb_array_elements(c.photos) photo
    WHERE $1 <> '' AND cp.point_type = 'phone' AND cp.normalized_value = $1
      AND photo->>'url' LIKE 'https://%' AND NOT COALESCE((photo->>'default')::boolean, false)
    UNION ALL
    SELECT c.source, c.account, c.source_kind, c.address_book_id, c.card_id, photo->>'url' AS url, 1 AS rank
    FROM @clean_contact_points cp
    JOIN @clean_contacts c ON c.source = cp.source AND c.account = cp.account AND c.card_id = cp.card_id
    CROSS JOIN LATERAL jsonb_array_elements(c.photos) photo
    WHERE $2 <> '' AND cp.point_type = 'email' AND cp.normalized_value = $2
      AND photo->>'url' LIKE 'https://%' AND NOT COALESCE((photo->>'default')::boolean, false)
    UNION ALL
    SELECT c.source, c.account, c.source_kind, c.address_book_id, c.card_id, photo->>'url' AS url, 2 AS rank
    FROM @clean_contacts c
    CROSS JOIN LATERAL jsonb_array_elements(c.photos) photo
    WHERE $3 <> '' AND lower(c.display_name) = lower($3)
      AND photo->>'url' LIKE 'https://%' AND NOT COALESCE((photo->>'default')::boolean, false)
) found
ORDER BY rank
LIMIT 1`

// NewTimelineNotifications shares the timeline's link builder, not a second
// set of per-source URL guesses. This worker never uses the read-only SQL role.
func NewTimelineNotifications(cfg config.Config, authSvc *pdwauth.Service, publicKey, privateKey, subscriber string) (*notifications.Service, error) {
	if err := notifications.ValidateVAPID(publicKey, privateKey); err != nil {
		return nil, err
	}
	transport := &notifications.Transport{Expo: push.NewExpoClient(cfg.ExpoAccessToken), VAPIDPublicKey: publicKey, VAPIDPrivateKey: privateKey, Subscriber: subscriber}
	svc, err := notifications.New(cfg.PostgresDatabaseURL, []byte(cfg.SecretToken), transport, nil)
	if err != nil {
		return nil, err
	}
	svc.PublicKey = publicKey
	svc.Render = func(row map[string]any) notifications.Alert {
		env := timelineLinkEnv{baseURL: cfg.BaseURL, slackDomains: map[string]string{}}
		if teamID := linkString(decodeLinkJSON(row["source_pk"]), "team_id"); teamID != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var domain string
			if svc.DB.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT domain FROM @slack_teams WHERE team_id=$1 LIMIT 1`), teamID).Scan(&domain) == nil {
				env.slackDomains[teamID] = domain
			}
		}
		row["context"] = appleGroupContext(row, func(account, chatID string) []string {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			rows, err := svc.DB.QueryContext(ctx, warehouse.ExpandRelations(appleGroupMembersSQL), account, chatID)
			if err != nil {
				return nil
			}
			defer rows.Close()
			var names []string
			for rows.Next() {
				var name string
				if rows.Scan(&name) == nil {
					names = append(names, name)
				}
			}
			if rows.Err() != nil {
				return nil
			}
			return names
		})
		return withMedia(renderTimelineNotification(row, cfg.BaseURL, env), row, notificationMediaLookup(svc.DB, cfg.BaseURL, authSvc, time.Now), time.Second)
	}
	return svc, nil
}

// renderPolicyOutcome draws a mutation policy outcome the outbox carries
// (mutations.PolicyOutcomeAlert): its own title, body and route, the action
// category (Undo after an automatic change, Approve/Deny when sent back) and
// the request the buttons act on.
func renderPolicyOutcome(row map[string]any) notifications.Alert {
	quiet, _ := row["quiet"].(bool)
	return notifications.Alert{
		Title:     notificationText(linkString(row, "title"), 90),
		Body:      notificationText(linkString(row, "snippet"), 280),
		Route:     linkString(row, "route"),
		ThreadID:  "mutations",
		Category:  linkString(row, "category"),
		Kind:      linkString(row, "kind"),
		RequestID: linkString(row, "request_id"),
		Quiet:     quiet,
		AndroidAlert: &notifications.AndroidAlertStyle{
			Source:     "Approvals",
			SourceKey:  "mutations",
			ChannelID:  push.AndroidMutationsChannelID,
			Color:      "#6750A4",
			GroupKey:   "pdw:mutations",
			GroupTitle: "Approvals",
		},
	}
}
