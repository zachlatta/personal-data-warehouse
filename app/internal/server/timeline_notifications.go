package server

import (
	"context"
	"database/sql"
	"html"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
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

var notificationSources = map[string]struct{ label, icon string }{
	"slack": {"Slack", "slack"}, "gmail": {"Gmail", "gmail"},
	"apple_messages": {"Messages", "messages"}, "android_messages": {"Messages", "messages"}, "whatsapp": {"WhatsApp", "whatsapp"},
	"google_drive": {"Google Drive", "drive"}, "calendar": {"Google Calendar", "calendar"},
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
	source := linkString(row, "source")
	brand, ok := notificationSources[source]
	if !ok {
		brand.label = source
		brand.icon = "pdw"
	}
	actor := notificationText(linkString(row, "actor"), 60)
	title := notificationText(linkString(row, "title"), 90)
	headline := actor
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
		a.Android = androidChatStyle(row, brand.label, headline, a.ThreadID)
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
func androidChatStyle(row map[string]any, label, sender, threadID string) *notifications.AndroidStyle {
	source := linkString(row, "source")
	pk := decodeLinkJSON(row["source_pk"])
	meta := decodeLinkJSON(row["metadata"])
	context := notificationText(linkString(row, "context"), 90)
	group := false
	title := context
	switch source {
	case "android_messages":
		group = meta["group"] == true
		title = notificationText(linkString(meta, "chat_name"), 90)
	case "apple_messages":
		group = strings.Contains(linkString(meta, "chat_id"), ";+;")
	case "whatsapp":
		group = strings.HasSuffix(linkString(pk, "chat_id"), "@g.us")
	case "slack":
		// D… is a direct message; channels and group DMs are conversations
		// with a name of their own.
		conversation := linkString(pk, "conversation_id")
		group = conversation != "" && !strings.HasPrefix(conversation, "D")
	}
	style := &notifications.AndroidStyle{Source: label, ConversationID: threadID, Group: group,
		SenderName: sender, SenderKey: source + ":" + sender, EventID: linkString(row, "event_id")}
	if group {
		if title == "" {
			title = label + " group"
		}
		style.ConversationTitle = title
	}
	return style
}

// avatarLookup finds an https photo of an alert's sender. It runs on the
// worker's claim of the outbox row, so withAvatar bounds it.
type avatarLookup func(ctx context.Context, row map[string]any) (string, error)

// withAvatar adds the sender's photo to a chat alert. A failed, empty or slow
// lookup leaves the avatar empty (the phone draws the sender's initial); it
// never delays the alert past the timeout and never drops it.
func withAvatar(a notifications.Alert, row map[string]any, lookup avatarLookup, timeout time.Duration) notifications.Alert {
	if a.Android == nil || lookup == nil {
		return a
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	found := make(chan string, 1)
	go func() {
		url, err := lookup(ctx, row)
		if err != nil {
			url = ""
		}
		found <- url
	}()
	select {
	case url := <-found:
		if strings.HasPrefix(url, "https://") && len(url) <= 600 {
			style := *a.Android
			style.AvatarURL = url
			a.Android = &style
		}
	case <-ctx.Done():
	}
	return a
}

var googlePhotoSize = regexp.MustCompile(`=s\d+(-c)?$`)

// notificationAvatar is the production avatarLookup: a Slack sender's profile
// image, or the contact card photo of a messages or WhatsApp sender, matched
// by phone number where the source has one and by name otherwise. Google's
// generated letter placeholders (`default: true`) are skipped: the phone draws
// a better initial itself.
func notificationAvatar(db *sql.DB) avatarLookup {
	return func(ctx context.Context, row map[string]any) (string, error) {
		pk := decodeLinkJSON(row["source_pk"])
		var url string
		switch linkString(row, "source") {
		case "slack":
			err := db.QueryRowContext(ctx, warehouse.ExpandRelations(notificationSlackAvatarSQL),
				linkString(pk, "account"), linkString(pk, "team_id"), linkString(pk, "conversation_id"), linkString(pk, "message_ts")).Scan(&url)
			return url, err
		case "android_messages", "apple_messages", "whatsapp":
			phone := ""
			if linkString(row, "source") == "android_messages" {
				var address string
				if db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT sender_address FROM @android_messages WHERE account=$1 AND device=$2 AND message_key=$3`),
					linkString(pk, "account"), linkString(pk, "device"), linkString(pk, "message_key")).Scan(&address) == nil {
					phone = notificationPhoneDigits(address)
				}
			}
			err := db.QueryRowContext(ctx, warehouse.ExpandRelations(notificationContactPhotoSQL), phone, strings.TrimSpace(linkString(row, "actor"))).Scan(&url)
			return googlePhotoSize.ReplaceAllString(url, "=s256-c"), err
		}
		return "", nil
	}
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

const notificationSlackAvatarSQL = `
SELECT COALESCE(NULLIF(u.raw_json, '')::jsonb->'profile'->>'image_192', NULLIF(u.raw_json, '')::jsonb->'profile'->>'image_72', '')
FROM @slack_messages m
JOIN @slack_users u ON u.account = m.account AND u.team_id = m.team_id AND u.user_id = m.user_id
WHERE m.account = $1 AND m.team_id = $2 AND m.conversation_id = $3 AND m.message_ts = $4
LIMIT 1`

const notificationContactPhotoSQL = `
SELECT url FROM (
    SELECT photo->>'url' AS url, 0 AS rank
    FROM @clean_contact_points cp
    JOIN @clean_contacts c ON c.source = cp.source AND c.account = cp.account AND c.card_id = cp.card_id
    CROSS JOIN LATERAL jsonb_array_elements(c.photos) photo
    WHERE $1 <> '' AND cp.point_type = 'phone' AND cp.normalized_value = $1
      AND photo->>'url' LIKE 'https://%' AND NOT COALESCE((photo->>'default')::boolean, false)
    UNION ALL
    SELECT photo->>'url', 1
    FROM @clean_contacts c
    CROSS JOIN LATERAL jsonb_array_elements(c.photos) photo
    WHERE $2 <> '' AND lower(c.display_name) = lower($2)
      AND photo->>'url' LIKE 'https://%' AND NOT COALESCE((photo->>'default')::boolean, false)
) found
ORDER BY rank
LIMIT 1`

// NewTimelineNotifications shares the timeline's link builder, not a second
// set of per-source URL guesses. This worker never uses the read-only SQL role.
func NewTimelineNotifications(cfg config.Config, publicKey, privateKey, subscriber string) (*notifications.Service, error) {
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
		return withAvatar(renderTimelineNotification(row, cfg.BaseURL, env), row, notificationAvatar(svc.DB), time.Second)
	}
	return svc, nil
}
