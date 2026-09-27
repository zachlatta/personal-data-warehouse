package server

import (
	"context"
	"html"
	"net/url"
	"regexp"
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
	"apple_messages": {"Messages", "messages"}, "whatsapp": {"WhatsApp", "whatsapp"},
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
	return a
}

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
		return renderTimelineNotification(row, cfg.BaseURL, env)
	}
	return svc, nil
}
