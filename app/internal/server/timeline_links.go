package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/deeplink"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// timelineDeepLink is the "open this in its source" affordance for one
// timeline row. URL is the link that works everywhere a browser does (a web
// permalink where the source has one, otherwise the app's own scheme); AppURL
// is an optional native-app scheme a phone should try first, because on iOS
// the web permalink for some sources opens Safari rather than the app.
// The wire shape is the shared one: the mutation review sends the same JSON
// for its Slack rows, and the iOS app opens both with one helper.
type timelineDeepLink = deeplink.Link

// timelineLinkEnv is what a deep link needs beyond the row itself: the Slack
// workspace domains (a permalink is <domain>.slack.com/archives/...) and this
// app's own base URL for the mutation review pages.
type timelineLinkEnv struct {
	slackDomains map[string]string
	baseURL      string
}

var (
	// A WhatsApp 1:1 chat id is the other party's phone number as a JID;
	// groups end in @g.us and broadcast lists have no openable URL.
	whatsappPhoneJIDPattern = regexp.MustCompile(`^(\d{5,20})@s\.whatsapp\.net$`)
	// Apple Messages 1:1 chat identifiers are a handle (phone or email); a
	// group chat's identifier is an opaque chatNNN token Messages cannot open
	// from a URL.
	appleMessagesHandlePattern = regexp.MustCompile(`^(\+?[0-9][0-9 ().-]{4,}|[^@\s]+@[^@\s]+)$`)
)

func linkString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	value, _ := m[key].(string)
	return value
}

func decodeLinkJSON(raw any) map[string]any {
	var text string
	switch v := raw.(type) {
	case string:
		text = v
	case json.RawMessage:
		text = string(v)
	case []byte:
		text = string(v)
	case map[string]any:
		return v
	}
	if text == "" {
		return nil
	}
	out := map[string]any{}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		return nil
	}
	return out
}

// timelineDeepLinkFor computes the source deep link for a timeline row from
// the row's own adapter, source_pk and metadata — nothing here needs a second
// query, so the list endpoint can attach it to every row for free. A source
// with no openable URL (health, photos, voice memos, a group chat) returns nil
// and the UI simply shows no button; a guessed link that opens the wrong
// thing is worse than none.
func timelineDeepLinkFor(row map[string]any, env timelineLinkEnv) *timelineDeepLink {
	pk := decodeLinkJSON(row["source_pk"])
	meta := decodeLinkJSON(row["metadata"])
	sourceTable, _ := row["source_table"].(string)
	sourceTable = warehouse.CurrentSourceTable(sourceTable)

	switch {
	case sourceTable == "slack_messages" || sourceTable == "slack_files":
		return slackDeepLink(pk, meta, env)
	case sourceTable == "gmail_messages":
		account, messageID := linkString(pk, "account"), linkString(pk, "message_id")
		if messageID == "" {
			return nil
		}
		return &timelineDeepLink{
			URL:   "https://mail.google.com/mail/?authuser=" + url.QueryEscape(account) + "#all/" + url.PathEscape(messageID),
			Label: "Gmail",
		}
	case sourceTable == "calendar_events":
		return calendarDeepLink(pk)
	case sourceTable == "google_drive_files":
		fileID := linkString(pk, "file_id")
		if fileID == "" {
			return nil
		}
		return &timelineDeepLink{
			URL:   "https://drive.google.com/open?id=" + url.QueryEscape(fileID) + "&authuser=" + url.QueryEscape(linkString(pk, "account")),
			Label: "Google Drive",
		}
	case sourceTable == "contact_cards" || sourceTable == "apple_contact_cards":
		return contactDeepLink(pk)
	case sourceTable == "ai_conversation_events":
		return agentSessionDeepLink(pk)
	case sourceTable == "whatsapp_messages":
		m := whatsappPhoneJIDPattern.FindStringSubmatch(linkString(pk, "chat_id"))
		if m == nil {
			return nil
		}
		return &timelineDeepLink{
			URL:    "https://wa.me/" + m[1],
			AppURL: "whatsapp://send?phone=" + m[1],
			Label:  "WhatsApp",
		}
	case sourceTable == "apple_messages":
		// chat.db chat ids are "<service>;<-|+>;<identifier>": "-" is a 1:1
		// chat whose identifier is the other party's handle, "+" a group whose
		// identifier is an opaque chatNNN token no URL can open.
		parts := strings.SplitN(linkString(meta, "chat_id"), ";", 3)
		if len(parts) != 3 || parts[1] != "-" || !appleMessagesHandlePattern.MatchString(parts[2]) {
			return nil
		}
		handle := strings.ReplaceAll(parts[2], " ", "")
		return &timelineDeepLink{
			URL:    "imessage://" + handle,
			AppURL: "sms:" + handle,
			Label:  "Messages",
		}
	case sourceTable == "apple_note_revisions":
		noteID := linkString(pk, "note_id")
		if noteID == "" {
			return nil
		}
		return &timelineDeepLink{
			URL:    "applenotes://showNote?identifier=" + url.QueryEscape(noteID),
			AppURL: "mobilenotes://showNote?identifier=" + url.QueryEscape(noteID),
			Label:  "Notes",
		}
	case sourceTable == "upstream_mutation_requests":
		if env.baseURL == "" {
			return nil
		}
		id := linkString(pk, "id")
		if id == "" {
			return nil
		}
		return &timelineDeepLink{URL: env.baseURL + mutations.ReviewPath + "/requests/" + url.PathEscape(id), Label: "Mutation review"}
	case sourceTable == "upstream_mutations":
		if env.baseURL == "" {
			return nil
		}
		target := env.baseURL + mutations.ReviewPath + "/requests"
		if requestID := linkString(meta, "request_id"); requestID != "" {
			target += "/" + url.PathEscape(requestID)
		}
		return &timelineDeepLink{URL: target, Label: "Mutation review"}
	}
	return nil
}

func slackDeepLink(pk, meta map[string]any, env timelineLinkEnv) *timelineDeepLink {
	teamID := linkString(pk, "team_id")
	return deeplink.Slack(
		teamID,
		linkString(pk, "conversation_id"),
		linkString(pk, "message_ts"),
		linkString(meta, "thread_ts"),
		env.slackDomains[teamID],
	)
}

// calendarDeepLink builds Google Calendar's event URL. The eid parameter is
// base64("<event id> <calendar email>") with the padding stripped — the same
// value Google puts in htmlLink, minus a round-trip to the source row.
func calendarDeepLink(pk map[string]any) *timelineDeepLink {
	eventID := linkString(pk, "event_id")
	if eventID == "" {
		return nil
	}
	calendar := linkString(pk, "calendar_id")
	if calendar == "" || calendar == "primary" {
		calendar = linkString(pk, "account")
	}
	eid := strings.TrimRight(base64.StdEncoding.EncodeToString([]byte(eventID+" "+calendar)), "=")
	return &timelineDeepLink{
		URL:   "https://calendar.google.com/calendar/event?eid=" + eid,
		Label: "Google Calendar",
	}
}

func contactDeepLink(pk map[string]any) *timelineDeepLink {
	cardID := linkString(pk, "card_id")
	if cardID == "" {
		return nil
	}
	switch linkString(pk, "source_kind") {
	case "google_contacts":
		// People API resource names are people/c<id>; the Contacts UI wants
		// the bare c<id>.
		person := strings.TrimPrefix(cardID, "people/")
		return &timelineDeepLink{
			URL:   "https://contacts.google.com/person/" + url.PathEscape(person) + "?authuser=" + url.QueryEscape(linkString(pk, "account")),
			Label: "Google Contacts",
		}
	case "apple_contacts":
		// Contacts.app resolves addressbook://<uid>; the stored card id is
		// "<uid>:ABPerson".
		uid := strings.TrimSuffix(cardID, ":ABPerson")
		return &timelineDeepLink{URL: "addressbook://" + url.PathEscape(uid), Label: "Contacts"}
	}
	return nil
}

func agentSessionDeepLink(pk map[string]any) *timelineDeepLink {
	sessionID := linkString(pk, "session_id")
	if sessionID == "" {
		return nil
	}
	switch linkString(pk, "source") {
	case "chatgpt":
		return &timelineDeepLink{URL: "https://chatgpt.com/c/" + url.PathEscape(sessionID), Label: "ChatGPT"}
	case "claude_desktop":
		return &timelineDeepLink{URL: "https://claude.ai/chat/" + url.PathEscape(sessionID), Label: "Claude"}
	}
	// CLI transcripts (claude_code, codex, openclaw, pi) live on disk on the
	// machine that ran them; there is nothing a URL can open.
	return nil
}

// --- Slack workspace domains ------------------------------------------------

const timelineSlackDomainsTTL = time.Hour

var timelineSlackDomainsSQL = `SELECT team_id, domain FROM ` + warehouse.SQLRelation("slack_teams")

// slackDomains returns team_id -> workspace domain, cached: base_slack.teams
// has one row per workspace and never changes, but a permalink without the
// domain lands on app.slack.com's team-id router, which the iOS app does not
// intercept.
func (s *timelineService) slackDomains(ctx context.Context) map[string]string {
	s.linksMu.Lock()
	defer s.linksMu.Unlock()
	if s.slackDomainCache != nil && time.Since(s.slackDomainFetched) < timelineSlackDomainsTTL {
		return s.slackDomainCache
	}
	domains := map[string]string{}
	result, err := s.source.QueryArgs(ctx, timelineSlackDomainsSQL, nil, 100)
	if err != nil {
		s.logger.WarnContext(ctx, "slack team domains unavailable; permalinks fall back to app.slack.com", "error", err)
		// Keep whatever we had rather than clearing it on a transient error.
		if s.slackDomainCache != nil {
			return s.slackDomainCache
		}
		return domains
	}
	for _, row := range result.Rows {
		teamID, _ := row["team_id"].(string)
		domain, _ := row["domain"].(string)
		if teamID != "" && domain != "" {
			domains[teamID] = domain
		}
	}
	s.slackDomainCache = domains
	s.slackDomainFetched = time.Now()
	return domains
}

func (s *timelineService) linkEnv(ctx context.Context) timelineLinkEnv {
	return timelineLinkEnv{slackDomains: s.slackDomains(ctx), baseURL: s.baseURL}
}

// attachDeepLink decorates one list/detail/context item with its "open in
// source" link, when the source has one.
func attachDeepLink(item map[string]any, row map[string]any, env timelineLinkEnv) {
	if link := timelineDeepLinkFor(row, env); link != nil {
		item["open"] = link
	}
}
