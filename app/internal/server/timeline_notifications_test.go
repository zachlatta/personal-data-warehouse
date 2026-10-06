package server

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTimelineNotificationSourcePresentationAndLinks(t *testing.T) {
	row := map[string]any{"source": "slack", "source_table": "slack_messages", "adapter": "slack_message", "event_id": "a/b", "actor": "<b>Sender</b>", "title": "Sender in #channel", "snippet": "Hello &amp; welcome\n<https://example.test|Read this>", "source_pk": map[string]any{"team_id": "T123", "conversation_id": "C123", "message_ts": "100.123"}, "metadata": map[string]any{"thread_ts": "99.100"}}
	a := renderTimelineNotification(row, "https://pdw.example", timelineLinkEnv{slackDomains: map[string]string{"T123": "example"}})
	if a.Title != "Sender" || !strings.Contains(a.Subtitle, "Slack") || a.Body != "Hello & welcome Read this" {
		t.Fatalf("poor preview: %+v", a)
	}
	if a.Open == nil || !strings.Contains(a.Open.URL, "thread_ts=99.100") || a.Route != "/timeline/slack_message/a%2Fb" {
		t.Fatalf("wrong destination: %+v", a)
	}
	if a.Icon != "https://pdw.example/app/notification-icons/slack.png" {
		t.Fatal(a.Icon)
	}
	row["snippet"] = strings.Repeat("🙂", 5000)
	raw, _ := json.Marshal(renderTimelineNotification(row, "https://pdw.example", timelineLinkEnv{}))
	if len(raw) > 3500 {
		t.Fatalf("oversized preview %d", len(raw))
	}
}
func TestUnknownSourceHasHonestFallback(t *testing.T) {
	a := renderTimelineNotification(map[string]any{"source": "unknown", "adapter": "new", "event_id": "1", "title": "Event"}, "https://pdw.example", timelineLinkEnv{})
	if a.Open != nil || a.Route != "/timeline/new/1" || a.Title != "Event" {
		t.Fatalf("invented link: %+v", a)
	}
}

func TestNotificationSampleRegressions(t *testing.T) {
	calendar := renderTimelineNotification(map[string]any{"source": "calendar", "title": "Intro <Team A - Team B>", "actor": "Organizer"}, "https://pdw.example", timelineLinkEnv{})
	if calendar.Subtitle != "Google Calendar · Intro <Team A - Team B>" || !strings.HasSuffix(calendar.Icon, "/calendar.png") {
		t.Fatalf("calendar: %+v", calendar)
	}
	slack := renderTimelineNotification(map[string]any{"source": "slack", "actor": "Sender", "context": "#project", "snippet": "*<https://example.test|Read this>*"}, "https://pdw.example", timelineLinkEnv{})
	if slack.Subtitle != "Slack · #project" || slack.Body != "Read this" {
		t.Fatalf("slack: %+v", slack)
	}
	unknown := renderTimelineNotification(map[string]any{"source": "whatsapp", "snippet": "[unknown message]"}, "https://pdw.example", timelineLinkEnv{})
	if unknown.Body != "Message preview unavailable — open to view" {
		t.Fatalf("unknown preview: %+v", unknown)
	}
}

// iOS stacks alerts by thread id. One thread per source put every Slack
// message, from every channel and DM, in one pile; the pile is now the
// conversation, the person, or the thing — never the source.
func TestNotificationsGroupByConversationNotBySource(t *testing.T) {
	cases := []struct {
		name string
		row  map[string]any
		want string
	}{
		{"slack conversation", map[string]any{"source": "slack", "source_pk": map[string]any{"team_id": "T1", "conversation_id": "C1", "message_ts": "1.0"}, "metadata": map[string]any{"thread_ts": "0.9"}}, "timeline:slack:T1:C1"},
		{"slack reply stays with its channel", map[string]any{"source": "slack", "source_pk": map[string]any{"team_id": "T1", "conversation_id": "C1", "message_ts": "2.0"}}, "timeline:slack:T1:C1"},
		{"gmail thread", map[string]any{"source": "gmail", "actor": "a@example.test", "source_pk": map[string]any{"account": "me", "message_id": "m2"}, "metadata": map[string]any{"thread_id": "th1"}}, "timeline:gmail:me:th1"},
		{"gmail without thread falls back to the sender", map[string]any{"source": "gmail", "actor": "a@example.test", "source_pk": map[string]any{"account": "me", "message_id": "m3"}}, "timeline:gmail:me:from:a@example.test"},
		{"imessage chat", map[string]any{"source": "apple_messages", "context": "Group", "source_pk": map[string]any{"account": "me", "message_id": "9"}, "metadata": map[string]any{"chat_id": "iMessage;+;chat123"}}, "timeline:apple_messages:me:iMessage;+;chat123"},
		{"whatsapp chat", map[string]any{"source": "whatsapp", "source_pk": map[string]any{"account": "me", "chat_id": "123@g.us", "message_id": "9"}}, "timeline:whatsapp:me:123@g.us"},
		{"drive file", map[string]any{"source": "google_drive", "source_pk": map[string]any{"account": "me", "file_id": "F1"}}, "timeline:google_drive:me:F1"},
		{"calendar by calendar", map[string]any{"source": "calendar", "context": "primary"}, "timeline:calendar:primary"},
		{"unknown source with a context", map[string]any{"source": "other", "context": "stream"}, "timeline:other:stream"},
		{"nothing to group by", map[string]any{"source": "other"}, "timeline:other"},
	}
	for _, tc := range cases {
		got := renderTimelineNotification(tc.row, "https://pdw.example", timelineLinkEnv{}).ThreadID
		if got != tc.want {
			t.Errorf("%s: thread %q, want %q", tc.name, got, tc.want)
		}
	}
	raw, _ := json.Marshal(map[string]any{"source_pk": map[string]any{"team_id": "T1", "conversation_id": "C1"}})
	var row map[string]any
	_ = json.Unmarshal(raw, &row)
	row["source"] = "slack"
	if got := renderTimelineNotification(row, "", timelineLinkEnv{}).ThreadID; got != "timeline:slack:T1:C1" {
		t.Fatalf("source_pk arriving as JSON text is not decoded: %q", got)
	}
}

// An iMessage group with no name has only chat.db's opaque identifier as its
// context, so its alert read "Messages · 0f6d2c1e9b8a47d5a3c2e1f0b9d8c7a6":
// nothing on the phone says which conversation to open. The alert names the
// people in it instead; a named group or a 1:1 chat keeps its own context.
func TestUnnamedIMessageGroupAlertNamesItsPeople(t *testing.T) {
	members := map[string][]string{"me|any;+;0f6d": {"Sam Example", "+15555550100"}, "me|any;+;big": {"A", "B", "C", "D", "E"}}
	lookup := func(account, chatID string) []string { return members[account+"|"+chatID] }
	row := func(chatID, context string) map[string]any {
		return map[string]any{"source": "apple_messages", "actor": "+15555550100", "context": context, "snippet": "Hi",
			"source_pk": map[string]any{"account": "me", "message_id": "m1"}, "metadata": map[string]any{"chat_id": chatID}}
	}
	cases := []struct {
		name string
		row  map[string]any
		want string
	}{
		{"unnamed group", row("any;+;0f6d", "0f6d"), "Messages · Sam Example, +15555550100"},
		{"big unnamed group", row("any;+;big", "big"), "Messages · A, B, C +2"},
		{"named group keeps its name", row("any;+;0f6d", "Dinner crew"), "Messages · Dinner crew"},
		{"1:1 chat is untouched", row("any;-;+15555550199", "+15555550199"), "Messages · +15555550199"},
		{"unknown roster keeps the identifier", row("any;+;nobody", "nobody"), "Messages · nobody"},
	}
	for _, tc := range cases {
		tc.row["context"] = appleGroupContext(tc.row, lookup)
		if got := renderTimelineNotification(tc.row, "", timelineLinkEnv{}).Subtitle; got != tc.want {
			t.Errorf("%s: subtitle %q, want %q", tc.name, got, tc.want)
		}
	}
	if got := appleGroupContext(map[string]any{"source": "slack", "context": "#general"}, lookup); got != "#general" {
		t.Fatalf("non-iMessage context rewritten: %q", got)
	}
}
