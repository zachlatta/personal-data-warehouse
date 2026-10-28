package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
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
		{"google messages chat", map[string]any{"source": "android_messages", "context": "+15555550100", "source_pk": map[string]any{"account": "me", "device": "pichu", "message_key": "content://mms/1"}, "metadata": map[string]any{"chat_key": "+15555550100"}}, "timeline:android_messages:me:+15555550100"},
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

// Chat sources carry what Android's conversation notification needs: who
// said it, which conversation, and for a group the group's own name.
func TestChatAlertsCarryTheConversationForAndroid(t *testing.T) {
	cases := []struct {
		name, title string
		row         map[string]any
		group       bool
	}{
		{"google messages 1:1", "", map[string]any{"source": "android_messages", "actor": "Sam Example", "context": "+15555550100", "event_id": "e1",
			"source_pk": map[string]any{"account": "me"}, "metadata": map[string]any{"chat_key": "+15555550100", "chat_name": "Sam Example", "group": false}}, false},
		{"google messages group", "Sam, Alex", map[string]any{"source": "android_messages", "actor": "Sam Example", "context": "rcs-group:G1", "event_id": "e2",
			"source_pk": map[string]any{"account": "me"}, "metadata": map[string]any{"chat_key": "rcs-group:G1", "chat_name": "Sam, Alex", "group": true}}, true},
		{"google messages group with no name", "Messages group", map[string]any{"source": "android_messages", "actor": "Sam", "context": "rcs-group:G2",
			"source_pk": map[string]any{"account": "me"}, "metadata": map[string]any{"chat_key": "rcs-group:G2", "group": true}}, true},
		{"imessage group", "Dinner crew", map[string]any{"source": "apple_messages", "actor": "Sam", "context": "Dinner crew",
			"source_pk": map[string]any{"account": "me"}, "metadata": map[string]any{"chat_id": "any;+;chat1"}}, true},
		{"whatsapp group", "Book club", map[string]any{"source": "whatsapp", "actor": "Sam", "context": "Book club", "source_pk": map[string]any{"account": "me", "chat_id": "1-2@g.us"}}, true},
		{"slack dm", "", map[string]any{"source": "slack", "actor": "Sam", "context": "Sam", "source_pk": map[string]any{"team_id": "T1", "conversation_id": "D1"}}, false},
		{"slack channel", "#project", map[string]any{"source": "slack", "actor": "Sam", "context": "#project", "source_pk": map[string]any{"team_id": "T1", "conversation_id": "C1"}}, true},
	}
	for _, tc := range cases {
		a := renderTimelineNotification(tc.row, "", timelineLinkEnv{})
		s := a.Android
		if s == nil {
			t.Errorf("%s: no chat style", tc.name)
			continue
		}
		if s.Group != tc.group || s.ConversationTitle != tc.title || s.ConversationID != a.ThreadID || s.SenderName != a.Title || s.Source == "" {
			t.Errorf("%s: %+v", tc.name, s)
		}
	}
	if renderTimelineNotification(map[string]any{"source": "gmail", "actor": "Sam"}, "", timelineLinkEnv{}).Android != nil {
		t.Fatal("an email is not a chat")
	}
	if a := renderTimelineNotification(map[string]any{"source": "android_messages", "actor": "Sam"}, "https://pdw.example", timelineLinkEnv{}); !strings.HasSuffix(a.Icon, "/messages.png") || !strings.HasPrefix(a.Subtitle, "Messages") {
		t.Fatalf("google messages branding: %+v", a)
	}
}

// The avatar lookup runs while the worker holds the outbox row: a failing,
// empty, non-https or hung lookup leaves the initial avatar and never holds
// the alert past its timeout.
func TestAvatarLookupNeverDelaysOrDropsAnAlert(t *testing.T) {
	row := map[string]any{"source": "apple_messages", "actor": "Sam", "snippet": "Hi"}
	alert := renderTimelineNotification(row, "", timelineLinkEnv{})
	found := func(url string, err error) avatarLookup {
		return func(context.Context, map[string]any) (string, error) { return url, err }
	}
	if got := withAvatar(alert, row, found("https://photos.example/sam.jpg", nil), time.Second); got.Android.AvatarURL != "https://photos.example/sam.jpg" {
		t.Fatalf("avatar not attached: %+v", got.Android)
	}
	if alert.Android.AvatarURL != "" {
		t.Fatal("withAvatar mutated the alert it was given")
	}
	for name, lookup := range map[string]avatarLookup{
		"error":     found("https://photos.example/sam.jpg", errors.New("db down")),
		"not https": found("http://photos.example/sam.jpg", nil),
		"empty":     found("", nil),
		"hung": func(context.Context, map[string]any) (string, error) {
			time.Sleep(2 * time.Second)
			return "https://photos.example/late.jpg", nil
		},
	} {
		start := time.Now()
		got := withAvatar(alert, row, lookup, 50*time.Millisecond)
		if got.Android == nil || got.Android.AvatarURL != "" || got.Title != "Sam" || time.Since(start) > time.Second {
			t.Errorf("%s: %+v after %s", name, got.Android, time.Since(start))
		}
	}
	plain := renderTimelineNotification(map[string]any{"source": "gmail", "actor": "Sam"}, "", timelineLinkEnv{})
	if got := withAvatar(plain, row, found("https://photos.example/sam.jpg", nil), time.Second); got.Android != nil {
		t.Fatal("a non-chat alert gained a chat style")
	}
}

func TestNotificationPhoneDigitsMatchContactPoints(t *testing.T) {
	for in, want := range map[string]string{"+1 (555) 555-0100": "15555550100", "5555550100": "15555550100", "+447700900123": "447700900123", "Sam": "", "12345": ""} {
		if got := notificationPhoneDigits(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}
