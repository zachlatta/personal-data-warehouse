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
