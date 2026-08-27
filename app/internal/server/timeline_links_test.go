package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

func linkFor(t *testing.T, adapter, sourceTable, pk, metadata string) *timelineDeepLink {
	t.Helper()
	row := map[string]any{
		"adapter": adapter, "source_table": sourceTable,
		"source_pk": pk, "metadata": metadata,
	}
	return timelineDeepLinkFor(row, timelineLinkEnv{
		slackDomains: map[string]string{"T0266FRGM": "hackclub"},
		baseURL:      "https://pdw.example.test",
	})
}

func TestTimelineDeepLinkSlackMessagePermalink(t *testing.T) {
	link := linkFor(t, "slack_message", "slack_messages",
		`{"account":"zrl","team_id":"T0266FRGM","conversation_id":"D06CU09AX52","message_ts":"1787777184.214849"}`,
		`{"thread_ts":"1787777184.214849"}`)
	if link == nil {
		t.Fatal("expected a Slack deep link")
	}
	if link.URL != "https://hackclub.slack.com/archives/D06CU09AX52/p1787777184214849" {
		t.Fatalf("url = %q", link.URL)
	}
	if link.Label != "Slack" {
		t.Fatalf("label = %q", link.Label)
	}
	// The native scheme opens the message directly in the desktop/mobile app.
	if link.AppURL != "slack://channel?team=T0266FRGM&id=D06CU09AX52&message=1787777184.214849" {
		t.Fatalf("app url = %q", link.AppURL)
	}
}

func TestTimelineDeepLinkSlackThreadReplyCarriesTheThread(t *testing.T) {
	link := linkFor(t, "slack_message", "slack_messages",
		`{"account":"zrl","team_id":"T0266FRGM","conversation_id":"C123","message_ts":"1700000001.000200"}`,
		`{"thread_ts":"1700000000.000100"}`)
	if link == nil || link.URL != "https://hackclub.slack.com/archives/C123/p1700000001000200?thread_ts=1700000000.000100&cid=C123" {
		t.Fatalf("link = %#v", link)
	}
}

func TestTimelineDeepLinkSlackUnknownTeamFallsBackToAppRedirect(t *testing.T) {
	row := map[string]any{
		"adapter": "slack_message", "source_table": "slack_messages",
		"source_pk": `{"account":"zrl","team_id":"TUNKNOWN","conversation_id":"C1","message_ts":"1.2"}`,
		"metadata":  `{}`,
	}
	link := timelineDeepLinkFor(row, timelineLinkEnv{})
	if link == nil || link.URL != "https://app.slack.com/client/TUNKNOWN/C1/p12" {
		t.Fatalf("link = %#v", link)
	}
}

func TestTimelineDeepLinkGmailOpensTheMessageInThatAccount(t *testing.T) {
	link := linkFor(t, "gmail_email", "gmail_messages", `{"account":"z@x.test","message_id":"18f1abc"}`, `{}`)
	if link == nil || link.URL != "https://mail.google.com/mail/?authuser=z%40x.test#all/18f1abc" {
		t.Fatalf("link = %#v", link)
	}
	if link.Label != "Gmail" {
		t.Fatalf("label = %q", link.Label)
	}
}

func TestTimelineDeepLinkCalendarEncodesTheEventID(t *testing.T) {
	link := linkFor(t, "calendar_event", "calendar_events",
		`{"account":"z@x.test","calendar_id":"primary","event_id":"abc123"}`, `{}`)
	// Google's eid is base64("<event_id> <calendar email>") without padding.
	if link == nil || link.URL != "https://calendar.google.com/calendar/event?eid=YWJjMTIzIHpAeC50ZXN0" {
		t.Fatalf("link = %#v", link)
	}
}

func TestTimelineDeepLinkCalendarUsesTheCalendarIDWhenNotPrimary(t *testing.T) {
	link := linkFor(t, "calendar_event", "calendar_events",
		`{"account":"z@x.test","calendar_id":"team@x.test","event_id":"abc123"}`, `{}`)
	if link == nil || link.URL != "https://calendar.google.com/calendar/event?eid=YWJjMTIzIHRlYW1AeC50ZXN0" {
		t.Fatalf("link = %#v", link)
	}
}

func TestTimelineDeepLinkDriveContactsAndSessions(t *testing.T) {
	cases := []struct {
		adapter, table, pk, want, label string
	}{
		{"drive_file", "google_drive_files", `{"account":"z@x.test","file_id":"F1"}`, "https://drive.google.com/open?id=F1&authuser=z%40x.test", "Google Drive"},
		{"contact_update", "contact_cards", `{"source":"google_people","account":"z@x.test","card_id":"people/c42","source_kind":"google_contacts","address_book_id":"people/me"}`, "https://contacts.google.com/person/c42?authuser=z%40x.test", "Google Contacts"},
		{"agent_session", "ai_conversation_events", `{"source":"chatgpt","session_id":"abc"}`, "https://chatgpt.com/c/abc", "ChatGPT"},
		{"agent_session", "ai_conversation_events", `{"source":"claude_desktop","session_id":"u-1"}`, "https://claude.ai/chat/u-1", "Claude"},
		{"agent_session_turn", "ai_conversation_events", `{"source":"chatgpt","session_id":"abc","event_uuid":"e"}`, "https://chatgpt.com/c/abc", "ChatGPT"},
		{"whatsapp_message", "whatsapp_messages", `{"account":"z","chat_id":"15551234567@s.whatsapp.net","message_id":"M"}`, "https://wa.me/15551234567", "WhatsApp"},
		{"mutation_request", "upstream_mutation_requests", `{"id":"req-1"}`, "https://pdw.example.test/mutation-review/requests/req-1", "Mutation review"},
	}
	for _, tc := range cases {
		link := linkFor(t, tc.adapter, tc.table, tc.pk, `{}`)
		if link == nil {
			t.Fatalf("%s: expected a link", tc.adapter)
		}
		if link.URL != tc.want || link.Label != tc.label {
			t.Fatalf("%s: got %#v want %s / %s", tc.adapter, link, tc.want, tc.label)
		}
	}
}

func TestTimelineDeepLinkMutationOpensItsRequest(t *testing.T) {
	link := linkFor(t, "mutation", "upstream_mutations", `{"id":"mut-1"}`, `{"request_id":"req-9"}`)
	if link == nil || link.URL != "https://pdw.example.test/mutation-review/requests/req-9" {
		t.Fatalf("link = %#v", link)
	}
	bare := linkFor(t, "mutation", "upstream_mutations", `{"id":"mut-1"}`, `{}`)
	if bare == nil || bare.URL != "https://pdw.example.test/mutation-review/requests" {
		t.Fatalf("link = %#v", bare)
	}
}

func TestTimelineDeepLinkAppleNotesUsesTheNotesScheme(t *testing.T) {
	link := linkFor(t, "apple_note_revision", "apple_note_revisions",
		`{"account":"z","note_id":"ACEA58DB-8A62-4117-ABB7-47DEF297D005","revision_id":"r"}`, `{}`)
	if link == nil {
		t.Fatal("expected a Notes link")
	}
	if link.URL != "applenotes://showNote?identifier=ACEA58DB-8A62-4117-ABB7-47DEF297D005" {
		t.Fatalf("url = %q", link.URL)
	}
	if link.AppURL != "mobilenotes://showNote?identifier=ACEA58DB-8A62-4117-ABB7-47DEF297D005" {
		t.Fatalf("app url = %q", link.AppURL)
	}
}

func TestTimelineDeepLinkAppleMessagesOpensTheChatForOneToOne(t *testing.T) {
	// A 1:1 chat's identifier is the other party's handle, which Messages
	// resolves through sms:/imessage:. A group chat's identifier is opaque.
	link := linkFor(t, "apple_message", "apple_messages",
		`{"account":"z","message_id":"M"}`,
		`{"chat_id":"iMessage;-;+15551234567","service":"iMessage"}`)
	if link == nil || link.URL != "imessage://+15551234567" || link.AppURL != "sms:+15551234567" {
		t.Fatalf("link = %#v", link)
	}
	group := linkFor(t, "apple_message", "apple_messages",
		`{"account":"z","message_id":"M"}`,
		`{"chat_id":"any;+;chat138687451987295488"}`)
	if group != nil {
		t.Fatalf("group chat should have no deep link, got %#v", group)
	}
}

func TestTimelineDeepLinkGroupWhatsAppAndUnknownAdaptersHaveNone(t *testing.T) {
	if link := linkFor(t, "whatsapp_message", "whatsapp_messages",
		`{"account":"z","chat_id":"123-456@g.us","message_id":"M"}`, `{}`); link != nil {
		t.Fatalf("group WhatsApp chat should have no link, got %#v", link)
	}
	if link := linkFor(t, "whoop_cycle", "whoop_cycles", `{"account":"z","cycle_id":"1"}`, `{}`); link != nil {
		t.Fatalf("whoop should have no link, got %#v", link)
	}
	if link := linkFor(t, "slack_message", "slack_messages", `{}`, `{}`); link != nil {
		t.Fatalf("missing pk should produce no link, got %#v", link)
	}
}

func TestTimelineListAndItemCarryDeepLinks(t *testing.T) {
	item := timelineEventRow("z@x.test|m1", 20, "2026-06-01T12:00:00Z")
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM " + warehouse.SQLRelation("timeline_events") + "\nWHERE ($1":     {Rows: []map[string]any{item}},
		"FROM " + warehouse.SQLRelation("timeline_events") + "\nWHERE adapter": {Rows: []map[string]any{item}},
		"FROM " + warehouse.SQLRelation("slack_teams"):                         {Rows: []map[string]any{{"team_id": "T1", "domain": "hackclub"}}},
	}}
	srv := newTimelineTestServer(t, runner)

	resp, body := timelineGET(t, srv, "/api/timeline?limit=10", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list got %d: %s", resp.StatusCode, body)
	}
	var page struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	link, _ := page.Items[0]["open"].(map[string]any)
	if link["url"] != "https://mail.google.com/mail/?authuser=z%40x.test#all/m1" || link["label"] != "Gmail" {
		t.Fatalf("list item open = %#v", page.Items[0]["open"])
	}

	resp, body = timelineGET(t, srv, "/api/timeline/item?adapter=gmail_email&event_id=z%40x.test%7Cm1", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("item got %d: %s", resp.StatusCode, body)
	}
	var detail struct {
		Item map[string]any `json:"item"`
	}
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatal(err)
	}
	if open, _ := detail.Item["open"].(map[string]any); open["url"] == "" {
		t.Fatalf("item open = %#v", detail.Item["open"])
	}
}

func TestTimelineItemContextReturnsTheSurroundingStream(t *testing.T) {
	anchor := timelineEventRow("z@x.test|m2", 20, "2026-06-01T12:00:00Z")
	before := timelineEventRow("z@x.test|m1", 19, "2026-06-01T11:00:00Z")
	after := timelineEventRow("z@x.test|m3", 21, "2026-06-01T13:00:00Z")
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM " + warehouse.SQLRelation("timeline_events") + "\nWHERE adapter": {Rows: []map[string]any{anchor}},
		"FROM " + warehouse.SQLRelation("timeline_context"):                    {Rows: []map[string]any{before, anchor, after}},
	}}
	srv := newTimelineTestServer(t, runner)
	resp, body := timelineGET(t, srv, "/api/timeline/item/context?adapter=gmail_email&event_id=z%40x.test%7Cm2&before=3&after=4", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		Items  []map[string]any `json:"items"`
		Before int              `json:"before"`
		After  int              `json:"after"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != 3 || payload.Before != 3 || payload.After != 4 {
		t.Fatalf("payload = %s", body)
	}
	if payload.Items[1]["is_anchor"] != true || payload.Items[0]["is_anchor"] != nil {
		t.Fatalf("anchor flag wrong: %s", body)
	}
	if open, _ := payload.Items[0]["open"].(map[string]any); open["url"] == "" {
		t.Fatalf("context rows must carry deep links: %s", body)
	}
	found := false
	for i := 0; i < runner.callCount(); i++ {
		call := runner.call(i)
		if strings.Contains(call.SQL, warehouse.SQLRelation("timeline_context")) {
			found = true
			if call.Args[0] != "gmail_email:z@x.test|m2" || call.Args[1] != 3 || call.Args[2] != 4 {
				t.Fatalf("context args = %#v", call.Args)
			}
		}
	}
	if !found {
		t.Fatal("timeline.context() never ran")
	}
}

func TestTimelineItemContextClampsAndValidates(t *testing.T) {
	anchor := timelineEventRow("e", 20, "2026-06-01T12:00:00Z")
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM " + warehouse.SQLRelation("timeline_context"): {Rows: []map[string]any{anchor}},
	}}
	srv := newTimelineTestServer(t, runner)
	if resp, _ := timelineGET(t, srv, "/api/timeline/item/context?adapter=gmail_email", true); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing event_id got %d", resp.StatusCode)
	}
	if resp, _ := timelineGET(t, srv, "/api/timeline/item/context?adapter=gmail_email&event_id=e&before=x", true); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad before got %d", resp.StatusCode)
	}
	resp, body := timelineGET(t, srv, "/api/timeline/item/context?adapter=gmail_email&event_id=e&before=500&after=500", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d: %s", resp.StatusCode, body)
	}
	for i := 0; i < runner.callCount(); i++ {
		call := runner.call(i)
		if !strings.Contains(call.SQL, warehouse.SQLRelation("timeline_context")) {
			continue
		}
		if call.Args[1] != timelineContextMaxWindow || call.Args[2] != timelineContextMaxWindow {
			t.Fatalf("window not clamped: %#v", call.Args)
		}
	}
}

func TestTimelineItemDetailIncludesConversationContext(t *testing.T) {
	anchor := timelineEventRow("z@x.test|m2", 20, "2026-06-01T12:00:00Z")
	before := timelineEventRow("z@x.test|m1", 19, "2026-06-01T11:00:00Z")
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM " + warehouse.SQLRelation("timeline_events") + "\nWHERE adapter": {Rows: []map[string]any{anchor}},
		"FROM " + warehouse.SQLRelation("timeline_context"):                    {Rows: []map[string]any{before, anchor}},
	}}
	srv := newTimelineTestServer(t, runner)
	resp, body := timelineGET(t, srv, "/api/timeline/item?adapter=gmail_email&event_id=z%40x.test%7Cm2", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		Context struct {
			Items []map[string]any `json:"items"`
		} `json:"context"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Context.Items) != 2 || payload.Context.Items[1]["is_anchor"] != true {
		t.Fatalf("context = %s", body)
	}
}
