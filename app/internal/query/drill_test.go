package query

import (
	"context"
	"strings"
	"testing"
)

func TestDrillSQLNamesTheRealRelationAndKeyOfAHit(t *testing.T) {
	cases := []struct {
		name        string
		sourceTable string
		sourcePK    any
		want        string
	}{
		{
			name:        "gmail, pk as decoded jsonb",
			sourceTable: "gmail_messages",
			sourcePK:    map[string]any{"account": "zach@hackclub.com", "message_id": "19eae12efa4ca3ca"},
			want:        drillWant("base_gmail.messages", ",body_text,body_markdown", "t.account = 'zach@hackclub.com' AND t.message_id = '19eae12efa4ca3ca'"),
		},
		{
			name:        "slack, pk as json text",
			sourceTable: "slack_messages",
			sourcePK:    `{"account": "zrl", "team_id": "T1", "message_ts": "1745522055.827709", "conversation_id": "D01"}`,
			want:        drillWant("base_slack.messages", "", "t.account = 'zrl' AND t.conversation_id = 'D01' AND t.message_ts = '1745522055.827709' AND t.team_id = 'T1'"),
		},
		{
			name:        "agent turn reads the mart, quotes escaped",
			sourceTable: "ai_conversation_events",
			sourcePK:    map[string]any{"source": "codex", "session_id": "o'brien", "event_uuid": "u1"},
			want:        drillWant("marts_ai_conversations.events", "", "t.event_uuid = 'u1' AND t.session_id = 'o''brien' AND t.source = 'codex'"),
		},
		{
			name:        "a session headline reads the session row, not all its events",
			sourceTable: "ai_conversation_events",
			sourcePK:    map[string]any{"source": "codex", "session_id": "s1"},
			want:        drillWant("marts_ai_conversations.sessions", "", "t.session_id = 's1' AND t.source = 'codex'"),
		},
		{
			name:        "numeric key stays a number",
			sourceTable: "calendar_events",
			sourcePK:    map[string]any{"account": "a", "calendar_id": "primary", "event_id": float64(42)},
			want:        drillWant("base_google_calendar.events", "", "t.account = 'a' AND t.calendar_id = 'primary' AND t.event_id = 42"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DrillSQL(tc.sourceTable, tc.sourcePK); got != tc.want {
				t.Fatalf("DrillSQL = %q\nwant      %q", got, tc.want)
			}
		})
	}
}

func TestDrillSQLIsEmptyWhenTheHitCannotBeResolved(t *testing.T) {
	for _, tc := range []struct {
		table string
		pk    any
	}{
		{"no_such_catalog_id", map[string]any{"id": "x"}},
		{"gmail_messages", nil},
		{"gmail_messages", "not json"},
		{"gmail_messages", map[string]any{}},
		{"gmail_messages", map[string]any{"account": map[string]any{"nested": true}}},
		{"", map[string]any{"id": "x"}},
	} {
		if got := DrillSQL(tc.table, tc.pk); got != "" {
			t.Fatalf("DrillSQL(%q, %#v) = %q, want empty", tc.table, tc.pk, got)
		}
	}
}

func TestEveryTimelineSourceTableResolvesToACatalogRelation(t *testing.T) {
	// The source_table values timeline adapters write (measured from
	// production 2026-10-13). Each must resolve, or its hits print no drill
	// and the agent goes back to guessing a relation name from the id.
	for _, id := range []string{
		"ai_conversation_events", "apple_contact_cards", "apple_messages", "apple_note_revisions",
		"calendar_events", "contact_cards", "google_drive_files", "agent_runs",
		"finance_observations", "finance_transactions", "gmail_messages", "hacker_news_items",
		"manual_finance_documents", "muse_files", "upstream_mutations", "upstream_mutation_requests",
		"photo_assets", "slack_files", "slack_messages", "marts_voice_memos_recordings",
		"whatsapp_messages", "whoop_cycles", "whoop_private_coach_messages", "whoop_recoveries",
		"whoop_sleeps", "whoop_workouts",
	} {
		if got := DrillSQL(id, map[string]any{"k": "v"}); !strings.HasPrefix(got, "SELECT kv.key, left(kv.value, ") {
			t.Errorf("source_table %q does not resolve: %q", id, got)
		}
	}
}

func TestSearchHitsCarryADrillStatement(t *testing.T) {
	hit := RawResult{
		Columns: []string{"source", "ref", "text", "source_table", "source_pk", "priority"},
		Rows: []map[string]any{{
			"source": "gmail", "ref": "gmail_email:zach@hackclub.com|abc", "text": "hello",
			"source_table": "gmail_messages", "priority": "direct",
			"source_pk": map[string]any{"account": "zach@hackclub.com", "message_id": "abc"},
		}},
	}
	runner := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: hit}}
	svc := NewService(runner, Options{})

	resp := svc.Search(context.Background(), SearchRequest{Query: "hello", Mode: "keyword"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	rows, ok := resp.Rows.([]map[string]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("rows = %#v", resp.Rows)
	}
	want := drillWant("base_gmail.messages", ",body_text,body_markdown", "t.account = 'zach@hackclub.com' AND t.message_id = 'abc'")
	if rows[0]["drill_sql"] != want {
		t.Fatalf("drill_sql = %#v, want %q", rows[0]["drill_sql"], want)
	}
	if resp.ColumnNames[len(resp.ColumnNames)-1] != "drill_sql" {
		t.Fatalf("columns = %#v", resp.ColumnNames)
	}
	if !strings.Contains(resp.Guidance, "drill_sql") {
		t.Fatalf("guidance must name drill_sql for the one-hop read: %q", resp.Guidance)
	}
}

func drillWant(relation, extraDropped, where string) string {
	return "SELECT kv.key, left(kv.value, 20000) AS value FROM " + relation +
		" t, jsonb_each_text(to_jsonb(t) - '{raw_json,payload_json,body_html,raw_metadata_json,blocks_json,body_markdown_full" + extraDropped + "}'::text[]) AS kv(key, value) WHERE " + where
}
