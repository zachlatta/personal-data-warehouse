package query

import (
	"context"
	"strings"
	"testing"
)

func TestContextReadsTimelineContextWithBoundParameters(t *testing.T) {
	runner := &fakeSearchRunner{
		argsResults: map[string]RawResult{
			contextSQL: {
				Columns: []string{"adapter", "event_id", "priority", "event_ts", "actor", "title", "snippet"},
				Rows: []map[string]any{
					{"adapter": "slack_message", "event_id": "zrl|T|C|1.0", "priority": "direct", "actor": "sam", "snippet": "hi"},
				},
			},
		},
	}
	svc := NewService(runner, Options{})
	resp := svc.Context(context.Background(), ContextRequest{Ref: " slack_message:zrl|T|C|1.0 ", Before: 2, After: 3})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	calls := runner.callsFor(contextSQL)
	if len(calls) != 1 || calls[0][0] != "slack_message:zrl|T|C|1.0" || calls[0][1] != 2 || calls[0][2] != 3 {
		t.Fatalf("calls = %#v", calls)
	}
	if resp.TotalRows != 1 || resp.Rows[0]["actor"] != "sam" || resp.Ref != "slack_message:zrl|T|C|1.0" {
		t.Fatalf("resp = %#v", resp)
	}
	// The three columns anyone reads, plus the ids for a further hop -- never
	// SELECT *, which returned metadata and full search_text (3.8 KB vs 0.5 KB).
	if strings.Contains(contextSQL, "*") || strings.Contains(contextSQL, "search_text") {
		t.Fatalf("contextSQL must name its columns: %s", contextSQL)
	}
}

func TestContextRefusesAnEmptyRefAndAnOversizedWindow(t *testing.T) {
	runner := &fakeSearchRunner{}
	svc := NewService(runner, Options{})
	for _, req := range []ContextRequest{
		{Ref: "", Before: 5, After: 5},
		{Ref: "gmail_email:a|b", Before: -1, After: 5},
		{Ref: "gmail_email:a|b", Before: 5, After: ContextMaxWindow + 1},
	} {
		if resp := svc.Context(context.Background(), req); resp.Error == "" {
			t.Fatalf("request %#v should be refused", req)
		}
	}
	if len(runner.statements) != 0 {
		t.Fatalf("a refused request must not reach Postgres: %#v", runner.statements)
	}
}

// --full answers the question agents asked after every context read: the
// whole text of these events. Without it they hand-built SQL against a
// relation they guessed (agent_sessions.turns with turn_index: 14 sessions
// in the 30 days to 2026-10-13).
func TestContextFullReturnsEachEventsWholeTextUpToACapAndItsDrill(t *testing.T) {
	runner := &fakeSearchRunner{
		argsResults: map[string]RawResult{
			contextFullSQL: {
				Columns: []string{"adapter", "event_id", "priority", "event_ts", "actor", "title", "text", "text_chars", "source_table", "source_pk"},
				Rows: []map[string]any{{
					"adapter": "agent_session_turn", "event_id": "codex|s1|54", "priority": "background", "actor": "assistant",
					"text": "the whole turn", "text_chars": int64(14), "source_table": "ai_conversation_events",
					"source_pk": map[string]any{"source": "codex", "session_id": "s1", "event_uuid": "u54"},
				}},
			},
		},
	}
	svc := NewService(runner, Options{})
	resp := svc.Context(context.Background(), ContextRequest{Ref: "agent_session_turn:codex|s1|54", Before: 1, After: 1, Full: true})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	calls := runner.callsFor(contextFullSQL)
	if len(calls) != 1 || calls[0][3] != ContextFullTextMaxChars {
		t.Fatalf("calls = %#v", calls)
	}
	row := resp.Rows[0]
	if row["text"] != "the whole turn" {
		t.Fatalf("row = %#v", row)
	}
	if row["drill_sql"] != drillWant("marts_ai_conversations.events", "", "t.event_uuid = 'u54' AND t.session_id = 's1' AND t.source = 'codex'") {
		t.Fatalf("drill_sql = %#v", row["drill_sql"])
	}
	if _, leaked := row["source_pk"]; leaked {
		t.Fatalf("the drill replaces source_table/source_pk in the response: %#v", row)
	}
}
