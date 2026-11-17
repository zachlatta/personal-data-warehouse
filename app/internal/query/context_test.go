package query

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type failedContextLookupRunner struct {
	fakeSearchRunner
	boundedLookup bool
}

func (r *failedContextLookupRunner) QueryArgs(ctx context.Context, sql string, args []any, cap int) (RawResult, error) {
	if sql == contextGmailAccountsSQL {
		if ctx.Err() != nil {
			return RawResult{}, ctx.Err()
		}
		return RawResult{Rows: []map[string]any{{"account": "test@example.invalid"}}}, nil
	}
	if sql == contextSQL {
		return RawResult{}, errors.New("context: no timeline event for ref gmail:abc")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 2*time.Second {
		return RawResult{}, errors.New("lookup has no short deadline")
	}
	r.boundedLookup = true
	return RawResult{}, context.DeadlineExceeded
}

func TestContextGmailLookupFailureStillGivesAccounts(t *testing.T) {
	r := &failedContextLookupRunner{}
	resp := NewService(r, Options{}).Context(context.Background(), ContextRequest{Ref: "gmail:abc"})
	for _, want := range []string{"test@example.invalid", `"thread_id":"abc"`, "zrl_bot_email_control__get_thread"} {
		if !strings.Contains(resp.Error, want) {
			t.Fatalf("missing %s: %s", want, resp.Error)
		}
	}
	if strings.Contains(resp.Error, "no synced mailbox holds") {
		t.Fatal("a failed lookup cannot prove absence")
	}
	if !r.boundedLookup {
		t.Fatal("recovery lookup must have a short deadline")
	}
}

func TestContextSuggestsExactCanonicalRef(t *testing.T) {
	r := &fakeSearchRunner{
		argsErrs:    map[string]error{contextSQL: errors.New("context: no timeline event for ref mail:account|abc")},
		argsResults: map[string]RawResult{contextRefSuggestionSQL: {Rows: []map[string]any{{"ref": "gmail_email:account|abc"}}}},
	}
	resp := NewService(r, Options{}).Context(context.Background(), ContextRequest{Ref: "mail:account|abc"})
	if !strings.Contains(resp.Error, "use the canonical ref gmail_email:account|abc") {
		t.Fatal(resp.Error)
	}
	if calls := r.callsFor(contextRefSuggestionSQL); len(calls) != 1 || calls[0][0] != "mail" || calls[0][1] != "account|abc" {
		t.Fatal(calls)
	}
}

func TestContextExplainsGuessedGmailRefs(t *testing.T) {
	for _, matched := range []bool{true, false} {
		r := &fakeSearchRunner{argsErrs: map[string]error{contextSQL: errors.New("context: no timeline event for ref gmail:abc")}, argsResults: map[string]RawResult{}}
		if matched {
			r.argsResults[contextGmailRefSuggestionSQL] = RawResult{Rows: []map[string]any{{"ref": "gmail_email:test@example.invalid|abc"}}}
		}
		r.argsResults[contextGmailAccountsSQL] = RawResult{Rows: []map[string]any{{"account": "test@example.invalid"}}}
		resp := NewService(r, Options{}).Context(context.Background(), ContextRequest{Ref: "gmail:abc"})
		want := "zrl_bot_email_control__get_thread"
		if matched {
			want = "gmail_email:test@example.invalid|abc"
		}
		if !strings.Contains(resp.Error, want) || resp.TotalRows != 0 {
			t.Fatal(resp)
		}
		if !matched && !strings.Contains(resp.Error, "test@example.invalid") {
			t.Fatal(resp)
		}
	}
}

func TestContextReadsTimelineContextWithBoundParameters(t *testing.T) {
	runner := &fakeSearchRunner{
		argsResults: map[string]RawResult{
			contextSQL: {
				Columns: []string{"adapter", "event_id", "priority", "occurred_at", "actor", "title", "snippet"},
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
				Columns: []string{"adapter", "event_id", "priority", "occurred_at", "actor", "title", "text", "text_chars", "source_table", "source_pk"},
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
