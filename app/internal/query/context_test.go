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
