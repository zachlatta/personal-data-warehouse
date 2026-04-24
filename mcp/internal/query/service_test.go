package query

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeRunner struct {
	results map[string]RawResult
	errs    map[string]error
}

func (f fakeRunner) Query(_ context.Context, sql string, maxRows int) (RawResult, error) {
	if err := f.errs[sql]; err != nil {
		return RawResult{}, err
	}
	result := f.results[sql]
	if maxRows > 0 && len(result.Rows) > maxRows {
		result.Rows = result.Rows[:maxRows]
	}
	return result, nil
}

func TestServiceExecuteTruncatesRowsAndFields(t *testing.T) {
	svc := NewService(fakeRunner{results: map[string]RawResult{
		"SELECT body FROM gmail_messages": {
			Columns: []string{"body"},
			Rows: []map[string]any{
				{"body": "abcdefghijklmnopqrstuvwxyz"},
				{"body": "second"},
				{"body": "third"},
			},
		},
	}}, Options{MaxRows: 2, MaxFieldChars: 10})

	resp := svc.Execute(context.Background(), []string{"SELECT body FROM gmail_messages"})
	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d", len(resp.Results))
	}
	result := resp.Results[0]
	if !result.Truncated.Rows {
		t.Fatal("expected row truncation")
	}
	if got := len(result.Rows); got != 2 {
		t.Fatalf("returned rows = %d", got)
	}
	if result.Rows[0]["body"] != "abcdefghij" {
		t.Fatalf("truncated field = %#v", result.Rows[0]["body"])
	}
	if len(result.Truncated.Fields) != 1 {
		t.Fatalf("field truncations = %d", len(result.Truncated.Fields))
	}
	field := result.Truncated.Fields[0]
	if field.Column != "body" || field.Row != 0 || field.ReturnedChars != 10 || field.OriginalChars != 26 {
		t.Fatalf("unexpected field truncation: %#v", field)
	}
	if !strings.Contains(field.Instructions, "substring(body") || !strings.Contains(field.Instructions, "length(body)") {
		t.Fatalf("instructions do not explain full retrieval: %q", field.Instructions)
	}
}

func TestServiceExecuteReportsPerQueryErrors(t *testing.T) {
	svc := NewService(fakeRunner{
		results: map[string]RawResult{"SELECT 1": {Columns: []string{"1"}, Rows: []map[string]any{{"1": 1}}}},
		errs:    map[string]error{"SELECT broken": errors.New("clickhouse failed")},
	}, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.Execute(context.Background(), []string{"SELECT broken", "SELECT 1", "DROP TABLE x"})
	if len(resp.Results) != 3 {
		t.Fatalf("results length = %d", len(resp.Results))
	}
	if !strings.Contains(resp.Results[0].Error, "clickhouse failed") {
		t.Fatalf("first error = %q", resp.Results[0].Error)
	}
	if resp.Results[1].Error != "" || len(resp.Results[1].Rows) != 1 {
		t.Fatalf("second result = %#v", resp.Results[1])
	}
	if !strings.Contains(resp.Results[2].Error, "read-only") {
		t.Fatalf("third error = %q", resp.Results[2].Error)
	}
}
