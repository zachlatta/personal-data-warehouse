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

type recordingRunner struct {
	results map[string]RawResult
	queries []string
}

func (r *recordingRunner) Query(_ context.Context, sql string, maxRows int) (RawResult, error) {
	r.queries = append(r.queries, sql)
	result := r.results[sql]
	if maxRows > 0 && len(result.Rows) > maxRows {
		result.Rows = result.Rows[:maxRows]
	}
	return result, nil
}

func TestServiceSchemaOverviewUsesShowDescribeAndSamples(t *testing.T) {
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT currentDatabase() AS database": {
			Columns: []string{"database"},
			Rows:    []map[string]any{{"database": "default"}},
		},
		"SHOW TABLES": {
			Columns: []string{"name"},
			Rows: []map[string]any{
				{"name": "gmail_messages"},
				{"name": "slack`messages"},
			},
		},
		"SELECT * FROM `gmail_messages` LIMIT 3": {
			Columns: []string{"id", "body"},
			Rows: []map[string]any{
				{"id": "msg-1", "body": "abcdefghijklmnopqrstuvwxyz"},
				{"id": "msg-2", "body": "short"},
			},
		},
		"SELECT * FROM `slack``messages` LIMIT 3": {
			Columns: []string{"channel_id"},
			Rows:    []map[string]any{{"channel_id": "C123"}},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.SchemaOverview(context.Background())

	wantQueries := []string{
		"SELECT currentDatabase() AS database",
		"SHOW TABLES",
		"SELECT * FROM `gmail_messages` LIMIT 3",
		"SELECT * FROM `slack``messages` LIMIT 3",
	}
	if strings.Join(runner.queries, "\n") != strings.Join(wantQueries, "\n") {
		t.Fatalf("queries = %#v, want %#v", runner.queries, wantQueries)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d, want 1", len(resp.Results))
	}
	wantCSV := strings.Join([]string{
		"# default.gmail_messages",
		"",
		"id,body",
		"msg-1,abcdefghijklmno",
		"msg-2,short",
		"",
		"# default.slack`messages",
		"",
		"channel_id",
		"C123",
	}, "\n") + "\n"
	result := resp.Results[0]
	if result.Error != "" {
		t.Fatalf("SchemaOverview returned error: %s", result.Error)
	}
	if result.CSV != wantCSV {
		t.Fatalf("CSV = %q, want %q", result.CSV, wantCSV)
	}
	if len(result.Truncated.Fields) != 1 {
		t.Fatalf("sample truncations = %#v", result.Truncated.Fields)
	}
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
	if want := "body\nabcdefghij\nsecond"; result.CSV != want {
		t.Fatalf("CSV = %q, want %q", result.CSV, want)
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
	truncCSV := result.Truncated.CSV()
	if !strings.Contains(truncCSV, "rows") || !strings.Contains(truncCSV, "field") || !strings.Contains(truncCSV, "substring(body") {
		t.Fatalf("truncation CSV does not explain truncation: %q", truncCSV)
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
	if !strings.Contains(resp.Results[0].CSV, "clickhouse failed") {
		t.Fatalf("first CSV = %q", resp.Results[0].CSV)
	}
	if resp.Results[1].Error != "" || resp.Results[1].CSV != "1\n1" {
		t.Fatalf("second result = %#v", resp.Results[1])
	}
	if !strings.Contains(resp.Results[2].Error, "read-only") {
		t.Fatalf("third error = %q", resp.Results[2].Error)
	}
}

func TestServiceExecuteEscapesCSVValues(t *testing.T) {
	svc := NewService(fakeRunner{results: map[string]RawResult{
		"SELECT subject, labels FROM gmail_messages": {
			Columns: []string{"subject", "labels"},
			Rows: []map[string]any{
				{"subject": "hello, \"world\"\nnext", "labels": []string{"INBOX", "STARRED"}},
			},
		},
	}}, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.Execute(context.Background(), []string{"SELECT subject, labels FROM gmail_messages"})
	want := "subject,labels\n\"hello, \"\"world\"\"\nnext\",\"[\"\"INBOX\"\",\"\"STARRED\"\"]\""
	if resp.Results[0].CSV != want {
		t.Fatalf("CSV = %q, want %q", resp.Results[0].CSV, want)
	}
}
