package query

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
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
	mu      sync.Mutex
	queries []string
}

func (r *recordingRunner) Query(_ context.Context, sql string, maxRows int) (RawResult, error) {
	r.mu.Lock()
	r.queries = append(r.queries, sql)
	r.mu.Unlock()
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
		"DESCRIBE TABLE `gmail_messages`": {
			Columns: []string{"name", "type", "default_type", "default_expression", "comment"},
			Rows: []map[string]any{
				{"name": "id", "type": "String"},
				{"name": "body", "type": "String"},
			},
		},
		"SELECT substring(toString(`id`), 1, 15) AS `id`, length(toString(`id`)) AS `__pdw_preview_len_0`, substring(toString(`body`), 1, 15) AS `body`, length(toString(`body`)) AS `__pdw_preview_len_1` FROM `gmail_messages` LIMIT 3": {
			Columns: []string{"id", "__pdw_preview_len_0", "body", "__pdw_preview_len_1"},
			Rows: []map[string]any{
				{"id": "msg-1", "__pdw_preview_len_0": 5, "body": "abcdefghijklmno", "__pdw_preview_len_1": 26},
				{"id": "msg-2", "__pdw_preview_len_0": 5, "body": "short", "__pdw_preview_len_1": 5},
			},
		},
		"DESCRIBE TABLE `slack``messages`": {
			Columns: []string{"name", "type", "default_type", "default_expression", "comment"},
			Rows: []map[string]any{
				{"name": "channel_id", "type": "String"},
			},
		},
		"SELECT substring(toString(`channel_id`), 1, 15) AS `channel_id`, length(toString(`channel_id`)) AS `__pdw_preview_len_0` FROM `slack``messages` LIMIT 3": {
			Columns: []string{"channel_id", "__pdw_preview_len_0"},
			Rows:    []map[string]any{{"channel_id": "C123", "__pdw_preview_len_0": 4}},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.SchemaOverview(context.Background())

	wantQueries := []string{
		"SELECT currentDatabase() AS database",
		"SHOW TABLES",
	}
	if strings.Join(runner.queries[:2], "\n") != strings.Join(wantQueries, "\n") {
		t.Fatalf("first queries = %#v, want %#v", runner.queries[:2], wantQueries)
	}
	wantSampleQueries := []string{
		"DESCRIBE TABLE `gmail_messages`",
		"SELECT substring(toString(`id`), 1, 15) AS `id`, length(toString(`id`)) AS `__pdw_preview_len_0`, substring(toString(`body`), 1, 15) AS `body`, length(toString(`body`)) AS `__pdw_preview_len_1` FROM `gmail_messages` LIMIT 3",
		"DESCRIBE TABLE `slack``messages`",
		"SELECT substring(toString(`channel_id`), 1, 15) AS `channel_id`, length(toString(`channel_id`)) AS `__pdw_preview_len_0` FROM `slack``messages` LIMIT 3",
	}
	gotSampleQueries := append([]string(nil), runner.queries[2:]...)
	slices.Sort(gotSampleQueries)
	slices.Sort(wantSampleQueries)
	if strings.Join(gotSampleQueries, "\n") != strings.Join(wantSampleQueries, "\n") {
		t.Fatalf("sample queries = %#v, want %#v", gotSampleQueries, wantSampleQueries)
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
	longTranscript := strings.Repeat("x", 24000)
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT body FROM gmail_messages": {
			Columns: []string{"body"},
			Rows: []map[string]any{
				{"body": longTranscript},
				{"body": "second"},
				{"body": "third"},
			},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 100000, MaxFieldChars: 4000, GetFieldMaxChars: 200000})

	resp := svc.Execute(context.Background(), []string{"SELECT body FROM gmail_messages"}, 1, "csv")
	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d", len(resp.Results))
	}
	result := resp.Results[0]
	if result.QueryID == "" {
		t.Fatalf("query_id was empty: %#v", result)
	}
	if result.TotalRows != 3 {
		t.Fatalf("total rows = %d", result.TotalRows)
	}
	preview, ok := result.Preview.(string)
	if !ok {
		t.Fatalf("preview type = %T", result.Preview)
	}
	if !strings.Contains(preview, "# TRUNCATIONS: ") {
		t.Fatalf("CSV preview did not include truncation metadata line: %q", preview)
	}
	if strings.Contains(preview, "substring(") || strings.Contains(preview, "length(body)") {
		t.Fatalf("preview leaked SQL substring instructions: %q", preview)
	}
	if len(result.Truncations) != 1 {
		t.Fatalf("field truncations = %d", len(result.Truncations))
	}
	field := result.Truncations[0]
	if field.Column != "body" || field.Row != 0 || field.Returned != 4000 || field.Total != 24000 {
		t.Fatalf("unexpected field truncation: %#v", field)
	}
	rawTruncations := strings.TrimPrefix(preview[strings.LastIndex(preview, "# TRUNCATIONS: "):], "# TRUNCATIONS: ")
	var parsed []FieldTruncation
	if err := json.Unmarshal([]byte(rawTruncations), &parsed); err != nil {
		t.Fatalf("truncation metadata is not parseable JSON: %v; %q", err, rawTruncations)
	}
	fieldResp := svc.GetField(result.QueryID, 0, "body", 0, 200000)
	if fieldResp.Error != "" {
		t.Fatalf("GetField returned error: %s", fieldResp.Error)
	}
	if fieldResp.Value != longTranscript || !fieldResp.EOF || fieldResp.TotalChars != 24000 || fieldResp.ReturnedChars != 24000 {
		t.Fatalf("GetField did not return full transcript: %#v", fieldResp)
	}
	if len(runner.queries) != 1 {
		t.Fatalf("expected one SQL execution, got %#v", runner.queries)
	}
}

func TestServiceExecuteReportsPerQueryErrors(t *testing.T) {
	svc := NewService(fakeRunner{
		results: map[string]RawResult{"SELECT 1": {Columns: []string{"1"}, Rows: []map[string]any{{"1": 1}}}},
		errs:    map[string]error{"SELECT broken": errors.New("clickhouse failed")},
	}, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.Execute(context.Background(), []string{"SELECT broken", "SELECT 1", "DROP TABLE x"}, 20, "csv")
	if len(resp.Results) != 3 {
		t.Fatalf("results length = %d", len(resp.Results))
	}
	if !strings.Contains(resp.Results[0].Error, "clickhouse failed") {
		t.Fatalf("first error = %q", resp.Results[0].Error)
	}
	if resp.Results[1].Error != "" || resp.Results[1].QueryID == "" || resp.Results[1].Preview != "1\n1" {
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

	resp := svc.Execute(context.Background(), []string{"SELECT subject, labels FROM gmail_messages"}, 20, "csv")
	want := "subject,labels\n\"hello, \"\"world\"\"\nnext\",\"[\"\"INBOX\"\",\"\"STARRED\"\"]\""
	if resp.Results[0].Preview != want {
		t.Fatalf("CSV = %q, want %q", resp.Results[0].Preview, want)
	}
}

func TestServiceGetRowsPaginatesCachedRowsAndInheritsFormat(t *testing.T) {
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT id, body FROM gmail_messages ORDER BY id": {
			Columns: []string{"id", "body"},
			Rows: []map[string]any{
				{"id": 1, "body": "one"},
				{"id": 2, "body": "two"},
				{"id": 3, "body": "three"},
			},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 100000})
	resp := svc.Execute(context.Background(), []string{"SELECT id, body FROM gmail_messages ORDER BY id"}, 1, "ndjson")

	rows := svc.GetRows(resp.Results[0].QueryID, 1, 2, "")
	if rows.Error != "" {
		t.Fatalf("GetRows error: %s", rows.Error)
	}
	if rows.Format != "ndjson" || rows.Rows != "{\"body\":\"two\",\"id\":2}\n{\"body\":\"three\",\"id\":3}" {
		t.Fatalf("unexpected paginated rows: %#v", rows)
	}
	if len(runner.queries) != 1 {
		t.Fatalf("GetRows re-executed SQL: %#v", runner.queries)
	}
}

func TestServiceGetFieldReadsTailsWithoutSQLSubstringArithmetic(t *testing.T) {
	rows := make([]map[string]any, 18)
	for i := range rows {
		rows[i] = map[string]any{"id": i, "transcript": strings.Repeat("head ", 1000) + "tail-marker"}
	}
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT id, transcript FROM voice_memo_transcripts ORDER BY id LIMIT 18": {
			Columns: []string{"id", "transcript"},
			Rows:    rows,
		},
	}}
	svc := NewService(runner, Options{MaxRows: 100000, MaxFieldChars: 20})

	resp := svc.Execute(context.Background(), []string{"SELECT id, transcript FROM voice_memo_transcripts ORDER BY id LIMIT 18"}, 18, "json")
	queryID := resp.Results[0].QueryID
	if queryID == "" {
		t.Fatalf("missing query_id: %#v", resp.Results[0])
	}
	for i := range rows {
		value := rows[i]["transcript"].(string)
		field := svc.GetField(queryID, i, "transcript", utf8RuneLen(value)-11, 11)
		if field.Error != "" {
			t.Fatalf("row %d GetField error: %s", i, field.Error)
		}
		if field.Value != "tail-marker" {
			t.Fatalf("row %d tail = %q", i, field.Value)
		}
	}
	if len(runner.queries) != 1 {
		t.Fatalf("expected one SQL execution, got %#v", runner.queries)
	}
	for _, sql := range runner.queries {
		if strings.Contains(strings.ToLower(sql), "substring") {
			t.Fatalf("unexpected substring SQL: %s", sql)
		}
	}
}

func TestServiceGrepRowsSearchesCachedResults(t *testing.T) {
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT id, transcript FROM voice_memo_transcripts": {
			Columns: []string{"id", "transcript"},
			Rows: []map[string]any{
				{"id": "a", "transcript": "nothing here"},
				{"id": "b", "transcript": "we discussed weighted projects yesterday"},
			},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 100000, MaxFieldChars: 20})
	resp := svc.Execute(context.Background(), []string{"SELECT id, transcript FROM voice_memo_transcripts"}, 2, "json")

	grep := svc.GrepRows(resp.Results[0].QueryID, "weighted projects", []string{"transcript"}, 100, 5)
	if grep.Error != "" {
		t.Fatalf("GrepRows error: %s", grep.Error)
	}
	if len(grep.Matches) != 1 {
		t.Fatalf("matches = %#v", grep.Matches)
	}
	match := grep.Matches[0]
	if match.RowIndex != 1 || match.Column != "transcript" || !strings.Contains(match.Context, "weighted projects") {
		t.Fatalf("unexpected match: %#v", match)
	}
	if len(runner.queries) != 1 {
		t.Fatalf("grep re-executed SQL: %#v", runner.queries)
	}
}

func TestServiceUnknownAndExpiredQueryIDErrorsAreActionable(t *testing.T) {
	svc := NewService(fakeRunner{results: map[string]RawResult{
		"SELECT body FROM gmail_messages": {Columns: []string{"body"}, Rows: []map[string]any{{"body": "hello"}}},
	}}, Options{QueryCacheTTL: time.Nanosecond})

	unknown := svc.GetRows("missing", 0, 1, "")
	if !strings.Contains(unknown.Error, "unknown or expired query_id") || !strings.Contains(unknown.Error, "re-run query") {
		t.Fatalf("unknown error not actionable: %q", unknown.Error)
	}
	resp := svc.Execute(context.Background(), []string{"SELECT body FROM gmail_messages"}, 1, "csv")
	time.Sleep(time.Millisecond)
	expired := svc.GetField(resp.Results[0].QueryID, 0, "body", 0, 10)
	if !strings.Contains(expired.Error, "unknown or expired query_id") || !strings.Contains(expired.Error, "server restarts") {
		t.Fatalf("expired error not actionable: %q", expired.Error)
	}
}

func TestServiceRejectsQueriesOverRowCap(t *testing.T) {
	svc := NewService(fakeRunner{results: map[string]RawResult{
		"SELECT body FROM gmail_messages": {
			Columns: []string{"body"},
			Rows: []map[string]any{
				{"body": "one"},
				{"body": "two"},
				{"body": "three"},
			},
		},
	}}, Options{MaxRows: 2})

	resp := svc.Execute(context.Background(), []string{"SELECT body FROM gmail_messages"}, 20, "csv")
	if !strings.Contains(resp.Results[0].Error, "more than MCP_MAX_ROWS") {
		t.Fatalf("expected row cap error, got %#v", resp.Results[0])
	}
}

func utf8RuneLen(value string) int {
	return len([]rune(value))
}
