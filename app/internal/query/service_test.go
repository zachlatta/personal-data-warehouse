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
	errs    map[string]error
	mu      sync.Mutex
	queries []string
	maxRows []int
}

func (r *recordingRunner) Query(_ context.Context, sql string, maxRows int) (RawResult, error) {
	r.mu.Lock()
	r.queries = append(r.queries, sql)
	r.maxRows = append(r.maxRows, maxRows)
	r.mu.Unlock()
	if err := r.errs[sql]; err != nil {
		return RawResult{}, err
	}
	result := r.results[sql]
	if maxRows > 0 && len(result.Rows) > maxRows {
		result.Rows = result.Rows[:maxRows]
	}
	return result, nil
}

func statement(question, sql string) Statement {
	return Statement{Question: question, SQL: sql}
}

func describeColumnsSQL(table string) string {
	return "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type " +
		"FROM pg_attribute a " +
		"JOIN pg_class c ON c.oid = a.attrelid " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE n.nspname = current_schema() AND c.relname = '" + strings.ReplaceAll(table, "'", "''") + "' " +
		"AND a.attnum > 0 AND NOT a.attisdropped " +
		"ORDER BY a.attnum"
}

func TestServiceSchemaOverviewUsesInformationSchemaAndSamples(t *testing.T) {
	showTablesSQL := "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name"
	describeCleanGmailSQL := describeColumnsSQL("clean_gmail_inbox")
	describeGmailSQL := describeColumnsSQL("gmail_messages")
	describeSlackSQL := describeColumnsSQL("slack\"messages")
	indexSQL := "SELECT t.relname AS name, regexp_replace(pg_get_indexdef(ix.indexrelid), '^.* USING ', '') AS def, CASE WHEN ix.indisprimary THEN ' [primary key]' WHEN ix.indisunique THEN ' [unique]' ELSE '' END AS flag FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid JOIN pg_class t ON t.oid = ix.indrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relkind IN ('r', 'p', 'm') ORDER BY t.relname, ix.indisprimary DESC, def"
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT current_database() AS database": {
			Columns: []string{"database"},
			Rows:    []map[string]any{{"database": "default"}},
		},
		showTablesSQL: {
			Columns: []string{"name"},
			Rows: []map[string]any{
				{"name": "clean_gmail_inbox"},
				{"name": "gmail_messages"},
				{"name": "slack\"messages"},
			},
		},
		"SELECT c.relname AS name, c.reltuples::bigint AS row_estimate FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'm') AND c.reltuples >= 0": {
			Columns: []string{"name", "row_estimate"},
			Rows: []map[string]any{
				{"name": "gmail_messages", "row_estimate": int64(1234567)},
				{"name": "slack\"messages", "row_estimate": int64(42)},
			},
		},
		indexSQL: {
			Columns: []string{"name", "def", "flag"},
			Rows: []map[string]any{
				{"name": "gmail_messages", "def": "btree (account, message_id)", "flag": " [primary key]"},
				{"name": "gmail_messages", "def": "gin (body_text gin_trgm_ops)", "flag": ""},
				{"name": "slack\"messages", "def": "btree (account, team_id, conversation_id, message_ts)", "flag": " [primary key]"},
				{"name": "slack\"messages", "def": "bm25 (text) WITH (text_config='english')", "flag": ""},
				{"name": "slack\"messages", "def": "gin (text gin_trgm_ops)", "flag": ""},
			},
		},
		describeCleanGmailSQL: {
			Columns: []string{"name", "type", "default_type", "default_expression", "comment"},
			Rows: []map[string]any{
				{"name": "thread_id", "type": "String"},
				{"name": "latest_subject", "type": "String"},
			},
		},
		"SELECT substring(\"thread_id\"::text from 1 for 15) AS \"thread_id\", char_length(\"thread_id\"::text) AS \"__pdw_preview_len_0\", substring(\"latest_subject\"::text from 1 for 15) AS \"latest_subject\", char_length(\"latest_subject\"::text) AS \"__pdw_preview_len_1\" FROM \"clean_gmail_inbox\" LIMIT 3": {
			Columns: []string{"thread_id", "__pdw_preview_len_0", "latest_subject", "__pdw_preview_len_1"},
			Rows:    []map[string]any{{"thread_id": "thread-1", "__pdw_preview_len_0": 8, "latest_subject": "hello inbox", "__pdw_preview_len_1": 11}},
		},
		describeGmailSQL: {
			Columns: []string{"name", "type", "default_type", "default_expression", "comment"},
			Rows: []map[string]any{
				{"name": "id", "type": "String"},
				{"name": "body", "type": "String"},
			},
		},
		"SELECT substring(\"id\"::text from 1 for 15) AS \"id\", char_length(\"id\"::text) AS \"__pdw_preview_len_0\", substring(\"body\"::text from 1 for 15) AS \"body\", char_length(\"body\"::text) AS \"__pdw_preview_len_1\" FROM \"gmail_messages\" LIMIT 3": {
			Columns: []string{"id", "__pdw_preview_len_0", "body", "__pdw_preview_len_1"},
			Rows: []map[string]any{
				{"id": "msg-1", "__pdw_preview_len_0": 5, "body": "abcdefghijklmno", "__pdw_preview_len_1": 26},
				{"id": "msg-2", "__pdw_preview_len_0": 5, "body": "short", "__pdw_preview_len_1": 5},
			},
		},
		describeSlackSQL: {
			Columns: []string{"name", "type", "default_type", "default_expression", "comment"},
			Rows: []map[string]any{
				{"name": "channel_id", "type": "String"},
			},
		},
		"SELECT substring(\"channel_id\"::text from 1 for 15) AS \"channel_id\", char_length(\"channel_id\"::text) AS \"__pdw_preview_len_0\" FROM \"slack\"\"messages\" LIMIT 3": {
			Columns: []string{"channel_id", "__pdw_preview_len_0"},
			Rows:    []map[string]any{{"channel_id": "C123", "__pdw_preview_len_0": 4}},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.SchemaOverview(context.Background())

	rowEstimateSQL := "SELECT c.relname AS name, c.reltuples::bigint AS row_estimate FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'm') AND c.reltuples >= 0"
	wantQueries := []string{
		"SELECT current_database() AS database",
		showTablesSQL,
		rowEstimateSQL,
		indexSQL,
	}
	if strings.Join(runner.queries[:4], "\n") != strings.Join(wantQueries, "\n") {
		t.Fatalf("first queries = %#v, want %#v", runner.queries[:4], wantQueries)
	}
	wantSampleQueries := []string{
		describeCleanGmailSQL,
		"SELECT substring(\"thread_id\"::text from 1 for 15) AS \"thread_id\", char_length(\"thread_id\"::text) AS \"__pdw_preview_len_0\", substring(\"latest_subject\"::text from 1 for 15) AS \"latest_subject\", char_length(\"latest_subject\"::text) AS \"__pdw_preview_len_1\" FROM \"clean_gmail_inbox\" LIMIT 3",
		describeGmailSQL,
		"SELECT substring(\"id\"::text from 1 for 15) AS \"id\", char_length(\"id\"::text) AS \"__pdw_preview_len_0\", substring(\"body\"::text from 1 for 15) AS \"body\", char_length(\"body\"::text) AS \"__pdw_preview_len_1\" FROM \"gmail_messages\" LIMIT 3",
		describeSlackSQL,
		"SELECT substring(\"channel_id\"::text from 1 for 15) AS \"channel_id\", char_length(\"channel_id\"::text) AS \"__pdw_preview_len_0\" FROM \"slack\"\"messages\" LIMIT 3",
	}
	gotSampleQueries := append([]string(nil), runner.queries[4:]...)
	slices.Sort(gotSampleQueries)
	slices.Sort(wantSampleQueries)
	if strings.Join(gotSampleQueries, "\n") != strings.Join(wantSampleQueries, "\n") {
		t.Fatalf("sample queries = %#v, want %#v", gotSampleQueries, wantSampleQueries)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d, want 1", len(resp.Results))
	}
	wantCSV := strings.Join([]string{
		"-- Reference these tables by their bare name in FROM/JOIN (e.g. FROM gmail_messages). Do not prefix them with the database name (\"default.\").",
		"-- Each column header below is annotated with its Postgres type in parentheses, e.g. is_deleted (bigint), to_addresses (text[]).",
		"-- Each table lists its indexes. A gin (col gin_trgm_ops) index makes ILIKE and ~/~* substring search fast on that one column: match the column directly, e.g. body_text ILIKE '%x%' OR subject ILIKE '%x%'. Wrapping columns in lower(a || b || ...) or casting (to_addresses::text) bypasses every index and forces a full table scan, so search each indexed column separately and keep un-indexed expressions out of the same OR.",
		"-- A bm25 (col) index supports relevance-ranked word search: SELECT ..., col <@> 'search terms' AS score FROM t ORDER BY col <@> 'search terms' LIMIT 20. Scores are negative (more negative = better; non-matches score 0); ALWAYS pair <@> with that ORDER BY plus a LIMIT, otherwise the index is not used. BM25 matches stemmed whole words only — no phrase queries and no typo tolerance — so for possibly-misspelled terms switch to the trigram indexes: WHERE col %> 'qery' ORDER BY word_similarity('qery', col) DESC LIMIT 20. Rule of thumb: <@> for topics and wording you trust, %> for fuzzy/typo'd terms, ILIKE for exact substrings.",
		"-- Cross-source search (the default way to find things across the warehouse): SELECT * FROM search_text('offer letter', 50). It fans out to the per-table BM25 indexes and returns (source, subsource, context, who, occurred_at, account, ref, text, score) ranked across EVERY source — gmail, gmail attachments, slack messages/channels/files, apple notes, imessage, whatsapp, meeting transcripts, calendar, contacts, agent runs/sessions, and mutations — with score lower (more negative) = better. Optional args: search_text(query, max_results, sources => ARRAY['slack','gmail'], since => '2026-03-01'). Use it for any broad 'find every mention of X' question; then drill into the underlying table via ref for full rows. (There is no cross-source view to scan with ILIKE — single-table BM25 <@> / trigram %> / ILIKE are for when you already know the table.)",
		"-- For meetings, search_text(query, sources => ARRAY['transcript']) ranks the raw transcripts and surfaces the per-recording 'action_items' and 'summary' subsources (enrichment already extracted the commitments — read those first). Summaries are lossy, though: before reporting an email request as unanswered or a question as open, search the full transcript text (and Slack DMs) dated AFTER the request — decisions are often made on calls and appear only in raw transcripts.",
		"-- Sample values below are previews truncated to 15 characters; query a table directly for full values.",
		"",
		"# clean_gmail_inbox",
		"",
		"thread_id (String),latest_subject (String)",
		"thread-1,hello inbox",
		"",
		"# gmail_messages (~1,234,567 rows, estimated)",
		"# indexes:",
		"#   btree (account, message_id) [primary key]",
		"#   gin (body_text gin_trgm_ops)",
		"",
		"id (String),body (String)",
		"msg-1,abcdefghijklmno",
		"msg-2,short",
		"",
		"# slack\"messages (~42 rows, estimated)",
		"# indexes:",
		"#   btree (account, team_id, conversation_id, message_ts) [primary key]",
		"#   bm25 (text) WITH (text_config='english')",
		"#   gin (text gin_trgm_ops)",
		"",
		"channel_id (String)",
		"C123",
	}, "\n") + "\n"
	result := resp.Results[0]
	if result.Error != "" {
		t.Fatalf("SchemaOverview returned error: %s", result.Error)
	}
	if result.CSV != wantCSV {
		t.Fatalf("CSV = %q, want %q", result.CSV, wantCSV)
	}
	// The schema overview deliberately emits no per-field truncation table:
	// every sample value is a fixed-width preview, so a truncation row per
	// field would be pure noise (and there is no cached query_id to fetch
	// fuller values with). The leading note states the preview cap instead.
	if !result.Truncated.Empty() {
		t.Fatalf("schema overview should not emit a truncation table, got %#v", result.Truncated)
	}
}

func TestFormatRowCount(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{7, "7"},
		{42, "42"},
		{999, "999"},
		{1000, "1,000"},
		{12345, "12,345"},
		{1234567, "1,234,567"},
		{33387274, "33,387,274"},
		{-1500, "-1,500"},
	}
	for _, c := range cases {
		if got := formatRowCount(c.in); got != c.want {
			t.Errorf("formatRowCount(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestServiceSchemaOverviewSkipsRowCountWhenLookupFails(t *testing.T) {
	const rowEstimateSQL = "SELECT c.relname AS name, c.reltuples::bigint AS row_estimate FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'm') AND c.reltuples >= 0"
	showTablesSQL := "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name"
	describeSQL := describeColumnsSQL("gmail_messages")
	sampleSQL := "SELECT substring(\"id\"::text from 1 for 15) AS \"id\", char_length(\"id\"::text) AS \"__pdw_preview_len_0\" FROM \"gmail_messages\" LIMIT 3"
	runner := &recordingRunner{
		results: map[string]RawResult{
			"SELECT current_database() AS database": {
				Columns: []string{"database"}, Rows: []map[string]any{{"database": "default"}},
			},
			showTablesSQL: {Columns: []string{"name"}, Rows: []map[string]any{{"name": "gmail_messages"}}},
			describeSQL: {
				Columns: []string{"name"},
				Rows:    []map[string]any{{"name": "id"}},
			},
			sampleSQL: {
				Columns: []string{"id", "__pdw_preview_len_0"},
				Rows:    []map[string]any{{"id": "msg-1", "__pdw_preview_len_0": 5}},
			},
		},
		errs: map[string]error{rowEstimateSQL: errors.New("pg_class lookup denied")},
	}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.SchemaOverview(context.Background())
	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d", len(resp.Results))
	}
	if resp.Results[0].Error != "" {
		t.Fatalf("SchemaOverview surfaced row-estimate failure as error: %q", resp.Results[0].Error)
	}
	if !strings.Contains(resp.Results[0].CSV, "# gmail_messages\n") {
		t.Fatalf("expected unannotated heading when row estimate lookup fails, got %q", resp.Results[0].CSV)
	}
}

func TestServiceExecuteTruncatesRowsAndFields(t *testing.T) {
	longTranscript := strings.Repeat("x", 24000)
	question := "What Gmail message bodies should be previewed?"
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

	resp := svc.Execute(context.Background(), []Statement{statement(question, "SELECT body FROM gmail_messages")}, 1, "csv")
	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d", len(resp.Results))
	}
	result := resp.Results[0]
	if result.QueryID == "" {
		t.Fatalf("query_id was empty: %#v", result)
	}
	entry, err := svc.cache.get(result.QueryID)
	if err != nil {
		t.Fatalf("cached query missing: %v", err)
	}
	if entry.Question != question {
		t.Fatalf("cached question = %q, want %q", entry.Question, question)
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
	fieldResp := svc.GetField(context.Background(), result.QueryID, 0, "body", 0, 200000)
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

func TestServiceExecuteRequiresQuestionPerSQLStatement(t *testing.T) {
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT 1": {Columns: []string{"1"}, Rows: []map[string]any{{"1": 1}}},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	noQueries := svc.Execute(context.Background(), nil, 20, "csv")
	if !strings.Contains(noQueries.Results[0].Error, "queries must contain at least one") {
		t.Fatalf("missing queries error = %#v", noQueries.Results[0])
	}
	blankQuestion := svc.Execute(context.Background(), []Statement{statement(" ", "SELECT 1")}, 20, "csv")
	if !strings.Contains(blankQuestion.Results[0].Error, "queries[0].question") {
		t.Fatalf("blank question error = %#v", blankQuestion.Results[0])
	}
	blankSQL := svc.Execute(context.Background(), []Statement{statement("What is one?", " ")}, 20, "csv")
	if !strings.Contains(blankSQL.Results[0].Error, "queries[0].sql") {
		t.Fatalf("blank sql error = %#v", blankSQL.Results[0])
	}
	if len(runner.queries) != 0 {
		t.Fatalf("invalid query inputs executed SQL: %#v", runner.queries)
	}
}

func TestServiceExecuteReportsPerQueryErrors(t *testing.T) {
	svc := NewService(fakeRunner{
		results: map[string]RawResult{"SELECT 1": {Columns: []string{"1"}, Rows: []map[string]any{{"1": 1}}}},
		errs:    map[string]error{"SELECT broken": errors.New("postgres failed")},
	}, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.Execute(context.Background(), []Statement{
		statement("What happens when Postgres returns an error?", "SELECT broken"),
		statement("Does a simple read-only query work?", "SELECT 1"),
		statement("Is a destructive query rejected?", "DROP TABLE x"),
	}, 20, "csv")
	if len(resp.Results) != 3 {
		t.Fatalf("results length = %d", len(resp.Results))
	}
	if !strings.Contains(resp.Results[0].Error, "postgres failed") {
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

	resp := svc.Execute(context.Background(), []Statement{statement("What Gmail subjects and labels need CSV escaping?", "SELECT subject, labels FROM gmail_messages")}, 20, "csv")
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
	resp := svc.Execute(context.Background(), []Statement{statement("Which Gmail messages should be paginated by id?", "SELECT id, body FROM gmail_messages ORDER BY id")}, 1, "ndjson")

	rows := svc.GetRows(context.Background(), resp.Results[0].QueryID, 1, 2, "")
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
		rows[i] = map[string]any{"recording_id": i, "transcript": strings.Repeat("head ", 1000) + "tail-marker"}
	}
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT recording_id, transcript FROM apple_voice_memos_enrichments ORDER BY recording_id LIMIT 18": {
			Columns: []string{"recording_id", "transcript"},
			Rows:    rows,
		},
	}}
	svc := NewService(runner, Options{MaxRows: 100000, MaxFieldChars: 20})

	resp := svc.Execute(context.Background(), []Statement{statement("Which transcript tails should be available without substring SQL?", "SELECT recording_id, transcript FROM apple_voice_memos_enrichments ORDER BY recording_id LIMIT 18")}, 18, "json")
	queryID := resp.Results[0].QueryID
	if queryID == "" {
		t.Fatalf("missing query_id: %#v", resp.Results[0])
	}
	for i := range rows {
		value := rows[i]["transcript"].(string)
		field := svc.GetField(context.Background(), queryID, i, "transcript", utf8RuneLen(value)-11, 11)
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
		"SELECT recording_id, transcript FROM apple_voice_memos_enrichments": {
			Columns: []string{"recording_id", "transcript"},
			Rows: []map[string]any{
				{"recording_id": "a", "transcript": "nothing here"},
				{"recording_id": "b", "transcript": "we discussed weighted projects yesterday"},
			},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 100000, MaxFieldChars: 20})
	resp := svc.Execute(context.Background(), []Statement{statement("Which transcripts mention weighted projects?", "SELECT recording_id, transcript FROM apple_voice_memos_enrichments")}, 2, "json")

	grep := svc.GrepRows(context.Background(), resp.Results[0].QueryID, "weighted projects", []string{"transcript"}, 100, 5)
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

	unknown := svc.GetRows(context.Background(), "missing", 0, 1, "")
	if !strings.Contains(unknown.Error, "unknown or expired query_id") || !strings.Contains(unknown.Error, "re-run query") {
		t.Fatalf("unknown error not actionable: %q", unknown.Error)
	}
	resp := svc.Execute(context.Background(), []Statement{statement("Which Gmail body should expire from the query cache?", "SELECT body FROM gmail_messages")}, 1, "csv")
	time.Sleep(time.Millisecond)
	expired := svc.GetField(context.Background(), resp.Results[0].QueryID, 0, "body", 0, 10)
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

	resp := svc.Execute(context.Background(), []Statement{statement("Does the Gmail body query exceed the row cap?", "SELECT body FROM gmail_messages")}, 20, "csv")
	if !strings.Contains(resp.Results[0].Error, "more than MCP_MAX_ROWS") {
		t.Fatalf("expected row cap error, got %#v", resp.Results[0])
	}
}

func utf8RuneLen(value string) int {
	return len([]rune(value))
}

func TestExecuteFullReturnsFullCSVWithoutCachingOrTruncation(t *testing.T) {
	bigBody := strings.Repeat("x", 250000)
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT id, body FROM gmail_messages ORDER BY id": {
			Columns: []string{"id", "body"},
			Rows: []map[string]any{
				{"id": 1, "body": bigBody},
				{"id": 2, "body": "second"},
			},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 1, MaxFieldChars: 10})

	resp := svc.ExecuteFull(context.Background(), "Show me every gmail body in order.", "SELECT id, body FROM gmail_messages ORDER BY id", "csv")
	if resp.Error != "" {
		t.Fatalf("ExecuteFull error: %s", resp.Error)
	}
	if resp.Question != "Show me every gmail body in order." {
		t.Fatalf("question = %q", resp.Question)
	}
	if resp.Format != "csv" || resp.TotalRows != 2 {
		t.Fatalf("unexpected metadata: %#v", resp)
	}
	body, ok := resp.Rows.(string)
	if !ok {
		t.Fatalf("rows type = %T", resp.Rows)
	}
	if !strings.Contains(body, bigBody) {
		t.Fatalf("ExecuteFull truncated the big body field; output length = %d", len(body))
	}
	if !slices.Equal(resp.ColumnNames, []string{"id", "body"}) {
		t.Fatalf("column names = %#v", resp.ColumnNames)
	}
	if status := svc.DebugCacheStatus(); len(status.Queries) != 0 {
		t.Fatalf("ExecuteFull populated query cache: %#v", status.Queries)
	}
	if len(runner.maxRows) != 1 || runner.maxRows[0] != FullQueryRowCap+1 {
		t.Fatalf("runner maxRows = %#v, want [%d]", runner.maxRows, FullQueryRowCap+1)
	}
}

func TestExecuteFullRejectsWriteSQL(t *testing.T) {
	svc := NewService(fakeRunner{}, Options{})
	resp := svc.ExecuteFull(context.Background(), "Delete everything?", "DELETE FROM gmail_messages", "csv")
	if resp.Error == "" {
		t.Fatalf("expected validator to reject DELETE, got %#v", resp)
	}
}

func TestExecuteFullRejectsBlankQuestion(t *testing.T) {
	svc := NewService(fakeRunner{}, Options{})
	resp := svc.ExecuteFull(context.Background(), "   ", "SELECT 1", "csv")
	if !strings.Contains(resp.Error, "question") {
		t.Fatalf("expected blank-question error, got %#v", resp)
	}
}

func TestExecuteFullJSONFormatReturnsRowSlice(t *testing.T) {
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT id FROM gmail_messages": {
			Columns: []string{"id"},
			Rows:    []map[string]any{{"id": 1}, {"id": 2}},
		},
	}}
	svc := NewService(runner, Options{})
	resp := svc.ExecuteFull(context.Background(), "Which message ids exist?", "SELECT id FROM gmail_messages", "json")
	if resp.Error != "" {
		t.Fatalf("ExecuteFull error: %s", resp.Error)
	}
	rows, ok := resp.Rows.([]map[string]any)
	if !ok {
		t.Fatalf("rows type = %T", resp.Rows)
	}
	if len(rows) != 2 || rows[0]["id"] != 1 || rows[1]["id"] != 2 {
		t.Fatalf("unexpected rows: %#v", rows)
	}

	encoded, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(encoded), `"rows":[{"id":1},{"id":2}]`) {
		t.Fatalf("encoded JSON missing rows array: %s", encoded)
	}
}
