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

func describeColumnsSQL(schema, table string) string {
	return "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type " +
		"FROM pg_attribute a " +
		"JOIN pg_class c ON c.oid = a.attrelid " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE n.nspname = '" + strings.ReplaceAll(schema, "'", "''") + "' AND c.relname = '" + strings.ReplaceAll(table, "'", "''") + "' " +
		"AND a.attnum > 0 AND NOT a.attisdropped " +
		"ORDER BY a.attnum"
}

func TestServiceSchemaOverviewUsesInformationSchemaAndSamples(t *testing.T) {
	showTablesSQL := "SELECT table_schema AS schema, table_name AS name FROM information_schema.tables WHERE table_schema = ANY(" + queryableSchemaArraySQL() + ") AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_schema, table_name"
	describeCleanGmailSQL := describeColumnsSQL("marts", "gmail_inbox")
	describeGmailSQL := describeColumnsSQL("gmail", "messages")
	describeSlackSQL := describeColumnsSQL("slack", "messages")
	indexSQL := "SELECT n.nspname AS schema, t.relname AS name, regexp_replace(pg_get_indexdef(ix.indexrelid), '^.* USING ', '') AS def, CASE WHEN ix.indisprimary THEN ' [primary key]' WHEN ix.indisunique THEN ' [unique]' ELSE '' END AS flag FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid JOIN pg_class t ON t.oid = ix.indrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = ANY(" + queryableSchemaArraySQL() + ") AND t.relkind IN ('r', 'p', 'm') ORDER BY n.nspname, t.relname, ix.indisprimary DESC, def"
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT current_database() AS database": {
			Columns: []string{"database"},
			Rows:    []map[string]any{{"database": "default"}},
		},
		showTablesSQL: {
			Columns: []string{"schema", "name"},
			Rows: []map[string]any{
				{"schema": "marts", "name": "gmail_inbox"},
				{"schema": "gmail", "name": "messages"},
				{"schema": "slack", "name": "messages"},
			},
		},
		"SELECT n.nspname AS schema, c.relname AS name, c.reltuples::bigint AS row_estimate FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ANY(" + queryableSchemaArraySQL() + ") AND c.relkind IN ('r', 'p', 'm') AND c.reltuples >= 0": {
			Columns: []string{"schema", "name", "row_estimate"},
			Rows: []map[string]any{
				{"schema": "gmail", "name": "messages", "row_estimate": int64(1234567)},
				{"schema": "slack", "name": "messages", "row_estimate": int64(42)},
			},
		},
		indexSQL: {
			Columns: []string{"schema", "name", "def", "flag"},
			Rows: []map[string]any{
				{"schema": "gmail", "name": "messages", "def": "btree (account, message_id)", "flag": " [primary key]"},
				{"schema": "gmail", "name": "messages", "def": "gin (body_text gin_trgm_ops)", "flag": ""},
				{"schema": "slack", "name": "messages", "def": "btree (account, team_id, conversation_id, message_ts)", "flag": " [primary key]"},
				{"schema": "slack", "name": "messages", "def": "bm25 (text) WITH (text_config='english')", "flag": ""},
				{"schema": "slack", "name": "messages", "def": "gin (text gin_trgm_ops)", "flag": ""},
			},
		},
		describeCleanGmailSQL: {
			Columns: []string{"name", "type", "default_type", "default_expression", "comment"},
			Rows: []map[string]any{
				{"name": "thread_id", "type": "String"},
				{"name": "latest_subject", "type": "String"},
			},
		},
		describeGmailSQL: {
			Columns: []string{"name", "type", "default_type", "default_expression", "comment"},
			Rows: []map[string]any{
				{"name": "id", "type": "String"},
				{"name": "body", "type": "String"},
			},
		},
		describeSlackSQL: {
			Columns: []string{"name", "type", "default_type", "default_expression", "comment"},
			Rows: []map[string]any{
				{"name": "channel_id", "type": "String"},
			},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.SchemaOverview(context.Background())

	rowEstimateSQL := "SELECT n.nspname AS schema, c.relname AS name, c.reltuples::bigint AS row_estimate FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ANY(" + queryableSchemaArraySQL() + ") AND c.relkind IN ('r', 'p', 'm') AND c.reltuples >= 0"
	wantQueries := []string{
		"SELECT current_database() AS database",
		showTablesSQL,
		rowEstimateSQL,
		indexSQL,
	}
	if strings.Join(runner.queries[:4], "\n") != strings.Join(wantQueries, "\n") {
		t.Fatalf("first queries = %#v, want %#v", runner.queries[:4], wantQueries)
	}
	// The overview describes each table's columns (concurrently, so the order is
	// nondeterministic) and runs NO per-row sample queries — dropping those is
	// what roughly halved the response size.
	wantDescribeQueries := []string{describeCleanGmailSQL, describeGmailSQL, describeSlackSQL}
	gotDescribeQueries := append([]string(nil), runner.queries[4:]...)
	slices.Sort(gotDescribeQueries)
	slices.Sort(wantDescribeQueries)
	if strings.Join(gotDescribeQueries, "\n") != strings.Join(wantDescribeQueries, "\n") {
		t.Fatalf("describe queries = %#v, want %#v", gotDescribeQueries, wantDescribeQueries)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d, want 1", len(resp.Results))
	}
	result := resp.Results[0]
	if result.Error != "" {
		t.Fatalf("SchemaOverview returned error: %s", result.Error)
	}
	for _, want := range []string{
		`-- Reference these tables by their schema-qualified name`,
		`SELECT * FROM search.search_text('offer letter', 50)`,
		`# gmail.messages (~1,234,567 rows, estimated)`,
		`# slack.messages (~42 rows, estimated)`,
		`# marts.gmail_inbox`,
		`#   btree (account, message_id) [primary key]`,
		`id (String),body (String)`,
		`channel_id (String)`,
	} {
		if !strings.Contains(result.CSV, want) {
			t.Fatalf("schema overview missing %q in:\n%s", want, result.CSV)
		}
	}
	// The overview emits only the column catalog (one header row per table, no
	// sampled values), so there is nothing to truncate.
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
	rowEstimateSQL := "SELECT n.nspname AS schema, c.relname AS name, c.reltuples::bigint AS row_estimate FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ANY(" + queryableSchemaArraySQL() + ") AND c.relkind IN ('r', 'p', 'm') AND c.reltuples >= 0"
	showTablesSQL := "SELECT table_schema AS schema, table_name AS name FROM information_schema.tables WHERE table_schema = ANY(" + queryableSchemaArraySQL() + ") AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_schema, table_name"
	describeSQL := describeColumnsSQL("gmail", "messages")
	runner := &recordingRunner{
		results: map[string]RawResult{
			"SELECT current_database() AS database": {
				Columns: []string{"database"}, Rows: []map[string]any{{"database": "default"}},
			},
			showTablesSQL: {Columns: []string{"schema", "name"}, Rows: []map[string]any{{"schema": "gmail", "name": "messages"}}},
			describeSQL: {
				Columns: []string{"name"},
				Rows:    []map[string]any{{"name": "id"}},
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
	if !strings.Contains(resp.Results[0].CSV, "# gmail.messages\n") {
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

func TestSchemaErrorHint(t *testing.T) {
	cases := []struct {
		name     string
		message  string
		sql      string
		want     []string // all must be present
		wantNone []string // none may be present
	}{
		{
			name:    "timestamptz compared to integer",
			message: "ERROR: operator does not exist: timestamp with time zone > integer (SQLSTATE 42883)",
			sql:     "SELECT 1 FROM slack_messages WHERE message_datetime > 1700000000",
			want:    []string{"compare it to a timestamp"},
		},
		{
			name:    "timestamptz compared to bigint",
			message: "ERROR: operator does not exist: timestamp with time zone >= bigint (SQLSTATE 42883)",
			sql:     "SELECT 1 FROM apple_messages.messages WHERE message_at >= 1700000000",
			want:    []string{"compare it to a timestamp"},
		},
		{
			name:    "time guess on a known single table names that table's column",
			message: `ERROR: column "ts" does not exist (SQLSTATE 42703)`,
			sql:     "SELECT ts FROM slack.messages LIMIT 1",
			want:    []string{"slack.messages", "message_datetime", "schema_overview"},
		},
		{
			name:    "time guess on AI conversation events names occurred_at",
			message: `ERROR: column "created_at" does not exist (SQLSTATE 42703)`,
			sql:     "SELECT created_at FROM marts.ai_conversation_events LIMIT 1",
			want:    []string{"marts.ai_conversation_events", "occurred_at"},
		},
		{
			name:     "time guess with ambiguous join falls back to full list",
			message:  `ERROR: column "ts" does not exist (SQLSTATE 42703)`,
			sql:      "SELECT ts FROM slack.messages JOIN gmail.messages ON true",
			want:     []string{"slack.messages.message_datetime", "gmail.messages.internal_date"},
			wantNone: []string{"the primary time column on"},
		},
		{
			name:     "structural column remap names the right column, not a time hint",
			message:  `ERROR: column "channel_id" does not exist (SQLSTATE 42703)`,
			sql:      "SELECT channel_id FROM slack.messages LIMIT 1",
			want:     []string{"conversation_id", "slack.conversations"},
			wantNone: []string{"primary time column", "message_datetime"},
		},
		{
			name:    "chat_jid remap",
			message: `ERROR: column "chat_jid" does not exist (SQLSTATE 42703)`,
			sql:     "SELECT chat_jid FROM whatsapp.messages LIMIT 1",
			want:    []string{"chat_id"},
		},
		{
			name:     "non-time unknown column gets generic schema_overview hint only",
			message:  `ERROR: column "frobnicate" does not exist (SQLSTATE 42703)`,
			sql:      "SELECT frobnicate FROM gmail.messages LIMIT 1",
			want:     []string{"schema_overview"},
			wantNone: []string{"primary time column", "conversation_id"},
		},
		{
			name:    "wrong table name remaps slack_channels",
			message: `ERROR: relation "slack_channels" does not exist (SQLSTATE 42P01)`,
			sql:     "SELECT * FROM slack_channels LIMIT 1",
			want:    []string{"slack.conversations", "schema_overview"},
		},
		{
			name:    "unknown table points at schema_overview",
			message: `ERROR: relation "made_up_table" does not exist (SQLSTATE 42P01)`,
			sql:     "SELECT * FROM made_up_table LIMIT 1",
			want:    []string{"schema_overview"},
		},
		{
			name:    "numeric cast of empty cursor_ts",
			message: `ERROR: invalid input syntax for type numeric: "" (SQLSTATE 22P02)`,
			sql:     "SELECT cursor_ts::numeric FROM slack.sync_state WHERE cursor_ts = ''",
			want:    []string{"cursor_ts", "NULLIF"},
		},
		{
			name:     "unrelated syntax error gets no hint",
			message:  "ERROR: syntax error at or near \"FROM\" (SQLSTATE 42601)",
			sql:      "SELECT FROM gmail.messages",
			wantNone: []string{"hint"},
		},
		{
			name:     "unrelated operator error gets no hint",
			message:  "ERROR: operator does not exist: text > integer (SQLSTATE 42883)",
			sql:      "SELECT 1 FROM gmail.messages WHERE subject > 1",
			wantNone: []string{"hint"},
		},
		{
			name:     "numeric cast unrelated to cursor_ts gets no hint",
			message:  `ERROR: invalid input syntax for type numeric: "abc" (SQLSTATE 22P02)`,
			sql:      "SELECT 'abc'::numeric",
			wantNone: []string{"hint"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := schemaErrorHint(c.message, c.sql)
			for _, want := range c.want {
				if !strings.Contains(got, want) {
					t.Fatalf("schemaErrorHint(%q) = %q, want it to contain %q", c.message, got, want)
				}
			}
			for _, none := range c.wantNone {
				if got != "" && strings.Contains(got, none) {
					t.Fatalf("schemaErrorHint(%q) = %q, want it to NOT contain %q", c.message, got, none)
				}
			}
			if len(c.want) == 0 {
				if got != "" {
					t.Fatalf("schemaErrorHint(%q) = %q, want empty", c.message, got)
				}
				// No hint means queryErrorWithHint returns the message unchanged.
				if combined := queryErrorWithHint(c.message, c.sql); combined != c.message {
					t.Fatalf("queryErrorWithHint(%q) = %q, want unchanged", c.message, combined)
				}
				return
			}
			// The hint must be appended to the original message, not replace it.
			combined := queryErrorWithHint(c.message, c.sql)
			if !strings.HasPrefix(combined, c.message) || !strings.Contains(combined, got) {
				t.Fatalf("queryErrorWithHint(%q) = %q, want original message plus hint", c.message, combined)
			}
		})
	}
}

func TestServiceExecuteAppendsDatetimeHintToError(t *testing.T) {
	const sql = "SELECT count(*) FROM whatsapp.messages WHERE message_at > 1700000000"
	svc := NewService(fakeRunner{
		errs: map[string]error{sql: errors.New("ERROR: operator does not exist: timestamp with time zone > integer (SQLSTATE 42883)")},
	}, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.Execute(context.Background(), []Statement{statement("How many recent WhatsApp messages?", sql)}, 20, "csv")
	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d, want 1", len(resp.Results))
	}
	if !strings.Contains(resp.Results[0].Error, "compare it to a timestamp") {
		t.Fatalf("error = %q, want it to contain the datetime hint", resp.Results[0].Error)
	}
}

func TestServiceExecuteFullAppendsMissingColumnHintToError(t *testing.T) {
	const sql = "SELECT count(*) FROM apple_messages.messages WHERE date_unix > 1700000000"
	svc := NewService(fakeRunner{
		errs: map[string]error{sql: errors.New(`ERROR: column "date_unix" does not exist (SQLSTATE 42703)`)},
	}, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.ExecuteFull(context.Background(), "How many recent iMessages?", sql, "csv")
	if !strings.Contains(resp.Error, "schema_overview") {
		t.Fatalf("error = %q, want it to contain the missing-column hint", resp.Error)
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
