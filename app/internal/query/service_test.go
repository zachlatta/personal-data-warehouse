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

// overviewColumnCatalogSQL and overviewPrimaryKeySQL replaced 108 per-relation
// describe queries: the overview no longer prints columns, so it no longer pays
// to fetch them one relation at a time.
func overviewColumnCatalogSQL() string {
	return "SELECT n.nspname AS schema, c.relname AS name, " +
		"count(*)::bigint AS column_count, " +
		"array_to_string(array_agg(a.attname ORDER BY a.attnum) " +
		"FILTER (WHERE format_type(a.atttypid, a.atttypmod) = 'timestamp with time zone'), ',') AS time_columns " +
		"FROM pg_attribute a " +
		"JOIN pg_class c ON c.oid = a.attrelid " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE n.nspname = ANY(" + queryableSchemaArraySQL() + ") " +
		"AND c.relkind IN ('r', 'p', 'm', 'v') AND a.attnum > 0 AND NOT a.attisdropped " +
		"GROUP BY 1, 2"
}

func overviewPrimaryKeySQL() string {
	return "SELECT n.nspname AS schema, c.relname AS name, " +
		"array_to_string(array_agg(a.attname ORDER BY k.ord), ',') AS pk_columns " +
		"FROM pg_class c " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary " +
		"CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) " +
		"JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum " +
		"WHERE n.nspname = ANY(" + queryableSchemaArraySQL() + ") " +
		"GROUP BY 1, 2"
}

func overviewRunner() *recordingRunner {
	showTablesSQL := "SELECT table_schema AS schema, table_name AS name FROM information_schema.tables WHERE table_schema = ANY(" + queryableSchemaArraySQL() + ") AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_schema, table_name"
	rowEstimateSQL := "SELECT n.nspname AS schema, c.relname AS name, c.reltuples::bigint AS row_estimate FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ANY(" + queryableSchemaArraySQL() + ") AND c.relkind IN ('r', 'p', 'm') AND c.reltuples >= 0"
	return &recordingRunner{results: map[string]RawResult{
		"SELECT current_database() AS database": {
			Columns: []string{"database"},
			Rows:    []map[string]any{{"database": "warehouse"}},
		},
		showTablesSQL: {
			Columns: []string{"schema", "name"},
			Rows: []map[string]any{
				{"schema": "gmail", "name": "messages"},
				{"schema": "marts_inbox", "name": "gmail_threads"},
				{"schema": "slack", "name": "messages"},
				{"schema": "timeline", "name": "events"},
				{"schema": "whoop", "name": "workouts"},
			},
		},
		rowEstimateSQL: {
			Columns: []string{"schema", "name", "row_estimate"},
			Rows: []map[string]any{
				{"schema": "gmail", "name": "messages", "row_estimate": int64(1234567)},
				{"schema": "slack", "name": "messages", "row_estimate": int64(42000000)},
				{"schema": "timeline", "name": "events", "row_estimate": int64(44000000)},
				{"schema": "whoop", "name": "workouts", "row_estimate": int64(321)},
			},
		},
		overviewColumnCatalogSQL(): {
			Columns: []string{"schema", "name", "column_count", "time_columns"},
			Rows: []map[string]any{
				{"schema": "gmail", "name": "messages", "column_count": int64(31), "time_columns": "internal_date,ingested_at"},
				{"schema": "marts_inbox", "name": "gmail_threads", "column_count": int64(12), "time_columns": ""},
				{"schema": "slack", "name": "messages", "column_count": int64(26), "time_columns": "message_datetime,ingested_at"},
				{"schema": "timeline", "name": "events", "column_count": int64(19), "time_columns": "event_ts,end_ts,ingest_ts"},
				// Two plausible event times and no curated entry: the overview must
				// say so rather than pick one.
				{"schema": "whoop", "name": "workouts", "column_count": int64(25), "time_columns": "start_at,end_at,synced_at"},
			},
		},
		overviewPrimaryKeySQL(): {
			Columns: []string{"schema", "name", "pk_columns"},
			Rows: []map[string]any{
				{"schema": "gmail", "name": "messages", "pk_columns": "account,message_id"},
				{"schema": "slack", "name": "messages", "pk_columns": "account,team_id,conversation_id,message_ts"},
				{"schema": "timeline", "name": "events", "pk_columns": "adapter,event_id"},
				{"schema": "whoop", "name": "workouts", "pk_columns": "account,workout_id"},
			},
		},
		describeColumnsSQL("timeline", "events"): {
			Columns: []string{"name", "type"},
			Rows: []map[string]any{
				{"name": "adapter", "type": "text"},
				{"name": "event_ts", "type": "timestamp with time zone"},
				{"name": "source_pk", "type": "jsonb"},
			},
		},
	}}
}

func TestSchemaOverviewListsRelationsWithKeysAndTimeColumns(t *testing.T) {
	runner := overviewRunner()
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.SchemaOverview(context.Background())

	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d, want 1", len(resp.Results))
	}
	result := resp.Results[0]
	if result.Error != "" {
		t.Fatalf("SchemaOverview returned error: %s", result.Error)
	}
	for _, want := range []string{
		// The three type rules no column name implies.
		"Booleans are bigint 0/1, not boolean",
		"JSON columns are text on the older sources and real jsonb on the newer ones",
		"Never prefix the database name",
		`describe_table('base_gmail.messages')`,
		"70% of failed warehouse queries are 42703 undefined-column",
		// The layer contract and the catalog's own start-here recommendation.
		"START HERE: timeline.events is the cross-source entry point",
		"base_<source>",
		"derived_<domain>",
		"marts_<domain>",
		"timeline.search_text('offer letter', 50)",
		"timeline.search_text_exact('offer letter', 50)",
		"base_* tables serve STRUCTURED predicates",
		// One line per relation: size, width, key, time column.
		"# gmail (1 relation)",
		"gmail.messages",
		"~1.2M",
		" 31 cols",
		"pk(account,message_id)",
		"time: internal_date",
		"pk(account,team_id,conversation_id,message_ts)",
		"time: message_datetime",
		// Views have no planner estimate and must not claim zero rows.
		"marts_inbox.gmail_threads",
		"view",
		// timeline.events keeps its columns inline; nothing else does.
		"timeline.events — full column catalog",
		"adapter (text),event_ts (timestamp with time zone),source_pk (jsonb)",
	} {
		if !strings.Contains(result.CSV, want) {
			t.Fatalf("schema overview missing %q in:\n%s", want, result.CSV)
		}
	}
	// The database name is interpolated, not left as a format verb.
	if strings.Contains(result.CSV, "%s") || strings.Contains(result.CSV, "%%") {
		t.Fatalf("schema overview leaked a format verb:\n%s", result.CSV)
	}
	if !strings.Contains(result.CSV, `("warehouse.")`) {
		t.Fatalf("schema overview should name the current database, got:\n%s", result.CSV)
	}
	// Columns for ordinary relations are describe_table's job now; emitting them
	// here is what made the overview too big to keep in context.
	if strings.Contains(result.CSV, "is_deleted (bigint)") || strings.Contains(result.CSV, "# indexes:") {
		t.Fatalf("overview should not carry per-relation columns or indexes:\n%s", result.CSV)
	}
	// Exactly one describe query, for timeline.events — not one per relation.
	describes := 0
	for _, q := range runner.queries {
		if strings.HasPrefix(q, "SELECT a.attname AS name, format_type(") {
			describes++
		}
	}
	if describes != 1 {
		t.Fatalf("expected exactly 1 per-relation describe query, got %d: %#v", describes, runner.queries)
	}
}

// Naming the wrong time column produces a query that runs, returns rows, and
// answers a different question than the caller asked — worse than an error.
func TestSchemaOverviewFlagsAmbiguousTimeColumnsInsteadOfGuessing(t *testing.T) {
	svc := NewService(overviewRunner(), Options{MaxRows: 5, MaxFieldChars: 100})

	csv := svc.SchemaOverview(context.Background()).Results[0].CSV

	if !strings.Contains(csv, "time: start_at|end_at  (ambiguous — confirm with describe_table)") {
		t.Fatalf("ambiguous time column should be flagged, got:\n%s", csv)
	}
	// synced_at is bookkeeping, not an event time, so it must not be offered.
	if strings.Contains(csv, "time: synced_at") {
		t.Fatalf("bookkeeping timestamp offered as the event time:\n%s", csv)
	}
}

// The overview is the required first call, so its size is part of the contract.
func TestSchemaOverviewStaysCompact(t *testing.T) {
	svc := NewService(overviewRunner(), Options{MaxRows: 5, MaxFieldChars: 100})

	csv := svc.SchemaOverview(context.Background()).Results[0].CSV

	// Preamble plus five relations. The real warehouse has ~108, and the budget
	// that matters is ~200 bytes per relation on top of a fixed ~5KB preamble.
	if len(csv) > 8000 {
		t.Fatalf("overview grew to %d bytes for 5 relations; the per-relation budget is what keeps it usable", len(csv))
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
	runner := overviewRunner()
	runner.errs = map[string]error{rowEstimateSQL: errors.New("pg_class lookup denied")}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.SchemaOverview(context.Background())
	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d", len(resp.Results))
	}
	if resp.Results[0].Error != "" {
		t.Fatalf("SchemaOverview surfaced row-estimate failure as error: %q", resp.Results[0].Error)
	}
	// A denied estimate must degrade to "view", never to a fabricated ~0 rows.
	if strings.Contains(resp.Results[0].CSV, "~0") {
		t.Fatalf("missing row estimate rendered as zero rows:\n%s", resp.Results[0].CSV)
	}
	if !strings.Contains(resp.Results[0].CSV, "gmail.messages") {
		t.Fatalf("expected the relation to still be listed, got:\n%s", resp.Results[0].CSV)
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
			want:    []string{"base_slack.messages", "message_datetime", "describe_table"},
		},
		{
			name:    "time guess on AI conversation events names occurred_at",
			message: `ERROR: column "created_at" does not exist (SQLSTATE 42703)`,
			sql:     "SELECT created_at FROM marts_ai_conversations.events LIMIT 1",
			want:    []string{"marts_ai_conversations.events", "occurred_at"},
		},
		{
			name:     "time guess with ambiguous join falls back to full list",
			message:  `ERROR: column "ts" does not exist (SQLSTATE 42703)`,
			sql:      "SELECT ts FROM slack.messages JOIN gmail.messages ON true",
			want:     []string{"base_slack.messages.message_datetime", "base_gmail.messages.internal_date"},
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
			// Postgres computes its own Levenshtein suggestion, which names the
			// real column; our vaguer "columns differ per source" line would only
			// dilute it, so it must not be appended after one.
			name:     "postgres suggestion suppresses the generic fallback",
			message:  `ERROR: column "message_ate" does not exist (SQLSTATE 42703) HINT: Perhaps you meant to reference the column "messages.message_at".`,
			sql:      "SELECT message_ate FROM whatsapp.messages LIMIT 1",
			wantNone: []string{"column names differ per source"},
		},
		{
			// 87 boolean-named columns in the warehouse are bigint 0/1, so this is
			// what `NOT is_from_me` / `= true` / `FILTER (WHERE is_read)` produce.
			// It used to come back as a bare Postgres error with no hint at all.
			name:    "boolean predicate on a bigint flag",
			message: "ERROR: argument of NOT must be type boolean, not type bigint (SQLSTATE 42804)",
			sql:     "SELECT count(*) FROM whatsapp.messages WHERE NOT is_from_me",
			want:    []string{"bigint 0/1", "is_from_me = 1"},
		},
		{
			name:    "FILTER on a bigint flag",
			message: "ERROR: argument of FILTER must be type boolean, not type bigint (SQLSTATE 42804)",
			sql:     "SELECT count(*) FILTER (WHERE is_read) FROM apple_messages.messages",
			want:    []string{"bigint 0/1"},
		},
		{
			name:    "bigint flag compared to a boolean literal",
			message: "ERROR: operator does not exist: bigint = boolean (SQLSTATE 42883)",
			sql:     "SELECT 1 FROM slack.messages WHERE is_deleted = true",
			want:    []string{"bigint 0/1"},
		},
		{
			// json-in-text columns: 71 of them, against 44 that really are jsonb.
			name:    "json operator on a text column",
			message: "ERROR: operator does not exist: text ->> unknown (SQLSTATE 42883)",
			sql:     "SELECT raw_metadata_json->>'x' FROM whatsapp.messages",
			want:    []string{"::jsonb"},
		},
		{
			name:    "subscripting a text json column",
			message: "ERROR: cannot subscript type text because it does not support subscripting (SQLSTATE 42804)",
			sql:     "SELECT raw_json['a'] FROM slack.messages",
			want:    []string{"::jsonb"},
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
			want:     []string{"describe_table"},
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
			name:    "unqualified search_text names the search-schema function",
			message: "ERROR: function search_text(unknown, integer) does not exist (SQLSTATE 42883)",
			sql:     "SELECT * FROM search_text('invoice', 50)",
			want:    []string{"timeline.search_text("},
		},
		{
			name:    "unqualified search_text_exact names the search-schema function",
			message: "ERROR: function search_text_exact(unknown, integer) does not exist (SQLSTATE 42883)",
			sql:     "SELECT * FROM search_text_exact('invoice', 50)",
			want:    []string{"timeline.search_text_exact("},
		},
		{
			name:    "public-qualified search_text still points at the search schema",
			message: "ERROR: function public.search_text(unknown, integer) does not exist (SQLSTATE 42883)",
			sql:     "SELECT * FROM public.search_text('invoice', 50)",
			want:    []string{"timeline.search_text("},
		},
		{
			name:    "unknown function is told to schema-qualify",
			message: "ERROR: function frobnicate(integer) does not exist (SQLSTATE 42883)",
			sql:     "SELECT frobnicate(1)",
			want:    []string{"schema-qualif"},
		},
		{
			name:    "numeric cast of empty cursor_ts",
			message: `ERROR: invalid input syntax for type numeric: "" (SQLSTATE 22P02)`,
			sql:     "SELECT cursor_ts::numeric FROM slack.sync_state WHERE cursor_ts = ''",
			want:    []string{"cursor_ts", "NULLIF"},
		},
		{
			name:    "statement timeout steers to the search layer",
			message: `ERROR: canceling statement due to statement timeout (SQLSTATE 57014)`,
			sql:     "SELECT * FROM gmail.messages WHERE body_text ILIKE '%offer%'",
			want:    []string{"timeline.search_text(", "timeline.search_text_exact("},
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
	if !strings.Contains(resp.Error, "describe_table") {
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
