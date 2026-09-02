package query

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
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

	// Preamble plus five relations. The real warehouse has ~147, and the budget
	// that matters is ~200 bytes per relation on top of the fixed preamble.
	// The preamble also carries the catalog-guidance legend, and two of the
	// five fixture relations (marts_inbox.gmail_threads, timeline.events)
	// carry a catalog comment of their own, which is what the extra ~750
	// bytes over the pre-guidance 7.3KB buys.
	if len(csv) > 9000 {
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

func TestServiceExecuteRequiresQuestionPerSQLStatement(t *testing.T) {
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT 1": {Columns: []string{"1"}, Rows: []map[string]any{{"1": 1}}},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	noQueries := svc.ExecuteBatchFull(context.Background(), nil, "csv")
	if !strings.Contains(noQueries.Results[0].Error, "queries must contain at least one") {
		t.Fatalf("missing queries error = %#v", noQueries.Results[0])
	}
	blankQuestion := svc.ExecuteBatchFull(context.Background(), []Statement{statement(" ", "SELECT 1")}, "csv")
	if !strings.Contains(blankQuestion.Results[0].Error, "queries[0].question") {
		t.Fatalf("blank question error = %#v", blankQuestion.Results[0])
	}
	blankSQL := svc.ExecuteBatchFull(context.Background(), []Statement{statement("What is one?", " ")}, "csv")
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

	resp := svc.ExecuteBatchFull(context.Background(), []Statement{
		statement("What happens when Postgres returns an error?", "SELECT broken"),
		statement("Does a simple read-only query work?", "SELECT 1"),
		statement("Is a destructive query rejected?", "DROP TABLE x"),
	}, "csv")
	if len(resp.Results) != 3 {
		t.Fatalf("results length = %d", len(resp.Results))
	}
	if !strings.Contains(resp.Results[0].Error, "postgres failed") {
		t.Fatalf("first error = %q", resp.Results[0].Error)
	}
	if resp.Results[1].Error != "" || resp.Results[1].Rows != "1\n1" {
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
			name:    "ranked search wrong column returns the SQL function contract",
			message: `ERROR: column "snippet" does not exist (SQLSTATE 42703)`,
			sql:     "SELECT snippet FROM timeline.search_text('invoice', 20)",
			want: []string{
				"source, subsource, context, who, occurred_at, account, ref, text, score, event_ts, title, source_table, source_pk, priority",
				"matched preview",
				"timeline.context",
			},
			wantNone: []string{"describe_table"},
		},
		{
			name:     "exact search time guess points to occurred_at",
			message:  `ERROR: column "event_time" does not exist (SQLSTATE 42703)`,
			sql:      "SELECT event_time FROM timeline.search_text_exact('invoice', 20)",
			want:     []string{"occurred_at", "timeline.search_text_exact"},
			wantNone: []string{"each source names its primary time column"},
		},
		{
			name:     "search joined to a relation does not assume which source missed",
			message:  `ERROR: column "thread_name" does not exist (SQLSTATE 42703)`,
			sql:      "SELECT thread_name FROM timeline.search_text('invoice', 20) hit JOIN base_slack.messages message ON true",
			want:     []string{"describe_table"},
			wantNone: []string{"returns exactly"},
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
			// A wrong NAMED parameter (limit_rows => 20) on an already-qualified
			// call is a signature miss, not a qualification miss: telling the
			// caller to schema-qualify what they already qualified sends them in
			// circles. Print the real signature instead.
			name:    "wrong named parameter on a qualified search call prints the signature",
			message: "ERROR: function timeline.search_text(unknown, limit_rows => integer) does not exist (SQLSTATE 42883)",
			sql:     "SELECT * FROM timeline.search_text('invoice', limit_rows => 20)",
			want: []string{
				"max_results integer DEFAULT 50",
				"sources text[] DEFAULT NULL",
				"since timestamptz DEFAULT NULL",
			},
			wantNone: []string{"schema-qualified just like its tables"},
		},
		{
			name:    "wrong argument shape on a qualified exact search prints the signature",
			message: "ERROR: function timeline.search_text_exact(unknown, unknown) does not exist (SQLSTATE 42883)",
			sql:     "SELECT * FROM timeline.search_text_exact('invoice', 'gmail')",
			want:    []string{"timeline.search_text_exact(query text", "max_results integer DEFAULT 50"},
		},
		{
			// The SQL function needs query embeddings a caller cannot type, so
			// the hint must send them to the ONE hybrid search — the tool —
			// rather than print a signature that invites an invented vector.
			name:     "a hybrid search attempted from SQL is redirected to the search tool",
			message:  "ERROR: function timeline.search_hybrid(unknown, unknown) does not exist (SQLSTATE 42883)",
			sql:      "SELECT * FROM timeline.search_hybrid('invoice', 10)",
			want:     []string{"`search` tool", "pdw search", "timeline.search_text("},
			wantNone: []string{"query_embedding text"},
		},
		{
			name:    "hybrid search wrong column returns the shared result contract",
			message: `ERROR: column "snippet" does not exist (SQLSTATE 42703)`,
			sql:     "SELECT snippet FROM timeline.search_hybrid('invoice', '[0.1]', 'model')",
			want:    []string{"timeline.search_hybrid() returns exactly", "event_ts", "source_pk"},
		},
		{
			// Pipeline-debugging sessions get pointed at ops.* sync state by the
			// docs, but the query role deliberately cannot read most of it. The
			// denial must name the supported health surfaces.
			name:    "ops sync state permission denial names the marts_ops surfaces",
			message: "ERROR: permission denied for table whoop_sync_state (SQLSTATE 42501)",
			sql:     "SELECT * FROM ops.whoop_sync_state",
			want:    []string{"marts_ops.pipeline_health", "marts_ops.table_freshness", "marts_ops.plaid_item_health"},
		},
		{
			name:     "permission denial outside ops gets no ops hint",
			message:  "ERROR: permission denied for schema private (SQLSTATE 42501)",
			sql:      "SELECT * FROM private.plaid_item_tokens",
			wantNone: []string{"marts_ops.pipeline_health"},
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

	resp := svc.ExecuteBatchFull(context.Background(), []Statement{statement("How many recent WhatsApp messages?", sql)}, "csv")
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

func TestServiceExecuteFullAppendsSearchResultContractToError(t *testing.T) {
	const sql = "SELECT title, body FROM timeline.search_text('invoice', 20)"
	svc := NewService(fakeRunner{
		errs: map[string]error{sql: errors.New(`ERROR: column "title" does not exist (SQLSTATE 42703)`)},
	}, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.ExecuteFull(context.Background(), "Which timeline events mention an invoice?", sql, "csv")
	for _, want := range []string{"occurred_at", "ref", "text", "score"} {
		if !strings.Contains(resp.Error, want) {
			t.Fatalf("error = %q, want search result contract to contain %q", resp.Error, want)
		}
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

	resp := svc.ExecuteBatchFull(context.Background(), []Statement{statement("What Gmail subjects and labels need CSV escaping?", "SELECT subject, labels FROM gmail_messages")}, "csv")
	want := "subject,labels\n\"hello, \"\"world\"\"\nnext\",\"[\"\"INBOX\"\",\"\"STARRED\"\"]\""
	if resp.Results[0].Rows != want {
		t.Fatalf("CSV = %q, want %q", resp.Results[0].Rows, want)
	}
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
	if len(runner.maxRows) != 1 || runner.maxRows[0] != FullQueryRowCap+1 {
		t.Fatalf("runner maxRows = %#v, want [%d]", runner.maxRows, FullQueryRowCap+1)
	}
}

func TestExecuteBatchFullIsTheBoundedCompleteMCPFlow(t *testing.T) {
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
	svc := NewService(runner, Options{MaxRows: 2, MaxFieldChars: 10})

	resp := svc.ExecuteBatchFull(context.Background(), []Statement{statement(
		"Show both Gmail bodies.",
		"SELECT id, body FROM gmail_messages ORDER BY id",
	)}, "csv")

	if len(resp.Results) != 1 || resp.Results[0].Error != "" {
		t.Fatalf("ExecuteBatchFull response: %#v", resp)
	}
	body, ok := resp.Results[0].Rows.(string)
	if !ok || !strings.Contains(body, bigBody) {
		t.Fatalf("MCP result truncated a long field: %T len=%d", resp.Results[0].Rows, len(body))
	}
	if len(runner.maxRows) != 1 || runner.maxRows[0] != 3 {
		t.Fatalf("runner maxRows = %#v, want MaxRows+1", runner.maxRows)
	}
}

func TestExecuteBatchFullRejectsResultsPastMCPRowCap(t *testing.T) {
	runner := &recordingRunner{results: map[string]RawResult{
		"SELECT n FROM numbers": {
			Columns: []string{"n"},
			Rows:    []map[string]any{{"n": 1}, {"n": 2}, {"n": 3}},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 2})

	resp := svc.ExecuteBatchFull(context.Background(), []Statement{statement(
		"Which numbers exist?", "SELECT n FROM numbers",
	)}, "json")

	if len(resp.Results) != 1 || !strings.Contains(resp.Results[0].Error, "MCP_MAX_ROWS (2)") {
		t.Fatalf("row cap was not enforced: %#v", resp)
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

func TestRawTextScanHintFiresOnlyForRawPatternScansOutsideTheTimeline(t *testing.T) {
	// The shape behind every statement timeout in 14 days of agent sessions:
	// ILIKE over a raw base_* table with no timeline reference.
	cases := map[string]bool{
		"SELECT * FROM base_slack.messages WHERE text ILIKE '%malted%' LIMIT 20":                                                            true,
		"SELECT * FROM base_gmail.messages m WHERE m.subject ~* 'invoice' ORDER BY internal_date DESC":                                      true,
		"SELECT * FROM base_slack.messages WHERE conversation_id = 'C1' AND message_datetime > now() - interval '1 day'":                    false,
		"SELECT * FROM timeline.search_text('malted', 20)":                                                                                  false,
		"SELECT e.* FROM timeline.events e JOIN base_slack.messages m ON m.message_ts = e.source_pk->>'ts' WHERE e.search_text ILIKE '%x%'": false,
		"SELECT * FROM marts_messages.messages WHERE body ILIKE '%x%'":                                                                      false,
	}
	for sql, want := range cases {
		got := rawTextScanHint(sql) != ""
		if got != want {
			t.Fatalf("rawTextScanHint(%q) fired=%v, want %v", sql, got, want)
		}
	}
	if hint := rawTextScanHint("SELECT * FROM base_slack.messages WHERE text ILIKE '%x%'"); !strings.Contains(hint, "timeline.search_text") {
		t.Fatalf("the hint must say what to use instead; got %q", hint)
	}
}

func TestTimelineEventsHintAsksForAnExplicitPriorityDecision(t *testing.T) {
	cases := map[string]bool{
		"SELECT event_ts, title FROM timeline.events ORDER BY event_ts DESC LIMIT 20": true,
		"SELECT count(*) FROM \"timeline\".\"events\"":                                true,
		"SELECT * FROM timeline.events WHERE priority IN ('self', 'direct', 'cc')":    false,
		"SELECT * FROM timeline.events e WHERE e.priority = ANY(ARRAY['noise'])":      false,
		"SELECT * FROM timeline.search_text('budget', priorities => ARRAY['self'])":   false,
		"SELECT * FROM marts_messages.messages":                                       false,
	}
	for sql, want := range cases {
		got := timelinePriorityHint(sql) != ""
		if got != want {
			t.Fatalf("timelinePriorityHint(%q) fired=%v, want %v", sql, got, want)
		}
	}
	hint := timelinePriorityHint("SELECT * FROM timeline.events LIMIT 20")
	for _, want := range []string{"priority IN", "self", "direct", "cc", "broad"} {
		if !strings.Contains(hint, want) {
			t.Fatalf("unscoped timeline hint must contain %q: %s", want, hint)
		}
	}
}

func TestExecuteFullReturnsTimelinePriorityHintBeforeRunning(t *testing.T) {
	const sql = "SELECT event_ts FROM timeline.events ORDER BY event_ts DESC LIMIT 5"
	runner := fakeRunner{results: map[string]RawResult{
		sql: {Columns: []string{"event_ts"}, Rows: []map[string]any{}},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 200})

	resp := svc.ExecuteFull(context.Background(), "What happened recently?", sql, "csv")

	if !strings.Contains(resp.Hint, "priority IN") {
		t.Fatalf("ExecuteFull hint = %q, want priority guidance", resp.Hint)
	}
}
