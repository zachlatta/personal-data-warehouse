package query

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func relationsNamedSQL(name string) string {
	return "SELECT table_schema AS schema, table_name AS name FROM information_schema.tables " +
		"WHERE table_schema = ANY(" + queryableSchemaArraySQL() + ") AND table_name = '" + name + "' " +
		"ORDER BY table_schema"
}

func relationsLikeSQL(pattern string) string {
	return "SELECT table_schema AS schema, table_name AS name FROM information_schema.tables " +
		"WHERE table_schema = ANY(" + queryableSchemaArraySQL() + ") AND table_name LIKE '" + pattern + "' " +
		"ORDER BY table_schema, table_name"
}

func rowEstimateSQLFor(schema, table string) string {
	return "SELECT c.reltuples::bigint AS row_estimate FROM pg_class c " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE n.nspname = '" + schema + "' AND c.relname = '" + table + "' " +
		"AND c.relkind IN ('r', 'p', 'm') AND c.reltuples >= 0"
}

func indexSQLFor(schema, table string) string {
	return "SELECT regexp_replace(pg_get_indexdef(ix.indexrelid), '^.* USING ', '') AS def, " +
		"CASE WHEN ix.indisprimary THEN ' [primary key]' WHEN ix.indisunique THEN ' [unique]' ELSE '' END AS flag " +
		"FROM pg_index ix " +
		"JOIN pg_class i ON i.oid = ix.indexrelid " +
		"JOIN pg_class t ON t.oid = ix.indrelid " +
		"JOIN pg_namespace n ON n.oid = t.relnamespace " +
		"WHERE n.nspname = '" + schema + "' AND t.relname = '" + table + "' " +
		"AND t.relkind IN ('r', 'p', 'm') " +
		"ORDER BY ix.indisprimary DESC, def"
}

func gmailMessagesRunner() fakeRunner {
	return fakeRunner{results: map[string]RawResult{
		relationsNamedSQL("messages"): {
			Columns: []string{"schema", "name"},
			Rows:    []map[string]any{{"schema": "gmail", "name": "messages"}},
		},
		rowEstimateSQLFor("gmail", "messages"): {
			Columns: []string{"row_estimate"},
			Rows:    []map[string]any{{"row_estimate": int64(1234567)}},
		},
		indexSQLFor("gmail", "messages"): {
			Columns: []string{"def", "flag"},
			Rows: []map[string]any{
				{"def": "btree (account, message_id)", "flag": " [primary key]"},
				{"def": "btree (internal_date DESC)", "flag": ""},
			},
		},
		describeColumnsSQL("gmail", "messages"): {
			Columns: []string{"name", "type"},
			Rows: []map[string]any{
				{"name": "account", "type": "text"},
				{"name": "internal_date", "type": "timestamp with time zone"},
				{"name": "is_deleted", "type": "bigint"},
			},
		},
	}}
}

func TestDescribeTableReturnsColumnsIndexesAndRowEstimate(t *testing.T) {
	svc := NewService(gmailMessagesRunner(), Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.DescribeTable(context.Background(), "gmail.messages")

	if len(resp.Results) != 1 {
		t.Fatalf("results length = %d, want 1", len(resp.Results))
	}
	result := resp.Results[0]
	if result.Error != "" {
		t.Fatalf("DescribeTable returned error: %s", result.Error)
	}
	for _, want := range []string{
		"# gmail.messages (~1,234,567 rows, estimated)",
		"# indexes:",
		"#   btree (account, message_id) [primary key]",
		"#   btree (internal_date DESC)",
		"account (text),internal_date (timestamp with time zone),is_deleted (bigint)",
	} {
		if !strings.Contains(result.CSV, want) {
			t.Fatalf("describe_table output missing %q in:\n%s", want, result.CSV)
		}
	}
}

func TestDescribeTableResolvesUnqualifiedName(t *testing.T) {
	svc := NewService(gmailMessagesRunner(), Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.DescribeTable(context.Background(), "messages")

	if resp.Results[0].Error != "" {
		t.Fatalf("unqualified name should resolve when only one schema has it: %s", resp.Results[0].Error)
	}
	if !strings.Contains(resp.Results[0].CSV, "# gmail.messages") {
		t.Fatalf("expected gmail.messages, got:\n%s", resp.Results[0].CSV)
	}
}

func TestDescribeTableStripsDatabasePrefix(t *testing.T) {
	svc := NewService(gmailMessagesRunner(), Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.DescribeTable(context.Background(), "postgres.gmail.messages")

	if resp.Results[0].Error != "" {
		t.Fatalf("database-qualified name should resolve: %s", resp.Results[0].Error)
	}
}

func TestDescribeTableAmbiguousNameListsCandidates(t *testing.T) {
	runner := fakeRunner{results: map[string]RawResult{
		relationsNamedSQL("events"): {
			Columns: []string{"schema", "name"},
			Rows: []map[string]any{
				{"schema": "claude_code", "name": "events"},
				{"schema": "timeline", "name": "events"},
			},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.DescribeTable(context.Background(), "events")

	err := resp.Results[0].Error
	if !strings.Contains(err, "ambiguous") ||
		!strings.Contains(err, "claude_code.events") ||
		!strings.Contains(err, "timeline.events") {
		t.Fatalf("ambiguous name should list every candidate, got: %s", err)
	}
}

// A bare "no such relation" is what produced the retry loops the whole change
// set targets, so every miss has to hand back something concrete to try next.
func TestDescribeTableUnknownRelationSuggestsNearMatches(t *testing.T) {
	runner := fakeRunner{results: map[string]RawResult{
		relationsNamedSQL("message"): {Columns: []string{"schema", "name"}},
		relationsLikeSQL("%message%"): {
			Columns: []string{"schema", "name"},
			Rows: []map[string]any{
				{"schema": "apple_messages", "name": "messages"},
				{"schema": "gmail", "name": "messages"},
			},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.DescribeTable(context.Background(), "message")

	err := resp.Results[0].Error
	if !strings.Contains(err, "apple_messages.messages") || !strings.Contains(err, "gmail.messages") {
		t.Fatalf("unknown relation should suggest near matches, got: %s", err)
	}
}

func TestDescribeTableAnswersTimelineSearchFunctions(t *testing.T) {
	// describe_table('timeline.search_text') used to dead-end with "no relation
	// named timeline.search_text" — the search entry points are functions, so
	// information_schema misses them, and there was no way to discover their
	// signature or output shape. Real sessions then guessed both and failed.
	runner := fakeRunner{results: map[string]RawResult{}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	for _, name := range []string{
		"timeline.search_text",
		"search_text",
		"timeline.search_text_exact",
		"timeline.search_hybrid",
		"timeline.context",
	} {
		resp := svc.DescribeTable(context.Background(), name)
		if resp.Results[0].Error != "" {
			t.Fatalf("%s should be a successful catalog response, got error: %s", name, resp.Results[0].Error)
		}
		msg := resp.Results[0].CSV
		if !strings.Contains(msg, "function, not a relation") {
			t.Fatalf("%s should describe the function, got: %s", name, msg)
		}
		if !strings.Contains(msg, "(") || !strings.Contains(msg, "returns") {
			t.Fatalf("%s should carry a signature and return shape, got: %s", name, msg)
		}
	}

	resp := svc.DescribeTable(context.Background(), "timeline.search_text")
	msg := resp.Results[0].CSV
	for _, want := range []string{
		"query text",
		"max_results integer DEFAULT 50",
		"sources text[] DEFAULT NULL",
		"since timestamptz DEFAULT NULL",
		"event_ts",
		"source_table",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("search_text description missing %q, got: %s", want, msg)
		}
	}

	// A bare "context" stays a relation lookup — the name is far too generic to
	// assume the timeline function.
	respContext := svc.DescribeTable(context.Background(), "context")
	if strings.Contains(respContext.Results[0].CSV, "function, not a relation") {
		t.Fatalf("bare 'context' must not resolve to the timeline function, got: %s", respContext.Results[0].CSV)
	}
}

func TestDescribeTableKnownWrongNameIsRemapped(t *testing.T) {
	runner := fakeRunner{results: map[string]RawResult{
		relationsNamedSQL("slack_channels"):    {Columns: []string{"schema", "name"}},
		relationsLikeSQL("%slack\\_channels%"): {Columns: []string{"schema", "name"}},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.DescribeTable(context.Background(), "slack_channels")

	if err := resp.Results[0].Error; !strings.Contains(err, "slack.conversations") {
		t.Fatalf("known wrong name should be remapped, got: %s", err)
	}
}

func TestDescribeTableWrongSchemaNamesTheRightOne(t *testing.T) {
	runner := fakeRunner{results: map[string]RawResult{
		relationsNamedSQL("widgets"): {
			Columns: []string{"schema", "name"},
			Rows:    []map[string]any{{"schema": "base_gmail", "name": "widgets"}},
		},
		relationsLikeSQL("%widgets%"): {
			Columns: []string{"schema", "name"},
			Rows:    []map[string]any{{"schema": "base_gmail", "name": "widgets"}},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.DescribeTable(context.Background(), "base_slack.widgets")

	if err := resp.Results[0].Error; !strings.Contains(err, "base_gmail.widgets") {
		t.Fatalf("wrong schema should name where the table actually lives, got: %s", err)
	}
}

// A pre-reorganization physical name is answered with its current location
// rather than a guess, so a query written against the old layout self-corrects
// in one step.
func TestDescribeTablePreReorgNameNamesItsNewLocation(t *testing.T) {
	svc := NewService(fakeRunner{results: map[string]RawResult{}}, Options{MaxRows: 5, MaxFieldChars: 100})
	for old, want := range map[string]string{
		"gmail.messages":          "base_gmail.messages",
		"marts.finance_net_worth": "marts_finance.net_worth",
		"photos.assets":           "derived_photos.assets",
		"slack.sync_state":        "ops.slack_sync_state",
	} {
		resp := svc.DescribeTable(context.Background(), old)
		if err := resp.Results[0].Error; !strings.Contains(err, want) {
			t.Fatalf("describe_table(%q) should point at %s, got: %s", old, want, err)
		}
	}
}

func TestDescribeTableRejectsBlankAndMalformedInput(t *testing.T) {
	svc := NewService(fakeRunner{results: map[string]RawResult{}}, Options{MaxRows: 5, MaxFieldChars: 100})

	for _, relation := range []string{"", "   ", "gmail.messages WHERE 1=1", "gmail.'messages'"} {
		resp := svc.DescribeTable(context.Background(), relation)
		if resp.Results[0].Error == "" {
			t.Fatalf("expected an error for %q", relation)
		}
	}
}

// Views carry no planner row estimate; the heading must simply omit it rather
// than claim zero rows.
func TestDescribeTableOmitsRowCountForViews(t *testing.T) {
	runner := fakeRunner{results: map[string]RawResult{
		relationsNamedSQL("gmail_threads"): {
			Columns: []string{"schema", "name"},
			Rows:    []map[string]any{{"schema": "marts_inbox", "name": "gmail_threads"}},
		},
		rowEstimateSQLFor("marts_inbox", "gmail_threads"): {Columns: []string{"row_estimate"}},
		indexSQLFor("marts_inbox", "gmail_threads"):       {Columns: []string{"def", "flag"}},
		describeColumnsSQL("marts_inbox", "gmail_threads"): {
			Columns: []string{"name", "type"},
			Rows:    []map[string]any{{"name": "thread_id", "type": "text"}},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.DescribeTable(context.Background(), "marts_inbox.gmail_threads")

	csv := resp.Results[0].CSV
	if !strings.Contains(csv, "# marts_inbox.gmail_threads\n") {
		t.Fatalf("view heading should carry no row estimate, got:\n%s", csv)
	}
	if strings.Contains(csv, "rows, estimated") {
		t.Fatalf("view heading should not claim a row estimate, got:\n%s", csv)
	}
	if strings.Contains(csv, "# indexes:") {
		t.Fatalf("view heading should not emit an empty index block, got:\n%s", csv)
	}
}

// The wrong column names in real transcripts are a long tail of one-offs
// (chat_jid, text_content, rowid, institution_name, ...), so a remap table can
// never cover them. Answering the miss with the relation's real columns is the
// only response that scales.
func TestUndefinedColumnErrorListsTheRelationsRealColumns(t *testing.T) {
	const sql = "SELECT chat_jid FROM whatsapp.messages LIMIT 1"
	runner := fakeRunner{
		results: map[string]RawResult{
			describeColumnsSQL("whatsapp", "messages"): {
				Columns: []string{"name", "type"},
				Rows: []map[string]any{
					{"name": "account", "type": "text"},
					{"name": "chat_id", "type": "text"},
					{"name": "message_at", "type": "timestamp with time zone"},
				},
			},
		},
		errs: map[string]error{sql: errors.New(`ERROR: column "chat_jid" does not exist (SQLSTATE 42703)`)},
	}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.ExecuteFull(context.Background(), "Which chat?", sql, "csv")

	if !strings.Contains(resp.Error, "columns on whatsapp.messages: account, chat_id, message_at") {
		t.Fatalf("error should list the relation's real columns, got: %s", resp.Error)
	}
}

// With several relations in play, naming one table's columns would mislead
// about which side of the join is wrong.
func TestUndefinedColumnErrorSkipsColumnListForJoins(t *testing.T) {
	const sql = "SELECT ts FROM slack.messages JOIN gmail.messages ON true"
	runner := fakeRunner{
		errs: map[string]error{sql: errors.New(`ERROR: column "ts" does not exist (SQLSTATE 42703)`)},
	}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	resp := svc.ExecuteFull(context.Background(), "When?", sql, "csv")

	if strings.Contains(resp.Error, "columns on") {
		t.Fatalf("ambiguous join should not name one table's columns, got: %s", resp.Error)
	}
}

func TestSoleRelationInSQL(t *testing.T) {
	cases := map[string]string{
		"SELECT 1 FROM gmail.messages LIMIT 1":                         "gmail.messages",
		"select * from \"whatsapp\".\"messages\" where 1=1":            "whatsapp.messages",
		"SELECT 1 FROM gmail.messages m JOIN gmail.messages n ON true": "gmail.messages",
		"SELECT 1 FROM slack.messages JOIN gmail.messages ON true":     "",
		"SELECT 1":               "",
		"SELECT 1 FROM messages": "",
	}
	for sql, want := range cases {
		ref, ok := soleRelationInSQL(sql)
		got := ""
		if ok {
			got = ref.DisplayName()
		}
		if got != want {
			t.Fatalf("soleRelationInSQL(%q) = %q, want %q", sql, got, want)
		}
	}
}
