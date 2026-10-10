package query

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Thirty days of real agent sessions produced 104 undefined-column (42703)
// errors and 20 permission-denied (42501) errors, every one of the latter on an
// ops.* sync-state table. Those two shapes are the warehouse's largest
// self-inflicted failure mode, and they are recoverable only if the error
// itself carries the answer: the caller is a model, and its next move is
// whatever the last message told it. These tests pin that.

// A join is where an agent knows LEAST about the schema, so it is exactly where
// withholding the column list costs the most. Naming one side's columns would
// have been misleading; naming both sides' is not.
func TestUndefinedColumnErrorListsEveryJoinedRelationsColumns(t *testing.T) {
	const sql = "SELECT e.text_content FROM timeline.events e JOIN base_gmail.messages m ON m.message_id = e.source_pk"
	runner := fakeRunner{
		results: map[string]RawResult{
			describeColumnsSQL("timeline", "events"): {
				Columns: []string{"name", "type"},
				Rows: []map[string]any{
					{"name": "event_ts", "type": "timestamp with time zone"},
					{"name": "snippet", "type": "text"},
				},
			},
			describeColumnsSQL("base_gmail", "messages"): {
				Columns: []string{"name", "type"},
				Rows: []map[string]any{
					{"name": "message_id", "type": "text"},
					{"name": "from_address", "type": "text"},
				},
			},
		},
		errs: map[string]error{sql: errors.New(`ERROR: column "text_content" does not exist (SQLSTATE 42703)`)},
	}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 200})

	resp := svc.ExecuteFull(context.Background(), "Which timeline events came from Gmail?", sql, "csv")

	for _, want := range []string{
		"columns on timeline.events: event_ts, snippet",
		"columns on base_gmail.messages: message_id, from_address",
	} {
		if !strings.Contains(resp.Error, want) {
			t.Fatalf("error should list %q, got: %s", want, resp.Error)
		}
	}
}

// Beyond a handful of relations the list stops being a hint and becomes a wall
// of text, so it is bounded rather than unbounded.
func TestUndefinedColumnErrorSkipsColumnListForManyRelations(t *testing.T) {
	const sql = "SELECT ts FROM a.one JOIN b.two ON true JOIN c.three ON true JOIN d.four ON true"
	runner := fakeRunner{errs: map[string]error{sql: errors.New(`ERROR: column "ts" does not exist (SQLSTATE 42703)`)}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 200})

	resp := svc.ExecuteFull(context.Background(), "When?", sql, "csv")

	if strings.Contains(resp.Error, "columns on") {
		t.Fatalf("a four-way join should not inline four column lists, got: %s", resp.Error)
	}
}

// timeline.events and the search functions describe the same events with
// different column names, and agents mix them constantly: search hits carry
// occurred_at/text, the table carries event_ts/snippet. The generic advice
// ("column names differ per source") does not say that.
func TestTimelineEventsColumnHintDistinguishesTheSearchFunctionShape(t *testing.T) {
	for _, column := range []string{"occurred_at", "text", "score"} {
		hint := schemaErrorHint(
			`ERROR: column "`+column+`" does not exist (SQLSTATE 42703)`,
			"SELECT "+column+" FROM timeline.events ORDER BY 1 DESC LIMIT 5",
		)
		for _, want := range []string{"event_ts", "snippet", "timeline.search_text"} {
			if !strings.Contains(hint, want) {
				t.Fatalf("hint for %q = %q, want it to name %q", column, hint, want)
			}
		}
	}
}

// The remaps answer the specific wrong names that recur across sessions, where
// the real column is not guessable from the wrong one.
func TestRecurringWrongColumnNamesAreRemapped(t *testing.T) {
	for _, c := range []struct {
		column string
		sql    string
		want   []string
	}{
		{"from_email", "SELECT from_email FROM base_gmail.messages LIMIT 1", []string{"from_address"}},
		{"text_content", "SELECT text_content FROM base_slack.messages LIMIT 1", []string{"snippet", "search_text"}},
		// The fortnight to 2026-09-30: `ref` on timeline.events was the single
		// most common wrong column (13 sessions) -- a search hit carries a ref,
		// the table carries adapter and event_id.
		{"ref", "SELECT * FROM timeline.events WHERE ref = 'gmail_email:a|b'", []string{"adapter", "event_id", "context"}},
		{"content", "SELECT content FROM marts_ai_conversations.events LIMIT 1", []string{"text"}},
		{"provider", "SELECT provider FROM marts_ai_conversations.sessions LIMIT 1", []string{"source"}},
		{"turn_index", "SELECT turn_index FROM marts_ai_conversations.events LIMIT 1", []string{"seq"}},
		{"body", "SELECT body FROM timeline.events LIMIT 1", []string{"snippet", "search_text"}},
	} {
		hint := schemaErrorHint(`ERROR: column "`+c.column+`" does not exist (SQLSTATE 42703)`, c.sql)
		for _, want := range c.want {
			if !strings.Contains(hint, want) {
				t.Fatalf("hint for %q = %q, want it to name %q", c.column, hint, want)
			}
		}
	}
}

// One source's real time column used on another source is the same mistake as
// inventing "ts", and it produces the same dead end. Deriving the guess list
// from the per-source time map means a new source cannot be forgotten here.
func TestOneSourcesTimeColumnUsedOnAnotherNamesTheRightColumn(t *testing.T) {
	hint := schemaErrorHint(
		`ERROR: column "message_datetime" does not exist (SQLSTATE 42703)`,
		"SELECT message_datetime FROM base_gmail.messages LIMIT 1",
	)
	if !strings.Contains(hint, "internal_date") {
		t.Fatalf("hint = %q, want it to name base_gmail.messages' real time column", hint)
	}
}

// Every one of the 20 permission errors was an ops.* sync-state table, and
// Postgres names only the bare relation in the message ("permission denied for
// table slack_sync_state"). The hint has to fire on that alone, because the
// caller may well have reached the table through a view or a quoted name.
func TestOpsPermissionHintFiresOnTheDeniedRelationName(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM ops.slack_sync_state LIMIT 1",
		`SELECT * FROM "ops"."slack_sync_state" LIMIT 1`,
		"SELECT * FROM slack_sync_state LIMIT 1",
	} {
		hint := schemaErrorHint("ERROR: permission denied for table slack_sync_state (SQLSTATE 42501)", sql)
		if !strings.Contains(hint, "marts_ops.table_freshness") {
			t.Fatalf("permission hint for %q = %q, want it to name marts_ops.table_freshness", sql, hint)
		}
	}
}

// A denial on something that is not an ops table must not be answered with
// pipeline-freshness advice.
func TestOpsPermissionHintDoesNotFireOnUnrelatedDenials(t *testing.T) {
	hint := schemaErrorHint(
		"ERROR: permission denied for table plaid_item_tokens (SQLSTATE 42501)",
		"SELECT * FROM private.plaid_item_tokens LIMIT 1",
	)
	if strings.Contains(hint, "marts_ops.table_freshness") {
		t.Fatalf("a private-schema denial should not be answered with freshness advice: %q", hint)
	}
}

// `limit_rows =>` is a parameter agents invented. The signature hint has to
// name the four that exist, priorities included, or the next attempt invents
// another one.
func TestWrongNamedParameterOnSearchTextNamesTheRealOnes(t *testing.T) {
	hint := schemaErrorHint(
		"ERROR: function timeline.search_text(unknown, limit_rows => integer) does not exist (SQLSTATE 42883)",
		"SELECT * FROM timeline.search_text('offer letter', limit_rows => 20)",
	)
	for _, want := range []string{"max_results", "sources", "since", "priorities"} {
		if !strings.Contains(hint, want) {
			t.Fatalf("hint = %q, want it to name the %q parameter", hint, want)
		}
	}
}

// The same has to hold for the exact and hybrid entry points, which share the
// parameter set.
func TestWrongNamedParameterOnEverySearchFunctionNamesPriorities(t *testing.T) {
	for _, function := range []string{"search_text", "search_text_exact", "search_hybrid"} {
		hint := schemaErrorHint(
			"ERROR: function timeline."+function+"(unknown, limit_rows => integer) does not exist (SQLSTATE 42883)",
			"SELECT * FROM timeline."+function+"('offer letter', limit_rows => 20)",
		)
		if !strings.Contains(hint, "priorities") {
			t.Fatalf("hint for %s = %q, want it to name priorities", function, hint)
		}
	}
}

// unclassified is accepted (querying for it is how you detect an adapter that
// stopped classifying) but it is a fail-loud sentinel, not a sixth tier. The
// error listed it beside the five real tiers with nothing to say otherwise.
func TestUnknownPriorityErrorSeparatesTheSentinelFromTheTiers(t *testing.T) {
	err := validateSearchPriorities([]string{"urgent"})
	if err == nil {
		t.Fatal("an unknown tier must error")
	}
	message := err.Error()
	for _, tier := range []string{"self", "direct", "cc", "noise", "background"} {
		if !strings.Contains(message, tier) {
			t.Fatalf("error = %q, want it to name the %q tier", message, tier)
		}
	}
	if !strings.Contains(message, "sentinel") {
		t.Fatalf("error = %q, want it to say unclassified is a fail-loud sentinel, not a tier", message)
	}
	if validateSearchPriorities([]string{"unclassified"}) != nil {
		t.Fatal("unclassified must still be accepted: it is how a classification outage is found")
	}
}

// The week to 2026-10-10: the recurring wrong names Postgres cannot correct
// itself, because the real name is too far away for its Levenshtein guess --
// or, for event_ts on the agent-session events, because its guess (event_type)
// is actively wrong.
func TestWeekToOctoberTenthWrongColumnsAreAnswered(t *testing.T) {
	for _, c := range []struct {
		column  string
		message string
		sql     string
		want    []string
	}{
		// six sessions
		{"operation_type", `ERROR: column "operation_type" does not exist (SQLSTATE 42703)`,
			"SELECT operation_type FROM ops.upstream_mutation_operations LIMIT 1", []string{"operation", "provider"}},
		// five sessions: the text of a Drive document, and of an agent turn
		{"content_text", `ERROR: column "content_text" does not exist (SQLSTATE 42703)`,
			"SELECT content_text FROM base_google_drive.files LIMIT 1", []string{"derived_documents.google_drive_file_texts", "marts_ai_conversations.events.text"}},
		// three sessions, each told "Perhaps you meant events.event_type"
		{"event_ts", `ERROR: column "event_ts" does not exist (SQLSTATE 42703) HINT: Perhaps you meant to reference the column "events.event_type".`,
			"SELECT event_ts, role FROM marts_ai_conversations.events WHERE session_id = 'x' ORDER BY event_ts", []string{"occurred_at"}},
	} {
		hint := schemaErrorHint(c.message, c.sql)
		for _, want := range c.want {
			if !strings.Contains(hint, want) {
				t.Fatalf("hint for %q = %q, want it to name %q", c.column, hint, want)
			}
		}
	}
}

// Four sessions in a week invented agent_sessions.turns (with turn_index); no
// catalog id is spelled like it, so it got the bare "no such relation".
func TestInventedAgentSessionRelationsPointAtTheMart(t *testing.T) {
	for _, rel := range []string{"agent_sessions.turns", "base_agent_sessions.turns", "agent_sessions.events", "agent_session_turns"} {
		hint := schemaErrorHint(`ERROR: relation "`+rel+`" does not exist (SQLSTATE 42P01)`, "SELECT * FROM "+rel+" LIMIT 1")
		for _, want := range []string{"marts_ai_conversations.events", "seq", "marts_ai_conversations.sessions"} {
			if !strings.Contains(hint, want) {
				t.Fatalf("hint for %q = %q, want it to name %q", rel, hint, want)
			}
		}
	}
}

// A built-in text function or LIKE applied to jsonb, or LIKE applied to a
// text[] column, was told to schema-qualify warehouse functions -- advice for
// a different mistake entirely.
func TestTypeMismatchOnJSONAndArrayColumnsSaysHowToCast(t *testing.T) {
	for _, c := range []struct {
		message string
		want    []string
	}{
		{`ERROR: function left(jsonb, integer) does not exist (SQLSTATE 42883)`, []string{"::text", "->>"}},
		{`ERROR: function lower(jsonb) does not exist (SQLSTATE 42883)`, []string{"::text"}},
		{`ERROR: operator does not exist: jsonb ~~ unknown (SQLSTATE 42883)`, []string{"::text", "->>"}},
		{`ERROR: operator does not exist: jsonb ~~* unknown (SQLSTATE 42883)`, []string{"::text"}},
		{`ERROR: operator does not exist: text[] ~~* unknown (SQLSTATE 42883)`, []string{"unnest", "array_to_string"}},
	} {
		hint := schemaErrorHint(c.message, "SELECT 1")
		if strings.Contains(hint, "schema-qualify") {
			t.Fatalf("hint for %q sends the caller to schema-qualify: %q", c.message, hint)
		}
		for _, want := range c.want {
			if !strings.Contains(hint, want) {
				t.Fatalf("hint for %q = %q, want it to name %q", c.message, hint, want)
			}
		}
	}
}

// The long tail of wrong names is mostly a real column with a word added or
// dropped -- operation_type for operation, account_name for account + name,
// value for point_value -- which is out of reach of Postgres' own suggestion.
func TestUndefinedColumnErrorNamesColumnsThatShareItsWords(t *testing.T) {
	for _, c := range []struct {
		missing string
		columns []string
		want    []string
		not     []string
	}{
		{"operation_type", []string{"id", "provider", "operation", "status"}, []string{"operation"}, []string{"status"}},
		{"account_name", []string{"account_id", "account", "name", "kind"}, []string{"account", "name"}, []string{"kind"}},
		{"value", []string{"display_name", "point_type", "point_value", "normalized_value"}, []string{"point_value", "normalized_value"}, []string{"point_type"}},
		{"user_name", []string{"user_id", "username", "text"}, []string{"username"}, []string{"text"}},
		{"check_name", []string{"component", "status"}, nil, []string{"component", "status"}},
	} {
		got := similarColumns(c.missing, c.columns)
		joined := "," + strings.Join(got, ",") + ","
		for _, want := range c.want {
			if !strings.Contains(joined, ","+want+",") {
				t.Fatalf("similarColumns(%q) = %v, want %q", c.missing, got, want)
			}
		}
		for _, not := range c.not {
			if strings.Contains(joined, ","+not+",") {
				t.Fatalf("similarColumns(%q) = %v, should not name %q", c.missing, got, not)
			}
		}
	}
}

func TestUndefinedColumnErrorLeadsWithTheSimilarColumns(t *testing.T) {
	const sql = "SELECT operation_type FROM ops.upstream_mutation_operations LIMIT 1"
	runner := fakeRunner{
		results: map[string]RawResult{
			describeColumnsSQL("ops", "upstream_mutation_operations"): {
				Columns: []string{"name", "type"},
				Rows: []map[string]any{
					{"name": "id", "type": "text"},
					{"name": "operation", "type": "text"},
				},
			},
		},
		errs: map[string]error{sql: errors.New(`ERROR: column "operation_type" does not exist (SQLSTATE 42703)`)},
	}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 200})
	resp := svc.ExecuteFull(context.Background(), "mutation kinds", sql, "csv")
	if !strings.Contains(resp.Error, `did you mean ops.upstream_mutation_operations.operation`) {
		t.Fatalf("error should name the similar column, got: %s", resp.Error)
	}
}
