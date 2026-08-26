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
