package warehouse

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// TestQueryableSchemasMatchPythonDefinition pins the Go schema list against the
// Python one, which is the definition the warehouse actually provisions. The two
// drifted once already: apple_contacts was added to Python's SOURCE_RAW_SCHEMAS
// but not here, so apple_contacts.cards was queryable yet invisible in
// schema_overview — an agent could only learn it existed by reading CLAUDE.md.
// Anything absent here cannot be discovered, which is the expensive failure.
func TestQueryableSchemasMatchPythonDefinition(t *testing.T) {
	const relationsPy = "../../../src/personal_data_warehouse/relations.py"
	source, err := os.ReadFile(relationsPy)
	if err != nil {
		t.Fatalf("read %s: %v", relationsPy, err)
	}
	want := append(pythonSchemaTuple(t, string(source), "SOURCE_RAW_SCHEMAS"),
		pythonSchemaTuple(t, string(source), "DERIVED_SCHEMAS")...)

	got := slices.Clone(QueryableSchemas)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("QueryableSchemas drifted from %s\nonly in Go:     %v\nonly in Python: %v",
			relationsPy, difference(got, want), difference(want, got))
	}
}

var pythonTupleEntry = regexp.MustCompile(`"([a-z_]+)"`)

func pythonSchemaTuple(t *testing.T, source, name string) []string {
	t.Helper()
	start := strings.Index(source, name+": tuple[str, ...] = (")
	if start < 0 {
		t.Fatalf("could not find %s in relations.py", name)
	}
	end := strings.Index(source[start:], ")")
	if end < 0 {
		t.Fatalf("unterminated %s tuple in relations.py", name)
	}
	matches := pythonTupleEntry.FindAllStringSubmatch(source[start:start+end], -1)
	schemas := make([]string, 0, len(matches))
	for _, match := range matches {
		schemas = append(schemas, match[1])
	}
	if len(schemas) == 0 {
		t.Fatalf("%s parsed as empty", name)
	}
	return schemas
}

func difference(a, b []string) []string {
	var only []string
	for _, value := range a {
		if !slices.Contains(b, value) {
			only = append(only, value)
		}
	}
	return only
}

func TestQualifySQLDoesNotRewriteCreateSchemaNames(t *testing.T) {
	stmt := `CREATE SCHEMA IF NOT EXISTS "upstream_mutations"`
	if got := QualifySQL(stmt); got != stmt {
		t.Fatalf("CREATE SCHEMA statement was rewritten: %s", got)
	}
}

func TestQualifySQLRewritesWhoopRelationReferences(t *testing.T) {
	got := QualifySQL(`SELECT sleep_id FROM whoop_sleeps ORDER BY start_at DESC`)
	want := `SELECT sleep_id FROM "whoop"."sleeps" ORDER BY start_at DESC`
	if got != want {
		t.Fatalf("qualified SQL mismatch\nwant: %s\n got: %s", want, got)
	}
}

func TestQualifySQLRewritesUpstreamMutationRelationReferences(t *testing.T) {
	got := QualifySQL(`SELECT id FROM upstream_mutations WHERE status = 'pending_review'`)
	want := `SELECT id FROM "upstream_mutations"."operations" WHERE status = 'pending_review'`
	if got != want {
		t.Fatalf("qualified SQL mismatch\nwant: %s\n got: %s", want, got)
	}
}

func TestQualifySQLRewritesPlaidRelations(t *testing.T) {
	got := QualifySQL(`SELECT account_id FROM plaid_accounts WHERE is_removed = 0`)
	want := `SELECT account_id FROM "plaid"."accounts" WHERE is_removed = 0`
	if got != want {
		t.Fatalf("qualified SQL mismatch\nwant: %s\n got: %s", want, got)
	}
}

func TestTimelineDetailRelationsUseCanonicalSchemas(t *testing.T) {
	cases := map[string]string{
		"alice_voice_recordings":          `"alice_voice_recordings"."recordings"`,
		"alice_voice_recording_artifacts": `"alice_voice_recordings"."artifacts"`,
		"finance_transactions":            `"finance"."transactions"`,
		"finance_observations":            `"finance"."observations"`,
		"manual_finance_documents":        `"manual_finance"."documents"`,
		"manual_finance_extractions":      `"manual_finance"."extractions"`,
		"apple_message_chats":             `"apple_messages"."chats"`,
	}
	for logical, want := range cases {
		if got := SQLRelation(logical); got != want {
			t.Fatalf("SQLRelation(%q) = %s, want %s", logical, got, want)
		}
	}
}
