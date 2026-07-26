package warehouse

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
)

const catalogJSON = "../../../src/personal_data_warehouse/warehouse_catalog.json"

type jsonCatalog struct {
	Version int `json:"version"`
	Schemas []struct {
		Name         string `json:"name"`
		Layer        string `json:"layer"`
		Discoverable bool   `json:"discoverable"`
	} `json:"schemas"`
	Objects []struct {
		ID          string `json:"id"`
		Kind        string `json:"kind"`
		Schema      string `json:"schema"`
		Name        string `json:"name"`
		QueryAccess string `json:"query_access"`
	} `json:"objects"`
	RenamedTimelineSourceTables map[string]string `json:"renamed_timeline_source_tables"`
}

func loadJSONCatalog(t *testing.T) jsonCatalog {
	t.Helper()
	raw, err := os.ReadFile(catalogJSON)
	if err != nil {
		t.Fatalf("read %s: %v", catalogJSON, err)
	}
	var catalog jsonCatalog
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatalf("parse %s: %v", catalogJSON, err)
	}
	return catalog
}

// TestGeneratedCatalogMatchesJSON pins the generated Go catalog against the one
// editable authority. The two hand-maintained lists drifted twice before this
// existed: apple_contacts was added to Python but not Go (so apple_contacts.cards
// was queryable yet invisible in schema_overview), and later the whole receipts
// domain existed only in Python. Anything absent here cannot be discovered,
// which is the expensive failure.
func TestGeneratedCatalogMatchesJSON(t *testing.T) {
	catalog := loadJSONCatalog(t)
	if catalog.Version != CatalogVersion {
		t.Fatalf("catalog version drift: json %d, generated %d", catalog.Version, CatalogVersion)
	}
	if len(catalog.Objects) != len(Objects) {
		t.Fatalf("object count drift: json %d, generated %d", len(catalog.Objects), len(Objects))
	}
	for i, want := range catalog.Objects {
		got := Objects[i]
		if got.ID != want.ID || got.Kind != want.Kind || got.Schema != want.Schema ||
			got.Name != want.Name || got.QueryAccess != want.QueryAccess {
			t.Fatalf("object %d drifted: json %+v, generated %+v", i, want, got)
		}
	}
	if len(catalog.Schemas) != len(Schemas) {
		t.Fatalf("schema count drift: json %d, generated %d", len(catalog.Schemas), len(Schemas))
	}
	for legacy, current := range catalog.RenamedTimelineSourceTables {
		if RenamedTimelineSourceTables[legacy] != current {
			t.Fatalf("renamed source_table %q drifted", legacy)
		}
	}
}

func TestQueryableSchemasAreThePublicLayersInOrder(t *testing.T) {
	catalog := loadJSONCatalog(t)
	var want []string
	for _, schema := range catalog.Schemas {
		if schema.Discoverable {
			want = append(want, schema.Name)
		}
	}
	slices.Sort(want)
	got := QueryableSchemas()
	if !slices.Equal(got, want) {
		t.Fatalf("QueryableSchemas drifted\n got: %v\nwant: %v", got, want)
	}
	// base_* → derived_* → marts_* → timeline is also plain alphabetical order,
	// so discovery, psql's \dn, and any ORDER BY table_schema agree.
	if !slices.IsSorted(got) {
		t.Fatalf("QueryableSchemas is not sorted: %v", got)
	}
	for _, schema := range got {
		if schema == "ops" || schema == "private" || schema == "internal" {
			t.Fatalf("implementation schema %q must not be discoverable", schema)
		}
	}
}

func TestSQLRelationRendersCatalogLocations(t *testing.T) {
	cases := map[string]string{
		"gmail_messages":                  `"base_gmail"."messages"`,
		"alice_voice_recordings":          `"base_alice_voice_recordings"."recordings"`,
		"alice_voice_recording_artifacts": `"base_alice_voice_recordings"."artifacts"`,
		"finance_transactions":            `"derived_finance"."transactions"`,
		"finance_observations":            `"derived_finance"."observations"`,
		"manual_finance_documents":        `"base_manual_finance"."documents"`,
		"manual_finance_extractions":      `"derived_finance"."document_extractions"`,
		"apple_message_chats":             `"base_apple_messages"."chats"`,
		"ai_conversation_events":          `"marts_ai_conversations"."events"`,
		"timeline_events":                 `"timeline"."events"`,
		"upstream_mutations":              `"ops"."upstream_mutation_operations"`,
	}
	for logical, want := range cases {
		if got := SQLRelation(logical); got != want {
			t.Fatalf("SQLRelation(%q) = %s, want %s", logical, got, want)
		}
	}
}

// SQLRelation panics on an unknown id rather than emitting a bare identifier:
// the old rewriter's silent pass-through is exactly how a stale public
// search_text() shadowed the real one for 16 days.
func TestSQLRelationPanicsOnUnknownRelation(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected SQLRelation to panic on an unknown relation")
		}
	}()
	_ = SQLRelation("definitely_not_a_relation")
}

func TestExpandRelationsExpandsMarkersOnly(t *testing.T) {
	got := ExpandRelations(`SELECT id FROM @upstream_mutations WHERE status = 'pending_review'`)
	want := `SELECT id FROM "ops"."upstream_mutation_operations" WHERE status = 'pending_review'`
	if got != want {
		t.Fatalf("expanded SQL mismatch\nwant: %s\n got: %s", want, got)
	}
	// A bare legacy name is left exactly as written, so Postgres rejects it
	// instead of the app quietly resolving it through the search_path.
	bare := `SELECT id FROM upstream_mutations`
	if got := ExpandRelations(bare); got != bare {
		t.Fatalf("bare relation name was rewritten: %s", got)
	}
}

func TestExpandRelationsSkipsLiteralsAndComments(t *testing.T) {
	sql := "SELECT '@gmail_messages' AS a -- @gmail_messages\nFROM @gmail_messages WHERE e LIKE '%@example.com'"
	got := ExpandRelations(sql)
	if !strings.Contains(got, "'@gmail_messages'") {
		t.Fatalf("string literal was expanded: %s", got)
	}
	if !strings.Contains(got, "-- @gmail_messages") {
		t.Fatalf("comment was expanded: %s", got)
	}
	if !strings.Contains(got, "'%@example.com'") {
		t.Fatalf("email literal was expanded: %s", got)
	}
	if !strings.Contains(got, `FROM "base_gmail"."messages"`) {
		t.Fatalf("relation marker was not expanded: %s", got)
	}
}

func TestExpandRelationsLeavesCreateSchemaAlone(t *testing.T) {
	stmt := `CREATE SCHEMA IF NOT EXISTS "ops"`
	if got := ExpandRelations(stmt); got != stmt {
		t.Fatalf("CREATE SCHEMA statement was rewritten: %s", got)
	}
}

func TestDrillDownRefusesUnreadableRelations(t *testing.T) {
	if _, ok := DrillDownRelation("gmail_messages"); !ok {
		t.Fatal("gmail_messages should be drillable")
	}
	// ops relations the app renders stay drillable; credentials never are.
	if _, ok := DrillDownRelation("agent_runs"); !ok {
		t.Fatal("agent_runs is an app-readable operational relation")
	}
	if _, ok := DrillDownRelation("plaid_item_tokens"); ok {
		t.Fatal("private credential storage must not be drillable")
	}
	if _, ok := DrillDownRelation("gmail_sync_state"); ok {
		t.Fatal("non-allowlisted ops state must not be drillable")
	}
	// Timeline rows written before the rename still resolve.
	rel, ok := DrillDownRelation("agent_session_events")
	if !ok || rel.Schema != "marts_ai_conversations" || rel.Name != "events" {
		t.Fatalf("historical source_table did not resolve: %+v ok=%v", rel, ok)
	}
}

func TestStartHereRecommendsTimeline(t *testing.T) {
	if StartHere.Schema != "timeline" {
		t.Fatalf("start_here schema = %q, want timeline", StartHere.Schema)
	}
	if !strings.Contains(strings.ToLower(StartHere.Headline), "timeline") {
		t.Fatalf("start_here headline does not name timeline: %q", StartHere.Headline)
	}
	if len(StartHere.Lines) == 0 {
		t.Fatal("start_here carries no guidance lines")
	}
}
