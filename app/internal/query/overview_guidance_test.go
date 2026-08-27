package query

import (
	"context"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// The catalog's schema and relation comments are published as real Postgres
// comments and were rendered by nothing. schema_overview is the surface 43% of
// agent sessions open with, and 18% open by naming a relation that does not
// exist, so these tests pin that the guidance now reaches the caller — and,
// just as importantly, that it stays selective enough to afford.

func renderTestOverview(t *testing.T) string {
	t.Helper()
	tables := make([]tableRef, 0, len(warehouse.Objects))
	for _, obj := range warehouse.Objects {
		if obj.IsRelation() && obj.Discoverable {
			tables = append(tables, tableRef{Schema: obj.Schema, Name: obj.Name})
		}
	}
	if len(tables) == 0 {
		t.Fatal("catalog exposes no discoverable relations")
	}
	svc := &Service{}
	return svc.renderOverview("pdw", tables, map[string]*relationFacts{}, "")
}

func TestOverviewMarksTheEntryPointAndNotItsDrillDowns(t *testing.T) {
	// The distinction this marker exists to draw. marts_photos.files is named
	// by the schema comment too, but as a description, not a nomination —
	// marking it would erase the signal.
	if !entryPointRelations["marts_photos.photos"] {
		t.Fatal("marts_photos.photos is the entry point and must be marked")
	}
	for _, drillDown := range []string{"marts_photos.files", "marts_photos.canonical_renditions"} {
		if entryPointRelations[drillDown] {
			t.Fatalf("%s is a drill-down, not an entry point", drillDown)
		}
	}
	if !entryPointRelations["timeline.events"] {
		t.Fatal("timeline.events is THE entry point and must be marked")
	}

	overview := renderTestOverview(t)
	if !strings.Contains(overview, "→ marts_photos.photos") {
		t.Fatalf("entry point is not marked in the rendered overview")
	}
	if strings.Contains(overview, "→ marts_photos.files") {
		t.Fatalf("drill-down was marked as an entry point")
	}

	// Every marked relation must really exist, or the marker sends a caller at
	// a name that 42P01s — the exact failure this whole change is fixing.
	known := map[string]bool{}
	for _, obj := range warehouse.Objects {
		if obj.IsRelation() {
			known[obj.Schema+"."+obj.Name] = true
		}
	}
	for display := range entryPointRelations {
		if !known[display] {
			t.Fatalf("entry point %q is not a relation in the catalog", display)
		}
	}
}

func TestOverviewRendersSchemaAndRelationGuidance(t *testing.T) {
	overview := renderTestOverview(t)
	// The schema's own headline, naming where to start.
	if !strings.Contains(overview, "Start with marts_photos.photos") {
		t.Fatal("schema headline is missing from the overview")
	}
	// A relation's own description, which is what tells .photos from .files.
	if !strings.Contains(overview, "Every rendition from every photo source") {
		t.Fatal("relation guidance is missing from the overview")
	}
	// The trap that a caller answering a cost-basis question needs before they
	// pick the relation, not after they quote a wrong number.
	if !strings.Contains(overview, "Plaid's lookback is a hard 730 days") {
		t.Fatal("marts_finance.investment_transactions must carry its lookback warning")
	}
	// And the legend that explains the two new line shapes.
	if !strings.Contains(overview, "READING THE LISTING") {
		t.Fatal("the overview must explain its own markers")
	}
}

func TestOverviewSkipsBoilerplateSchemaHeadlines(t *testing.T) {
	// The 21 base_* and 6 generic derived_* comments are one templated
	// sentence with the source name substituted. The preamble's layer table
	// already says it; printing it 27 more times costs ~3.2KB to say nothing.
	for _, schema := range []string{"base_slack", "base_gmail", "derived_photos", "derived_finance"} {
		if _, ok := schemaHeadlines[schema]; ok {
			t.Fatalf("%s carries a boilerplate comment and must not spend budget on it", schema)
		}
	}
	for _, schema := range []string{"marts_photos", "marts_finance", "derived_search"} {
		if _, ok := schemaHeadlines[schema]; !ok {
			t.Fatalf("%s names a specific relation and must be rendered", schema)
		}
	}
	overview := renderTestOverview(t)
	if strings.Contains(overview, "Faithful slack source data") {
		t.Fatal("boilerplate base_* comment leaked into the overview")
	}
}

// TestOverviewGuidanceStaysWithinBudget is the guard on the constraint that
// shaped the whole rendering. The overview is the required first call and every
// agent pays for it; the previous full-column dump was ~61KB and callers lost
// it. Guidance is worth real tokens, but not unbounded ones.
func TestOverviewGuidanceStaysWithinBudget(t *testing.T) {
	overview := renderTestOverview(t)
	guidance := 0
	for _, line := range strings.Split(overview, "\n") {
		if strings.HasPrefix(line, "#   ") || strings.HasPrefix(line, "      ") {
			guidance += len(line) + 1
		}
	}
	// 8000 held until 2026-08-27, when two START HERE relations were added
	// (marts_files.attachments, marts_ops.timeline_priority_mix) and one
	// schema line with them. Each is a headline agents need on the first call;
	// the budget moves for those, not for prose.
	if guidance > 8600 {
		t.Fatalf("catalog guidance renders %d bytes; keep it selective (cap 8600)", guidance)
	}
	if guidance < 3000 {
		t.Fatalf("catalog guidance renders only %d bytes; it is not reaching the caller", guidance)
	}
}

// TestDescribeTableCarriesTheFullRelationComment is the other half of the
// two-tier design: the overview prints a capped first sentence, and this is
// where the rest lives. Without it the truncation would be a loss rather than a
// pointer — and before this change the catalog's prose was rendered by neither.
func TestDescribeTableCarriesTheFullRelationComment(t *testing.T) {
	const relation = "marts_photos.canonical_renditions"
	comment := relationHeadlines[relation]
	if comment == "" {
		t.Skipf("%s carries no catalog comment", relation)
	}
	runner := fakeRunner{results: map[string]RawResult{
		relationsNamedSQL("canonical_renditions"): {
			Columns: []string{"schema", "name"},
			Rows:    []map[string]any{{"schema": "marts_photos", "name": "canonical_renditions"}},
		},
		rowEstimateSQLFor("marts_photos", "canonical_renditions"): {Columns: []string{"row_estimate"}},
		indexSQLFor("marts_photos", "canonical_renditions"):       {Columns: []string{"def", "flag"}},
		describeColumnsSQL("marts_photos", "canonical_renditions"): {
			Columns: []string{"name", "type"},
			Rows:    []map[string]any{{"name": "photo_id", "type": "text"}},
		},
	}}
	svc := NewService(runner, Options{MaxRows: 5, MaxFieldChars: 100})

	csv := svc.DescribeTable(context.Background(), relation).Results[0].CSV

	// The whole comment must be present, unwrapped, including the part the
	// overview cut off.
	flat := strings.Join(strings.Fields(strings.ReplaceAll(csv, "#", " ")), " ")
	if !strings.Contains(flat, strings.Join(strings.Fields(comment), " ")) {
		t.Fatalf("describe_table dropped the catalog comment for %s:\n%s", relation, csv)
	}
	capped := capRunes(firstSentence(comment), overviewRelationCommentChars)
	if !strings.HasSuffix(capped, "…") {
		t.Fatalf("fixture no longer exercises truncation; pick a longer comment (%q)", capped)
	}
}

func TestOverviewRelationCommentIsCappedAndPointsAtDescribeTable(t *testing.T) {
	// Truncation here is only safe because describe_table prints the rest, so
	// the cut is marked and the full text has somewhere to live.
	long := "Exactly one enrichable still per logical photo (the 1280px thumbnail), shaped for the shared file-enrichment runner. Not a general photo listing."
	got := capRunes(firstSentence(long), overviewRelationCommentChars)
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("an over-long description must mark its cut: %q", got)
	}
	if len([]rune(got)) > overviewRelationCommentChars+1 {
		t.Fatalf("capped description is %d runes: %q", len([]rune(got)), got)
	}
	// A short comment is left exactly as the catalog wrote it.
	if got := capRunes(firstSentence("Start here for photos. One row per photo."), overviewRelationCommentChars); got != "Start here for photos." {
		t.Fatalf("short description was altered: %q", got)
	}
	// Em dashes are multi-byte; a byte-slice cut would corrupt them.
	dashes := strings.Repeat("a—b ", 60)
	if !strings.ContainsRune(capRunes(dashes, 40), '—') {
		t.Fatal("capRunes must not split multi-byte runes")
	}
}
