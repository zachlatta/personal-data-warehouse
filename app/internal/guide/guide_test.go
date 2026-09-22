package guide

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// The guide is prose an agent executes: it types the relation names and the
// commands it finds here. These tests keep that prose pinned to the code and
// the catalog, which is the whole reason it moved into the repository.

func renderAll(t *testing.T, surface Surface) map[string]string {
	t.Helper()
	out := map[string]string{"": mustRender(t, surface, ""), "full": mustRender(t, surface, "full")}
	for _, topic := range Topics(surface) {
		out[topic.Name] = mustRender(t, surface, topic.Name)
	}
	return out
}

func mustRender(t *testing.T, surface Surface, topic string) string {
	t.Helper()
	text, err := Render(surface, topic)
	if err != nil {
		t.Fatalf("render %s %q: %v", surface, topic, err)
	}
	if strings.TrimSpace(text) == "" {
		t.Fatalf("render %s %q: empty", surface, topic)
	}
	return text
}

func TestEveryTopicHasATemplateAndRendersOnBothSurfaces(t *testing.T) {
	for _, surface := range []Surface{SurfaceCLI, SurfaceMCP} {
		renderAll(t, surface)
	}
	if len(Topics(SurfaceMCP)) >= len(Topics(SurfaceCLI)) {
		t.Fatal("the ingest topic is CLI-only and must be hidden from MCP")
	}
}

func TestUnknownTopicNamesTheRealOnes(t *testing.T) {
	_, err := Render(SurfaceCLI, "finances")
	if err == nil {
		t.Fatal("expected an error for an unknown topic")
	}
	if !strings.Contains(err.Error(), "finance") || !strings.Contains(err.Error(), "agent-sessions") {
		t.Fatalf("error should list the topics: %v", err)
	}
	if _, err := Render(SurfaceMCP, "Agent_Sessions"); err != nil {
		t.Fatalf("topic lookup should tolerate case and underscores: %v", err)
	}
	if _, err := Render(SurfaceMCP, "ingest"); err == nil {
		t.Fatal("ingest is CLI-only and must not render for MCP")
	}
}

// qualifiedRelation matches schema.relation by shape, so a schema that never
// existed is caught too. A trailing `*` is a glob in prose and is skipped.
var qualifiedRelation = regexp.MustCompile(`\b((?:base|derived|marts|timeline|ops|private)(?:_[a-z0-9_]+)?)\.([a-z_][a-z0-9_]*|\*)`)

func TestEveryRelationTheGuideNamesExistsInTheCatalog(t *testing.T) {
	live := map[string]bool{}
	for _, obj := range warehouse.Objects {
		live[obj.Schema+"."+obj.Name] = true
	}
	var offenders []string
	for _, path := range TemplatePaths() {
		raw, err := ReadTemplate(path)
		if err != nil {
			t.Fatal(err)
		}
		for lineno, line := range strings.Split(raw, "\n") {
			for _, match := range qualifiedRelation.FindAllStringSubmatch(line, -1) {
				if match[2] == "*" {
					continue
				}
				token := match[1] + "." + match[2]
				if !live[token] && !deliberatelyWrongNames[token] {
					offenders = append(offenders, path+":"+itoa(lineno+1)+": "+token)
				}
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("the guide names relations that do not exist:\n  %s", strings.Join(offenders, "\n  "))
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

// deliberatelyWrongNames are the names the guide lists as what agents keep
// guessing, beside the real one. They must stay wrong, so they are exempt.
var deliberatelyWrongNames = map[string]bool{
	"marts.ai_conversation_events": true,
}

func TestGuideRendersTheGeneratedPriorityContract(t *testing.T) {
	for _, surface := range []Surface{SurfaceCLI, SurfaceMCP} {
		text := mustRender(t, surface, "")
		for _, tier := range warehouse.TimelinePriorities.Tiers {
			if !strings.Contains(text, "`"+tier.Name+"`") || !strings.Contains(text, tier.Meaning) {
				t.Fatalf("%s guide omits generated tier %q", surface, tier.Name)
			}
		}
		for _, selection := range warehouse.TimelinePriorities.SelectionGuide {
			if !strings.Contains(text, selection.Intent) {
				t.Fatalf("%s guide omits generated selection %q", surface, selection.Intent)
			}
		}
		if !strings.Contains(text, warehouse.TimelinePriorities.Sentinel.Name) {
			t.Fatalf("%s guide does not explain the %s sentinel", surface, warehouse.TimelinePriorities.Sentinel.Name)
		}
	}
}

func TestEachSurfaceSpellsItsOwnCalls(t *testing.T) {
	cli := renderAll(t, SurfaceCLI)
	mcp := renderAll(t, SurfaceMCP)
	for name, text := range cli {
		if !strings.Contains(text, "pdw ") {
			t.Fatalf("CLI %q never mentions a pdw command", name)
		}
		if strings.Contains(text, "describe_table`") && !strings.Contains(text, "pdw columns") {
			t.Fatalf("CLI %q teaches the MCP tool name instead of `pdw columns`", name)
		}
	}
	for name, text := range mcp {
		for _, cliOnly := range []string{"pdw search", "pdw sql", "pdw columns", "pdw schema", "pdw call", "pdw ingest", "--priority", "--output"} {
			// The MCP guide may name the CLI once, in the command map, to
			// say the same warehouse exists there; it must not teach it.
			if strings.Count(text, cliOnly) > 1 {
				t.Fatalf("MCP %q teaches the CLI spelling %q more than once", name, cliOnly)
			}
		}
	}
	main := mcp[""]
	for _, tool := range []string{"`search`", "`query`", "`describe_table`", "`schema_overview`", "`propose_mutation_help`", "`get_object`", "`readme`"} {
		if !strings.Contains(main, tool) {
			t.Fatalf("MCP guide does not name the %s tool", tool)
		}
	}
	for _, cmd := range []string{"pdw search", "pdw sql", "pdw columns", "pdw schema", "pdw call", "pdw readme", "pdw login"} {
		if !strings.Contains(cli[""], cmd) {
			t.Fatalf("CLI guide does not name %q", cmd)
		}
	}
}

func TestGuideTeachesNoRefusedOrInventedCommand(t *testing.T) {
	// The CLI refuses these with a redirect; the guide must not teach them
	// except as the thing not to type.
	text := mustRender(t, SurfaceCLI, "")
	for _, forbidden := range []string{"pdw call search", "pdw call sql ", "pdw call query", "pdw call schema_overview", "pdw call describe_table"} {
		idx := strings.Index(text, forbidden)
		if idx < 0 {
			continue
		}
		window := text[max(0, idx-400):idx]
		if !strings.Contains(window, "do not exist") && !strings.Contains(window, "invent") {
			t.Fatalf("CLI guide teaches %q outside the do-not-type list", forbidden)
		}
	}
}

func TestMainGuideStaysReadableInOneSitting(t *testing.T) {
	// The brief is read by every session, so its size is a per-session tax:
	// measured over twelve real sessions the old 14 KB main page plus the
	// fleet skill cost 25-45 KB before the first data call, whatever the
	// question was. The full guide is still capped so it stays one sitting.
	for _, surface := range []Surface{SurfaceCLI, SurfaceMCP} {
		brief := mustRender(t, surface, "")
		if n := len(brief); n > 8_000 {
			t.Fatalf("%s brief guide is %d bytes; move detail into the full guide or a topic (cap 8000)", surface, n)
		}
		if n := len(brief); n < 3_000 {
			t.Fatalf("%s brief guide is only %d bytes; it lost content", surface, n)
		}
		full := mustRender(t, surface, "full")
		if n := len(full); n > 14_000 {
			t.Fatalf("%s full guide is %d bytes; move detail into a topic (cap 14000)", surface, n)
		}
		if n := len(full); n < 6_000 {
			t.Fatalf("%s full guide is only %d bytes; it lost content", surface, n)
		}
	}
}

func TestBriefNamesTheFullGuide(t *testing.T) {
	if !strings.Contains(mustRender(t, SurfaceCLI, ""), "pdw readme full") {
		t.Fatal("CLI brief does not say how to reach the full guide")
	}
	if !strings.Contains(mustRender(t, SurfaceMCP, ""), `{"topic": "full"}`) {
		t.Fatal("MCP brief does not say how to reach the full guide")
	}
}

func TestMainGuideIndexesEveryTopic(t *testing.T) {
	for _, surface := range []Surface{SurfaceCLI, SurfaceMCP} {
		text := mustRender(t, surface, "")
		for _, topic := range Topics(surface) {
			if !strings.Contains(text, "`"+topic.Name+"`") {
				t.Fatalf("%s guide does not index topic %q", surface, topic.Name)
			}
		}
	}
	if strings.Contains(mustRender(t, SurfaceMCP, ""), "`ingest` —") {
		t.Fatal("MCP guide indexes the CLI-only ingest topic")
	}
}

func TestRenderedGuideHasNoTemplateResidue(t *testing.T) {
	for _, surface := range []Surface{SurfaceCLI, SurfaceMCP} {
		for name, text := range renderAll(t, surface) {
			if strings.Contains(text, "{{") || strings.Contains(text, "}}") || strings.Contains(text, "<no value>") {
				t.Fatalf("%s %q has unrendered template text", surface, name)
			}
			if strings.Contains(text, "\n\n\n") {
				t.Fatalf("%s %q has a run of blank lines", surface, name)
			}
		}
	}
}

func TestConnectionsGuideTeachesSchemaFirstAndOutput(t *testing.T) {
	for _, surface := range []Surface{SurfaceCLI, SurfaceMCP} {
		text := mustRender(t, surface, "connections")
		for _, want := range []string{"before the first call", "parallel", "not automatically retried"} {
			if !strings.Contains(text, want) {
				t.Errorf("%s missing %q", surface, want)
			}
		}
		if surface == SurfaceCLI {
			for _, want := range []string{"pdw describe <tool>", "--output text", "--output structured", "< input.json", "nonzero", "do not repeat a write"} {
				if !strings.Contains(text, want) {
					t.Errorf("CLI missing %q", want)
				}
			}
		} else if strings.Contains(text, "--output") {
			t.Error("CLI flags leaked into MCP guide")
		}
	}
}
