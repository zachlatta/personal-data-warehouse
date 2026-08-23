package query

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeSearchRunner implements both Runner (the hybrid-probe path) and
// ArgsRunner (the parameterized search statements), recording every
// parameterized call so tests can assert on the statement and bind values.
type fakeSearchRunner struct {
	fakeRunner
	argsResults map[string]RawResult
	argsErrs    map[string]error
	statements  []string
	args        [][]any
}

func (r *fakeSearchRunner) QueryArgs(_ context.Context, statement string, args []any, maxRows int) (RawResult, error) {
	r.statements = append(r.statements, statement)
	r.args = append(r.args, args)
	if err := r.argsErrs[statement]; err != nil {
		return RawResult{}, err
	}
	result := r.argsResults[statement]
	if maxRows > 0 && len(result.Rows) > maxRows {
		result.Rows = result.Rows[:maxRows]
	}
	return result, nil
}

type fakeEmbedder struct {
	model   string
	vectors [][]float64
	err     error
	calls   int
}

func (f *fakeEmbedder) Model() string { return f.model }

func (f *fakeEmbedder) Embed(context.Context, string) ([][]float64, error) {
	f.calls++
	return f.vectors, f.err
}

func searchHit() RawResult {
	return RawResult{
		Columns: []string{"source", "who", "text", "score"},
		Rows: []map[string]any{
			{"source": "slack", "who": "zach", "text": "offer letter attached", "score": -3.2},
		},
	}
}

func hybridProbeResult(installed bool) map[string]RawResult {
	return map[string]RawResult{
		searchHybridProbeSQL: {
			Columns: []string{"installed"},
			Rows:    []map[string]any{{"installed": installed}},
		},
	}
}

func TestSearchKeywordModeRunsSearchText(t *testing.T) {
	runner := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: searchHit()}}
	svc := NewService(runner, Options{})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter", Mode: "keyword"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	if resp.Mode != SearchModeKeyword || resp.FallbackReason != "" {
		t.Fatalf("mode = %q fallback = %q", resp.Mode, resp.FallbackReason)
	}
	if len(runner.statements) != 1 || runner.statements[0] != searchTextSQL {
		t.Fatalf("statements = %#v", runner.statements)
	}
	args := runner.args[0]
	if len(args) != 4 || args[0] != "offer letter" || args[1] != searchDefaultMaxResults || args[2] != nil || args[3] != nil {
		t.Fatalf("args = %#v", args)
	}
	if resp.TotalRows != 1 || len(resp.ColumnNames) != 4 {
		t.Fatalf("total_rows = %d columns = %#v", resp.TotalRows, resp.ColumnNames)
	}
	rows, ok := resp.Rows.([]map[string]any)
	if !ok || len(rows) != 1 || rows[0]["text"] != "offer letter attached" {
		t.Fatalf("rows = %#v", resp.Rows)
	}
}

func TestSearchExactModeRunsSearchTextExact(t *testing.T) {
	runner := &fakeSearchRunner{argsResults: map[string]RawResult{searchExactSQL: searchHit()}}
	svc := NewService(runner, Options{})

	resp := svc.Search(context.Background(), SearchRequest{Query: "1441.52", Mode: "exact"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	if resp.Mode != SearchModeExact {
		t.Fatalf("mode = %q", resp.Mode)
	}
	if len(runner.statements) != 1 || runner.statements[0] != searchExactSQL {
		t.Fatalf("statements = %#v", runner.statements)
	}
}

func TestSearchPassesMaxResultsSourcesAndSince(t *testing.T) {
	runner := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: searchHit()}}
	svc := NewService(runner, Options{})

	resp := svc.Search(context.Background(), SearchRequest{
		Query:      "reunion",
		Mode:       "keyword",
		MaxResults: 7,
		Sources:    []string{"slack", "gmail"},
		Since:      "2026-03-01",
	})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	args := runner.args[0]
	if args[1] != 7 {
		t.Fatalf("max_results arg = %#v", args[1])
	}
	sources, ok := args[2].([]string)
	if !ok || len(sources) != 2 || sources[0] != "slack" || sources[1] != "gmail" {
		t.Fatalf("sources arg = %#v", args[2])
	}
	if args[3] != "2026-03-01" {
		t.Fatalf("since arg = %#v", args[3])
	}
}

func TestSearchHybridModeEmbedsAndRunsSearchHybrid(t *testing.T) {
	runner := &fakeSearchRunner{
		fakeRunner:  fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{searchHybridSQL: searchHit()},
	}
	embedder := &fakeEmbedder{model: "test-model", vectors: [][]float64{{0.5, -1.25}}}
	svc := NewService(runner, Options{SearchEmbedder: embedder})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	if resp.Mode != SearchModeHybrid || resp.FallbackReason != "" {
		t.Fatalf("mode = %q fallback = %q", resp.Mode, resp.FallbackReason)
	}
	if embedder.calls != 1 {
		t.Fatalf("embedder calls = %d", embedder.calls)
	}
	if len(runner.statements) != 1 || runner.statements[0] != searchHybridSQL {
		t.Fatalf("statements = %#v", runner.statements)
	}
	args := runner.args[0]
	if len(args) != 7 {
		t.Fatalf("args = %#v", args)
	}
	if args[0] != "offer letter" || args[1] != "[0.5,-1.25]" || args[2] != "test-model" || args[3] != searchDefaultMaxResults {
		t.Fatalf("args = %#v", args)
	}
	// One query representation means no alternate vector, and search_hybrid
	// skips its second ANN leg on a NULL rather than scanning twice.
	if args[6] != nil {
		t.Fatalf("alternate embedding = %#v, want nil", args[6])
	}
}

func TestSearchHybridPassesBothQueryRepresentations(t *testing.T) {
	// An instruction-tuned deployment returns the instructed and the raw
	// vector; both must reach search_hybrid, because each neighbourhood holds
	// answers the other misses.
	runner := &fakeSearchRunner{
		fakeRunner:  fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{searchHybridSQL: searchHit()},
	}
	embedder := &fakeEmbedder{model: "test-model", vectors: [][]float64{{0.5, -1.25}, {1, 0}}}
	svc := NewService(runner, Options{SearchEmbedder: embedder})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	args := runner.args[0]
	if len(args) != 7 {
		t.Fatalf("args = %#v", args)
	}
	if args[1] != "[0.5,-1.25]" {
		t.Fatalf("primary embedding = %#v", args[1])
	}
	if args[6] != "[1,0]" {
		t.Fatalf("alternate embedding = %#v", args[6])
	}
	if embedder.calls != 1 {
		t.Fatalf("both representations must ride in one request; calls = %d", embedder.calls)
	}
}

func TestSearchHybridFallsBackWhenEmbeddingsUnconfigured(t *testing.T) {
	runner := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: searchHit()}}
	svc := NewService(runner, Options{})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	if resp.Mode != SearchModeKeyword {
		t.Fatalf("mode = %q, want keyword fallback", resp.Mode)
	}
	if resp.FallbackReason != "embeddings unconfigured: set SEARCH_EMBEDDINGS_API_KEY or SEARCH_EMBEDDINGS_BASE_URL" {
		t.Fatalf("fallback_reason = %q", resp.FallbackReason)
	}
	if len(runner.statements) != 1 || runner.statements[0] != searchTextSQL {
		t.Fatalf("statements = %#v", runner.statements)
	}
}

func TestSearchHybridFallsBackWhenHybridFunctionMissing(t *testing.T) {
	runner := &fakeSearchRunner{
		fakeRunner:  fakeRunner{results: hybridProbeResult(false)},
		argsResults: map[string]RawResult{searchTextSQL: searchHit()},
	}
	embedder := &fakeEmbedder{model: "test-model", vectors: [][]float64{{1}}}
	svc := NewService(runner, Options{SearchEmbedder: embedder})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	if resp.Mode != SearchModeKeyword {
		t.Fatalf("mode = %q, want keyword fallback", resp.Mode)
	}
	if resp.FallbackReason != "search_hybrid not installed: postgres image lacks pgvector" {
		t.Fatalf("fallback_reason = %q", resp.FallbackReason)
	}
	if embedder.calls != 0 {
		t.Fatalf("embedder should not be called when search_hybrid is missing; calls = %d", embedder.calls)
	}
	if len(runner.statements) != 1 || runner.statements[0] != searchTextSQL {
		t.Fatalf("statements = %#v", runner.statements)
	}
}

func TestSearchHybridFallsBackWhenEmbeddingFails(t *testing.T) {
	runner := &fakeSearchRunner{
		fakeRunner:  fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{searchTextSQL: searchHit()},
	}
	embedder := &fakeEmbedder{model: "test-model", err: errors.New("endpoint down")}
	svc := NewService(runner, Options{SearchEmbedder: embedder})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	if resp.Mode != SearchModeKeyword {
		t.Fatalf("mode = %q, want keyword fallback", resp.Mode)
	}
	if !strings.Contains(resp.FallbackReason, "embedding request failed") || !strings.Contains(resp.FallbackReason, "endpoint down") {
		t.Fatalf("fallback_reason = %q", resp.FallbackReason)
	}
	if len(runner.statements) != 1 || runner.statements[0] != searchTextSQL {
		t.Fatalf("statements = %#v", runner.statements)
	}
}

func TestSearchRejectsMissingQuery(t *testing.T) {
	runner := &fakeSearchRunner{}
	svc := NewService(runner, Options{})

	resp := svc.Search(context.Background(), SearchRequest{Query: "   "})
	if resp.Error == "" || !strings.Contains(resp.Error, "query must be") {
		t.Fatalf("error = %q", resp.Error)
	}
	if len(runner.statements) != 0 {
		t.Fatalf("no SQL should run on a missing query; statements = %#v", runner.statements)
	}
}

func TestSearchRejectsUnknownMode(t *testing.T) {
	runner := &fakeSearchRunner{}
	svc := NewService(runner, Options{})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter", Mode: "semantic"})
	if resp.Error == "" || !strings.Contains(resp.Error, `"semantic"`) {
		t.Fatalf("error = %q", resp.Error)
	}
	if len(runner.statements) != 0 {
		t.Fatalf("no SQL should run on an unknown mode; statements = %#v", runner.statements)
	}
}

func TestSearchRequiresParameterizedRunner(t *testing.T) {
	svc := NewService(fakeRunner{}, Options{})
	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter", Mode: "keyword"})
	if resp.Error == "" || !strings.Contains(resp.Error, "parameterized") {
		t.Fatalf("error = %q", resp.Error)
	}
}

func TestSearchSurfacesQueryErrorsWithHints(t *testing.T) {
	runner := &fakeSearchRunner{argsErrs: map[string]error{
		searchTextSQL: errors.New("ERROR: canceling statement due to statement timeout (SQLSTATE 57014)"),
	}}
	svc := NewService(runner, Options{})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter", Mode: "keyword"})
	if resp.Error == "" || !strings.Contains(resp.Error, "statement timeout") {
		t.Fatalf("error = %q", resp.Error)
	}
	// The shared query-error hint machinery applies to search failures too.
	if !strings.Contains(resp.Error, "hint:") {
		t.Fatalf("error should carry a recovery hint: %q", resp.Error)
	}
}

func TestSearchTruncatesLongFieldsLikeQueryResults(t *testing.T) {
	long := strings.Repeat("x", 50)
	runner := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: {
		Columns: []string{"text"},
		Rows:    []map[string]any{{"text": long}},
	}}}
	svc := NewService(runner, Options{MaxFieldChars: 10})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter", Mode: "keyword"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	rows := resp.Rows.([]map[string]any)
	if rows[0]["text"] != strings.Repeat("x", 10) {
		t.Fatalf("truncated text = %q", rows[0]["text"])
	}
	if len(resp.Truncations) != 1 || resp.Truncations[0].Total != 50 || resp.Truncations[0].Returned != 10 {
		t.Fatalf("truncations = %#v", resp.Truncations)
	}
}

func TestSearchHintsWhenTheQueryIsPhrasedAsASentence(t *testing.T) {
	// The caller is itself an LLM, so query rewriting belongs in guidance, not
	// in an extra model in the search path. It is worth guiding: on the labeled
	// benchmark, sentence-shaped queries score MRR 0.27 against 0.53 for
	// term-bag queries, and rewording the nine unanswerable questions as the
	// words their answering record would contain recovered five of them, from
	// nothing-in-the-top-50 to ranks 10, 10, 12, 15 and 48.
	runner := &fakeSearchRunner{
		fakeRunner:  fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{searchHybridSQL: searchHit()},
	}
	embedder := &fakeEmbedder{model: "test-model", vectors: [][]float64{{0.5}}}
	svc := NewService(runner, Options{SearchEmbedder: embedder})

	resp := svc.Search(context.Background(), SearchRequest{
		Query: "how long our money lasts at the current pace of expenses",
	})
	if resp.Hint == "" {
		t.Fatal("a sentence-shaped query should carry a reformulation hint")
	}
	if !strings.Contains(resp.Hint, "record") {
		t.Fatalf("the hint must say what to rewrite TOWARD; got %q", resp.Hint)
	}
}

func TestSearchDoesNotHintOnATermBagQuery(t *testing.T) {
	// A hint on every response is noise nobody reads. Term-bag queries are
	// already the strong case, so they get nothing.
	runner := &fakeSearchRunner{
		fakeRunner:  fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{searchHybridSQL: searchHit()},
	}
	embedder := &fakeEmbedder{model: "test-model", vectors: [][]float64{{0.5}}}
	svc := NewService(runner, Options{SearchEmbedder: embedder})

	for _, query := range []string{
		"vision insurance VSP EyeMed glasses member ID",
		"Honda Ridgeline rear differential fluid",
		"admin/api-keys",
	} {
		resp := svc.Search(context.Background(), SearchRequest{Query: query})
		if resp.Hint != "" {
			t.Fatalf("%q should not be hinted; got %q", query, resp.Hint)
		}
	}
}

func TestSearchHintSurvivesTheKeywordFallback(t *testing.T) {
	// The advice is about phrasing, not about which retriever ran, so a
	// deployment without embeddings needs it just as much.
	runner := &fakeSearchRunner{
		fakeRunner:  fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{searchTextSQL: searchHit()},
	}
	svc := NewService(runner, Options{})

	resp := svc.Search(context.Background(), SearchRequest{
		Query: "notice that someone will be unreachable for a while",
	})
	if resp.Mode != SearchModeKeyword || resp.FallbackReason == "" {
		t.Fatalf("expected the keyword fallback; mode=%q reason=%q", resp.Mode, resp.FallbackReason)
	}
	if resp.Hint == "" {
		t.Fatal("the phrasing hint must not depend on the retriever that ran")
	}
}
