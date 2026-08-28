package query

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSearchRunner implements both Runner (the hybrid-probe path) and
// ArgsRunner (the parameterized search statements), recording every
// parameterized call so tests can assert on the statement and bind values.
type fakeSearchRunner struct {
	fakeRunner
	argsResults map[string]RawResult
	argsErrs    map[string]error
	delays      map[string]time.Duration
	mu          sync.Mutex
	active      int
	maxActive   int
	statements  []string
	args        [][]any
}

func (r *fakeSearchRunner) QueryArgs(_ context.Context, statement string, args []any, maxRows int) (RawResult, error) {
	r.mu.Lock()
	r.statements = append(r.statements, statement)
	r.args = append(r.args, args)
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	r.mu.Unlock()
	if delay := r.delays[statement]; delay > 0 {
		time.Sleep(delay)
	}
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	if err := r.argsErrs[statement]; err != nil {
		return RawResult{}, err
	}
	result := r.argsResults[statement]
	if maxRows > 0 && len(result.Rows) > maxRows {
		result.Rows = result.Rows[:maxRows]
	}
	return result, nil
}

func (r *fakeSearchRunner) callsFor(statement string) [][]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	var calls [][]any
	for index, got := range r.statements {
		if got == statement {
			calls = append(calls, r.args[index])
		}
	}
	return calls
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

func searchRefRows(refs ...string) RawResult {
	rows := make([]map[string]any, 0, len(refs))
	for _, ref := range refs {
		rows = append(rows, map[string]any{"ref": ref})
	}
	return RawResult{Columns: []string{"ref"}, Rows: rows}
}

func semanticHits() RawResult {
	return RawResult{
		Columns: []string{"ref", "best", "fuse", "chunk_id"},
		Rows: []map[string]any{{
			"ref": "slack_message:zrl|T1|C1|1.0", "best": int64(1),
			"fuse": 1.0 / 61.0, "chunk_id": "chunk-1",
		}},
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
	if len(args) != 5 || args[0] != "offer letter" || args[1] != searchDefaultMaxResults || args[2] != nil || args[3] != nil || args[4] != nil {
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

func TestSearchDefaultResultCountBoundsLLMOutput(t *testing.T) {
	if searchDefaultMaxResults != 20 {
		t.Fatalf("default max_results = %d, want 20", searchDefaultMaxResults)
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

func TestSearchHybridModeFansIndependentLegsOutThenFuses(t *testing.T) {
	runner := &fakeSearchRunner{
		fakeRunner: fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{
			searchHybridLexicalSQL:  searchRefRows("gmail_email:a|m1"),
			searchHybridSemanticSQL: semanticHits(),
			searchHybridExactSQL:    searchRefRows(),
			searchHybridFuseSQL:     searchHit(),
		},
		delays: map[string]time.Duration{
			searchHybridLexicalSQL:  25 * time.Millisecond,
			searchHybridSemanticSQL: 25 * time.Millisecond,
			searchHybridExactSQL:    25 * time.Millisecond,
		},
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
	if runner.maxActive < 3 {
		t.Fatalf("hybrid retrieval legs ran serially; max concurrent calls = %d", runner.maxActive)
	}
	if len(runner.statements) != 4 || runner.statements[len(runner.statements)-1] != searchHybridFuseSQL {
		t.Fatalf("statements = %#v", runner.statements)
	}
	for _, statement := range []string{searchHybridLexicalSQL, searchHybridSemanticSQL, searchHybridExactSQL, searchHybridFuseSQL} {
		if !slices.Contains(runner.statements, statement) {
			t.Fatalf("missing %q from statements %#v", statement, runner.statements)
		}
	}
	semanticArgs := runner.callsFor(searchHybridSemanticSQL)
	if len(semanticArgs) != 1 || len(semanticArgs[0]) != 6 {
		t.Fatalf("semantic args = %#v", semanticArgs)
	}
	if semanticArgs[0][0] != "[0.5,-1.25]" || semanticArgs[0][1] != "test-model" || semanticArgs[0][2] != searchDefaultMaxResults {
		t.Fatalf("semantic args = %#v", semanticArgs[0])
	}
	if semanticArgs[0][5] != nil {
		t.Fatalf("the original query leg must keep the measured full candidate pool: %#v", semanticArgs[0])
	}
	fuseCalls := runner.callsFor(searchHybridFuseSQL)
	if len(fuseCalls) != 1 {
		t.Fatalf("fuse calls = %#v", fuseCalls)
	}
	args := fuseCalls[0]
	if len(args) != 6 {
		t.Fatalf("args = %#v", args)
	}
	if args[0] != "offer letter" || args[1] != searchDefaultMaxResults {
		t.Fatalf("args = %#v", args)
	}
	if refs, ok := args[2].([]string); !ok || len(refs) != 1 || refs[0] != "gmail_email:a|m1" {
		t.Fatalf("lexical refs = %#v", args[2])
	}
	if semanticJSON, ok := args[3].(string); !ok || !strings.Contains(semanticJSON, "chunk-1") {
		t.Fatalf("semantic JSON = %#v", args[3])
	}
}

func TestSearchHybridPassesBothQueryRepresentations(t *testing.T) {
	// An instruction-tuned deployment returns the instructed and the raw
	// vector; both must reach search_hybrid, because each neighbourhood holds
	// answers the other misses.
	runner := &fakeSearchRunner{
		fakeRunner: fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{
			searchHybridSemanticSQL: semanticHits(),
			searchHybridFuseSQL:     searchHit(),
		},
	}
	embedder := &fakeEmbedder{model: "test-model", vectors: [][]float64{{0.5, -1.25}, {1, 0}}}
	svc := NewService(runner, Options{SearchEmbedder: embedder})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	calls := runner.callsFor(searchHybridSemanticSQL)
	if len(calls) != 2 {
		t.Fatalf("semantic calls = %#v", calls)
	}
	want := map[any]bool{"[0.5,-1.25]": true, "[1,0]": true}
	for _, args := range calls {
		if !want[args[0]] {
			t.Fatalf("unexpected semantic embedding = %#v", args[0])
		}
	}
	if embedder.calls != 1 {
		t.Fatalf("both representations must ride in one request; calls = %d", embedder.calls)
	}
}

func TestSearchHybridBoundsOnlyTheExtraTermBagLegs(t *testing.T) {
	runner := &fakeSearchRunner{
		fakeRunner: fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{
			searchHybridSemanticSQL: semanticHits(),
			searchHybridFuseSQL:     searchHit(),
		},
	}
	embedder := &fakeEmbedder{model: "test-model", vectors: [][]float64{
		{1, 0}, {0, 1}, {2, 0}, {0, 2},
	}}
	svc := NewService(runner, Options{SearchEmbedder: embedder})

	resp := svc.Search(context.Background(), SearchRequest{Query: "what is still owed to the vet clinic"})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	calls := runner.callsFor(searchHybridSemanticSQL)
	if len(calls) != 4 {
		t.Fatalf("semantic calls = %#v", calls)
	}
	wantLimit := map[any]any{
		"[1,0]": nil, "[0,1]": nil,
		"[2,0]": searchHybridTermBagMinCandidates,
		"[0,2]": searchHybridTermBagMinCandidates,
	}
	for _, args := range calls {
		if len(args) != 6 || args[5] != wantLimit[args[0]] {
			t.Fatalf("semantic args = %#v, want candidate limit %#v", args, wantLimit[args[0]])
		}
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
	// The embedder MAY have been called: the probe and the embedding request
	// are deliberately overlapped (two independent round trips, one to
	// Postgres and one to the GPU box), so a host without pgvector pays one
	// speculative embed per search while every properly equipped host saves
	// the probe's latency on every search. What must not change is the
	// verdict: the probe still decides, and it still names the fix.
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

func TestSearchResponseGuidesTheNextStep(t *testing.T) {
	withHit := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: searchHit()}}
	resp := NewService(withHit, Options{}).Search(context.Background(), SearchRequest{Query: "offer letter", Mode: "keyword"})
	if !strings.Contains(resp.Guidance, "timeline.context") || !strings.Contains(resp.Guidance, "source_table") {
		t.Fatalf("hit guidance is not actionable: %q", resp.Guidance)
	}

	withoutHit := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: {}}}
	resp = NewService(withoutHit, Options{}).Search(context.Background(), SearchRequest{Query: "missing", Mode: "keyword"})
	for _, want := range []string{"not proof of absence", "exact", "ILIKE"} {
		if !strings.Contains(resp.Guidance, want) {
			t.Fatalf("zero-result guidance missing %q: %q", want, resp.Guidance)
		}
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
		argsResults: map[string]RawResult{searchHybridFuseSQL: searchHit()},
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

func bulkHits(tiers ...string) RawResult {
	rows := make([]map[string]any, 0, len(tiers))
	for i, tier := range tiers {
		rows = append(rows, map[string]any{"source": "gmail", "ref": fmt.Sprintf("r%d", i), "priority": tier})
	}
	return RawResult{Columns: []string{"source", "ref", "priority"}, Rows: rows}
}

func TestSearchHintsWhenAnUnscopedResultIsMostlyNoise(t *testing.T) {
	// 14 days of agent sessions: the priorities filter appeared on 6% of search
	// calls, and unscoped searches came back 8/20 noise + 6/20 background. The
	// hint is measured on THIS response, so a scoped call or a clean result
	// set gets nothing.
	mostlyBulk := bulkHits("noise", "noise", "background", "noise", "direct", "self")
	runner := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: mostlyBulk}}
	resp := NewService(runner, Options{}).Search(context.Background(), SearchRequest{Query: "offer letter", Mode: "keyword"})
	if !strings.Contains(resp.Hint, "priorities") || !strings.Contains(resp.Hint, "direct") {
		t.Fatalf("an unscoped, mostly-noise result must suggest scoping by tier; got %q", resp.Hint)
	}

	scoped := NewService(runner, Options{}).Search(context.Background(), SearchRequest{
		Query: "offer letter", Mode: "keyword", Priorities: []string{"noise"},
	})
	if strings.Contains(scoped.Hint, "priorities") {
		t.Fatalf("a scoped call already chose its tiers; got %q", scoped.Hint)
	}

	clean := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: bulkHits("self", "direct", "cc", "noise", "direct")}}
	resp = NewService(clean, Options{}).Search(context.Background(), SearchRequest{Query: "offer letter", Mode: "keyword"})
	if strings.Contains(resp.Hint, "priorities") {
		t.Fatalf("a mostly-attention result must not be nagged; got %q", resp.Hint)
	}

	tiny := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: bulkHits("noise", "noise")}}
	resp = NewService(tiny, Options{}).Search(context.Background(), SearchRequest{Query: "offer letter", Mode: "keyword"})
	if strings.Contains(resp.Hint, "priorities") {
		t.Fatalf("two rows say nothing about the mix; got %q", resp.Hint)
	}
}

func TestSearchDoesNotHintOnATermBagQuery(t *testing.T) {
	// A hint on every response is noise nobody reads. Term-bag queries are
	// already the strong case, so they get nothing.
	runner := &fakeSearchRunner{
		fakeRunner:  fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{searchHybridFuseSQL: searchHit()},
	}
	embedder := &fakeEmbedder{model: "test-model", vectors: [][]float64{{0.5}}}
	svc := NewService(runner, Options{SearchEmbedder: embedder})

	for _, query := range []string{
		"vision insurance VSP EyeMed glasses member ID",
		"pickup truck rear differential fluid",
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

func TestSearchScopesEveryModeToPriorityTiers(t *testing.T) {
	// Priority is the difference between "what needs my attention" and 48M
	// rows, so it has to reach the SQL call in EVERY mode -- including the
	// hybrid path, whose extra parameters make it the easy one to forget.
	cases := []struct {
		mode      string
		statement string
		index     int
	}{
		{SearchModeKeyword, searchTextSQL, 4},
		{SearchModeExact, searchExactSQL, 4},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			runner := &fakeSearchRunner{
				fakeRunner:  fakeRunner{results: hybridProbeResult(true)},
				argsResults: map[string]RawResult{tc.statement: searchHit()},
			}
			opts := Options{}
			if tc.mode == SearchModeHybrid {
				opts.SearchEmbedder = &fakeEmbedder{model: "test-model", vectors: [][]float64{{1}}}
			}
			svc := NewService(runner, opts)

			resp := svc.Search(context.Background(), SearchRequest{
				Query:      "offer letter",
				Mode:       tc.mode,
				Priorities: []string{"self", "direct"},
			})
			if resp.Error != "" {
				t.Fatalf("error: %s", resp.Error)
			}
			if len(runner.args) != 1 {
				t.Fatalf("args = %#v", runner.args)
			}
			tiers, ok := runner.args[0][tc.index].([]string)
			if !ok || len(tiers) != 2 || tiers[0] != "self" || tiers[1] != "direct" {
				t.Fatalf("priorities missing from %s args: %#v", tc.mode, runner.args[0])
			}
		})
	}
	t.Run(SearchModeHybrid, func(t *testing.T) {
		runner := &fakeSearchRunner{
			fakeRunner: fakeRunner{results: hybridProbeResult(true)},
			argsResults: map[string]RawResult{
				searchHybridFuseSQL: searchHit(),
			},
		}
		svc := NewService(runner, Options{
			SearchEmbedder: &fakeEmbedder{model: "test-model", vectors: [][]float64{{1}}},
		})

		resp := svc.Search(context.Background(), SearchRequest{
			Query:      "offer letter",
			Mode:       SearchModeHybrid,
			Priorities: []string{"self", "direct"},
		})
		if resp.Error != "" {
			t.Fatalf("error: %s", resp.Error)
		}
		for _, check := range []struct {
			statement string
			index     int
		}{
			{searchHybridExactSQL, 4},
			{searchHybridFuseSQL, 5},
		} {
			calls := runner.callsFor(check.statement)
			if len(calls) != 1 {
				t.Fatalf("%s calls = %#v", check.statement, calls)
			}
			tiers, ok := calls[0][check.index].([]string)
			if !ok || !slices.Equal(tiers, []string{"self", "direct"}) {
				t.Fatalf("priorities missing from %s args: %#v", check.statement, calls[0])
			}
		}
	})
}

func TestSearchWithoutPrioritiesBindsNull(t *testing.T) {
	// Omitting the filter must be byte-identical to the old behavior: a NULL
	// bind, which the SQL side reads as "every tier". Binding an empty array
	// instead would be a silent corpus-wide change of meaning.
	runner := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: searchHit()}}
	svc := NewService(runner, Options{})

	svc.Search(context.Background(), SearchRequest{Query: "offer letter", Mode: "keyword", Priorities: []string{}})
	if runner.args[0][4] != nil {
		t.Fatalf("empty priorities must bind NULL; args = %#v", runner.args[0])
	}
}

func TestSearchRejectsUnknownPriorityTier(t *testing.T) {
	// Loud, and naming the valid set: silently dropping the token would answer
	// a tier-scoped question with the whole corpus, which reads as a correct
	// answer to anyone downstream.
	runner := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: searchHit()}}
	svc := NewService(runner, Options{})

	resp := svc.Search(context.Background(), SearchRequest{
		Query:      "offer letter",
		Mode:       "keyword",
		Priorities: []string{"self", "urgent"},
	})
	if resp.Error == "" {
		t.Fatal("an unknown priority tier must be an error, not a silently wider search")
	}
	if !strings.Contains(resp.Error, `unknown priority "urgent"`) {
		t.Fatalf("error = %q", resp.Error)
	}
	for _, tier := range SearchPriorities {
		if !strings.Contains(resp.Error, tier) {
			t.Fatalf("error must list every valid tier (missing %q): %q", tier, resp.Error)
		}
	}
	if len(runner.statements) != 0 {
		t.Fatalf("rejected search must not reach the database; statements = %#v", runner.statements)
	}
}

func TestSearchHitsCarryPriority(t *testing.T) {
	// A hit that does not say which tier it came from cannot be triaged, and
	// an agent filtering by tier has no way to show its work. The column list
	// is explicit precisely so this cannot drift silently.
	if !strings.Contains(searchResultColumns, "priority") {
		t.Fatalf("searchResultColumns must select priority: %q", searchResultColumns)
	}
	for _, statement := range []string{searchTextSQL, searchExactSQL, searchHybridFuseSQL} {
		if !strings.Contains(statement, "priority") {
			t.Fatalf("statement must select priority: %q", statement)
		}
	}
}

func TestSearchSQLPassesPrioritiesToEverySQLFunction(t *testing.T) {
	// The SQL functions gained a trailing priorities parameter; the probe has
	// to match the new signature too or hybrid silently falls back to keyword
	// on a perfectly healthy deployment.
	if !strings.Contains(searchTextSQL, "$5::text[]") || !strings.Contains(searchExactSQL, "$5::text[]") {
		t.Fatalf("search_text/search_text_exact must pass priorities: %q %q", searchTextSQL, searchExactSQL)
	}
	if !strings.Contains(searchHybridExactSQL, "$5::text[]") || !strings.Contains(searchHybridFuseSQL, "$6::text[]") {
		t.Fatalf("parallel exact/fuse helpers must pass priorities: %q %q", searchHybridExactSQL, searchHybridFuseSQL)
	}
	for _, function := range []string{"search_hybrid", "search_hybrid_semantic", "search_hybrid_exact", "search_hybrid_fuse"} {
		if !strings.Contains(searchHybridProbeSQL, function) {
			t.Fatalf("the hybrid probe must require %s: %q", function, searchHybridProbeSQL)
		}
	}
}
