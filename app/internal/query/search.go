package query

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// Search modes. Hybrid is the default: semantic+keyword retrieval through
// timeline.search_hybrid, falling back to the keyword path whenever embeddings
// or the SQL function are unavailable so the tool always answers.
const (
	SearchModeHybrid  = "hybrid"
	SearchModeKeyword = "keyword"
	SearchModeExact   = "exact"
)

// Twenty keeps the default MCP response small enough for an agent to inspect
// in full. Callers doing recall work can still request up to the SQL-side cap.
// The earlier default of 50 routinely produced tens of thousands of output
// tokens, so agents piped the response through ad-hoc Python or `head` and lost
// the metadata and lower-ranked hits anyway.
const searchDefaultMaxResults = 20

const (
	searchHitGuidance = "For a chat/channel or agent-turn hit, read neighboring events with " +
		"timeline.context(ref, 5, 5). If the hit is still insufficient, use source_table and " +
		"source_pk for a one-hop drill-down to the authoritative row; raise max_results explicitly " +
		"only when you need more recall."
	searchEmptyGuidance = "No hits is not proof of absence. Retry once with fewer terms or the " +
		"vocabulary the answering record would contain; use mode exact for an identifier or literal " +
		"phrase, widen sources/since if scoped, or raise max_results for more recall. Do not fall " +
		"back to ILIKE over raw body columns."
)

// searchPhrasingHint is advice for the caller's NEXT search, not an error. The
// caller here is itself a language model, which is why query rewriting lives in
// guidance rather than in another model inside the search path: it costs
// nothing and needs no new dependency.
//
// It is worth saying. On the labeled benchmark, sentence-shaped queries score
// MRR 0.27 where term-bag queries score 0.53, and rewording the nine questions
// that returned nothing useful -- as the words their ANSWERING RECORD would
// contain rather than the words of the question -- recovered five of them, from
// nothing in the top 50 to ranks 10, 10, 12, 15 and 48.
const searchPhrasingHint = "This query reads like a sentence. Retrieval here is measurably better " +
	"when a query is phrased as the words the ANSWERING RECORD would contain rather than the " +
	"words of the question: \"how long our money lasts\" finds nothing, \"runway burn rate months " +
	"of cash remaining\" finds it. If the results below miss, re-issue with the vocabulary the " +
	"record itself would use (an email's subject line, a statement's column heading, the phrase a " +
	"person would actually have typed)."

// searchSentenceWords are the function words that separate a question from a
// bag of search terms. Counting them is crude, and deliberately so: the
// alternative is another model call to classify a string. Two or more means the
// query is carrying grammar rather than search terms, which is exactly the
// shape the benchmark says retrieves worst.
var searchSentenceWords = map[string]bool{
	"a": true, "an": true, "the": true, "my": true, "our": true, "your": true, "their": true,
	"is": true, "are": true, "was": true, "were": true, "will": true, "would": true, "can": true,
	"of": true, "for": true, "with": true, "that": true, "this": true, "at": true, "on": true,
	"in": true, "to": true, "from": true, "and": true, "or": true, "by": true, "about": true,
	"how": true, "what": true, "when": true, "where": true, "why": true, "who": true, "which": true,
	"did": true, "does": true, "do": true, "should": true, "could": true, "me": true, "i": true,
}

// searchHintFor returns advice for the caller, or "" when the query is already
// in the shape that retrieves well. Hinting every response is noise nobody
// reads, so the strong case gets nothing.
func searchHintFor(query string) string {
	fields := strings.Fields(strings.ToLower(query))
	if len(fields) < 5 {
		return ""
	}
	sentenceWords := 0
	for _, field := range fields {
		if searchSentenceWords[strings.Trim(field, ".,!?;:'\"")] {
			sentenceWords++
		}
	}
	if sentenceWords < 2 {
		return ""
	}
	return searchPhrasingHint
}

// ArgsRunner is the parameterized-query capability search needs: caller
// values (the query text, source tokens, since bound) ride as bind parameters
// instead of being spliced into SQL. *PostgresRunner implements it with the
// same read-only role and statement-timeout machinery every query gets.
type ArgsRunner interface {
	QueryArgs(ctx context.Context, statement string, args []any, maxRows int) (RawResult, error)
}

// searchResultColumns is the shared hit shape all three timeline search
// functions return; the explicit list keeps the tool's output stable even if
// the SQL functions grow trailing columns.
const searchResultColumns = "source, subsource, context, who, occurred_at, account, ref, text, score, event_ts, title, source_table, source_pk, priority"

// The int/text[]/timestamptz casts are load-bearing: pgx binds a Go int as
// bigint and there is no implicit bigint→integer cast during function
// resolution, so an uncast $2 would fail to match the functions' integer
// max_results parameter.
const (
	searchTextSQL   = "SELECT " + searchResultColumns + " FROM timeline.search_text($1, $2::integer, $3::text[], $4::timestamptz, $5::text[])"
	searchExactSQL  = "SELECT " + searchResultColumns + " FROM timeline.search_text_exact($1, $2::integer, $3::text[], $4::timestamptz, $5::text[])"
	searchHybridSQL = "SELECT " + searchResultColumns + " FROM timeline.search_hybrid($1, $2, $3, $4::integer, $5::text[], $6::timestamptz, $7, $8::text[])"

	// searchHybridProbeSQL reports whether timeline.search_hybrid exists with
	// the exact signature the hybrid path calls. Deployments whose Postgres
	// image lacks pgvector never install it, and the probe is what keeps the
	// tool answering (via keyword fallback) instead of erroring there.
	searchHybridProbeSQL = "SELECT to_regprocedure('timeline.search_hybrid(text,text,text,integer,text[],timestamptz,text,text[])') IS NOT NULL AS installed"
)

// Fallback reasons the response carries when hybrid mode ran the keyword path
// instead. They name the fix, not just the condition.
const (
	searchFallbackEmbeddingsUnconfigured = "embeddings unconfigured: set SEARCH_EMBEDDINGS_API_KEY or SEARCH_EMBEDDINGS_BASE_URL"
	searchFallbackHybridNotInstalled     = "search_hybrid not installed: postgres image lacks pgvector"
)

// SearchPriorities are the timeline attention tiers a search may be scoped to,
// in enum declaration order (highest attention first). They mirror
// timeline.timeline_priority exactly; the SQL side validates too, but doing it
// here means a mistyped tier costs no round trip and the error can name the
// whole set.
var SearchPriorities = []string{"self", "direct", "cc", "noise", "background", "unclassified"}

// validateSearchPriorities returns an error naming the valid set on the first
// unknown token. Silently dropping it would be the worst outcome: the caller
// asked for one tier and would get the entire 48M-row corpus back, with no
// signal that the filter it asked for never applied.
func validateSearchPriorities(priorities []string) error {
	for _, priority := range priorities {
		if slices.Contains(SearchPriorities, priority) {
			continue
		}
		return fmt.Errorf("unknown priority %q; valid priorities are %s",
			priority, strings.Join(SearchPriorities, ", "))
	}
	return nil
}

// SearchRequest is the search tool's input.
type SearchRequest struct {
	Query      string
	MaxResults int
	Sources    []string
	Since      string
	Mode       string
	// Priorities scopes the search to timeline attention tiers. Empty means
	// every tier, which is what omitting it has always meant.
	Priorities []string
}

// SearchResponse mirrors the query tool's result shape: JSON row maps with the
// same per-field truncation, plus search metadata. Mode is the mode that
// actually executed; FallbackReason is set when hybrid was requested but the
// keyword path ran instead; Hint carries retrieval advice for the NEXT call.
type SearchResponse struct {
	Query          string            `json:"query"`
	Mode           string            `json:"mode"`
	FallbackReason string            `json:"fallback_reason,omitempty"`
	Hint           string            `json:"hint,omitempty"`
	Guidance       string            `json:"guidance,omitempty"`
	TotalRows      int               `json:"total_rows"`
	ColumnNames    []string          `json:"column_names,omitempty"`
	Rows           any               `json:"rows,omitempty"`
	Truncations    []FieldTruncation `json:"truncations,omitempty"`
	Error          string            `json:"error,omitempty"`
}

// Search runs one retrieval call against the timeline corpus. Hybrid mode
// needs both an embeddings client and the timeline.search_hybrid SQL function;
// missing either degrades to keyword search with a FallbackReason rather than
// failing, because a working keyword answer beats an error about
// infrastructure the caller cannot fix mid-question.
func (s *Service) Search(ctx context.Context, req SearchRequest) SearchResponse {
	resp := SearchResponse{Query: strings.TrimSpace(req.Query)}
	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = SearchModeHybrid
	}
	resp.Mode = mode
	if resp.Query == "" {
		resp.Error = "query must be a non-empty search string"
		return resp
	}
	switch mode {
	case SearchModeHybrid, SearchModeKeyword, SearchModeExact:
	default:
		resp.Error = fmt.Sprintf("mode must be %q, %q, or %q; got %q", SearchModeHybrid, SearchModeKeyword, SearchModeExact, mode)
		return resp
	}
	if err := validateSearchPriorities(req.Priorities); err != nil {
		resp.Error = err.Error()
		return resp
	}
	runner, ok := s.runner.(ArgsRunner)
	if !ok {
		resp.Error = "search requires a parameterized-query runner"
		return resp
	}
	maxResults := req.MaxResults
	if maxResults <= 0 {
		maxResults = searchDefaultMaxResults
	}
	// Phrasing advice is about the query, not about which retriever ran, so it
	// is set before the mode is decided and survives the keyword fallback.
	resp.Hint = searchHintFor(resp.Query)

	var sources any
	if len(req.Sources) > 0 {
		sources = req.Sources
	}
	var since any
	if trimmed := strings.TrimSpace(req.Since); trimmed != "" {
		since = trimmed
	}
	var priorities any
	if len(req.Priorities) > 0 {
		priorities = req.Priorities
	}

	statement := ""
	var args []any
	switch mode {
	case SearchModeExact:
		statement = searchExactSQL
		args = []any{resp.Query, maxResults, sources, since, priorities}
	case SearchModeKeyword:
		statement = searchTextSQL
		args = []any{resp.Query, maxResults, sources, since, priorities}
	case SearchModeHybrid:
		vectors, reason := s.hybridQueryVectors(ctx, resp.Query)
		if reason != "" {
			resp.Mode = SearchModeKeyword
			resp.FallbackReason = reason
			statement = searchTextSQL
			args = []any{resp.Query, maxResults, sources, since, priorities}
			break
		}
		// The second vector is optional: search_hybrid scans one ANN leg per
		// vector and skips the second with a one-time filter when it is NULL.
		var alternate any
		if len(vectors) > 1 {
			alternate = vectors[1]
		}
		statement = searchHybridSQL
		args = []any{resp.Query, vectors[0], s.embedder.Model(), maxResults, sources, since, alternate, priorities}
	}

	started := time.Now()
	s.logger.InfoContext(ctx, "search started", "query", resp.Query, "mode", resp.Mode, "fallback_reason", resp.FallbackReason, "max_results", maxResults, "sources", req.Sources, "since", req.Since, "priorities", req.Priorities)
	raw, err := runner.QueryArgs(ctx, statement, args, maxResults)
	if err != nil {
		resp.Error = s.queryErrorMessage(ctx, err.Error(), statement)
		s.logger.ErrorContext(ctx, "search failed", "query", resp.Query, "mode", resp.Mode, "sql", statement, "error", err, "duration", time.Since(started))
		return resp
	}
	resp.ColumnNames = append([]string(nil), raw.Columns...)
	resp.TotalRows = len(raw.Rows)
	if resp.TotalRows == 0 {
		resp.Guidance = searchEmptyGuidance
	} else {
		resp.Guidance = searchHitGuidance
	}
	resp.Rows, resp.Truncations, err = s.formatRows(raw.Columns, raw.Rows, 0, len(raw.Rows), "json")
	if err != nil {
		resp.Error = err.Error()
		s.logger.ErrorContext(ctx, "search encoding failed", "query", resp.Query, "mode", resp.Mode, "error", err, "duration", time.Since(started))
		return resp
	}
	s.logger.InfoContext(ctx, "search completed", "query", resp.Query, "mode", resp.Mode, "fallback_reason", resp.FallbackReason, "rows", resp.TotalRows, "duration", time.Since(started))
	return resp
}

// hybridQueryVectors embeds the query for the hybrid path, returning one
// literal per query representation (instructed and raw, when the deployment
// configures an instruction prefix). A non-empty reason means hybrid is
// unavailable and the caller should run keyword search, reporting the reason
// so a misconfigured deployment is visible instead of silently degraded.
func (s *Service) hybridQueryVectors(ctx context.Context, queryText string) ([]string, string) {
	if s.embedder == nil {
		return nil, searchFallbackEmbeddingsUnconfigured
	}
	// The catalog probe is a Postgres round trip and the embedding call is an
	// HTTP round trip to the GPU box; neither input depends on the other, so
	// running them back to back spent the probe's latency for nothing on every
	// single hybrid search. Overlap them.
	var (
		installed bool
		probeErr  error
		vectors   [][]float64
		embedErr  error
	)
	// Deliberately a plain group, not WithContext: neither leg should cancel
	// the other. A failed probe still wants the embedding's error text for the
	// log, and cancelling the probe on a slow embedder would turn a degraded
	// GPU box into a misleading "probe failed" fallback reason.
	var group errgroup.Group
	group.Go(func() error {
		installed, probeErr = s.searchHybridInstalled(ctx)
		return nil
	})
	group.Go(func() error {
		vectors, embedErr = s.embedder.Embed(ctx, queryText)
		return nil
	})
	_ = group.Wait()
	// The probe's verdict is reported first: "postgres image lacks pgvector" is
	// the actionable one, and on such a host the speculative embedding is
	// simply discarded.
	if probeErr != nil {
		return nil, "search_hybrid probe failed: " + probeErr.Error()
	}
	if !installed {
		return nil, searchFallbackHybridNotInstalled
	}
	if embedErr != nil {
		return nil, "embedding request failed: " + embedErr.Error()
	}
	if len(vectors) == 0 {
		return nil, "embedding request returned no vectors"
	}
	literals := make([]string, 0, len(vectors))
	for _, vector := range vectors {
		literals = append(literals, VectorLiteral(vector))
	}
	return literals, ""
}

// searchHybridInstalled probes for timeline.search_hybrid. Probed per request
// rather than cached: it is one O(1) catalog lookup, and caching a negative
// would hide the function for a whole process lifetime after it gets installed.
func (s *Service) searchHybridInstalled(ctx context.Context) (bool, error) {
	result, err := s.runner.Query(ctx, searchHybridProbeSQL, 1)
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return false, nil
	}
	installed, ok := result.Rows[0]["installed"].(bool)
	return ok && installed, nil
}
