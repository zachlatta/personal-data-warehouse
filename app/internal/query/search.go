package query

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Search modes. Hybrid is the default: semantic+keyword retrieval through
// timeline.search_hybrid, falling back to the keyword path whenever embeddings
// or the SQL function are unavailable so the tool always answers.
const (
	SearchModeHybrid  = "hybrid"
	SearchModeKeyword = "keyword"
	SearchModeExact   = "exact"
)

const searchDefaultMaxResults = 50

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
const searchResultColumns = "source, subsource, context, who, occurred_at, account, ref, text, score, event_ts, title, source_table, source_pk"

// The int/text[]/timestamptz casts are load-bearing: pgx binds a Go int as
// bigint and there is no implicit bigint→integer cast during function
// resolution, so an uncast $2 would fail to match the functions' integer
// max_results parameter.
const (
	searchTextSQL   = "SELECT " + searchResultColumns + " FROM timeline.search_text($1, $2::integer, $3::text[], $4::timestamptz)"
	searchExactSQL  = "SELECT " + searchResultColumns + " FROM timeline.search_text_exact($1, $2::integer, $3::text[], $4::timestamptz)"
	searchHybridSQL = "SELECT " + searchResultColumns + " FROM timeline.search_hybrid($1, $2, $3, $4::integer, $5::text[], $6::timestamptz)"

	// searchHybridProbeSQL reports whether timeline.search_hybrid exists with
	// the exact signature the hybrid path calls. Deployments whose Postgres
	// image lacks pgvector never install it, and the probe is what keeps the
	// tool answering (via keyword fallback) instead of erroring there.
	searchHybridProbeSQL = "SELECT to_regprocedure('timeline.search_hybrid(text,text,text,integer,text[],timestamptz)') IS NOT NULL AS installed"
)

// Fallback reasons the response carries when hybrid mode ran the keyword path
// instead. They name the fix, not just the condition.
const (
	searchFallbackEmbeddingsUnconfigured = "embeddings unconfigured: set SEARCH_EMBEDDINGS_API_KEY or SEARCH_EMBEDDINGS_BASE_URL"
	searchFallbackHybridNotInstalled     = "search_hybrid not installed: postgres image lacks pgvector"
)

// SearchRequest is the search tool's input.
type SearchRequest struct {
	Query      string
	MaxResults int
	Sources    []string
	Since      string
	Mode       string
}

// SearchResponse mirrors the query tool's result shape: JSON row maps with the
// same per-field truncation, plus search metadata. Mode is the mode that
// actually executed; FallbackReason is set when hybrid was requested but the
// keyword path ran instead.
type SearchResponse struct {
	Query          string            `json:"query"`
	Mode           string            `json:"mode"`
	FallbackReason string            `json:"fallback_reason,omitempty"`
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
	runner, ok := s.runner.(ArgsRunner)
	if !ok {
		resp.Error = "search requires a parameterized-query runner"
		return resp
	}
	maxResults := req.MaxResults
	if maxResults <= 0 {
		maxResults = searchDefaultMaxResults
	}

	var sources any
	if len(req.Sources) > 0 {
		sources = req.Sources
	}
	var since any
	if trimmed := strings.TrimSpace(req.Since); trimmed != "" {
		since = trimmed
	}

	statement := ""
	var args []any
	switch mode {
	case SearchModeExact:
		statement = searchExactSQL
		args = []any{resp.Query, maxResults, sources, since}
	case SearchModeKeyword:
		statement = searchTextSQL
		args = []any{resp.Query, maxResults, sources, since}
	case SearchModeHybrid:
		vector, reason := s.hybridQueryVector(ctx, resp.Query)
		if reason != "" {
			resp.Mode = SearchModeKeyword
			resp.FallbackReason = reason
			statement = searchTextSQL
			args = []any{resp.Query, maxResults, sources, since}
			break
		}
		statement = searchHybridSQL
		args = []any{resp.Query, vector, s.embedder.Model(), maxResults, sources, since}
	}

	started := time.Now()
	s.logger.InfoContext(ctx, "search started", "query", resp.Query, "mode", resp.Mode, "fallback_reason", resp.FallbackReason, "max_results", maxResults, "sources", req.Sources, "since", req.Since)
	raw, err := runner.QueryArgs(ctx, statement, args, maxResults)
	if err != nil {
		resp.Error = s.queryErrorMessage(ctx, err.Error(), statement)
		s.logger.ErrorContext(ctx, "search failed", "query", resp.Query, "mode", resp.Mode, "sql", statement, "error", err, "duration", time.Since(started))
		return resp
	}
	resp.ColumnNames = append([]string(nil), raw.Columns...)
	resp.TotalRows = len(raw.Rows)
	resp.Rows, resp.Truncations, err = s.formatRows(raw.Columns, raw.Rows, 0, len(raw.Rows), "json")
	if err != nil {
		resp.Error = err.Error()
		s.logger.ErrorContext(ctx, "search encoding failed", "query", resp.Query, "mode", resp.Mode, "error", err, "duration", time.Since(started))
		return resp
	}
	s.logger.InfoContext(ctx, "search completed", "query", resp.Query, "mode", resp.Mode, "fallback_reason", resp.FallbackReason, "rows", resp.TotalRows, "duration", time.Since(started))
	return resp
}

// hybridQueryVector embeds the query for the hybrid path. A non-empty reason
// means hybrid is unavailable and the caller should run keyword search,
// reporting the reason so a misconfigured deployment is visible instead of
// silently degraded.
func (s *Service) hybridQueryVector(ctx context.Context, queryText string) (string, string) {
	if s.embedder == nil {
		return "", searchFallbackEmbeddingsUnconfigured
	}
	installed, err := s.searchHybridInstalled(ctx)
	if err != nil {
		return "", "search_hybrid probe failed: " + err.Error()
	}
	if !installed {
		return "", searchFallbackHybridNotInstalled
	}
	vector, err := s.embedder.Embed(ctx, queryText)
	if err != nil {
		return "", "embedding request failed: " + err.Error()
	}
	return VectorLiteral(vector), ""
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
