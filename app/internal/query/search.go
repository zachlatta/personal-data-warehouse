package query

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"golang.org/x/sync/errgroup"
)

// Search modes. Hybrid is the default: semantic+keyword retrieval through the
// timeline.search_hybrid_* helpers, falling back to the keyword path whenever
// embeddings or those SQL functions are unavailable so the tool always answers.
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
	searchHitGuidance = "For an email, chat/channel, or agent-turn hit, read the conversation " +
		"around it with timeline.context(ref, 5, 5) — a Gmail hit returns its thread, a Slack hit " +
		"its thread or channel, a message its chat. If the hit is still insufficient, use source_table and " +
		"source_pk for a one-hop drill-down to the authoritative row; raise max_results explicitly " +
		"only when you need more recall."
	searchEmptyGuidance = "No hits is not proof of absence. Retry once with FEWER, more distinctive " +
		"words -- a name, an id, a product, an amount, a subject-line phrase -- rather than more of " +
		"them; use mode exact for an identifier or literal phrase, widen sources/since if scoped, " +
		"or raise max_results for more recall. Do not fall back to ILIKE over raw body columns."
)

// searchPhrasingHint is advice for the caller's NEXT search, not an error. The
// caller here is itself a language model, which is why query rewriting lives in
// guidance rather than in another model inside the search path: it costs
// nothing and needs no new dependency.
//
// It is worth saying. On the labeled benchmark (re-measured 2026-08-27, 68
// cases, hybrid), sentence-shaped queries score MRR 0.29 where term bags score
// 0.42 and bare identifiers 0.68, and rewording the nine questions that
// returned nothing useful -- as the words their ANSWERING RECORD would contain
// rather than the words of the question -- recovered five of them, from nothing
// in the top 50 to ranks 10, 10, 12, 15 and 48.
const searchPhrasingHint = "This query reads like a sentence. Retrieval here is measurably better " +
	"when a query is the FEW most distinctive words the ANSWERING RECORD would contain rather than " +
	"the words of the question: \"how long our money lasts\" finds nothing, \"runway burn rate months " +
	"of cash remaining\" finds it. If the results below miss, re-issue with a short anchor in the " +
	"record's own vocabulary (a name, an id, a product, an amount, an email's subject line, the " +
	"phrase a person would actually have typed)."

// searchLongBagHint fires on a long query that carries no anchor at all -- no
// capitalized name, no number, no identifier punctuation. Measured on the
// labeled set 2026-08-27, adding generic words to a distinctive anchor HURTS:
// "Mt Foolery" ranks #1 and "Woody Mt Foolery cancelled postponed weather" is
// not in the top 50; "Sunbeam Marrakesh" #5 against "customs duty charged to
// receive package shirt Sunbeam" #41. Each generic term dilutes both the BM25
// score and the embedding neighbourhood, and rank fusion then averages the
// anchor away. The rule is deliberately crude (word count and a lexical anchor
// test) for the same reason the sentence test is: the alternative is a model
// call to classify a string.
const searchLongBagHint = "This is a long query with no distinctive anchor (no name, id, number or " +
	"identifier). Measured on the labeled benchmark, adding generic words to a distinctive term " +
	"makes retrieval WORSE, not better: \"Mt Foolery\" ranks first while \"Woody Mt Foolery cancelled " +
	"postponed weather\" is not in the top 50. If the results below miss, re-issue with the two to " +
	"four most distinctive words the record itself would contain, and prefer several short " +
	"searches over one long one."

// searchLongBagMinWords is the length at which an unanchored query starts to
// read as a generic bag: "vision insurance VSP EyeMed glasses member ID" is
// seven words and anchored, "startup founder venture capital book
// recommendations reading list" is eight and not.
const searchLongBagMinWords = 7

// searchAttentionHint fires when an UNSCOPED search came back mostly noise and
// background. `noise` alone is ~82% of the corpus, so leaving every tier in is
// the usual reason a search returns newsletters and orchestrator turns; the
// audit of 14 days of agent sessions found the priorities filter on 6% of
// search calls. Measured on this response, not asserted in general: the hint
// only appears when more than half of what came back is in those two tiers.
const searchAttentionHint = "Most of these hits are noise/background (bulk mail, bots, the warehouse's " +
	"own machinery). If the question is about attention or people rather than the whole corpus, " +
	"re-issue with priorities [\"self\",\"direct\",\"cc\"] to search only what a person sent or received."

// searchAttentionHintFor returns the attention hint when it applies to this
// result set, or "". A scoped call already chose its tiers; a tiny result set
// says nothing about the mix.
func searchAttentionHintFor(requested []string, rows []map[string]any) string {
	if len(requested) > 0 || len(rows) < searchAttentionHintMinRows {
		return ""
	}
	bulk := 0
	for _, row := range rows {
		switch fmt.Sprint(row["priority"]) {
		case "noise", "background":
			bulk++
		}
	}
	if bulk*2 <= len(rows) {
		return ""
	}
	return searchAttentionHint
}

const searchAttentionHintMinRows = 5

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

func searchQueryIsSentence(query string) bool {
	fields := strings.Fields(strings.ToLower(query))
	if len(fields) < 5 {
		return false
	}
	sentenceWords := 0
	for _, field := range fields {
		if searchSentenceWords[strings.Trim(field, ".,!?;:'\"")] {
			sentenceWords++
		}
	}
	return sentenceWords >= 2
}

// searchTermBag removes only the function words used by the sentence detector,
// preserving identifiers, amounts, punctuation, names, and the caller's own
// vocabulary. It is deliberately not a synonym rewriter: the resulting extra
// embeddings are deterministic and auditable, and never invent a term that was
// absent from the query.
func searchTermBag(query string) string {
	fields := strings.Fields(query)
	kept := make([]string, 0, len(fields))
	for _, field := range fields {
		token := strings.Trim(strings.ToLower(field), ".,!?;:'\"")
		if searchSentenceWords[token] {
			continue
		}
		kept = append(kept, field)
	}
	return strings.Join(kept, " ")
}

// searchHintFor returns advice for the caller, or "" when the query is already
// in the shape that retrieves well. Hinting every response is noise nobody
// reads, so the strong case gets nothing.
func searchHintFor(query string) string {
	if searchQueryIsSentence(query) {
		return searchPhrasingHint
	}
	if searchQueryIsLongUnanchoredBag(query) {
		return searchLongBagHint
	}
	return ""
}

// searchQueryIsLongUnanchoredBag reports a query of searchLongBagMinWords or
// more words in which no word is an anchor. An anchor is a capitalized word
// past the first (a name), a word carrying a digit (an amount, a code, a
// date), or one carrying identifier punctuation (a path, a handle, a ticket).
func searchQueryIsLongUnanchoredBag(query string) bool {
	fields := strings.Fields(query)
	if len(fields) < searchLongBagMinWords {
		return false
	}
	for i, field := range fields {
		if searchWordIsAnchor(field, i == 0) {
			return false
		}
	}
	return true
}

func searchWordIsAnchor(word string, first bool) bool {
	trimmed := strings.Trim(word, ".,!?;:'\"")
	if trimmed == "" {
		return false
	}
	for _, r := range trimmed {
		if unicode.IsDigit(r) || strings.ContainsRune("#$@/_-", r) {
			return true
		}
	}
	if first {
		// A sentence-initial capital says nothing; only the acronym shape
		// ("DDP", "VSP") counts at position zero.
		return len(trimmed) > 1 && strings.ToUpper(trimmed) == trimmed
	}
	return unicode.IsUpper([]rune(trimmed)[0])
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
	searchTextSQL  = "SELECT " + searchResultColumns + " FROM timeline.search_text($1, $2::integer, $3::text[], $4::timestamptz, $5::text[])"
	searchExactSQL = "SELECT " + searchResultColumns + " FROM timeline.search_text_exact($1, $2::integer, $3::text[], $4::timestamptz, $5::text[])"
	// timeline.search_hybrid itself has no Go-side statement: the app builds
	// hybrid from the parallel legs below, and the SQL function remains the
	// direct-SQL entry point for callers writing SQL by hand. A constant the
	// app never executes is a claim nothing checks, which is why it is gone.

	// Hybrid's expensive retrieval legs return compact evidence independently.
	// Search fans these statements out over the connection pool and gives their
	// refs/ranks to search_hybrid_fuse, keeping all ranking constants in SQL.
	searchHybridLexicalSQL  = "SELECT ref FROM timeline.search_text($1, $2::integer, $3::text[], $4::timestamptz, $5::text[])"
	searchHybridSemanticSQL = "SELECT ref, best, fuse, chunk_id FROM timeline.search_hybrid_semantic($1, $2, $3::integer, $4::text[], $5::timestamptz, $6::integer)"
	searchHybridExactSQL    = "SELECT ref FROM timeline.search_hybrid_exact($1, $2::integer, $3::text[], $4::timestamptz, $5::text[])"
	searchHybridFuseSQL     = "SELECT " + searchResultColumns + " FROM timeline.search_hybrid_fuse($1, $2::integer, $3::text[], $4::jsonb, $5::text[], $6::text[])"

	// searchHybridProbeSQL reports whether timeline.search_hybrid exists with
	// the exact signature the hybrid path calls. Deployments whose Postgres
	// image lacks pgvector never install it, and the probe is what keeps the
	// tool answering (via keyword fallback) instead of erroring there.
	searchHybridProbeSQL = "SELECT " +
		"to_regprocedure('timeline.search_hybrid(text,text,text,integer,text[],timestamptz,text,text[])') IS NOT NULL " +
		"AND to_regprocedure('timeline.search_hybrid_semantic(text,text,integer,text[],timestamptz,integer)') IS NOT NULL " +
		"AND to_regprocedure('timeline.search_hybrid_exact(text,integer,text[],timestamptz,text[])') IS NOT NULL " +
		"AND to_regprocedure('timeline.search_hybrid_fuse(text,integer,text[],jsonb,text[],text[])') IS NOT NULL AS installed"
)

// Content-word variants only need the top of their ANN neighborhoods to
// deliver the measured quality gain. Repeating the original vectors' 1,000-row
// floor made warm searches slower despite the parallel fan-out. Keep at least
// two candidates per requested result for unusually deep calls, with a floor
// validated against the expanded live-agent benchmark.
const (
	searchHybridTermBagMinCandidates = 200
	searchHybridTermBagMultiplier    = 2
)

// Fallback reasons the response carries when hybrid mode ran the keyword path
// instead. They name the fix, not just the condition.
const (
	searchFallbackEmbeddingsUnconfigured = "embeddings unconfigured: set SEARCH_EMBEDDINGS_API_KEY or SEARCH_EMBEDDINGS_BASE_URL"
	searchFallbackHybridNotInstalled     = "search_hybrid not installed: postgres image lacks pgvector"
)

// SearchPriorities is every label a search may be scoped to: the five real
// tiers plus the unclassified sentinel, in enum declaration order. They mirror
// timeline.timeline_priority exactly; the SQL side validates too, but doing it
// here means a mistyped tier costs no round trip and the error can name the
// tiers that exist. Use SearchPriorityTiers when the question is "what are the
// tiers" rather than "is this accepted".
var SearchPriorities = []string{"self", "direct", "cc", "noise", "background", "unclassified"}

// SearchPriorityTiers are the five real attention tiers. 'unclassified' is
// deliberately not one of them: it is the fail-loud sentinel for a row an
// adapter has not classified, and it must never appear in steady state.
// Scoping a search to it is how a classification outage is FOUND, so it stays
// accepted -- but an error that lists it beside the five, as this one used to,
// teaches the caller it is a sixth tier.
var SearchPriorityTiers = []string{"self", "direct", "cc", "noise", "background"}

// validateSearchPriorities returns an error naming the valid set on the first
// unknown token. Silently dropping it would be the worst outcome: the caller
// asked for one tier and would get the entire 48M-row corpus back, with no
// signal that the filter it asked for never applied.
func validateSearchPriorities(priorities []string) error {
	for _, priority := range priorities {
		if slices.Contains(SearchPriorities, priority) {
			continue
		}
		return fmt.Errorf("unknown priority %q; the attention tiers are %s (most attention first). "+
			"'unclassified' is also accepted, but it is a fail-loud sentinel for rows an adapter has not classified, not a sixth tier",
			priority, strings.Join(SearchPriorityTiers, ", "))
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
	var hybridVectors []string
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
		hybridVectors = vectors
		statement = searchHybridFuseSQL
	}

	started := time.Now()
	s.logger.InfoContext(ctx, "search started", "query", resp.Query, "mode", resp.Mode, "fallback_reason", resp.FallbackReason, "max_results", maxResults, "sources", req.Sources, "since", req.Since, "priorities", req.Priorities)
	var raw RawResult
	var err error
	if len(hybridVectors) > 0 {
		raw, err = s.runHybridSearch(
			ctx, runner, resp.Query, maxResults, sources, since, priorities,
			hybridVectors, s.embedder.Model(),
		)
	} else {
		raw, err = runner.QueryArgs(ctx, statement, args, maxResults)
	}
	if err != nil {
		resp.Error = s.queryErrorMessage(ctx, err.Error(), statement)
		s.logger.ErrorContext(ctx, "search failed", "query", resp.Query, "mode", resp.Mode, "sql", statement, "error", err, "duration", time.Since(started))
		return resp
	}
	resp.ColumnNames = append([]string(nil), raw.Columns...)
	resp.TotalRows = len(raw.Rows)
	if attention := searchAttentionHintFor(req.Priorities, raw.Rows); attention != "" {
		resp.Hint = strings.TrimSpace(resp.Hint + " " + attention)
	}
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

// runHybridSearch fans BM25, one ANN call per query representation, and the
// short-literal leg across independent read-only transactions. PostgreSQL does
// not parallelize HNSW scans inside the old monolithic function; at production
// scale that left one core saturated while a 28-vCPU host sat 80-92% idle.
// Separate pooled connections make wall time approximately max(legs)+fusion
// instead of sum(legs), while search_hybrid_fuse remains the single authority
// for RRF weights and result shaping.
func (s *Service) runHybridSearch(
	ctx context.Context,
	runner ArgsRunner,
	query string,
	maxResults int,
	sources any,
	since any,
	priorities any,
	vectors []string,
	embeddingModel string,
) (RawResult, error) {
	var (
		lexical  RawResult
		exact    RawResult
		semantic = make([]RawResult, len(vectors))
	)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		lexical, err = runner.QueryArgs(
			groupCtx, searchHybridLexicalSQL,
			[]any{query, maxResults, sources, since, priorities}, maxResults,
		)
		if err != nil {
			return fmt.Errorf("hybrid lexical leg: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		var err error
		exact, err = runner.QueryArgs(
			groupCtx, searchHybridExactSQL,
			[]any{query, maxResults, sources, since, priorities}, maxResults,
		)
		if err != nil {
			return fmt.Errorf("hybrid literal leg: %w", err)
		}
		return nil
	})
	for index, vector := range vectors {
		index, vector := index, vector
		group.Go(func() error {
			var err error
			var candidateLimit any
			if index >= 2 {
				limit := maxResults * searchHybridTermBagMultiplier
				if limit < searchHybridTermBagMinCandidates {
					limit = searchHybridTermBagMinCandidates
				}
				candidateLimit = limit
			}
			// The SQL function owns a measured 40-200 or 1000-2000 row
			// bound for the original forms. Extra deterministic term-bag
			// forms use the smaller measured override above. maxRows=0 lets
			// SQL return every event inside whichever candidate bound applies.
			semantic[index], err = runner.QueryArgs(
				groupCtx, searchHybridSemanticSQL,
				[]any{vector, embeddingModel, maxResults, sources, since, candidateLimit}, 0,
			)
			if err != nil {
				return fmt.Errorf("hybrid semantic leg %d: %w", index+1, err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return RawResult{}, err
	}

	semanticRows := make([]map[string]any, 0)
	for _, leg := range semantic {
		semanticRows = append(semanticRows, leg.Rows...)
	}
	semanticJSON, err := json.Marshal(semanticRows)
	if err != nil {
		return RawResult{}, fmt.Errorf("encode hybrid semantic evidence: %w", err)
	}
	return runner.QueryArgs(
		ctx, searchHybridFuseSQL,
		[]any{
			query, maxResults, searchRefs(lexical), string(semanticJSON),
			searchRefs(exact), priorities,
		},
		maxResults,
	)
}

func searchRefs(result RawResult) []string {
	refs := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if ref, ok := row["ref"].(string); ok && ref != "" {
			refs = append(refs, ref)
		}
	}
	return refs
}

// hybridQueryVectors embeds the query for the hybrid path, returning one
// literal per query representation: instructed and raw, plus instructed and
// raw content-word forms when the query carries removable sentence grammar.
// A non-empty reason means hybrid is unavailable and the caller should run
// keyword search, reporting the reason so a misconfigured deployment is
// visible instead of silently degraded.
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
