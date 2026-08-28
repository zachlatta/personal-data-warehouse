package query

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

type Options struct {
	MaxRows            int
	MaxFieldChars      int
	QueryCacheMaxBytes int64
	GetFieldMaxChars   int
	QueryCacheTTL      time.Duration
	DebugCacheTool     bool
	Logger             *slog.Logger
	// SearchEmbedder powers the search tool's hybrid mode. Nil means
	// embeddings are unconfigured and hybrid searches fall back to keyword.
	SearchEmbedder Embedder
}

type Runner interface {
	Query(ctx context.Context, sql string, maxRows int) (RawResult, error)
}

type RawResult struct {
	Columns []string
	Rows    []map[string]any
}

type Statement struct {
	Question string
	SQL      string
}

type statementValidationError struct {
	Message      string
	Index        int
	Question     string
	SQL          string
	HasStatement bool
}

func (e statementValidationError) logFields() []any {
	fields := []any{"error", e.Message}
	if e.HasStatement {
		fields = append(fields, "index", e.Index, "question", e.Question, "sql", e.SQL)
	}
	return fields
}

type Service struct {
	runner   Runner
	opts     Options
	logger   *slog.Logger
	cache    *queryCache
	embedder Embedder
}

type tableRef struct {
	Schema string
	Name   string
}

func (t tableRef) SQLName() string {
	return warehouse.QuoteIdent(t.Schema) + "." + warehouse.QuoteIdent(t.Name)
}

func (t tableRef) DisplayName() string {
	return t.Schema + "." + t.Name
}

func queryableSchemaArraySQL() string {
	schemas := warehouse.QueryableSchemas()
	quoted := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		quoted = append(quoted, warehouse.SQLString(schema))
	}
	return "ARRAY[" + strings.Join(quoted, ",") + "]"
}

type Response struct {
	Results []Result `json:"results"`
}

type QueryResponse struct {
	Results []QueryResult `json:"results"`
}

type QueryResult struct {
	SQL         string            `json:"sql"`
	QueryID     string            `json:"query_id,omitempty"`
	TotalRows   int               `json:"total_rows,omitempty"`
	ColumnNames []string          `json:"column_names,omitempty"`
	Format      string            `json:"format,omitempty"`
	Preview     any               `json:"preview,omitempty"`
	Truncations []FieldTruncation `json:"truncations,omitempty"`
	Error       string            `json:"error,omitempty"`
	// Hint is advice about the statement's SHAPE, attached before it runs and
	// regardless of whether it succeeds: the query still executes.
	Hint string `json:"hint,omitempty"`
}

type RowsResponse struct {
	QueryID     string            `json:"query_id"`
	Offset      int               `json:"offset"`
	Limit       int               `json:"limit"`
	TotalRows   int               `json:"total_rows"`
	ColumnNames []string          `json:"column_names"`
	Format      string            `json:"format"`
	Rows        any               `json:"rows"`
	Truncations []FieldTruncation `json:"truncations,omitempty"`
	Error       string            `json:"error,omitempty"`
}

type FieldResponse struct {
	QueryID       string `json:"query_id"`
	Row           int    `json:"row"`
	Column        string `json:"column"`
	Offset        int    `json:"offset"`
	TotalChars    int    `json:"total_chars"`
	ReturnedChars int    `json:"returned_chars"`
	Value         string `json:"value"`
	EOF           bool   `json:"eof,omitempty"`
	Error         string `json:"error,omitempty"`
}

type GrepResponse struct {
	QueryID string      `json:"query_id"`
	Pattern string      `json:"pattern"`
	Matches []GrepMatch `json:"matches"`
	Error   string      `json:"error,omitempty"`
}

type GrepMatch struct {
	RowIndex   int    `json:"row_index"`
	Column     string `json:"column"`
	MatchStart int    `json:"match_start"`
	MatchEnd   int    `json:"match_end"`
	Context    string `json:"context"`
}

// FullQueryRowCap is the safety ceiling ExecuteFull applies to a single
// statement. It exists so a stray `SELECT * FROM huge_table` does not stream
// hundreds of millions of rows into a terminal; callers needing more should
// page with LIMIT/OFFSET.
const FullQueryRowCap = 1_000_000

// FullQueryResponse is the response for the CLI sql tool. Rows holds the
// formatted body (CSV string, JSON array, or NDJSON string depending on
// Format). No caching, no per-field truncation — that's the whole point of
// this surface vs. the MCP query tool.
type FullQueryResponse struct {
	Question    string   `json:"question"`
	SQL         string   `json:"sql"`
	Format      string   `json:"format"`
	ColumnNames []string `json:"column_names,omitempty"`
	TotalRows   int      `json:"total_rows"`
	Truncated   bool     `json:"truncated,omitempty"`
	Rows        any      `json:"rows,omitempty"`
	Error       string   `json:"error,omitempty"`
	Hint        string   `json:"hint,omitempty"`
}

type DebugCacheStatus struct {
	TotalBytes int64             `json:"total_bytes"`
	MaxBytes   int64             `json:"max_bytes"`
	TTLSeconds int64             `json:"ttl_seconds"`
	Queries    []DebugCacheEntry `json:"queries"`
}

type DebugCacheEntry struct {
	QueryID    string `json:"query_id"`
	SQL        string `json:"sql"`
	Rows       int    `json:"rows"`
	Bytes      int64  `json:"bytes"`
	AgeSeconds int64  `json:"age_seconds"`
}

type Result struct {
	SQL       string     `json:"sql"`
	CSV       string     `json:"csv,omitempty"`
	Error     string     `json:"error,omitempty"`
	Truncated Truncation `json:"truncated,omitempty"`
}

type Truncation struct {
	Rows          bool              `json:"rows,omitempty"`
	MaxRows       int               `json:"max_rows,omitempty"`
	MaxFieldChars int               `json:"max_field_chars,omitempty"`
	Fields        []FieldTruncation `json:"fields,omitempty"`
}

type FieldTruncation struct {
	Row      int    `json:"row"`
	Column   string `json:"column"`
	Returned int    `json:"returned"`
	Total    int    `json:"total"`
}

func (t Truncation) Empty() bool {
	return !t.Rows && len(t.Fields) == 0
}

func (t Truncation) CSV() string {
	rows := make([]map[string]any, 0, 1+len(t.Fields))
	if t.Rows {
		rows = append(rows, map[string]any{
			"type":            "rows",
			"max_rows":        t.MaxRows,
			"max_field_chars": t.MaxFieldChars,
		})
	}
	for _, field := range t.Fields {
		rows = append(rows, map[string]any{
			"type":            "field",
			"row":             field.Row,
			"column":          field.Column,
			"returned":        field.Returned,
			"total":           field.Total,
			"max_rows":        t.MaxRows,
			"max_field_chars": t.MaxFieldChars,
		})
	}
	out, err := rowsToCSV([]string{"type", "row", "column", "returned", "total", "max_rows", "max_field_chars"}, rows)
	if err != nil {
		return ""
	}
	return out
}

func NewService(runner Runner, opts Options) *Service {
	if opts.MaxRows <= 0 {
		opts.MaxRows = 100000
	}
	if opts.MaxFieldChars <= 0 {
		opts.MaxFieldChars = 4000
	}
	if opts.QueryCacheMaxBytes <= 0 {
		opts.QueryCacheMaxBytes = 256 * 1024 * 1024
	}
	if opts.GetFieldMaxChars <= 0 {
		opts.GetFieldMaxChars = 200000
	}
	if opts.QueryCacheTTL <= 0 {
		opts.QueryCacheTTL = 30 * time.Minute
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{runner: runner, opts: opts, logger: logger.With("component", "query"), cache: newQueryCache(opts.QueryCacheMaxBytes, opts.QueryCacheTTL), embedder: opts.SearchEmbedder}
}

func normalizeStatements(statements []Statement) ([]Statement, statementValidationError) {
	if len(statements) == 0 {
		return nil, statementValidationError{Message: "queries must contain at least one {question, sql} statement; legacy sql array input is no longer accepted"}
	}
	normalized := make([]Statement, 0, len(statements))
	for i, statement := range statements {
		question := strings.TrimSpace(statement.Question)
		sql := strings.TrimSpace(statement.SQL)
		if question == "" {
			return nil, statementValidationError{
				Message:      fmt.Sprintf("queries[%d].question must be a concise plain-English question this SQL statement is trying to answer", i),
				Index:        i,
				Question:     question,
				SQL:          sql,
				HasStatement: true,
			}
		}
		if sql == "" {
			return nil, statementValidationError{
				Message:      fmt.Sprintf("queries[%d].sql must contain a read-only Postgres SQL statement", i),
				Index:        i,
				Question:     question,
				SQL:          sql,
				HasStatement: true,
			}
		}
		normalized = append(normalized, Statement{Question: question, SQL: sql})
	}
	return normalized, statementValidationError{}
}

func statementQuestions(statements []Statement) []string {
	questions := make([]string, 0, len(statements))
	for _, statement := range statements {
		questions = append(questions, statement.Question)
	}
	return questions
}

func (s *Service) Execute(ctx context.Context, statements []Statement, previewRows int, format string) QueryResponse {
	statements, validationErr := normalizeStatements(statements)
	if validationErr.Message != "" {
		s.logger.WarnContext(ctx, "query batch rejected", validationErr.logFields()...)
		return QueryResponse{Results: []QueryResult{{Error: validationErr.Message}}}
	}
	format = normalizeFormat(format)
	if previewRows < 0 {
		previewRows = 0
	}
	if previewRows == 0 {
		previewRows = 20
	}

	started := time.Now()
	questions := statementQuestions(statements)
	s.logger.InfoContext(ctx, "query batch started", "statements", len(statements), "questions", questions, "max_rows", s.opts.MaxRows, "max_field_chars", s.opts.MaxFieldChars, "format", format, "preview_rows", previewRows)
	results := make([]QueryResult, 0, len(statements))
	for _, statement := range statements {
		queryStarted := time.Now()
		result := QueryResult{SQL: statement.SQL, Format: format, Hint: rawTextScanHint(statement.SQL)}
		if err := ValidateReadOnlySQL(statement.SQL); err != nil {
			result.Error = err.Error()
			results = append(results, result)
			s.logger.WarnContext(ctx, "query rejected by read-only validator", "question", statement.Question, "sql", statement.SQL, "error", result.Error)
			continue
		}
		s.logger.DebugContext(ctx, "query started", "question", statement.Question, "sql", statement.SQL)
		raw, err := s.runner.Query(ctx, statement.SQL, s.opts.MaxRows+1)
		if err != nil {
			result.Error = s.queryErrorMessage(ctx, err.Error(), statement.SQL)
			results = append(results, result)
			s.logger.ErrorContext(ctx, "query failed", "question", statement.Question, "sql", statement.SQL, "error", err, "duration", time.Since(queryStarted))
			continue
		}
		if len(raw.Rows) > s.opts.MaxRows {
			result.Error = fmt.Sprintf("query returned more than MCP_MAX_ROWS (%d) rows; add a LIMIT or narrower WHERE clause and re-run the query", s.opts.MaxRows)
			results = append(results, result)
			s.logger.WarnContext(ctx, "query rejected by row cap", "question", statement.Question, "sql", statement.SQL, "rows_seen", len(raw.Rows), "max_rows", s.opts.MaxRows, "duration", time.Since(queryStarted))
			continue
		}
		entry := &queryCacheEntry{
			ID:        newQueryID(),
			Question:  statement.Question,
			SQL:       statement.SQL,
			Columns:   append([]string(nil), raw.Columns...),
			Rows:      copyRows(raw.Rows),
			Format:    format,
			CreatedAt: time.Now(),
		}
		entry.SizeBytes = estimateEntrySize(entry)
		if err := s.cache.add(entry); err != nil {
			result.Error = err.Error()
			s.logger.ErrorContext(ctx, "query result encoding failed", "question", statement.Question, "sql", statement.SQL, "error", err, "duration", time.Since(queryStarted))
		} else {
			result.QueryID = entry.ID
			result.TotalRows = len(entry.Rows)
			result.ColumnNames = entry.Columns
			result.Preview, result.Truncations, err = s.formatRows(entry.Columns, entry.Rows, 0, minInt(previewRows, len(entry.Rows)), format)
			if err != nil {
				result.Error = err.Error()
			}
			s.logger.InfoContext(ctx, "query completed", "question", statement.Question, "sql", statement.SQL, "query_id", entry.ID, "rows", len(entry.Rows), "columns", len(raw.Columns), "truncated_fields", len(result.Truncations), "duration", time.Since(queryStarted))
		}
		results = append(results, result)
	}
	s.logger.InfoContext(ctx, "query batch completed", "statements", len(statements), "questions", questions, "duration", time.Since(started))
	return QueryResponse{Results: results}
}

func (s *Service) GetRows(ctx context.Context, queryID string, offset, limit int, format string) RowsResponse {
	entry, err := s.cache.get(queryID)
	if err != nil {
		return RowsResponse{QueryID: queryID, Error: err.Error()}
	}
	s.logger.InfoContext(ctx, "get_rows using cached query", "query_id", queryID, "question", entry.Question, "offset", offset, "limit", limit)
	if offset < 0 {
		return RowsResponse{QueryID: queryID, Error: "offset must be >= 0"}
	}
	if limit <= 0 {
		limit = 50
	}
	if offset > len(entry.Rows) {
		offset = len(entry.Rows)
	}
	if format == "" {
		format = entry.Format
	}
	format = normalizeFormat(format)
	end := minInt(offset+limit, len(entry.Rows))
	rows, truncations, err := s.formatRows(entry.Columns, entry.Rows, offset, end, format)
	resp := RowsResponse{
		QueryID:     queryID,
		Offset:      offset,
		Limit:       limit,
		TotalRows:   len(entry.Rows),
		ColumnNames: append([]string(nil), entry.Columns...),
		Format:      format,
		Rows:        rows,
		Truncations: truncations,
	}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp
}

func (s *Service) GetField(ctx context.Context, queryID string, row int, column string, offset, length int) FieldResponse {
	resp := FieldResponse{QueryID: queryID, Row: row, Column: column, Offset: offset}
	entry, err := s.cache.get(queryID)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	s.logger.InfoContext(ctx, "get_field using cached query", "query_id", queryID, "question", entry.Question, "row", row, "column", column, "offset", offset, "length", length)
	if row < 0 || row >= len(entry.Rows) {
		if len(entry.Rows) == 0 {
			resp.Error = fmt.Sprintf("row %d is out of range for query_id %s; the cached result has 0 rows", row, queryID)
		} else {
			resp.Error = fmt.Sprintf("row %d is out of range for query_id %s; valid rows are 0 through %d", row, queryID, len(entry.Rows)-1)
		}
		return resp
	}
	if !containsString(entry.Columns, column) {
		resp.Error = fmt.Sprintf("unknown column %q for query_id %s; available columns: %s", column, queryID, strings.Join(entry.Columns, ", "))
		return resp
	}
	if offset < 0 {
		resp.Error = "offset must be >= 0"
		return resp
	}
	if length <= 0 {
		length = 50000
	}
	if length > s.opts.GetFieldMaxChars {
		length = s.opts.GetFieldMaxChars
	}
	value := csvValue(entry.Rows[row][column])
	runes := []rune(value)
	resp.TotalChars = len(runes)
	if offset > len(runes) {
		offset = len(runes)
		resp.Offset = offset
	}
	end := minInt(offset+length, len(runes))
	resp.Value = string(runes[offset:end])
	resp.ReturnedChars = end - offset
	resp.EOF = end >= len(runes)
	return resp
}

func (s *Service) GrepRows(ctx context.Context, queryID, pattern string, columns []string, limit, contextChars int) GrepResponse {
	resp := GrepResponse{QueryID: queryID, Pattern: pattern}
	entry, err := s.cache.get(queryID)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	s.logger.InfoContext(ctx, "grep_rows using cached query", "query_id", queryID, "question", entry.Question, "columns", len(columns), "limit", limit)
	if pattern == "" {
		resp.Error = "pattern must not be empty"
		return resp
	}
	if limit <= 0 {
		limit = 100
	}
	if contextChars < 0 {
		contextChars = 0
	} else if contextChars == 0 {
		contextChars = 200
	}
	if len(columns) == 0 {
		columns = entry.Columns
	} else {
		for _, column := range columns {
			if !containsString(entry.Columns, column) {
				resp.Error = fmt.Sprintf("unknown column %q for query_id %s; available columns: %s", column, queryID, strings.Join(entry.Columns, ", "))
				return resp
			}
		}
	}
	re, err := regexp.Compile("(?i)" + pattern)
	if err != nil {
		resp.Error = "invalid regex pattern: " + err.Error()
		return resp
	}
	for rowIndex, row := range entry.Rows {
		for _, column := range columns {
			value := csvValue(row[column])
			runeOffsets := byteToRuneOffsets(value)
			for _, loc := range re.FindAllStringIndex(value, -1) {
				start := runeOffsets[loc[0]]
				end := runeOffsets[loc[1]]
				runes := []rune(value)
				contextStart := maxInt(0, start-contextChars)
				contextEnd := minInt(len(runes), end+contextChars)
				resp.Matches = append(resp.Matches, GrepMatch{
					RowIndex:   rowIndex,
					Column:     column,
					MatchStart: start,
					MatchEnd:   end,
					Context:    string(runes[contextStart:contextEnd]),
				})
				if len(resp.Matches) >= limit {
					return resp
				}
			}
		}
	}
	return resp
}

// ExecuteFull runs a single read-only SQL statement and returns the entire
// result formatted as CSV/JSON/NDJSON. It bypasses the query cache, applies
// no per-field truncation, and only enforces the FullQueryRowCap safety
// limit. Used by the CLI-only sql tool. The question is a concise
// plain-English description of what the SQL is trying to answer; it is
// required so that server logs always carry the caller's intent alongside
// the SQL, the same way the MCP query tool does.
func (s *Service) ExecuteFull(ctx context.Context, question, sql, format string) FullQueryResponse {
	question = strings.TrimSpace(question)
	sql = strings.TrimSpace(sql)
	format = normalizeFormat(format)
	resp := FullQueryResponse{Question: question, SQL: sql, Format: format}
	if question == "" {
		resp.Error = "question must be a concise plain-English question this SQL statement is trying to answer"
		s.logger.WarnContext(ctx, "sql rejected: missing question", "sql", sql)
		return resp
	}
	if sql == "" {
		resp.Error = "sql must be a non-empty Postgres SQL statement"
		return resp
	}
	if err := ValidateReadOnlySQL(sql); err != nil {
		resp.Error = err.Error()
		s.logger.WarnContext(ctx, "sql rejected by read-only validator", "question", question, "sql", sql, "error", resp.Error)
		return resp
	}
	started := time.Now()
	s.logger.InfoContext(ctx, "sql started", "question", question, "sql", sql, "format", format, "row_cap", FullQueryRowCap)
	resp.Hint = rawTextScanHint(sql)
	raw, err := s.runner.Query(ctx, sql, FullQueryRowCap+1)
	if err != nil {
		resp.Error = s.queryErrorMessage(ctx, err.Error(), sql)
		s.logger.ErrorContext(ctx, "sql failed", "question", question, "sql", sql, "error", err, "duration", time.Since(started))
		return resp
	}
	rows := raw.Rows
	if len(rows) > FullQueryRowCap {
		rows = rows[:FullQueryRowCap]
		resp.Truncated = true
	}
	resp.ColumnNames = append([]string(nil), raw.Columns...)
	resp.TotalRows = len(rows)
	formatted, err := formatFullRows(raw.Columns, rows, format)
	if err != nil {
		resp.Error = err.Error()
		s.logger.ErrorContext(ctx, "sql encoding failed", "question", question, "sql", sql, "error", err, "duration", time.Since(started))
		return resp
	}
	resp.Rows = formatted
	s.logger.InfoContext(ctx, "sql completed", "question", question, "sql", sql, "rows", len(rows), "truncated", resp.Truncated, "duration", time.Since(started))
	return resp
}

func formatFullRows(columns []string, rows []map[string]any, format string) (any, error) {
	switch format {
	case "json":
		out := make([]map[string]any, len(rows))
		for i, row := range rows {
			copied := make(map[string]any, len(row))
			for k, v := range row {
				copied[k] = v
			}
			out[i] = copied
		}
		return out, nil
	case "ndjson":
		lines := make([]string, 0, len(rows))
		for _, row := range rows {
			data, err := json.Marshal(row)
			if err != nil {
				return nil, err
			}
			lines = append(lines, string(data))
		}
		return strings.Join(lines, "\n"), nil
	default:
		return rowsToCSV(columns, rows)
	}
}

func (s *Service) DebugCacheStatus() DebugCacheStatus {
	return s.cache.status()
}

func (s *Service) formatRows(columns []string, rows []map[string]any, start, end int, format string) (any, []FieldTruncation, error) {
	truncatedRows, truncations := s.truncateRowsForOutput(rows[start:end])
	switch format {
	case "json":
		return truncatedRows, truncations, nil
	case "ndjson":
		lines := make([]string, 0, len(truncatedRows))
		for _, row := range truncatedRows {
			data, err := json.Marshal(row)
			if err != nil {
				return nil, nil, err
			}
			lines = append(lines, string(data))
		}
		return strings.Join(lines, "\n"), truncations, nil
	default:
		csvText, err := rowsToCSV(columns, truncatedRows)
		if err != nil {
			return nil, nil, err
		}
		if len(truncations) > 0 {
			data, err := json.Marshal(truncations)
			if err != nil {
				return nil, nil, err
			}
			csvText += "\n# TRUNCATIONS: " + string(data)
		}
		return csvText, truncations, nil
	}
}

func (s *Service) truncateRowsForOutput(rows []map[string]any) ([]map[string]any, []FieldTruncation) {
	out := make([]map[string]any, 0, len(rows))
	var truncations []FieldTruncation
	for rowIndex, row := range rows {
		copied := make(map[string]any, len(row))
		for column, value := range row {
			switch v := value.(type) {
			case string:
				chars := utf8.RuneCountInString(v)
				if chars > s.opts.MaxFieldChars {
					runes := []rune(v)
					copied[column] = string(runes[:s.opts.MaxFieldChars])
					truncations = append(truncations, FieldTruncation{
						Row:      rowIndex,
						Column:   column,
						Returned: s.opts.MaxFieldChars,
						Total:    chars,
					})
				} else {
					copied[column] = v
				}
			default:
				copied[column] = value
			}
		}
		out = append(out, copied)
	}
	return out, truncations
}

func (s *Service) SchemaOverview(ctx context.Context) Response {
	showTablesSQL := "SELECT table_schema AS schema, table_name AS name FROM information_schema.tables WHERE table_schema = ANY(" + queryableSchemaArraySQL() + ") AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_schema, table_name"
	const currentDatabaseSQL = "SELECT current_database() AS database"
	started := time.Now()
	schemaResult := Result{SQL: "SELECT current_database() + information_schema.tables + SELECT * FROM <each table> LIMIT 3"}
	s.logger.InfoContext(ctx, "schema overview started")

	databaseResult, err := s.runner.Query(ctx, currentDatabaseSQL, 1)
	if err != nil {
		schemaResult.Error = err.Error()
		schemaResult.CSV = errorCSV(schemaResult.Error)
		s.logger.ErrorContext(ctx, "schema overview database lookup failed", "sql", currentDatabaseSQL, "error", err, "duration", time.Since(started))
		return Response{Results: []Result{schemaResult}}
	}
	database := currentDatabaseName(databaseResult)
	if database == "" {
		database = "default"
	}

	tablesResult, err := s.runner.Query(ctx, showTablesSQL, 0)
	if err != nil {
		schemaResult.Error = err.Error()
		schemaResult.CSV = errorCSV(schemaResult.Error)
		s.logger.ErrorContext(ctx, "schema overview table listing failed", "sql", showTablesSQL, "error", err, "duration", time.Since(started))
		return Response{Results: []Result{schemaResult}}
	}
	tables := schemaTableRefs(tablesResult)
	s.logger.InfoContext(ctx, "schema overview tables listed", "tables", len(tables))

	facts := s.overviewCatalog(ctx, tables)

	// timeline.events is the one relation whose columns stay inline: every
	// search result hands back a ref into it, so a caller that has to make a
	// second call before it can follow one is paying for the drill-through on
	// every single search.
	timelineColumns := ""
	for _, table := range tables {
		if table.DisplayName() == "timeline.events" {
			timelineColumns = s.describeTable(ctx, table).CSV
			break
		}
	}

	schemaResult.CSV = s.renderOverview(database, tables, facts, timelineColumns)
	s.logger.InfoContext(ctx, "schema overview completed", "database", database, "tables", len(tables), "bytes", len(schemaResult.CSV), "duration", time.Since(started))
	return Response{Results: []Result{schemaResult}}
}

// tableRowEstimates returns a map of schema-qualified table name → planner row estimate
// (pg_class.reltuples) for tables in queryable warehouse schemas. Estimates are O(1)
// catalog lookups and never block on a heap scan, so the schema overview can
// expose rough table sizes without forcing clients to write SELECT COUNT(*).
// Views and tables with no estimate yet (never analyzed) are omitted.
func (s *Service) tableRowEstimates(ctx context.Context) map[string]int64 {
	sql := "SELECT n.nspname AS schema, c.relname AS name, c.reltuples::bigint AS row_estimate " +
		"FROM pg_class c " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE n.nspname = ANY(" + queryableSchemaArraySQL() + ") " +
		"AND c.relkind IN ('r', 'p', 'm') " +
		"AND c.reltuples >= 0"
	started := time.Now()
	result, err := s.runner.Query(ctx, sql, 0)
	if err != nil {
		s.logger.WarnContext(ctx, "schema overview row estimate lookup failed", "sql", sql, "error", err, "duration", time.Since(started))
		return nil
	}
	out := make(map[string]int64, len(result.Rows))
	for _, row := range result.Rows {
		schema, _ := row["schema"].(string)
		name, _ := row["name"].(string)
		if schema == "" || name == "" {
			continue
		}
		key := schema + "." + name
		if estimate, ok := int64Value(row["row_estimate"]); ok {
			out[key] = estimate
		}
	}
	s.logger.DebugContext(ctx, "schema overview row estimates loaded", "tables", len(out), "duration", time.Since(started))
	return out
}

// formatRowCount renders an integer with thousands separators (e.g. 1234567 → "1,234,567").
func formatRowCount(n int64) string {
	if n < 0 {
		return "-" + formatRowCount(-n)
	}
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	first := len(s) % 3
	if first == 0 {
		first = 3
	}
	var b strings.Builder
	b.Grow(len(s) + (len(s)-1)/3)
	b.WriteString(s[:first])
	for i := first; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

// describeTable returns a table's column catalog: a single CSV header row of
// `name (type)` columns and no data rows. The overview deliberately does NOT
// sample row values — at the previous 15-char-per-field truncation the samples
// were close to noise yet were ~45% of the response, pushing the whole overview
// past MCP token limits so callers could not even read it. The column names and
// their exact Postgres types are the part that stops guessing; for real values,
// query the table directly.
func (s *Service) describeTable(ctx context.Context, table tableRef) Result {
	// Pull each column's precise type via format_type (e.g. text[], bigint,
	// timestamp with time zone) rather than information_schema.data_type, which
	// collapses every array to the unhelpful "ARRAY". Callers use these types to
	// avoid writing predicates the planner rejects, such as `is_deleted = false`
	// against a bigint or `to_addresses ILIKE ...` against a text[].
	describeSQL := describeColumnsSQLFor(table)
	started := time.Now()
	result := Result{SQL: describeSQL}
	s.logger.DebugContext(ctx, "schema overview describe started", "table", table.DisplayName(), "sql", describeSQL)
	describeResult, err := s.runner.Query(ctx, describeSQL, 0)
	if err != nil {
		result.Error = err.Error()
		result.CSV = errorCSV(result.Error)
		s.logger.ErrorContext(ctx, "schema overview describe failed", "table", table.DisplayName(), "sql", describeSQL, "error", err, "duration", time.Since(started))
		return result
	}
	columns := describedColumnNames(describeResult)
	columnTypes := describedColumnTypes(describeResult)
	if len(columns) == 0 {
		result.CSV = ""
		s.logger.DebugContext(ctx, "schema overview describe skipped empty table schema", "table", table.DisplayName(), "duration", time.Since(started))
		return result
	}
	result.CSV, err = rowsToCSVWithHeaders(columnHeadersWithTypes(columns, columnTypes), columns, nil)
	if err != nil {
		result.Error = err.Error()
		result.CSV = errorCSV(result.Error)
		s.logger.ErrorContext(ctx, "schema overview describe encoding failed", "table", table.DisplayName(), "error", err, "duration", time.Since(started))
		return result
	}
	s.logger.DebugContext(ctx, "schema overview table described", "table", table.DisplayName(), "columns", len(columns), "duration", time.Since(started))
	return result
}

func (s *Service) truncateRows(rows []map[string]any) ([]map[string]any, Truncation) {
	return s.truncateRowsWithMaxRows(rows, s.opts.MaxRows)
}

func (s *Service) truncateRowsWithMaxRows(rows []map[string]any, maxRows int) ([]map[string]any, Truncation) {
	trunc := Truncation{MaxRows: maxRows, MaxFieldChars: s.opts.MaxFieldChars}
	if len(rows) > maxRows {
		trunc.Rows = true
		rows = rows[:maxRows]
	}
	out := make([]map[string]any, 0, len(rows))
	for rowIndex, row := range rows {
		copied := make(map[string]any, len(row))
		for column, value := range row {
			switch v := value.(type) {
			case string:
				copied[column] = s.truncateString(rowIndex, column, v, &trunc)
			default:
				copied[column] = value
			}
		}
		out = append(out, copied)
	}
	return out, trunc
}

func (s *Service) truncateString(rowIndex int, column, value string, trunc *Truncation) string {
	chars := utf8.RuneCountInString(value)
	if chars <= s.opts.MaxFieldChars {
		return value
	}
	runes := []rune(value)
	trunc.Fields = append(trunc.Fields, FieldTruncation{
		Row:      rowIndex,
		Column:   column,
		Returned: s.opts.MaxFieldChars,
		Total:    chars,
	})
	return string(runes[:s.opts.MaxFieldChars])
}

func rowsToCSV(columns []string, rows []map[string]any) (string, error) {
	return rowsToCSVWithHeaders(columns, columns, rows)
}

// rowsToCSVWithHeaders is rowsToCSV with a separate header row, so the visible
// column labels (e.g. `is_deleted (bigint)`) can differ from the keys used to
// look up each row's values (e.g. `is_deleted`). headers and columns must be
// the same length and order.
func rowsToCSVWithHeaders(headers, columns []string, rows []map[string]any) (string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(headers); err != nil {
		return "", err
	}
	for _, row := range rows {
		record := make([]string, len(columns))
		for i, column := range columns {
			record[i] = csvValue(row[column])
		}
		if err := writer.Write(record); err != nil {
			return "", err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

func csvValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		if data, err := json.Marshal(value); err == nil {
			return string(data)
		}
		return fmt.Sprint(value)
	}
}

// queryErrorWithHint appends a one-line recovery hint to a Postgres error
// message when the error matches a common agent mistake against this warehouse.
// Schema-guessing is the recurring tax: agents guess the wrong per-source time
// column, the wrong Slack table/column names (slack_channels/channel_id instead
// of slack.conversations/conversation_id), compare a timestamptz column to an
// epoch integer, or ::numeric-cast the text cursor_ts. Each surfaces as an
// opaque Postgres error with no recovery path, so the *next* attempt is another
// blind guess. The hint names the correct table/column at the moment of the
// error — in the agent's own context, every time, regardless of whether it read
// schema_overview — which is the only thing that actually gets it learned. When
// the error is not one of these shapes the message is returned unchanged. The
// originating SQL is threaded in so the hint can name the exact column for the
// table actually queried instead of reciting the whole per-source map.
// queryErrorMessage builds the caller-facing error: the Postgres message (now
// carrying the server's own DETAIL/HINT), our recovery hint, and — for an
// undefined column — the real column list of every relation the statement
// references. The wrong column names are a long tail of one-offs, so the only fix
// that scales is answering each miss with the columns that do exist instead of
// sending the caller back to the catalog.
func (s *Service) queryErrorMessage(ctx context.Context, message, sql string) string {
	out := queryErrorWithHint(message, sql)
	if !strings.Contains(message, "42703") {
		return out
	}
	refs := relationsInSQL(sql)
	if len(refs) == 0 || len(refs) > maxHintRelations {
		return out
	}
	parts := make([]string, 0, len(refs))
	for _, ref := range refs {
		columns := s.relationColumnNames(ctx, ref)
		if len(columns) == 0 {
			continue
		}
		if len(columns) > maxHintColumns {
			columns = append(columns[:maxHintColumns:maxHintColumns], "...")
		}
		parts = append(parts, fmt.Sprintf("columns on %s: %s", ref.DisplayName(), strings.Join(columns, ", ")))
	}
	if len(parts) == 0 {
		return out
	}
	return out + " (" + strings.Join(parts, "; ") + ")"
}

// maxHintRelations bounds how many relations get their columns inlined. Two or
// three is a join a caller can still read; past that the hint stops being a
// hint and becomes a wall of text, and describe_table is the right tool.
const maxHintRelations = 3

// maxHintColumns bounds the inlined column list. The widest warehouse relation
// is ~51 columns, so this is a guard against a future wide table rather than a
// limit anything hits today.
const maxHintColumns = 80

// rawTextScanHint warns BEFORE a statement runs when its shape is the one
// that times out: a pattern match (ILIKE, LIKE, ~, ~*, regexp_*) over a raw
// base_* table with no timeline reference anywhere in the statement. In 14
// days of agent sessions 324 SQL calls did exactly this, and every statement
// timeout in that window was one of them; the timeout hint arrived ten
// seconds too late. The statement still runs -- a bounded ILIKE over a small
// table is fine -- so this is advice, not a fence.
func rawTextScanHint(sql string) string {
	lower := strings.ToLower(sql)
	if strings.Contains(lower, "timeline.") {
		return ""
	}
	if !rawBaseTableRef.MatchString(lower) || !rawTextScanOp.MatchString(lower) {
		return ""
	}
	return "(hint: this pattern-matches text in a raw base_* table without going through the timeline. " +
		"That is the query shape that hits the statement timeout on the large sources. For text search use " +
		"timeline.search_text('needle', 50, sources => ARRAY['slack']) or timeline.search_text_exact('needle', 50) " +
		"and drill to the raw row via source_table/source_pk; keep raw-table predicates to indexed columns and a time bound.)"
}

var (
	rawBaseTableRef = regexp.MustCompile(`\bbase_[a-z0-9_]+\.[a-z0-9_]+`)
	rawTextScanOp   = regexp.MustCompile(`\bilike\b|\blike\b|\bsimilar to\b|~\*?|\bregexp_(?:matches|like|replace|count)\s*\(|\bposition\s*\(`)
)

func queryErrorWithHint(message, sql string) string {
	if hint := schemaErrorHint(message, sql); hint != "" {
		return message + " " + hint
	}
	return message
}

// timeColumns maps each event-bearing warehouse table to its canonical primary
// time column. Every column here is timestamp with time zone. Sources name this
// column differently, which is the single most common wrong guess, so the order
// is preserved to render a stable per-source list in the fallback hint.
var timeColumns = []struct{ table, column string }{
	{"base_gmail.messages", "internal_date"},
	{"base_slack.messages", "message_datetime"},
	{"base_apple_messages.messages", "message_at"},
	{"base_apple_messages.chat_messages", "message_date"},
	{"base_whatsapp.messages", "message_at"},
	{"marts_ai_conversations.events", "occurred_at"},
	{"base_google_calendar.events", "start_at"},
	{"base_apple_notes.notes", "modified_at"},
	{"base_apple_notes.revisions", "modified_at"},
	{"base_apple_voice_memos.files", "recorded_at"},
	{"base_google_contacts.cards", "source_updated_at"},
	{"base_google_drive.files", "modified_time"},
}

// timeGuessColumns are the generic names agents reach for when they want a
// table's event time. None of them is the real column on the table being
// queried whenever a 42703 fires, so seeing one means "they wanted the time
// column" and we answer with the real one. updated_at/synced_at/created_at are
// included because, while real bookkeeping columns on some tables, an agent
// using them as the event time on a table that lacks them is the same mistake.
var timeGuessColumns = map[string]bool{
	"ts": true, "ts_utc": true, "tsutc": true, "timestamp": true, "timestamptz": true,
	"time": true, "datetime": true, "date": true, "date_unix": true, "date_utc": true,
	"date_ns": true, "created": true, "created_at": true, "created_date": true,
	"createddate": true, "create_time": true, "creation_time": true, "inserted_at": true,
	"event_time": true, "event_at": true, "event_date": true, "sent_at": true,
	"sent_date": true, "received_at": true, "received_date": true, "start_time": true,
	"starttime": true, "last_seen_at": true, "last_seen": true, "occurred": true,
	"occurred_date": true, "message_time": true, "msg_time": true, "epoch": true,
	"unix_time": true, "unix_ts": true, "updated": true, "updated_at": true,
	"synced": true, "synced_at": true,
}

func init() {
	// A source's REAL time column used against a different source dead-ends
	// exactly like an invented name, and the answer is the same one. Deriving
	// these from the per-source map means a new source cannot be forgotten.
	for _, tc := range timeColumns {
		timeGuessColumns[tc.column] = true
	}
}

// columnRemaps point a specific wrong column name at the right one. These are
// structural renames (not time columns) that recur across sessions.
var columnRemaps = map[string]string{
	"channel_id": "Slack tables use conversation_id, and the channels table is named base_slack.conversations (not slack_channels)",
	"channel":    "Slack tables use conversation_id, and the channels table is named base_slack.conversations (not slack_channels)",
	"chat_jid":   "use chat_id — base_apple_messages.messages/base_whatsapp.messages and their chat tables key on chat_id, not chat_jid",
	"from_email": "base_gmail.messages stores the sender in from_address; to_addresses/cc_addresses/bcc_addresses are text[], so match them with EXISTS (SELECT 1 FROM unnest(to_addresses) a WHERE a ILIKE '%…%')",
	"text_content": "no warehouse relation has a text_content column — timeline.events carries the preview in snippet and the indexed body in search_text, " +
		"the search functions return the matched preview as text, and each source's raw table names its body differently (base_slack.messages.text, base_gmail.messages.body_text, base_whatsapp.messages.body_text)",
	"body_content": "no warehouse relation has a body_content column — timeline.events carries the preview in snippet and the indexed body in search_text; " +
		"raw tables name it per source (base_slack.messages.text, base_gmail.messages.body_text, base_whatsapp.messages.body_text)",
}

// tableRemaps point a wrong table name at the right one. The catalog supplies
// one entry per stable logical id (those ids read like table names — they are
// what timeline rows and older docs use — but they are catalog identifiers, not
// SQL), plus the historical timeline source_table tokens. The literals below
// are the recurring human synonyms no catalog can derive.
var tableRemaps = map[string]string{
	"slack_channels":     "base_slack.conversations",
	"slack_channel":      "base_slack.conversations",
	"slack_conversation": "base_slack.conversations",
	"slack_message":      "base_slack.messages",
	"gmail_message":      "base_gmail.messages",
	"apple_message":      "base_apple_messages.messages",
	"marts":              "the marts split by domain: marts_inbox, marts_contacts, marts_messages, marts_ai_conversations, marts_photos, marts_calendar, marts_finance, marts_receipts",
	"search":             "the search functions moved into timeline: timeline.search_text(...) / timeline.search_text_exact(...)",
	"searchable_text":    "timeline.search_text('needle', 50) for ranked search, or timeline.search_text_exact('needle', 50) for literal substrings",
}

func init() {
	for _, obj := range warehouse.Objects {
		if !obj.Discoverable {
			continue
		}
		if _, manual := tableRemaps[obj.ID]; manual {
			continue
		}
		tableRemaps[obj.ID] = obj.Schema + "." + obj.Name
	}
	for legacy, current := range warehouse.RenamedTimelineSourceTables {
		if _, manual := tableRemaps[legacy]; manual {
			continue
		}
		tableRemaps[legacy] = warehouse.DisplayRelation(current)
	}
	// Pre-reorganization physical names, so a query written against the old
	// layout is answered with the new location instead of a bare "no such
	// relation". This is an error hint only — nothing resolves SQL through it.
	for old, current := range warehouse.PreviousLocations {
		if _, manual := tableRemaps[old]; !manual {
			tableRemaps[old] = current
		}
	}
}

var quotedIdentifierRe = regexp.MustCompile(`"([^"]+)"`)

func schemaErrorHint(message, sql string) string {
	if hint := statementTimeoutHint(message); hint != "" {
		return hint
	}
	if hint := datetimeOperatorHint(message); hint != "" {
		return hint
	}
	if hint := undefinedFunctionHint(message); hint != "" {
		return hint
	}
	if hint := undefinedTableHint(message); hint != "" {
		return hint
	}
	if hint := undefinedColumnHint(message, sql); hint != "" {
		return hint
	}
	if hint := bigintBooleanHint(message); hint != "" {
		return hint
	}
	if hint := textJSONHint(message); hint != "" {
		return hint
	}
	if hint := numericCastHint(message, sql); hint != "" {
		return hint
	}
	if hint := opsPermissionHint(message, sql); hint != "" {
		return hint
	}
	return ""
}

// opsPermissionHint fires when the query role is denied on an ops.* sync-state
// table (SQLSTATE 42501). Pipeline-debugging sessions get pointed at ops.* by
// the operational docs, but most of that schema is deliberately unreadable by
// the query role — 10 sessions in 30 days hit this wall with no recovery path.
// The supported health surfaces answer the same questions.
func opsPermissionHint(message, sql string) string {
	if !strings.Contains(message, "42501") && !strings.Contains(message, "permission denied") {
		return ""
	}
	// Postgres names only the bare relation ("permission denied for table
	// slack_sync_state"), and the caller may have reached it through a quoted
	// name or a view, so the SQL text alone is not a reliable trigger.
	if !strings.Contains(strings.ToLower(sql), "ops.") && !deniedRelationIsOps(message) {
		return ""
	}
	return "(hint: most ops.* sync-state tables are internal and not readable by the query role. For pipeline and freshness debugging read marts_ops.table_freshness (per-table last write, newest event, and age), marts_ops.pipeline_health (per-pipeline status + last error), marts_ops.mart_view_health, marts_ops.timeline_adapter_health, and marts_ops.plaid_item_health — those are the supported surfaces, and they answer the same questions.)"
}

// opsRelationNames are the bare table names living in the ops schema, so a
// denial naming only the relation can still be recognized. Built from the
// catalog rather than a literal list, which would rot on the first rename.
var opsRelationNames = map[string]bool{}

func init() {
	for _, obj := range warehouse.Objects {
		if obj.Schema == "ops" {
			opsRelationNames[strings.ToLower(obj.Name)] = true
		}
	}
}

func deniedRelationIsOps(message string) bool {
	for _, marker := range []string{"permission denied for table ", "permission denied for view ", "permission denied for relation "} {
		_, after, ok := strings.Cut(message, marker)
		if !ok {
			continue
		}
		fields := strings.Fields(after)
		if len(fields) == 0 {
			continue
		}
		if opsRelationNames[strings.ToLower(strings.Trim(fields[0], `"`))] {
			return true
		}
	}
	return false
}

// bigintBooleanHint fires when a caller writes an honest boolean predicate
// against one of the warehouse's boolean-named columns. Every one of them is
// bigint 0/1 (the source stores are SQLite-derived), so NOT flag, flag = true,
// and FILTER (WHERE flag) all raise 42804 — or 42883 for the `= boolean`
// comparison — with no indication that the column is the problem.
func bigintBooleanHint(message string) string {
	boolArgument := strings.Contains(message, "must be type boolean, not type bigint")
	boolComparison := strings.Contains(message, "operator does not exist") &&
		strings.Contains(message, "bigint = boolean")
	if !boolArgument && !boolComparison {
		return ""
	}
	return "(hint: warehouse boolean-named columns are bigint 0/1, not boolean — write `is_from_me = 1` / `is_deleted = 0`, not `NOT is_from_me`, `= true`, or `FILTER (WHERE is_read)`. Run describe_table for a relation's exact column types.)"
}

// textJSONHint fires when a JSON operator is applied to a column that is stored
// as text. The warehouse is genuinely mixed — the older sources keep JSON in
// text columns and the newer ones use real jsonb — so the caller cannot infer
// this from the column name and has to be told which one they hit.
func textJSONHint(message string) string {
	jsonOperator := strings.Contains(message, "operator does not exist") &&
		strings.Contains(message, "text -") &&
		(strings.Contains(message, "->") || strings.Contains(message, "->>") || strings.Contains(message, "#>"))
	subscript := strings.Contains(message, "cannot subscript type text")
	if !jsonOperator && !subscript {
		return ""
	}
	return "(hint: that column stores JSON as text, not jsonb — cast it first, e.g. raw_json::jsonb ->> 'key'. JSON columns are text on the older sources (slack, gmail, apple_*, whatsapp, the agent-session event tables) and real jsonb on the newer ones (plaid, whoop, google_drive, manual_finance, ...); run describe_table to see which one this is.)"
}

// statementTimeoutHint fires when a query exhausts the server's statement
// budget (SQLSTATE 57014). The common cause is text-scanning raw tables, so
// the recovery path names the search layer rather than suggesting a retry.
func statementTimeoutHint(message string) string {
	if strings.Contains(message, "canceling statement due to statement timeout") {
		return "(hint: the query exceeded the server's statement budget and a retry will too. Narrow it with selective indexed predicates and LIMIT; for text search use timeline.search_text('needle', 50) for ranked keyword search or timeline.search_text_exact('needle', 50) for literal substrings instead of ILIKE/regex over raw text columns. If the timed-out query WAS a search call, scope it down instead of retrying: pass sources => ARRAY['slack'] and/or since => '2026-01-01', and lower max_results.)"
	}
	return ""
}

// datetimeOperatorHint fires when a timestamptz column is compared to an epoch
// int, e.g. message_at > 1700000000. Postgres reports SQLSTATE 42883
// ("operator does not exist") naming both sides.
func datetimeOperatorHint(message string) string {
	if strings.Contains(message, "operator does not exist") &&
		strings.Contains(message, "timestamp with time zone") &&
		(strings.Contains(message, "integer") || strings.Contains(message, "bigint") || strings.Contains(message, "numeric")) {
		return "(hint: that column is timestamp with time zone — compare it to a timestamp, not an epoch integer, e.g. message_at >= '2026-01-01' instead of message_at > 1700000000.)"
	}
	return ""
}

// functionRemaps point a bare function name at its schema-qualified form. The
// search entry points are the ones agents reach for unqualified, because that
// is how they were callable before the schema reorganization moved them.
var functionRemaps = map[string]string{
	"search_text":         "timeline.search_text",
	"search_text_exact":   "timeline.search_text_exact",
	"search_hybrid":       "timeline.search_hybrid",
	"search_text_sources": "timeline.search_text_sources",
}

// searchHitColumns is the exact output shape of timeline.search_text,
// timeline.search_text_exact, and timeline.search_hybrid — mirror of the
// composite type built in postgres.py (hit_type_columns_sql). It drifted once
// already: the 2026-08-19 overhaul added event_ts/title/source_table/source_pk
// and the hint here kept describing the nine-column shape for a day.
const searchHitColumns = "source, subsource, context, who, occurred_at, account, ref, text, score, event_ts, title, source_table, source_pk, priority"

// searchFunctionSignatures are printed when a caller misses a search
// function's signature — a wrong named parameter (limit_rows =>) or wrong
// argument shape on an already-qualified call. Telling that caller to
// schema-qualify what they already qualified sends them in circles.
var searchFunctionSignatures = map[string]string{
	"search_text":       "timeline.search_text(query text, max_results integer DEFAULT 50, sources text[] DEFAULT NULL, since timestamptz DEFAULT NULL, priorities text[] DEFAULT NULL)",
	"search_text_exact": "timeline.search_text_exact(query text, max_results integer DEFAULT 50, sources text[] DEFAULT NULL, since timestamptz DEFAULT NULL, priorities text[] DEFAULT NULL)",
	"search_hybrid":     "timeline.search_hybrid(query text, query_embedding text, embedding_model text, max_results integer DEFAULT 50, sources text[] DEFAULT NULL, since timestamptz DEFAULT NULL, priorities text[] DEFAULT NULL)",
}

var undefinedFunctionRe = regexp.MustCompile(`function ([a-zA-Z0-9_."]+)\s*\(`)

// undefinedFunctionHint fires on a missing function (SQLSTATE 42883). Query
// sessions run with Postgres' default '"$user", public' search_path — the same
// reason relations must be schema-qualified — so a bare search_text('needle')
// resolves to nothing. This error is the GOOD outcome: until the pre-schema-
// reorganization public.search_text copy was dropped, that call resolved to a
// stale function whose per-branch exception guard silently returned zero rows,
// so search looked empty instead of misqualified.
func undefinedFunctionHint(message string) string {
	if !strings.Contains(message, "42883") || !strings.Contains(message, "does not exist") {
		return ""
	}
	match := undefinedFunctionRe.FindStringSubmatch(message)
	if match == nil {
		return ""
	}
	qualified := strings.ToLower(strings.ReplaceAll(match[1], `"`, ""))
	name := qualified
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	if remap, ok := functionRemaps[name]; ok {
		if name == "search_hybrid" {
			// Hybrid search needs query embeddings the caller cannot produce
			// by hand, so printing the seven-argument signature sends an
			// agent to invent an embedding literal. There is one hybrid
			// search: the search tool (pdw search on the CLI). Measured
			// 2026-08-28: six sessions in 14 days tried this from SQL.
			return "(hint: hybrid search is the `search` tool (`pdw search '<terms>'` on the CLI, \"priorities\": [...] / --priority to scope by tier) — it embeds the query for you and fuses the semantic, keyword and literal legs. From SQL, timeline.search_text('needle', 50, priorities => ARRAY['self','direct']) is the ranked keyword path.)"
		}
		signature, hasSignature := searchFunctionSignatures[name]
		if hasSignature && (strings.HasPrefix(qualified, "timeline.") || strings.Contains(message, "=>")) {
			// The caller already qualified the function (or used a named
			// argument): the miss is the signature, not the schema path.
			return fmt.Sprintf("(hint: the signature is %s — the named parameters are max_results, sources, since, priorities — returning (%s).)", signature, searchHitColumns)
		}
		return fmt.Sprintf("(hint: call %s(...) — queries carry no warehouse search_path, so its functions must be schema-qualified just like its tables.)", remap)
	}
	return "(hint: no such function — queries carry no warehouse search_path, so schema-qualify warehouse functions: timeline.search_text('needle', 50) for ranked search, timeline.search_text_exact('needle', 50) for literal substrings. Run schema_overview for the rest.)"
}

// undefinedTableHint fires on a missing relation (SQLSTATE 42P01). A known
// wrong name (slack_channels) is mapped to the right one; anything else points
// at schema_overview for the canonical table list.
func undefinedTableHint(message string) string {
	if !strings.Contains(message, "42P01") || !strings.Contains(message, "does not exist") {
		return ""
	}
	rel := strings.ToLower(quotedIdentifier(message))
	if remap, ok := tableRemaps[rel]; ok {
		return fmt.Sprintf("(hint: there is no %s relation — use %s. Run schema_overview for the exact relation names.)", rel, remap)
	}
	return "(hint: no such relation — run schema_overview for the relation list, or describe_table('<name>') to have the closest matches named for you.)"
}

// undefinedColumnHint fires on a missing column (SQLSTATE 42703). A structural
// rename is corrected by name; a time-column guess is answered with the exact
// time column for the table actually queried (or the full per-source list when
// the table is ambiguous); anything else points at schema_overview.
func undefinedColumnHint(message, sql string) string {
	if !strings.Contains(message, "42703") || !strings.Contains(message, "does not exist") {
		return ""
	}
	if hint := searchResultColumnHint(sql); hint != "" {
		return hint
	}
	col := strings.ToLower(quotedIdentifier(message))
	if hint := timelineEventsColumnHint(col, sql); hint != "" {
		return hint
	}
	if remap, ok := columnRemaps[col]; ok {
		return "(hint: " + remap + ". Run describe_table('<schema.table>') for the full column list.)"
	}
	if timeGuessColumns[col] {
		if table, column := soleTimeTable(sql); table != "" {
			return fmt.Sprintf("(hint: the primary time column on %s is %s (timestamp with time zone) — sources name it differently, so don't guess. Run describe_table('%s') for the rest.)", table, column, table)
		}
		return "(hint: each source names its primary time column differently — " + timeColumnsList() + ", all timestamp with time zone. Run describe_table('<schema.table>') for the exact columns.)"
	}
	// Postgres computes its own Levenshtein suggestion and we now surface it
	// verbatim. When it fired, it is strictly better than this generic advice —
	// it names the actual column — so don't append a second, vaguer hint after it.
	if strings.Contains(message, "HINT: Perhaps you meant") {
		return ""
	}
	return "(hint: column names differ per source — run describe_table('<schema.table>') for the exact columns and their (type) annotations.)"
}

// searchHitOnlyColumns exist on a search FUNCTION's result and nowhere on
// timeline.events. They are the single most confused pair in the warehouse:
// the table and the functions describe the same events under different names.
var searchHitOnlyColumns = map[string]bool{
	"occurred_at": true, "text": true, "score": true, "subsource": true,
	"account": true, "ref": true, "who": true,
}

// timelineEventsColumnHint fires when a search-hit column name is used against
// timeline.events itself. The generic "column names differ per source" advice
// is true and useless here, because the caller did not confuse two sources —
// they confused the table with the function that searches it.
func timelineEventsColumnHint(col, sql string) string {
	if !searchHitOnlyColumns[col] {
		return ""
	}
	lower := strings.ToLower(sql)
	if !containsWord(lower, "timeline.events") || searchFunctionCallRe.MatchString(sql) {
		return ""
	}
	return fmt.Sprintf(
		"(hint: %q is a column of the search FUNCTIONS' result, not of timeline.events. The table uses event_ts (time), actor (who), title, snippet (preview) and search_text (the indexed body), plus priority/source/source_table/source_pk. "+
			"timeline.search_text('needle', 50) returns (%s) instead — pick the one whose names you want.)",
		col, searchHitColumns)
}

var searchFunctionCallRe = regexp.MustCompile(`(?i)(?:"?timeline"?\s*\.\s*)?"?(search_text(?:_exact)?|search_hybrid)"?\s*\(`)

// searchResultColumnHint keeps cross-source search inside SQL while making its
// table-valued function contract recoverable at the exact point callers guess
// a relation-style column (title/body/event_time are the recurring examples).
// describe_table cannot help here because search_text* are functions, not
// relations, so the generic undefined-column advice sends callers in circles.
func searchResultColumnHint(sql string) string {
	// Once a search hit is joined to another relation, the missing column may
	// belong to either side. Fall back to the generic relation guidance rather
	// than confidently attributing it to the function result.
	if containsWord(strings.ToLower(sql), "join") {
		return ""
	}
	match := searchFunctionCallRe.FindStringSubmatch(sql)
	if match == nil {
		return ""
	}
	function := "timeline." + strings.ToLower(match[1])
	return fmt.Sprintf(
		"(hint: %s() returns exactly (%s) — use occurred_at (or its mirror event_ts) for time, text for the matched preview, and title for the headline; source_table + source_pk drill straight to the source row, and timeline.context(ref) fetches the surrounding events.)",
		function,
		searchHitColumns,
	)
}

// numericCastHint catches the classic Slack sync-state trap: cursor_ts is text
// and is often the empty string, so ::numeric raises SQLSTATE 22P02 on it.
func numericCastHint(message, sql string) string {
	if !strings.Contains(message, "22P02") || !strings.Contains(message, "numeric") {
		return ""
	}
	if !strings.Contains(strings.ToLower(sql), "cursor_ts") {
		return ""
	}
	return "(hint: ops.slack_sync_state.cursor_ts is text and is often '' (empty), which breaks ::numeric — guard it with NULLIF(cursor_ts, '')::numeric.)"
}

// soleTimeTable returns the table and its primary time column when the SQL
// references exactly one known event-bearing table. When zero or several
// appear (e.g. a join), it returns empty so the caller falls back to the full
// per-source list rather than risk naming the wrong table's column.
func soleTimeTable(sql string) (string, string) {
	lower := strings.ToLower(sql)
	table, column := "", ""
	for _, tc := range timeColumns {
		if !containsWord(lower, tc.table) {
			continue
		}
		if table != "" && table != tc.table {
			return "", ""
		}
		table, column = tc.table, tc.column
	}
	return table, column
}

func containsWord(haystack, word string) bool {
	re := regexp.MustCompile(`(^|[^a-z0-9_])` + regexp.QuoteMeta(word) + `($|[^a-z0-9_])`)
	return re.MatchString(haystack)
}

func timeColumnsList() string {
	parts := make([]string, 0, len(timeColumns))
	for _, tc := range timeColumns {
		parts = append(parts, tc.table+"."+tc.column)
	}
	return strings.Join(parts, ", ")
}

func quotedIdentifier(message string) string {
	m := quotedIdentifierRe.FindStringSubmatch(message)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

func errorCSV(message string) string {
	out, err := rowsToCSV([]string{"error"}, []map[string]any{{"error": message}})
	if err != nil {
		return "error\n" + message
	}
	return out
}

func schemaTableRefs(result RawResult) []tableRef {
	tables := make([]tableRef, 0, len(result.Rows))
	for _, row := range result.Rows {
		schema := rowString(row, "schema")
		name := rowString(row, "name")
		if schema != "" && name != "" {
			tables = append(tables, tableRef{Schema: schema, Name: name})
		}
	}
	return tables
}

func describedColumnNames(result RawResult) []string {
	columns := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		name := rowString(row, "name")
		if name != "" {
			columns = append(columns, name)
		}
	}
	return columns
}

func describedColumnTypes(result RawResult) map[string]string {
	types := make(map[string]string, len(result.Rows))
	for _, row := range result.Rows {
		name := rowString(row, "name")
		if name == "" {
			continue
		}
		types[name] = rowString(row, "type")
	}
	return types
}

// columnHeadersWithTypes renders the per-table CSV header so each column name
// carries its Postgres type inline in parentheses, e.g. `is_deleted (bigint)`,
// `to_addresses (text[])`. Callers see the exact type on the column itself
// instead of guessing from truncated sample values, which is what led them to
// write predicates the planner rejects (`is_deleted = false` against a bigint,
// `to_addresses ILIKE ...` against a text[]).
func columnHeadersWithTypes(columns []string, types map[string]string) []string {
	headers := make([]string, len(columns))
	for i, column := range columns {
		typeName := types[column]
		if typeName == "" {
			typeName = "unknown"
		}
		headers[i] = column + " (" + typeName + ")"
	}
	return headers
}

func currentDatabaseName(result RawResult) string {
	if len(result.Rows) == 0 {
		return ""
	}
	name := rowString(result.Rows[0], "database")
	if name == "" && len(result.Columns) == 1 {
		name = rowString(result.Rows[0], result.Columns[0])
	}
	return name
}

func rowString(row map[string]any, column string) string {
	value, ok := row[column]
	if !ok || value == nil {
		return ""
	}
	return csvValue(value)
}

func quotePostgresIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

type queryCache struct {
	mu         sync.Mutex
	entries    map[string]*list.Element
	lru        *list.List
	maxBytes   int64
	ttl        time.Duration
	totalBytes int64
}

type queryCacheEntry struct {
	ID         string
	Question   string
	SQL        string
	Columns    []string
	Rows       []map[string]any
	Format     string
	SizeBytes  int64
	CreatedAt  time.Time
	LastAccess time.Time
}

func newQueryCache(maxBytes int64, ttl time.Duration) *queryCache {
	return &queryCache{
		entries:  make(map[string]*list.Element),
		lru:      list.New(),
		maxBytes: maxBytes,
		ttl:      ttl,
	}
}

func (c *queryCache) add(entry *queryCacheEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictExpiredLocked(time.Now())
	if entry.SizeBytes > c.maxBytes {
		return fmt.Errorf("query result is %d bytes, which exceeds MCP_QUERY_CACHE_MAX_BYTES (%d); narrow the query or raise the cache limit", entry.SizeBytes, c.maxBytes)
	}
	for c.totalBytes+entry.SizeBytes > c.maxBytes {
		c.evictOldestLocked()
	}
	entry.LastAccess = entry.CreatedAt
	elem := c.lru.PushFront(entry)
	c.entries[entry.ID] = elem
	c.totalBytes += entry.SizeBytes
	return nil
}

func (c *queryCache) get(queryID string) (*queryCacheEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.evictExpiredLocked(now)
	elem, ok := c.entries[queryID]
	if !ok {
		return nil, fmt.Errorf("unknown or expired query_id %q; re-run query to create a fresh query_id (server restarts invalidate cached query_ids)", queryID)
	}
	entry := elem.Value.(*queryCacheEntry)
	entry.LastAccess = now
	c.lru.MoveToFront(elem)
	return entry, nil
}

func (c *queryCache) status() DebugCacheStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.evictExpiredLocked(now)
	status := DebugCacheStatus{
		TotalBytes: c.totalBytes,
		MaxBytes:   c.maxBytes,
		TTLSeconds: int64(c.ttl.Seconds()),
		Queries:    make([]DebugCacheEntry, 0, len(c.entries)),
	}
	for elem := c.lru.Front(); elem != nil; elem = elem.Next() {
		entry := elem.Value.(*queryCacheEntry)
		status.Queries = append(status.Queries, DebugCacheEntry{
			QueryID:    entry.ID,
			SQL:        entry.SQL,
			Rows:       len(entry.Rows),
			Bytes:      entry.SizeBytes,
			AgeSeconds: int64(now.Sub(entry.CreatedAt).Seconds()),
		})
	}
	return status
}

func (c *queryCache) evictExpiredLocked(now time.Time) {
	for elem := c.lru.Back(); elem != nil; {
		prev := elem.Prev()
		entry := elem.Value.(*queryCacheEntry)
		if now.Sub(entry.CreatedAt) > c.ttl {
			c.removeLocked(elem)
		}
		elem = prev
	}
}

func (c *queryCache) evictOldestLocked() {
	elem := c.lru.Back()
	if elem != nil {
		c.removeLocked(elem)
	}
}

func (c *queryCache) removeLocked(elem *list.Element) {
	entry := elem.Value.(*queryCacheEntry)
	delete(c.entries, entry.ID)
	c.totalBytes -= entry.SizeBytes
	c.lru.Remove(elem)
}

func copyRows(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		copied := make(map[string]any, len(row))
		for key, value := range row {
			copied[key] = value
		}
		out = append(out, copied)
	}
	return out
}

func estimateEntrySize(entry *queryCacheEntry) int64 {
	size := int64(len(entry.ID) + len(entry.Question) + len(entry.SQL) + len(entry.Format))
	for _, column := range entry.Columns {
		size += int64(len(column))
	}
	for _, row := range entry.Rows {
		for _, column := range entry.Columns {
			size += int64(len(column))
			size += int64(len(csvValue(row[column])))
		}
	}
	return size
}

func newQueryID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func normalizeFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json", "ndjson":
		return strings.ToLower(strings.TrimSpace(format))
	default:
		return "csv"
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func byteToRuneOffsets(value string) map[int]int {
	offsets := make(map[int]int, utf8.RuneCountInString(value)+1)
	runeIndex := 0
	for byteIndex := range value {
		offsets[byteIndex] = runeIndex
		runeIndex++
	}
	offsets[len(value)] = runeIndex
	return offsets
}
