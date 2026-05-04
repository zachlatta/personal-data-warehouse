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
)

type Options struct {
	MaxRows            int
	MaxFieldChars      int
	QueryCacheMaxBytes int64
	GetFieldMaxChars   int
	QueryCacheTTL      time.Duration
	DebugCacheTool     bool
	Logger             *slog.Logger
}

type Runner interface {
	Query(ctx context.Context, sql string, maxRows int) (RawResult, error)
}

type RawResult struct {
	Columns []string
	Rows    []map[string]any
}

type Service struct {
	runner Runner
	opts   Options
	logger *slog.Logger
	cache  *queryCache
}

const schemaSampleRows = 3
const schemaSampleFieldChars = 15
const schemaSampleConcurrency = 16

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
	return &Service{runner: runner, opts: opts, logger: logger.With("component", "query"), cache: newQueryCache(opts.QueryCacheMaxBytes, opts.QueryCacheTTL)}
}

func (s *Service) Execute(ctx context.Context, sql []string, previewRows int, format string) QueryResponse {
	if len(sql) == 0 {
		err := "sql must contain at least one statement"
		s.logger.WarnContext(ctx, "query batch rejected", "error", err)
		return QueryResponse{Results: []QueryResult{{Error: err}}}
	}
	format = normalizeFormat(format)
	if previewRows < 0 {
		previewRows = 0
	}
	if previewRows == 0 {
		previewRows = 20
	}

	started := time.Now()
	s.logger.InfoContext(ctx, "query batch started", "statements", len(sql), "max_rows", s.opts.MaxRows, "max_field_chars", s.opts.MaxFieldChars, "format", format, "preview_rows", previewRows)
	results := make([]QueryResult, 0, len(sql))
	for _, statement := range sql {
		queryStarted := time.Now()
		result := QueryResult{SQL: statement, Format: format}
		if err := ValidateReadOnlySQL(statement); err != nil {
			result.Error = err.Error()
			results = append(results, result)
			s.logger.WarnContext(ctx, "query rejected by read-only validator", "sql", statement, "error", result.Error)
			continue
		}
		s.logger.DebugContext(ctx, "query started", "sql", statement)
		raw, err := s.runner.Query(ctx, statement, s.opts.MaxRows+1)
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			s.logger.ErrorContext(ctx, "query failed", "sql", statement, "error", err, "duration", time.Since(queryStarted))
			continue
		}
		if len(raw.Rows) > s.opts.MaxRows {
			result.Error = fmt.Sprintf("query returned more than MCP_MAX_ROWS (%d) rows; add a LIMIT or narrower WHERE clause and re-run the query", s.opts.MaxRows)
			results = append(results, result)
			s.logger.WarnContext(ctx, "query rejected by row cap", "sql", statement, "rows_seen", len(raw.Rows), "max_rows", s.opts.MaxRows, "duration", time.Since(queryStarted))
			continue
		}
		entry := &queryCacheEntry{
			ID:        newQueryID(),
			SQL:       statement,
			Columns:   append([]string(nil), raw.Columns...),
			Rows:      copyRows(raw.Rows),
			Format:    format,
			CreatedAt: time.Now(),
		}
		entry.SizeBytes = estimateEntrySize(entry)
		if err := s.cache.add(entry); err != nil {
			result.Error = err.Error()
			s.logger.ErrorContext(ctx, "query result encoding failed", "sql", statement, "error", err, "duration", time.Since(queryStarted))
		} else {
			result.QueryID = entry.ID
			result.TotalRows = len(entry.Rows)
			result.ColumnNames = entry.Columns
			result.Preview, result.Truncations, err = s.formatRows(entry.Columns, entry.Rows, 0, minInt(previewRows, len(entry.Rows)), format)
			if err != nil {
				result.Error = err.Error()
			}
			s.logger.InfoContext(ctx, "query completed", "sql", statement, "query_id", entry.ID, "rows", len(entry.Rows), "columns", len(raw.Columns), "truncated_fields", len(result.Truncations), "duration", time.Since(queryStarted))
		}
		results = append(results, result)
	}
	s.logger.InfoContext(ctx, "query batch completed", "statements", len(sql), "duration", time.Since(started))
	return QueryResponse{Results: results}
}

func (s *Service) GetRows(queryID string, offset, limit int, format string) RowsResponse {
	entry, err := s.cache.get(queryID)
	if err != nil {
		return RowsResponse{QueryID: queryID, Error: err.Error()}
	}
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

func (s *Service) GetField(queryID string, row int, column string, offset, length int) FieldResponse {
	resp := FieldResponse{QueryID: queryID, Row: row, Column: column, Offset: offset}
	entry, err := s.cache.get(queryID)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
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

func (s *Service) GrepRows(queryID, pattern string, columns []string, limit, contextChars int) GrepResponse {
	resp := GrepResponse{QueryID: queryID, Pattern: pattern}
	entry, err := s.cache.get(queryID)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
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
	const showTablesSQL = "SHOW TABLES"
	const currentDatabaseSQL = "SELECT currentDatabase() AS database"
	started := time.Now()
	schemaResult := Result{SQL: "SELECT currentDatabase() + SHOW TABLES + SELECT * FROM <each table> LIMIT 3"}
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
	tables := tableNames(tablesResult)
	s.logger.InfoContext(ctx, "schema overview tables listed", "tables", len(tables))

	var out strings.Builder
	overviewTrunc := Truncation{MaxRows: schemaSampleRows, MaxFieldChars: schemaSampleFieldChars}
	sampleResults := s.sampleTables(ctx, tables)
	for i, table := range tables {
		sample := sampleResults[i]
		if sample.Error != "" {
			schemaResult.Error = sample.Error
			schemaResult.CSV = sample.CSV
			return Response{Results: []Result{schemaResult}}
		}
		if i > 0 {
			out.WriteString("\n")
		}
		out.WriteString("# ")
		out.WriteString(database)
		out.WriteString(".")
		out.WriteString(table)
		out.WriteString("\n\n")
		out.WriteString(sample.CSV)
		out.WriteString("\n")
		overviewTrunc.Fields = append(overviewTrunc.Fields, sample.Truncated.Fields...)
	}

	schemaResult.CSV = out.String()
	schemaResult.Truncated = overviewTrunc
	s.logger.InfoContext(ctx, "schema overview completed", "database", database, "tables", len(tables), "truncated_fields", len(overviewTrunc.Fields), "duration", time.Since(started))
	return Response{Results: []Result{schemaResult}}
}

func (s *Service) sampleTables(ctx context.Context, tables []string) []Result {
	results := make([]Result, len(tables))
	sem := make(chan struct{}, schemaSampleConcurrency)
	var wg sync.WaitGroup
	for i, table := range tables {
		i, table := i, table
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[i] = Result{Error: ctx.Err().Error(), CSV: errorCSV(ctx.Err().Error())}
				return
			}
			results[i] = s.sampleRows(ctx, table)
		}()
	}
	wg.Wait()
	return results
}

func (s *Service) sampleRows(ctx context.Context, table string) Result {
	tableIdentifier := quoteClickHouseIdentifier(table)
	describeSQL := "DESCRIBE TABLE " + tableIdentifier
	started := time.Now()
	result := Result{SQL: describeSQL}
	s.logger.DebugContext(ctx, "schema overview describe started", "table", table, "sql", describeSQL)
	describeResult, err := s.runner.Query(ctx, describeSQL, 0)
	if err != nil {
		result.Error = err.Error()
		result.CSV = errorCSV(result.Error)
		s.logger.ErrorContext(ctx, "schema overview describe failed", "table", table, "sql", describeSQL, "error", err, "duration", time.Since(started))
		return result
	}
	columns := describedColumnNames(describeResult)
	if len(columns) == 0 {
		result.CSV = ""
		s.logger.DebugContext(ctx, "schema overview sample skipped empty table schema", "table", table, "duration", time.Since(started))
		return result
	}

	sampleSQL := previewSampleSQL(tableIdentifier, columns)
	result.SQL = sampleSQL
	s.logger.DebugContext(ctx, "schema overview sample started", "table", table, "sql", sampleSQL)
	raw, err := s.runner.Query(ctx, sampleSQL, schemaSampleRows)
	if err != nil {
		result.Error = err.Error()
		result.CSV = errorCSV(result.Error)
		s.logger.ErrorContext(ctx, "schema overview sample failed", "table", table, "sql", sampleSQL, "error", err, "duration", time.Since(started))
		return result
	}
	rows, trunc := previewRows(columns, raw.Rows)
	result.Truncated = trunc
	result.CSV, err = rowsToCSV(columns, rows)
	if err != nil {
		result.Error = err.Error()
		result.CSV = errorCSV(result.Error)
		s.logger.ErrorContext(ctx, "schema overview sample encoding failed", "table", table, "sql", sampleSQL, "error", err, "duration", time.Since(started))
		return result
	}
	s.logger.InfoContext(ctx, "schema overview table sampled", "table", table, "rows", len(rows), "columns", len(columns), "truncated_fields", len(trunc.Fields), "duration", time.Since(started))
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
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(columns); err != nil {
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

func errorCSV(message string) string {
	out, err := rowsToCSV([]string{"error"}, []map[string]any{{"error": message}})
	if err != nil {
		return "error\n" + message
	}
	return out
}

func tableNames(result RawResult) []string {
	tables := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		name := rowString(row, "name")
		if name == "" && len(result.Columns) == 1 {
			name = rowString(row, result.Columns[0])
		}
		if name != "" {
			tables = append(tables, name)
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

func previewSampleSQL(tableIdentifier string, columns []string) string {
	expressions := make([]string, 0, 2*len(columns))
	for i, column := range columns {
		identifier := quoteClickHouseIdentifier(column)
		lengthAlias := quoteClickHouseIdentifier(previewLengthColumn(i))
		expressions = append(expressions,
			fmt.Sprintf("substring(toString(%s), 1, %d) AS %s", identifier, schemaSampleFieldChars, identifier),
			fmt.Sprintf("length(toString(%s)) AS %s", identifier, lengthAlias),
		)
	}
	return fmt.Sprintf("SELECT %s FROM %s LIMIT %d", strings.Join(expressions, ", "), tableIdentifier, schemaSampleRows)
}

func previewRows(columns []string, rows []map[string]any) ([]map[string]any, Truncation) {
	trunc := Truncation{MaxRows: schemaSampleRows, MaxFieldChars: schemaSampleFieldChars}
	out := make([]map[string]any, 0, len(rows))
	for rowIndex, row := range rows {
		copied := make(map[string]any, len(columns))
		for columnIndex, column := range columns {
			preview := csvValue(row[column])
			copied[column] = preview
			originalChars := intValue(row[previewLengthColumn(columnIndex)])
			if originalChars > schemaSampleFieldChars {
				trunc.Fields = append(trunc.Fields, FieldTruncation{
					Row:      rowIndex,
					Column:   column,
					Returned: utf8.RuneCountInString(preview),
					Total:    originalChars,
				})
			}
		}
		out = append(out, copied)
	}
	return out, trunc
}

func previewLengthColumn(index int) string {
	return fmt.Sprintf("__pdw_preview_len_%d", index)
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

func quoteClickHouseIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func intValue(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	case float32:
		return int(v)
	case float64:
		return int(v)
	case json.Number:
		n, _ := v.Int64()
		return int(n)
	case string:
		n, _ := strconv.Atoi(v)
		return n
	default:
		return 0
	}
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
	size := int64(len(entry.ID) + len(entry.SQL) + len(entry.Format))
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
