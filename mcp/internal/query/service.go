package query

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"
)

type Options struct {
	MaxRows       int
	MaxFieldChars int
	Logger        *slog.Logger
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
}

const schemaSampleRows = 3
const schemaSampleFieldChars = 15

type Response struct {
	Results []Result `json:"results"`
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
	Row           int    `json:"row"`
	Column        string `json:"column"`
	ReturnedChars int    `json:"returned_chars"`
	OriginalChars int    `json:"original_chars"`
	Instructions  string `json:"instructions"`
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
			"returned_chars":  field.ReturnedChars,
			"original_chars":  field.OriginalChars,
			"max_rows":        t.MaxRows,
			"max_field_chars": t.MaxFieldChars,
			"instructions":    field.Instructions,
		})
	}
	out, err := rowsToCSV([]string{"type", "row", "column", "returned_chars", "original_chars", "max_rows", "max_field_chars", "instructions"}, rows)
	if err != nil {
		return ""
	}
	return out
}

func NewService(runner Runner, opts Options) *Service {
	if opts.MaxRows <= 0 {
		opts.MaxRows = 100
	}
	if opts.MaxFieldChars <= 0 {
		opts.MaxFieldChars = 4000
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{runner: runner, opts: opts, logger: logger.With("component", "query")}
}

func (s *Service) Execute(ctx context.Context, sql []string) Response {
	if len(sql) == 0 {
		err := "sql must contain at least one statement"
		s.logger.WarnContext(ctx, "query batch rejected", "error", err)
		return Response{Results: []Result{{Error: err, CSV: errorCSV(err)}}}
	}

	started := time.Now()
	s.logger.InfoContext(ctx, "query batch started", "statements", len(sql), "max_rows", s.opts.MaxRows, "max_field_chars", s.opts.MaxFieldChars)
	results := make([]Result, 0, len(sql))
	for _, statement := range sql {
		queryStarted := time.Now()
		result := Result{SQL: statement}
		if err := ValidateReadOnlySQL(statement); err != nil {
			result.Error = err.Error()
			result.CSV = errorCSV(result.Error)
			results = append(results, result)
			s.logger.WarnContext(ctx, "query rejected by read-only validator", "sql", statement, "error", result.Error)
			continue
		}
		s.logger.DebugContext(ctx, "query started", "sql", statement)
		raw, err := s.runner.Query(ctx, statement, s.opts.MaxRows+1)
		if err != nil {
			result.Error = err.Error()
			result.CSV = errorCSV(result.Error)
			results = append(results, result)
			s.logger.ErrorContext(ctx, "query failed", "sql", statement, "error", err, "duration", time.Since(queryStarted))
			continue
		}
		rows, trunc := s.truncateRows(raw.Rows)
		result.Truncated = trunc
		result.CSV, err = rowsToCSV(raw.Columns, rows)
		if err != nil {
			result.Error = err.Error()
			result.CSV = errorCSV(result.Error)
			s.logger.ErrorContext(ctx, "query result encoding failed", "sql", statement, "error", err, "duration", time.Since(queryStarted))
		} else {
			s.logger.InfoContext(ctx, "query completed", "sql", statement, "rows", len(rows), "columns", len(raw.Columns), "truncated_rows", trunc.Rows, "truncated_fields", len(trunc.Fields), "duration", time.Since(queryStarted))
		}
		results = append(results, result)
	}
	s.logger.InfoContext(ctx, "query batch completed", "statements", len(sql), "duration", time.Since(started))
	return Response{Results: results}
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
	for i, table := range tables {
		sample := s.sampleRows(ctx, table)
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

func (s *Service) sampleRows(ctx context.Context, table string) Result {
	sampleSQL := fmt.Sprintf("SELECT * FROM %s LIMIT %d", quoteClickHouseIdentifier(table), schemaSampleRows)
	started := time.Now()
	result := Result{SQL: sampleSQL}
	s.logger.DebugContext(ctx, "schema overview sample started", "table", table, "sql", sampleSQL)
	raw, err := s.runner.Query(ctx, sampleSQL, schemaSampleRows)
	if err != nil {
		result.Error = err.Error()
		result.CSV = errorCSV(result.Error)
		s.logger.ErrorContext(ctx, "schema overview sample failed", "table", table, "sql", sampleSQL, "error", err, "duration", time.Since(started))
		return result
	}
	rows, trunc := s.truncateSampleRows(raw.Rows)
	result.Truncated = trunc
	result.CSV, err = rowsToCSV(raw.Columns, rows)
	if err != nil {
		result.Error = err.Error()
		result.CSV = errorCSV(result.Error)
		s.logger.ErrorContext(ctx, "schema overview sample encoding failed", "table", table, "sql", sampleSQL, "error", err, "duration", time.Since(started))
		return result
	}
	s.logger.DebugContext(ctx, "schema overview sample completed", "table", table, "rows", len(rows), "columns", len(raw.Columns), "truncated_rows", trunc.Rows, "truncated_fields", len(trunc.Fields), "duration", time.Since(started))
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

func (s *Service) truncateSampleRows(rows []map[string]any) ([]map[string]any, Truncation) {
	trunc := Truncation{MaxRows: schemaSampleRows, MaxFieldChars: schemaSampleFieldChars}
	if len(rows) > schemaSampleRows {
		trunc.Rows = true
		rows = rows[:schemaSampleRows]
	}
	out := make([]map[string]any, 0, len(rows))
	for rowIndex, row := range rows {
		copied := make(map[string]any, len(row))
		for column, value := range row {
			copied[column] = s.truncatePreviewValue(rowIndex, column, csvValue(value), &trunc)
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
		Row:           rowIndex,
		Column:        column,
		ReturnedChars: s.opts.MaxFieldChars,
		OriginalChars: chars,
		Instructions:  fullFieldInstructions(column, s.opts.MaxFieldChars),
	})
	return string(runes[:s.opts.MaxFieldChars])
}

func (s *Service) truncatePreviewValue(rowIndex int, column, value string, trunc *Truncation) string {
	chars := utf8.RuneCountInString(value)
	if chars <= schemaSampleFieldChars {
		return value
	}
	runes := []rune(value)
	trunc.Fields = append(trunc.Fields, FieldTruncation{
		Row:           rowIndex,
		Column:        column,
		ReturnedChars: schemaSampleFieldChars,
		OriginalChars: chars,
		Instructions:  fullFieldInstructions(column, schemaSampleFieldChars),
	})
	return string(runes[:schemaSampleFieldChars])
}

func fullFieldInstructions(column string, chunkSize int) string {
	return fmt.Sprintf("This field was truncated. To retrieve it fully, first query length(%s), then request chunks with substring(%s, start, %d) using 1-based start offsets smaller than the reported length. Alternatively raise MCP_MAX_FIELD_CHARS on the server.", column, column, chunkSize)
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
