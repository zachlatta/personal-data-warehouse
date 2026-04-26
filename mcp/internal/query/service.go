package query

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

type Options struct {
	MaxRows       int
	MaxFieldChars int
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
}

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
	return &Service{runner: runner, opts: opts}
}

func (s *Service) Execute(ctx context.Context, sql []string) Response {
	if len(sql) == 0 {
		err := "sql must contain at least one statement"
		return Response{Results: []Result{{Error: err, CSV: errorCSV(err)}}}
	}

	results := make([]Result, 0, len(sql))
	for _, statement := range sql {
		result := Result{SQL: statement}
		if err := ValidateReadOnlySQL(statement); err != nil {
			result.Error = err.Error()
			result.CSV = errorCSV(result.Error)
			results = append(results, result)
			continue
		}
		raw, err := s.runner.Query(ctx, statement, s.opts.MaxRows+1)
		if err != nil {
			result.Error = err.Error()
			result.CSV = errorCSV(result.Error)
			results = append(results, result)
			continue
		}
		rows, trunc := s.truncateRows(raw.Rows)
		result.Truncated = trunc
		result.CSV, err = rowsToCSV(raw.Columns, rows)
		if err != nil {
			result.Error = err.Error()
			result.CSV = errorCSV(result.Error)
		}
		results = append(results, result)
	}
	return Response{Results: results}
}

func (s *Service) truncateRows(rows []map[string]any) ([]map[string]any, Truncation) {
	trunc := Truncation{MaxRows: s.opts.MaxRows, MaxFieldChars: s.opts.MaxFieldChars}
	if len(rows) > s.opts.MaxRows {
		trunc.Rows = true
		rows = rows[:s.opts.MaxRows]
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
		Row:           rowIndex,
		Column:        column,
		ReturnedChars: s.opts.MaxFieldChars,
		OriginalChars: chars,
		Instructions:  fullFieldInstructions(column, s.opts.MaxFieldChars),
	})
	return string(runes[:s.opts.MaxFieldChars])
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
