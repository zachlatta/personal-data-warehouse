package common

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
)

// SQLToolQuery runs a read-only statement through the app's `sql` tool (the
// same static-bearer HTTP API `pdw sql` uses) and returns the rows. Local
// helpers use it for the few warehouse reads they need (enriched voice memo
// titles, a Slack enterprise's workspaces) because a laptop has neither the
// warehouse credential nor a route to Postgres.
func SQLToolQuery(baseURL, clientName, token, question, statement string, timeout time.Duration) ([]map[string]any, error) {
	if clientName == "" {
		clientName = "pdw"
	}
	client, err := cliclient.New(baseURL, clientName, token)
	if err != nil {
		return nil, err
	}
	input, err := json.Marshal(map[string]string{
		"question": question,
		"sql":      statement,
		// The server's name for newline-delimited JSON; an unknown format
		// silently falls back to csv, so this string matters.
		"format": "ndjson",
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := client.CallTool(ctx, "sql", input)
	if err != nil {
		return nil, err
	}
	var payload struct {
		Error string          `json:"error"`
		Rows  json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		return nil, fmt.Errorf("decode sql tool response: %w", err)
	}
	if payload.Error != "" {
		return nil, errors.New(payload.Error)
	}
	var rowsText string
	if len(payload.Rows) > 0 {
		if err := json.Unmarshal(payload.Rows, &rowsText); err != nil {
			// Rows may already be a JSON array.
			var rows []map[string]any
			if err := json.Unmarshal(payload.Rows, &rows); err != nil {
				return nil, fmt.Errorf("decode sql tool rows: %w", err)
			}
			return rows, nil
		}
	}
	var rows []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(rowsText))
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("decode sql tool row: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, scanner.Err()
}

// SQLLiteral quotes a string for interpolation into a SQL statement.
func SQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
