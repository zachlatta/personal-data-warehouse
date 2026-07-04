package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresRunner struct {
	db *sql.DB
}

func NewPostgresRunner(databaseURL string, timeout time.Duration) (*PostgresRunner, error) {
	logger := slog.Default().With("component", "postgres")
	started := time.Now()
	logger.Info("opening Postgres connection", "timeout", timeout)
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		logger.Error("Postgres open failed", "error", err)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	if timeout <= 0 {
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	}
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		logger.Error("Postgres ping failed", "error", err, "duration", time.Since(started))
		return nil, err
	}
	logger.Info("Postgres connection ready", "duration", time.Since(started))
	return &PostgresRunner{db: db}, nil
}

func (r *PostgresRunner) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	err := r.db.Close()
	if err != nil {
		slog.Default().With("component", "postgres").Error("Postgres close failed", "error", err)
	} else {
		slog.Default().With("component", "postgres").Info("Postgres connection closed")
	}
	return err
}

func (r *PostgresRunner) Query(ctx context.Context, statement string, maxRows int) (RawResult, error) {
	return r.QueryArgs(ctx, statement, nil, maxRows)
}

// QueryArgs runs a parameterized statement ($1-style placeholders). App-owned
// SQL (like the timeline endpoints) uses this so caller-supplied values ride
// as bind parameters instead of being spliced into SQL text.
func (r *PostgresRunner) QueryArgs(ctx context.Context, statement string, args []any, maxRows int) (RawResult, error) {
	logger := slog.Default().With("component", "postgres")
	started := time.Now()
	logger.DebugContext(ctx, "Postgres query dispatch", "sql", statement, "max_rows", maxRows)
	rows, err := r.db.QueryContext(ctx, statement, args...)
	if err != nil {
		logger.ErrorContext(ctx, "Postgres query dispatch failed", "sql", statement, "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		logger.ErrorContext(ctx, "Postgres columns read failed", "sql", statement, "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}
	result := RawResult{Columns: columns}
	for rows.Next() {
		if maxRows > 0 && len(result.Rows) >= maxRows {
			break
		}
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			logger.ErrorContext(ctx, "Postgres row scan failed", "sql", statement, "error", err, "duration", time.Since(started))
			return RawResult{}, err
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			row[column] = normalizeValue(values[i])
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		logger.ErrorContext(ctx, "Postgres rows iteration failed", "sql", statement, "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}
	logger.DebugContext(ctx, "Postgres query returned", "sql", statement, "rows", len(result.Rows), "columns", len(result.Columns), "duration", time.Since(started))
	return result, nil
}

func normalizeValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	}
	if _, err := json.Marshal(value); err == nil {
		return value
	}
	return fmt.Sprint(value)
}
