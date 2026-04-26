package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseRunner struct {
	db *sql.DB
}

func NewClickHouseRunner(clickhouseURL string, timeout time.Duration) (*ClickHouseRunner, error) {
	logger := slog.Default().With("component", "clickhouse")
	started := time.Now()
	logger.Info("opening ClickHouse connection", "timeout", timeout)
	opts, err := clickhouse.ParseDSN(clickhouseURL)
	if err != nil {
		logger.Error("ClickHouse DSN parse failed", "error", err)
		return nil, err
	}
	if timeout > 0 {
		opts.DialTimeout = timeout
		opts.ReadTimeout = timeout
	}
	db := clickhouse.OpenDB(opts)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	if timeout <= 0 {
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	}
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		logger.Error("ClickHouse ping failed", "error", err, "duration", time.Since(started))
		return nil, err
	}
	logger.Info("ClickHouse connection ready", "duration", time.Since(started))
	return &ClickHouseRunner{db: db}, nil
}

func (r *ClickHouseRunner) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	err := r.db.Close()
	if err != nil {
		slog.Default().With("component", "clickhouse").Error("ClickHouse close failed", "error", err)
	} else {
		slog.Default().With("component", "clickhouse").Info("ClickHouse connection closed")
	}
	return err
}

func (r *ClickHouseRunner) Query(ctx context.Context, statement string, maxRows int) (RawResult, error) {
	logger := slog.Default().With("component", "clickhouse")
	started := time.Now()
	logger.DebugContext(ctx, "ClickHouse query dispatch", "sql", statement, "max_rows", maxRows)
	rows, err := r.db.QueryContext(ctx, statement)
	if err != nil {
		logger.ErrorContext(ctx, "ClickHouse query dispatch failed", "sql", statement, "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		logger.ErrorContext(ctx, "ClickHouse columns read failed", "sql", statement, "error", err, "duration", time.Since(started))
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
			logger.ErrorContext(ctx, "ClickHouse row scan failed", "sql", statement, "error", err, "duration", time.Since(started))
			return RawResult{}, err
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			row[column] = normalizeValue(values[i])
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		logger.ErrorContext(ctx, "ClickHouse rows iteration failed", "sql", statement, "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}
	logger.DebugContext(ctx, "ClickHouse query returned", "sql", statement, "rows", len(result.Rows), "columns", len(result.Columns), "duration", time.Since(started))
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
