package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresRunner struct {
	db           *sql.DB
	queryTimeout time.Duration
	queryRole    string
}

func NewPostgresRunner(databaseURL string, timeout time.Duration) (*PostgresRunner, error) {
	return NewPostgresRunnerWithRole(databaseURL, timeout, "")
}

func NewPostgresRunnerWithRole(databaseURL string, timeout time.Duration, queryRole string) (*PostgresRunner, error) {
	if _, err := queryRoleSQL(queryRole); err != nil {
		return nil, err
	}
	logger := slog.Default().With("component", "postgres")
	started := time.Now()
	logger.Info("opening Postgres connection", "timeout", timeout)
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		logger.Error("Postgres open failed", "error", err)
		return nil, err
	}
	effectiveTimeout := timeout
	if effectiveTimeout <= 0 {
		effectiveTimeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), effectiveTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		logger.Error("Postgres ping failed", "error", err, "duration", time.Since(started))
		return nil, err
	}
	logger.Info("Postgres connection ready", "duration", time.Since(started))
	return &PostgresRunner{db: db, queryTimeout: effectiveTimeout, queryRole: queryRole}, nil
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

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		logger.ErrorContext(ctx, "Postgres begin read-only tx failed", "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	timeoutMs := r.queryTimeout.Milliseconds()
	if timeoutMs <= 0 {
		timeoutMs = 30000
	}
	// SET LOCAL is effective here because every query runs inside an explicit
	// transaction; under autocommit it would only affect the SET statement.
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL statement_timeout = %d", timeoutMs)); err != nil {
		logger.ErrorContext(ctx, "Postgres set statement_timeout failed", "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}
	if roleSQL, err := queryRoleSQL(r.queryRole); err != nil {
		return RawResult{}, err
	} else if roleSQL != "" {
		if _, err := tx.ExecContext(ctx, roleSQL); err != nil {
			logger.ErrorContext(ctx, "Postgres assume query role failed", "role", r.queryRole, "error", err, "duration", time.Since(started))
			return RawResult{}, err
		}
	}

	rows, err := tx.QueryContext(ctx, statement, args...)
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

var postgresRoleIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func queryRoleSQL(role string) (string, error) {
	if role == "" {
		return "", nil
	}
	if !postgresRoleIdentifier.MatchString(role) {
		return "", fmt.Errorf("query Postgres role must be a valid identifier")
	}
	return `SET LOCAL ROLE "` + role + `"`, nil
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
