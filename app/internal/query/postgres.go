package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresRunner struct {
	db           *sql.DB
	queryTimeout time.Duration
	queryRole    string
}

// Pool limits for the warehouse connection. database/sql defaults to
// MaxIdleConns=2 and MaxOpenConns=UNLIMITED, and both halves are wrong here.
// Unlimited lets a burst of concurrent tool calls race the warehouse's
// max_connections=60 -- which is shared with Dagster and every other client --
// and be refused by the server instead of queued in this process. Idle=2 means
// a third concurrent search opens a fresh connection across the tailnet, pays
// the TLS handshake, and then throws the connection away.
const (
	postgresMaxOpenConns    = 25
	postgresMaxIdleConns    = 10
	postgresConnMaxLifetime = 30 * time.Minute
)

func configurePool(db *sql.DB) {
	db.SetMaxOpenConns(postgresMaxOpenConns)
	db.SetMaxIdleConns(postgresMaxIdleConns)
	db.SetConnMaxLifetime(postgresConnMaxLifetime)
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
	configurePool(db)
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
	return r.QueryArgsWithTimeout(ctx, statement, args, maxRows, r.queryTimeout)
}

// QueryArgsWithTimeout runs like QueryArgs but with an explicit statement
// budget. The default budget is sized for the agent-facing query surface;
// app-internal background work with a legitimately longer runtime (the
// timeline sidebar's full-table count aggregates) passes its own.
func (r *PostgresRunner) QueryArgsWithTimeout(ctx context.Context, statement string, args []any, maxRows int, timeout time.Duration) (RawResult, error) {
	logger := slog.Default().With("component", "postgres")
	started := time.Now()
	logger.DebugContext(ctx, "Postgres query dispatch", "sql", statement, "max_rows", maxRows)

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		logger.ErrorContext(ctx, "Postgres begin read-only tx failed", "error", err, "duration", time.Since(started))
		return RawResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	timeoutMs := timeout.Milliseconds()
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
		err = withPostgresDiagnostics(err)
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

// postgresError carries a Postgres error whose text has been widened to
// include the server's own DETAIL and HINT.
type postgresError struct {
	err  error
	text string
}

func (e *postgresError) Error() string { return e.text }
func (e *postgresError) Unwrap() error { return e.err }

// withPostgresDiagnostics restores the diagnostics pgx drops on the floor.
// pgconn.PgError.Error() renders only "severity: message (SQLSTATE code)", so
// the server's DETAIL and — far more usefully — its HINT never reached callers.
// Postgres computes a Levenshtein-based suggestion for undefined columns and
// tables ("Perhaps you meant to reference the column messages.message_at"),
// which is exactly the answer for the long tail of one-off wrong column names
// that no hand-maintained remap table can cover. It was there all along and we
// were discarding it.
func withPostgresDiagnostics(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	var extra []string
	if pgErr.Detail != "" {
		extra = append(extra, "DETAIL: "+pgErr.Detail)
	}
	if pgErr.Hint != "" {
		extra = append(extra, "HINT: "+pgErr.Hint)
	}
	if len(extra) == 0 {
		return err
	}
	return &postgresError{err: err, text: pgErr.Error() + " " + strings.Join(extra, " ")}
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
