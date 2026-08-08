// Package chatgptsession persists the chatgpt.com web session credential that
// the local `pdw chatgpt publish-session` helper captures, so the server-side
// Dagster poller can authenticate to the ChatGPT backend API.
//
// The table is the same chatgpt_sessions table the Python warehouse manages
// (see ensure_chatgpt_session_table in postgres.py); the CREATE here is the
// idempotent twin so the very first publish (before any poll) succeeds.
package chatgptsession

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// Ack is the (secret-free) acknowledgement returned to the client.
type Ack struct {
	Account       string    `json:"account"`
	SessionKey    string    `json:"session_key"`
	SourceBrowser string    `json:"source_browser"`
	TokenSHA256   string    `json:"token_sha256"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Store upserts a session credential keyed by (account, session_key).
type Store interface {
	Upsert(ctx context.Context, account, sessionKey, sessionToken, sourceBrowser string, now time.Time) (Ack, error)
}

// PostgresStore is the production Store.
type PostgresStore struct {
	db      *sql.DB
	timeout time.Duration
}

// NewPostgresStore opens a lazy connection pool (it does not dial until first
// use, so constructing one is safe even when the DB is unreachable).
func NewPostgresStore(databaseURL string, timeout time.Duration) (*PostgresStore, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{db: db, timeout: timeout}, nil
}

func (s *PostgresStore) Close() error { return s.db.Close() }

// Take the schema from the catalog rather than restating it, so a catalog
// move cannot leave the app provisioning an orphan schema.
var createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS " +
	warehouse.QuoteIdent(warehouse.SchemaOf("chatgpt_sessions"))

var createTableSQL = `
CREATE TABLE IF NOT EXISTS ` + warehouse.SQLRelation("chatgpt_sessions") + ` (
    account text NOT NULL,
    session_key text NOT NULL DEFAULT 'default',
    session_token text NOT NULL DEFAULT '',
    source_browser text NOT NULL DEFAULT '',
    token_sha256 text NOT NULL DEFAULT '',
    published_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    sync_version bigint NOT NULL DEFAULT 1,
    expired_at timestamptz,
    expired_token_sha256 text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'ok',
    error text NOT NULL DEFAULT '',
    PRIMARY KEY (account, session_key)
)`

var ensureColumnSQL = []string{
	`ALTER TABLE ` + warehouse.SQLRelation("chatgpt_sessions") + ` ADD COLUMN IF NOT EXISTS expired_at timestamptz`,
	`ALTER TABLE ` + warehouse.SQLRelation("chatgpt_sessions") + ` ADD COLUMN IF NOT EXISTS expired_token_sha256 text NOT NULL DEFAULT ''`,
	`ALTER TABLE ` + warehouse.SQLRelation("chatgpt_sessions") + ` ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'ok'`,
	`ALTER TABLE ` + warehouse.SQLRelation("chatgpt_sessions") + ` ADD COLUMN IF NOT EXISTS error text NOT NULL DEFAULT ''`,
}

var upsertSQL = `
INSERT INTO ` + warehouse.SQLRelation("chatgpt_sessions") + ` (
    account, session_key, session_token, source_browser, token_sha256,
    published_at, updated_at, sync_version
) VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
ON CONFLICT (account, session_key) DO UPDATE SET
    session_token = EXCLUDED.session_token,
    source_browser = EXCLUDED.source_browser,
    token_sha256 = EXCLUDED.token_sha256,
    published_at = EXCLUDED.published_at,
    updated_at = EXCLUDED.updated_at,
    sync_version = EXCLUDED.sync_version`

func (s *PostgresStore) Upsert(ctx context.Context, account, sessionKey, sessionToken, sourceBrowser string, now time.Time) (Ack, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, createSchemaSQL); err != nil {
		return Ack{}, fmt.Errorf("ensure private schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, createTableSQL); err != nil {
		return Ack{}, fmt.Errorf("ensure chatgpt_sessions: %w", err)
	}
	for _, statement := range ensureColumnSQL {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return Ack{}, fmt.Errorf("migrate chatgpt_sessions health state: %w", err)
		}
	}
	sum := sha256.Sum256([]byte(sessionToken))
	tokenSHA := hex.EncodeToString(sum[:])
	now = now.UTC()
	if _, err := s.db.ExecContext(ctx, upsertSQL,
		account, sessionKey, sessionToken, sourceBrowser, tokenSHA, now, now.UnixMicro(),
	); err != nil {
		return Ack{}, fmt.Errorf("upsert chatgpt_sessions: %w", err)
	}
	return Ack{
		Account:       account,
		SessionKey:    sessionKey,
		SourceBrowser: sourceBrowser,
		TokenSHA256:   tokenSHA,
		UpdatedAt:     now,
	}, nil
}
