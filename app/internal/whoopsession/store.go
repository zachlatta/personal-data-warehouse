// Package whoopsession persists the app.whoop.com browser session that the
// local `pdw whoop publish-session` helper captures, so the server-side Dagster
// poller can authenticate to WHOOP's private API.
//
// WHOOP enforces MFA, so there is no unattended login: the browser session is
// the credential. It is a 24h access token plus a 30-day refresh token, and
// every refresh issues a NEW refresh token -- so the poller, not this endpoint,
// keeps the credential alive. This table is the handoff point between the two.
//
// The table is the same private.whoop_private_sessions the Python warehouse
// manages; the CREATE here is the idempotent twin so the very first publish
// (before any poll has run) succeeds.
package whoopsession

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

// Ack is the secret-free acknowledgement returned to the client.
type Ack struct {
	Account            string    `json:"account"`
	SessionKey         string    `json:"session_key"`
	SourceBrowser      string    `json:"source_browser"`
	RefreshTokenSHA256 string    `json:"refresh_token_sha256"`
	AccessExpiresAt    time.Time `json:"access_expires_at"`
	RefreshExpiresAt   time.Time `json:"refresh_expires_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// Store upserts a session credential keyed by (account, session_key).
type Store interface {
	Upsert(ctx context.Context, cred Credential, now time.Time) (Ack, error)
}

// Credential is one captured browser session.
type Credential struct {
	Account          string
	SessionKey       string
	AccessToken      string
	RefreshToken     string
	AccessExpiresAt  time.Time
	RefreshExpiresAt time.Time
	SourceBrowser    string
}

type PostgresStore struct {
	db      *sql.DB
	timeout time.Duration
}

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

// Take the schema from the catalog rather than restating it, so a catalog move
// cannot leave the app provisioning an orphan schema.
var createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS " +
	warehouse.QuoteIdent(warehouse.SchemaOf("whoop_private_sessions"))

var createTableSQL = `
CREATE TABLE IF NOT EXISTS ` + warehouse.SQLRelation("whoop_private_sessions") + ` (
    account text NOT NULL,
    session_key text NOT NULL DEFAULT 'default',
    access_token text NOT NULL DEFAULT '',
    refresh_token text NOT NULL DEFAULT '',
    access_expires_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
    refresh_expires_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
    refresh_token_sha256 text NOT NULL DEFAULT '',
    source_browser text NOT NULL DEFAULT '',
    published_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    sync_version bigint NOT NULL DEFAULT 1,
    status text NOT NULL DEFAULT 'ok',
    error text NOT NULL DEFAULT '',
    PRIMARY KEY (account, session_key)
)`

// A publish is an explicit human repair action, so it always wins over whatever
// the poller last rotated in, and it clears any action_required state.
var upsertSQL = `
INSERT INTO ` + warehouse.SQLRelation("whoop_private_sessions") + ` (
    account, session_key, access_token, refresh_token, access_expires_at,
    refresh_expires_at, refresh_token_sha256, source_browser,
    published_at, updated_at, sync_version, status, error
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10, 'ok', '')
ON CONFLICT (account, session_key) DO UPDATE SET
    access_token = EXCLUDED.access_token,
    refresh_token = EXCLUDED.refresh_token,
    access_expires_at = EXCLUDED.access_expires_at,
    refresh_expires_at = EXCLUDED.refresh_expires_at,
    refresh_token_sha256 = EXCLUDED.refresh_token_sha256,
    source_browser = EXCLUDED.source_browser,
    published_at = EXCLUDED.published_at,
    updated_at = EXCLUDED.updated_at,
    sync_version = EXCLUDED.sync_version,
    status = 'ok',
    error = ''`

func (s *PostgresStore) Upsert(ctx context.Context, cred Credential, now time.Time) (Ack, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, createSchemaSQL); err != nil {
		return Ack{}, fmt.Errorf("ensure private schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, createTableSQL); err != nil {
		return Ack{}, fmt.Errorf("ensure whoop_private_sessions: %w", err)
	}
	sum := sha256.Sum256([]byte(cred.RefreshToken))
	refreshSHA := hex.EncodeToString(sum[:])
	now = now.UTC()
	if _, err := s.db.ExecContext(ctx, upsertSQL,
		cred.Account, cred.SessionKey, cred.AccessToken, cred.RefreshToken,
		cred.AccessExpiresAt.UTC(), cred.RefreshExpiresAt.UTC(), refreshSHA,
		cred.SourceBrowser, now, now.UnixMicro(),
	); err != nil {
		return Ack{}, fmt.Errorf("upsert whoop_private_sessions: %w", err)
	}
	return Ack{
		Account:            cred.Account,
		SessionKey:         cred.SessionKey,
		SourceBrowser:      cred.SourceBrowser,
		RefreshTokenSHA256: refreshSHA,
		AccessExpiresAt:    cred.AccessExpiresAt.UTC(),
		RefreshExpiresAt:   cred.RefreshExpiresAt.UTC(),
		UpdatedAt:          now,
	}, nil
}
