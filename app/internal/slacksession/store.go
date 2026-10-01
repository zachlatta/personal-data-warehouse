// Package slacksession persists the Slack *client* session that Zach pastes
// into `pdw slack publish-session`. The sync polls Slack with it (rate-limited far
// less than the workspace token) and reviewed Slack writes spend it too.
//
// The credential is two parts that are useless apart: an `xoxc-` token and the
// `d` cookie. Both are stored here because both must be replayed together.
//
// The table is the same private.slack_sessions the Python warehouse manages;
// the CREATE here is the idempotent twin so the very first publish succeeds
// before any sync has run.
package slacksession

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
	Account         string    `json:"account"`
	SessionKey      string    `json:"session_key"`
	SourceApp       string    `json:"source_app"`
	TokenSHA256     string    `json:"token_sha256"`
	TeamID          string    `json:"team_id"`
	EnterpriseID    string    `json:"enterprise_id"`
	UserID          string    `json:"user_id"`
	CookieExpiresAt time.Time `json:"cookie_expires_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// Store upserts a session credential keyed by (account, session_key).
type Store interface {
	Upsert(ctx context.Context, cred Credential, now time.Time) (Ack, error)
}

// Credential is one captured Slack client session.
type Credential struct {
	Account       string
	SessionKey    string
	SessionToken  string
	SessionCookie string
	TeamID        string
	EnterpriseID  string
	UserID        string
	TeamURL       string
	SourceApp     string
	// UserAgent is the browser that minted the session; the sync sends it with
	// every request so Slack sees the session's own browser, not a stand-in.
	UserAgent       string
	CookieExpiresAt time.Time
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
	warehouse.QuoteIdent(warehouse.SchemaOf("slack_sessions"))

var createTableSQL = `
CREATE TABLE IF NOT EXISTS ` + warehouse.SQLRelation("slack_sessions") + ` (
    account text NOT NULL,
    session_key text NOT NULL DEFAULT 'default',
    session_token text NOT NULL DEFAULT '',
    session_cookie text NOT NULL DEFAULT '',
    token_sha256 text NOT NULL DEFAULT '',
    team_id text NOT NULL DEFAULT '',
    enterprise_id text NOT NULL DEFAULT '',
    user_id text NOT NULL DEFAULT '',
    team_url text NOT NULL DEFAULT '',
    source_app text NOT NULL DEFAULT '',
    user_agent text NOT NULL DEFAULT '',
    cookie_expires_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
    published_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    sync_version bigint NOT NULL DEFAULT 1,
    status text NOT NULL DEFAULT 'ok',
    error text NOT NULL DEFAULT '',
    PRIMARY KEY (account, session_key)
)`

// CREATE TABLE IF NOT EXISTS never revisits a table provisioned before a
// column existed; the Python ensure path reconciles the same column.
var addUserAgentSQL = "ALTER TABLE " + warehouse.SQLRelation("slack_sessions") +
	" ADD COLUMN IF NOT EXISTS user_agent text NOT NULL DEFAULT ''"

// A publish is an explicit human repair action, so it always wins over whatever
// the sync last recorded, and it clears any action_required state.
var upsertSQL = `
INSERT INTO ` + warehouse.SQLRelation("slack_sessions") + ` (
    account, session_key, session_token, session_cookie, token_sha256,
    team_id, enterprise_id, user_id, team_url, source_app, user_agent,
    cookie_expires_at, published_at, updated_at, sync_version, status, error
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $14, $11, $12, $12, $13, 'ok', '')
ON CONFLICT (account, session_key) DO UPDATE SET
    session_token = EXCLUDED.session_token,
    session_cookie = EXCLUDED.session_cookie,
    token_sha256 = EXCLUDED.token_sha256,
    team_id = EXCLUDED.team_id,
    enterprise_id = EXCLUDED.enterprise_id,
    user_id = EXCLUDED.user_id,
    team_url = EXCLUDED.team_url,
    source_app = EXCLUDED.source_app,
    user_agent = EXCLUDED.user_agent,
    cookie_expires_at = EXCLUDED.cookie_expires_at,
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
		return Ack{}, fmt.Errorf("ensure slack_sessions: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, addUserAgentSQL); err != nil {
		return Ack{}, fmt.Errorf("ensure slack_sessions.user_agent: %w", err)
	}
	sum := sha256.Sum256([]byte(cred.SessionToken))
	tokenSHA := hex.EncodeToString(sum[:])
	now = now.UTC()
	if _, err := s.db.ExecContext(ctx, upsertSQL,
		cred.Account, cred.SessionKey, cred.SessionToken, cred.SessionCookie, tokenSHA,
		cred.TeamID, cred.EnterpriseID, cred.UserID, cred.TeamURL, cred.SourceApp,
		cred.CookieExpiresAt.UTC(), now, now.UnixMicro(), cred.UserAgent,
	); err != nil {
		return Ack{}, fmt.Errorf("upsert slack_sessions: %w", err)
	}
	return Ack{
		Account:         cred.Account,
		SessionKey:      cred.SessionKey,
		SourceApp:       cred.SourceApp,
		TokenSHA256:     tokenSHA,
		TeamID:          cred.TeamID,
		EnterpriseID:    cred.EnterpriseID,
		UserID:          cred.UserID,
		CookieExpiresAt: cred.CookieExpiresAt.UTC(),
		UpdatedAt:       now,
	}, nil
}
