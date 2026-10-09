package accesstokens

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// epoch is the warehouse's "never" (see "Absence is the epoch, not NULL").
var epoch = time.Unix(0, 0).UTC()

// The Python warehouse creates the same table in
// ensure_app_access_token_tables (postgres.py); this is the idempotent twin so
// the first token works before any Dagster run.
var ensureStatements = []string{
	"CREATE SCHEMA IF NOT EXISTS " + warehouse.QuoteIdent(warehouse.SchemaOf("app_access_tokens")),
	`CREATE TABLE IF NOT EXISTS @app_access_tokens (
		id text PRIMARY KEY,
		kind text NOT NULL,
		scope text NOT NULL,
		client_name text NOT NULL DEFAULT '',
		label text NOT NULL DEFAULT '',
		created_by text NOT NULL DEFAULT '',
		secret_sha256 text NOT NULL,
		created_at timestamptz NOT NULL DEFAULT now(),
		last_used_at timestamptz NOT NULL DEFAULT now(),
		idle_timeout_seconds bigint NOT NULL DEFAULT 0,
		expires_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		revoked_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		grant_ttl_seconds bigint NOT NULL DEFAULT 0,
		redeemed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		redeemed_token_id text NOT NULL DEFAULT '',
		updated_at timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS app_access_tokens_updated_idx ON @app_access_tokens (updated_at)`,
}

const tokenColumns = `id, kind, scope, client_name, label, created_by, secret_sha256, created_at, last_used_at,
	idle_timeout_seconds, expires_at, revoked_at, grant_ttl_seconds, redeemed_at, redeemed_token_id`

// PostgresStore keeps tokens in private.app_access_tokens.
type PostgresStore struct {
	db       *sql.DB
	timeout  time.Duration
	ensureMu sync.Mutex
	ensured  bool
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

// EnsureTables creates the table once per process.
func (s *PostgresStore) EnsureTables(ctx context.Context) error {
	s.ensureMu.Lock()
	defer s.ensureMu.Unlock()
	if s.ensured {
		return nil
	}
	for _, statement := range ensureStatements {
		if _, err := s.db.ExecContext(ctx, warehouse.ExpandRelations(statement)); err != nil {
			return fmt.Errorf("ensure app_access_tokens: %w", err)
		}
	}
	s.ensured = true
	return nil
}

func (s *PostgresStore) begin(ctx context.Context) (context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	if err := s.EnsureTables(ctx); err != nil {
		cancel()
		return nil, nil, err
	}
	return ctx, cancel, nil
}

func (s *PostgresStore) Insert(ctx context.Context, token Token, hash string) error {
	ctx, cancel, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return insertToken(ctx, s.db, token, hash)
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insertToken(ctx context.Context, db execer, token Token, hash string) error {
	_, err := db.ExecContext(ctx, warehouse.ExpandRelations(`
		INSERT INTO @app_access_tokens (`+tokenColumns+`, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $8)`),
		token.ID, token.Kind, string(token.Scope), token.ClientName, token.Label, token.CreatedBy, hash,
		token.CreatedAt.UTC(), token.LastUsedAt.UTC(), int64(token.IdleTimeout/time.Second),
		toDB(token.ExpiresAt), toDB(token.RevokedAt), int64(token.GrantTTL/time.Second),
		toDB(token.RedeemedAt), token.RedeemedTokenID)
	return err
}

type scanner interface{ Scan(dest ...any) error }

func scanToken(row scanner) (Token, string, error) {
	var t Token
	var scope, hash string
	var idle, grant int64
	var expires, revoked, redeemed time.Time
	if err := row.Scan(&t.ID, &t.Kind, &scope, &t.ClientName, &t.Label, &t.CreatedBy, &hash, &t.CreatedAt, &t.LastUsedAt,
		&idle, &expires, &revoked, &grant, &redeemed, &t.RedeemedTokenID); err != nil {
		return Token{}, "", err
	}
	t.Scope = pdwauth.Scope(scope)
	t.IdleTimeout = time.Duration(idle) * time.Second
	t.GrantTTL = time.Duration(grant) * time.Second
	t.ExpiresAt, t.RevokedAt, t.RedeemedAt = fromDB(expires), fromDB(revoked), fromDB(redeemed)
	t.CreatedAt, t.LastUsedAt = t.CreatedAt.UTC(), t.LastUsedAt.UTC()
	return t, hash, nil
}

func (s *PostgresStore) Get(ctx context.Context, id string) (Token, string, error) {
	ctx, cancel, err := s.begin(ctx)
	if err != nil {
		return Token{}, "", err
	}
	defer cancel()
	token, hash, err := scanToken(s.db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT `+tokenColumns+` FROM @app_access_tokens WHERE id = $1`), id))
	if errors.Is(err, sql.ErrNoRows) {
		return Token{}, "", ErrNotFound
	}
	return token, hash, err
}

func (s *PostgresStore) Touch(ctx context.Context, id string, at time.Time) error {
	ctx, cancel, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	_, err = s.db.ExecContext(ctx, warehouse.ExpandRelations(
		`UPDATE @app_access_tokens SET last_used_at = $2, updated_at = $2 WHERE id = $1 AND last_used_at < $2`), id, at.UTC())
	return err
}

func (s *PostgresStore) List(ctx context.Context) ([]Token, error) {
	ctx, cancel, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	rows, err := s.db.QueryContext(ctx, warehouse.ExpandRelations(`SELECT `+tokenColumns+` FROM @app_access_tokens ORDER BY created_at DESC, id`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Token
	for rows.Next() {
		token, _, err := scanToken(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, token)
	}
	return out, rows.Err()
}

func (s *PostgresStore) Revoke(ctx context.Context, id string, at time.Time) error {
	ctx, cancel, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	var found bool
	err = s.db.QueryRowContext(ctx, warehouse.ExpandRelations(`
		WITH target AS (SELECT id FROM @app_access_tokens WHERE id = $1),
		updated AS (
			UPDATE @app_access_tokens SET revoked_at = $2, updated_at = $2
			WHERE id = $1 AND revoked_at = '1970-01-01 00:00:00+00'::timestamptz
		)
		SELECT EXISTS (SELECT 1 FROM target)`), id, at.UTC()).Scan(&found)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	return nil
}

// Redeem holds the code's row lock across the check, the insert of the minted
// token and the redeemed stamp, so of concurrent redemptions exactly one mints.
func (s *PostgresStore) Redeem(ctx context.Context, codeID string, mint func(Token, string) (Token, error), mintedHash string, at time.Time) (Token, error) {
	ctx, cancel, err := s.begin(ctx)
	if err != nil {
		return Token{}, err
	}
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Token{}, err
	}
	defer func() { _ = tx.Rollback() }()
	code, hash, err := scanToken(tx.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT `+tokenColumns+` FROM @app_access_tokens WHERE id = $1 FOR UPDATE`), codeID))
	if errors.Is(err, sql.ErrNoRows) {
		return Token{}, ErrNotFound
	}
	if err != nil {
		return Token{}, err
	}
	minted, err := mint(code, hash)
	if err != nil {
		return Token{}, err
	}
	if err := insertToken(ctx, tx, minted, mintedHash); err != nil {
		return Token{}, err
	}
	if _, err := tx.ExecContext(ctx, warehouse.ExpandRelations(
		`UPDATE @app_access_tokens SET redeemed_at = $2, redeemed_token_id = $3, updated_at = $2 WHERE id = $1`),
		codeID, at.UTC(), minted.ID); err != nil {
		return Token{}, err
	}
	if err := tx.Commit(); err != nil {
		return Token{}, err
	}
	return minted, nil
}

func toDB(t time.Time) time.Time {
	if t.IsZero() {
		return epoch
	}
	return t.UTC()
}

func fromDB(t time.Time) time.Time {
	if !t.After(epoch) {
		return time.Time{}
	}
	return t.UTC()
}
