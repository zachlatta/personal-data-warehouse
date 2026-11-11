package externalfeeds

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// ensureStatements mirrors Python's TableSpec and ensure_external_feed_tables.
var ensureStatements = []string{
	"CREATE SCHEMA IF NOT EXISTS " + warehouse.QuoteIdent(warehouse.SchemaOf("external_feed_items")),
	`CREATE SEQUENCE IF NOT EXISTS @external_feed_seq`,
	`CREATE TABLE IF NOT EXISTS @external_feed_items (
 feed text NOT NULL DEFAULT '',item_key text NOT NULL DEFAULT '',sync_version bigint NOT NULL DEFAULT 0,
 payload_json text NOT NULL DEFAULT '',recorded_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00',
 PRIMARY KEY(feed,item_key))`,
	`CREATE INDEX IF NOT EXISTS external_feed_items_cursor_idx ON @external_feed_items(feed,sync_version)`,
}

type PostgresStore struct {
	db      *sql.DB
	mu      sync.Mutex
	ensured bool
	expand  func(string) string
}

func NewPostgresStore(url string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{db: db, expand: warehouse.ExpandRelations}, nil
}
func (s *PostgresStore) Close() error { return s.db.Close() }
func (s *PostgresStore) ensure(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ensured {
		return nil
	}
	for _, q := range ensureStatements {
		if _, err := s.db.ExecContext(ctx, s.expand(q)); err != nil {
			return err
		}
	}
	s.ensured = true
	return nil
}

func (s *PostgresStore) Page(ctx context.Context, feed Definition, since uint64, limit int) ([]map[string]any, uint64, bool, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, since, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, since, false, err
	}
	defer tx.Rollback()
	// Every refresh takes the same transaction lock BEFORE allocating arrival
	// numbers. A later reader cannot advance past an earlier uncommitted arrival.
	// Scope the lock by physical table so isolated warehouses do not contend.
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, s.expand("@external_feed_items")); err != nil {
		return nil, since, false, err
	}
	source := s.expand(feed.SourceSQL)
	_, err = tx.ExecContext(ctx, s.expand(`INSERT INTO @external_feed_items AS old(feed,item_key,sync_version,payload_json,recorded_at)
 SELECT $1,item_key,nextval('@external_feed_seq'::regclass),payload_json,recorded_at FROM (`)+source+`) eligible
 ON CONFLICT(feed,item_key) DO UPDATE SET sync_version=EXCLUDED.sync_version,payload_json=EXCLUDED.payload_json,recorded_at=EXCLUDED.recorded_at
 WHERE old.payload_json IS DISTINCT FROM EXCLUDED.payload_json`, feed.Scope)
	if err != nil {
		return nil, since, false, err
	}
	// Removing a stale transcription, deleted memo or aged-out item cannot move
	// another item's cursor. If it becomes ready again it gets a NEW arrival.
	_, err = tx.ExecContext(ctx, s.expand(`DELETE FROM @external_feed_items stored WHERE feed=$1 AND NOT EXISTS (SELECT 1 FROM (`)+source+`) eligible WHERE eligible.item_key=stored.item_key)`, feed.Scope)
	if err != nil {
		return nil, since, false, err
	}
	result, err := tx.QueryContext(ctx, s.expand(`SELECT sync_version,payload_json FROM @external_feed_items WHERE feed=$1 AND sync_version>$2 ORDER BY sync_version LIMIT $3`), feed.Scope, since, limit+1)
	if err != nil {
		return nil, since, false, err
	}
	rows := []map[string]any{}
	more := false
	for result.Next() {
		var n uint64
		var raw string
		if err = result.Scan(&n, &raw); err != nil {
			break
		}
		if len(rows) == limit {
			more = true
			break
		}
		var row map[string]any
		if err = json.Unmarshal([]byte(raw), &row); err != nil {
			break
		}
		rows = append(rows, row)
		since = n
	}
	scanErr := result.Err()
	result.Close()
	if err != nil {
		return nil, since, false, err
	}
	if scanErr != nil {
		return nil, since, false, scanErr
	}
	if err = tx.Commit(); err != nil {
		return nil, since, false, err
	}
	return rows, since, more, nil
}
