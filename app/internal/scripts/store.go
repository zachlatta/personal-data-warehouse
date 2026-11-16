package scripts

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

type Store struct {
	db      *sql.DB
	table   string
	schema  string
	mu      sync.Mutex
	ensured bool
}

func NewStore(url string) (*Store, error) {
	db, e := sql.Open("pgx", url)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	return &Store{db: db, table: warehouse.SQLRelation("script_runs"), schema: warehouse.SchemaOf("script_runs")}, nil
}
func (s *Store) Close() error { return s.db.Close() }

// Schema is ensured on first write too: the Go deploy precedes Python ensure.
func (s *Store) Record(ctx context.Context, r Record) error {
	s.mu.Lock()
	if !s.ensured {
		_, e := s.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+warehouse.QuoteIdent(s.schema))
		if e == nil {
			_, e = s.db.ExecContext(ctx, StoreDDL(s.table))
		}
		if e != nil {
			s.mu.Unlock()
			return e
		}
		s.ensured = true
	}
	s.mu.Unlock()
	ops := append([]Operation{}, r.Operations...)
	innerErrors := 0
	for i := range ops {
		if ops[i].Error != nil {
			innerErrors++
			code := ops[i].Error.Code // keep SQLSTATE shape, never values or SQL text
			ops[i].Error = &Error{Code: code, Message: sqlState(ops[i].Error.Message)}
		}
	}
	encoded, _ := json.Marshal(ops)
	_, err := s.db.ExecContext(ctx, `INSERT INTO `+s.table+` (occurred_at,run_id,client,token_id,surface,duration_ms,operations_json,inner_errors,bytes_returned,truncated,outcome)
 VALUES (now(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, r.RunID, r.Client, r.TokenID, r.Surface, r.DurationMS, string(encoded), innerErrors, r.BytesReturned, boolInt(r.Truncated), r.Outcome)
	return err
}
func sqlState(message string) string {
	for i := 0; i+14 <= len(message); i++ {
		if message[i:i+9] == "SQLSTATE " {
			return message[i : i+14]
		}
	}
	return ""
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func StoreDDL(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
 occurred_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
 run_id text NOT NULL DEFAULT '', client text NOT NULL DEFAULT '', token_id text NOT NULL DEFAULT '',
 surface text NOT NULL DEFAULT '', duration_ms bigint NOT NULL DEFAULT 0,
 operations_json text NOT NULL DEFAULT '', inner_errors bigint NOT NULL DEFAULT 0,
 bytes_returned bigint NOT NULL DEFAULT 0, truncated bigint NOT NULL DEFAULT 0,
 outcome text NOT NULL DEFAULT '', PRIMARY KEY (run_id))`
}
