package query

import (
	"database/sql"
	"testing"
)

func TestPostgresPoolIsBoundedAndKeepsConnectionsWarm(t *testing.T) {
	// sql.Open is lazy, so this configures a real *sql.DB without a server.
	// The bound matters: unlimited open connections against the warehouse's
	// shared max_connections=60 turns a burst of tool calls into server-side
	// refusals instead of in-process queueing, and the stdlib's idle default of
	// 2 makes every third concurrent search pay a fresh tailnet TLS handshake.
	db, err := sql.Open("pgx", "postgres://user@127.0.0.1:1/postgres")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	configurePool(db)

	stats := db.Stats()
	if stats.MaxOpenConnections != postgresMaxOpenConns {
		t.Fatalf("MaxOpenConnections = %d, want %d", stats.MaxOpenConnections, postgresMaxOpenConns)
	}
	if postgresMaxOpenConns <= 0 || postgresMaxOpenConns >= 60 {
		t.Fatalf("open cap %d must be positive and well under the server's max_connections", postgresMaxOpenConns)
	}
	if postgresMaxIdleConns < 10 || postgresMaxIdleConns > postgresMaxOpenConns {
		t.Fatalf("idle cap %d must keep connections warm without exceeding the open cap", postgresMaxIdleConns)
	}
	if postgresConnMaxLifetime <= 0 {
		t.Fatalf("connections must be recycled, got lifetime %s", postgresConnMaxLifetime)
	}
}
