package query

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestPostgresRunnerUsesRealPostgresDatabaseURL(t *testing.T) {
	postgresURL := os.Getenv("POSTGRES_DATABASE_URL")
	if postgresURL == "" {
		t.Skip("POSTGRES_DATABASE_URL is not set")
	}
	runner, err := NewPostgresRunner(postgresURL, 5*time.Second)
	if err != nil {
		t.Fatalf("NewPostgresRunner returned error: %v", err)
	}
	defer runner.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := runner.Query(ctx, "SELECT 1 AS ok", 2)
	if err != nil {
		t.Fatalf("real Postgres query failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["ok"] == nil {
		t.Fatalf("unexpected result: %#v", result)
	}
}
