package query

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestClickHouseRunnerUsesRealClickHouseURL(t *testing.T) {
	clickhouseURL := os.Getenv("CLICKHOUSE_URL")
	if clickhouseURL == "" {
		t.Skip("CLICKHOUSE_URL is not set")
	}
	runner, err := NewClickHouseRunner(clickhouseURL, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClickHouseRunner returned error: %v", err)
	}
	defer runner.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := runner.Query(ctx, "SELECT 1 AS ok", 2)
	if err != nil {
		t.Fatalf("real ClickHouse query failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["ok"] == nil {
		t.Fatalf("unexpected result: %#v", result)
	}
}
