package mcpproxy

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// The canonical pytest suite runs this against its provisioned, isolated
// warehouse schema. go test alone never guesses or uses a production URL.
func TestPostgresStoreIntegration(t *testing.T) {
	url := os.Getenv("PDW_PROXY_TEST_DATABASE_URL")
	schema := os.Getenv("PDW_PROXY_TEST_SCHEMA")
	if url == "" || schema == "" {
		t.Skip("exercised by tests/test_mcp_proxy.py in uv run pytest")
	}
	if !strings.HasPrefix(schema, "pdw_test_") {
		t.Fatal("integration test requires an isolated test schema")
	}
	store, err := NewPostgresStore(url, "test-encryption-key")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.schema = schema
	store.relation = warehouse.QuoteIdent(schema) + `."mcp_connections"`
	ctx := context.Background()
	if err := store.Update(ctx, "skills", func(c *record) error { c.Token.AccessToken = "stored-secret"; return nil }); err != nil {
		t.Fatal(err)
	}
	var payload string
	if err := store.db.QueryRowContext(ctx, `SELECT payload FROM `+store.relation+` WHERE name='skills'`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(payload, "stored-secret") {
		t.Fatal("plaintext credential in database")
	}
	reopened, err := NewPostgresStore(url, "test-encryption-key")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.schema = store.schema
	reopened.relation = store.relation
	rows, err := reopened.List(ctx)
	if err != nil || len(rows) != 1 || rows[0].Token.AccessToken != "stored-secret" {
		t.Fatalf("restart: %+v %v", rows, err)
	}
	if err := store.Update(ctx, "skills", func(c *record) error { c.Token.AccessToken = "uncommitted"; return errors.New("rollback") }); err == nil {
		t.Fatal("rollback accepted")
	}
	rows, err = reopened.List(ctx)
	if err != nil || rows[0].Token.AccessToken != "stored-secret" {
		t.Fatal("failed mutation persisted")
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := reopened.Update(ctx, "skills", func(c *record) error { c.Clients = append(c.Clients, "client"); return nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	rows, err = store.List(ctx)
	if err != nil || len(rows[0].Clients) != 10 {
		t.Fatal("concurrent updates were lost")
	}

	versionBefore := rows[0].Version

	// A master secret rotation: the new secret current, the old one previous.
	rotated, err := NewPostgresStore(url, "rotated-encryption-key", "test-encryption-key")
	if err != nil {
		t.Fatal(err)
	}
	defer rotated.Close()
	rotated.schema, rotated.relation = store.schema, store.relation
	if rows, err := rotated.List(ctx); err != nil || len(rows) != 1 {
		t.Fatalf("previous-secret rows unreadable during rotation: %+v %v", rows, err)
	}
	if n, err := rotated.ResealUnderCurrentSecret(ctx); err != nil || n != 1 {
		t.Fatalf("reseal = %d, %v; want 1", n, err)
	}
	if n, err := rotated.ResealUnderCurrentSecret(ctx); err != nil || n != 0 {
		t.Fatalf("second reseal = %d, %v; want 0", n, err)
	}
	after, err := NewPostgresStore(url, "rotated-encryption-key")
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	after.schema, after.relation = store.schema, store.relation
	rows, err = after.List(ctx)
	if err != nil || len(rows) != 1 || rows[0].Token.AccessToken != "stored-secret" || len(rows[0].Clients) != 10 {
		t.Fatalf("after the previous secret is dropped: %+v %v", rows, err)
	}
	if rows[0].Version != versionBefore {
		t.Fatalf("re-encryption bumped the connection version %d -> %d", versionBefore, rows[0].Version)
	}
}
