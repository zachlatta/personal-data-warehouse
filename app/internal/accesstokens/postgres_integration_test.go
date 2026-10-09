package accesstokens

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// postgresService opens a real Postgres when PDW_TEST_POSTGRES_URL points at a
// throwaway database, and skips otherwise: every other test here runs against
// the memory store, so without this the SQL that production runs is untested.
func postgresService(t *testing.T) (*Service, *PostgresStore, *clock) {
	t.Helper()
	url := strings.TrimSpace(os.Getenv("PDW_TEST_POSTGRES_URL"))
	if url == "" {
		t.Skip("PDW_TEST_POSTGRES_URL is not set")
	}
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	c := &clock{now: time.Now().UTC().Truncate(time.Microsecond)}
	return NewService(store, c.Now, nil), store, c
}

func TestPostgresStoreRoundTripsTheTokenLifecycle(t *testing.T) {
	svc, store, c := postgresService(t)
	ctx := context.Background()
	busy := mustIssue(t, svc, IssueRequest{ClientName: "pg-busy", Label: "integration", IdleTimeout: 2 * time.Hour, CreatedBy: "test"})
	idle := mustIssue(t, svc, IssueRequest{ClientName: "pg-idle", IdleTimeout: 2 * time.Hour})
	bounded := mustIssue(t, svc, IssueRequest{ClientName: "pg-bounded", TTL: 3 * time.Hour})

	got, hash, err := store.Get(ctx, busy.Token.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Label != "integration" || got.IdleTimeout != 2*time.Hour || !got.ExpiresAt.IsZero() || !got.RevokedAt.IsZero() || got.Scope != pdwauth.ScopeFull {
		t.Fatalf("round trip = %+v", got)
	}
	if strings.Contains(busy.Secret, hash) || len(hash) != 64 {
		t.Fatalf("stored hash looks wrong: %q", hash)
	}
	if b, _, _ := store.Get(ctx, bounded.Token.ID); !b.ExpiresAt.Equal(bounded.Token.ExpiresAt) {
		t.Fatalf("expires_at = %v, want %v", b.ExpiresAt, bounded.Token.ExpiresAt)
	}

	for i := 0; i < 3; i++ {
		c.Advance(time.Hour)
		if _, err := authenticate(svc, busy.Secret); err != nil {
			t.Fatalf("busy hour %d: %v", i, err)
		}
	}
	if _, err := authenticate(svc, idle.Secret); err == nil {
		t.Fatal("idle token survived 3h unused against a 2h idle limit")
	}
	if _, err := authenticate(svc, bounded.Secret); err == nil {
		t.Fatal("bounded token survived its 3h ttl")
	}
	if got, _, _ := store.Get(ctx, busy.Token.ID); !got.LastUsedAt.Equal(c.Now()) {
		t.Fatalf("last_used_at = %v, want %v", got.LastUsedAt, c.Now())
	}

	if err := svc.Revoke(ctx, busy.Token.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := authenticate(svc, busy.Secret); err == nil {
		t.Fatal("revoked token authenticated")
	}
	if err := svc.Revoke(ctx, "nonexistent00000"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoke unknown: %v", err)
	}

	tokens, err := svc.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, tok := range tokens {
		seen[tok.ID] = tok.Status(c.Now())
	}
	if seen[busy.Token.ID] != StatusRevoked || seen[idle.Token.ID] != StatusIdleExpired || seen[bounded.Token.ID] != StatusExpired {
		t.Fatalf("statuses = %v", seen)
	}
}

func TestPostgresStoreRedeemsABootstrapCodeExactlyOnce(t *testing.T) {
	svc, store, c := postgresService(t)
	ctx := context.Background()
	code, err := svc.CreateBootstrap(ctx, BootstrapRequest{ClientName: "pg-cowork", CreatedBy: "claude", TokenTTL: 2 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var winners []Issued
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if issued, err := svc.Redeem(ctx, code.Code); err == nil {
				mu.Lock()
				winners = append(winners, issued)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(winners) != 1 {
		t.Fatalf("%d concurrent redemptions succeeded, want 1", len(winners))
	}
	issued := winners[0]
	row, _, err := store.Get(ctx, code.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.RedeemedTokenID != issued.Token.ID || row.RedeemedAt.IsZero() {
		t.Fatalf("code row = %+v", row)
	}
	p, err := authenticate(svc, issued.Secret)
	if err != nil || p.Scope != pdwauth.ScopeAgent {
		t.Fatalf("agent token: %+v %v", p, err)
	}
	c.Advance(2*time.Hour + time.Second)
	if _, err := authenticate(svc, issued.Secret); err == nil {
		t.Fatal("agent token outlived its ttl")
	}
}
