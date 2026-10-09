package accesstokens

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestService(t *testing.T) (*Service, *clock) {
	t.Helper()
	c := &clock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	return NewService(NewMemoryStore(), c.Now, nil), c
}

func mustIssue(t *testing.T, svc *Service, req IssueRequest) Issued {
	t.Helper()
	issued, err := svc.Issue(context.Background(), req)
	if err != nil {
		t.Fatalf("issue %+v: %v", req, err)
	}
	return issued
}

func authenticate(svc *Service, raw string) (pdwauth.Principal, error) {
	return svc.AuthenticateToken(context.Background(), raw)
}

func rejectedBecause(t *testing.T, err error) string {
	t.Helper()
	var rejected *pdwauth.TokenRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("error = %v (%T), want a TokenRejectedError", err, err)
	}
	return rejected.Reason
}

func TestManyIssuedTokensAuthenticateIndependently(t *testing.T) {
	svc, _ := newTestService(t)
	phone := mustIssue(t, svc, IssueRequest{ClientName: "ios", CreatedBy: "codex"})
	laptop := mustIssue(t, svc, IssueRequest{ClientName: "laptop", CreatedBy: "codex"})
	if phone.Secret == laptop.Secret || phone.Token.ID == laptop.Token.ID {
		t.Fatal("two issued tokens share a secret or id")
	}
	if !strings.HasPrefix(phone.Secret, "pdw_"+phone.Token.ID+"_") {
		t.Fatalf("secret %q does not carry its id %q", phone.Secret, phone.Token.ID)
	}
	for _, issued := range []Issued{phone, laptop} {
		p, err := authenticate(svc, issued.Secret)
		if err != nil {
			t.Fatalf("authenticate %s: %v", issued.Token.ID, err)
		}
		if p.TokenID != issued.Token.ID || p.Scope != pdwauth.ScopeFull {
			t.Fatalf("principal = %+v", p)
		}
	}
	// Revoking one leaves the other working.
	if err := svc.Revoke(context.Background(), phone.Token.ID); err != nil {
		t.Fatal(err)
	}
	if reason := rejectedBecause(t, func() error { _, err := authenticate(svc, phone.Secret); return err }()); !strings.Contains(reason, "revoked") {
		t.Fatalf("reason = %q", reason)
	}
	if _, err := authenticate(svc, laptop.Secret); err != nil {
		t.Fatalf("laptop after phone revoked: %v", err)
	}
}

func TestATokenUnusedPastItsIdleWindowIsDeauthorizedWhileAUsedOneLives(t *testing.T) {
	svc, c := newTestService(t)
	idle := mustIssue(t, svc, IssueRequest{ClientName: "forgotten", IdleTimeout: 30 * 24 * time.Hour})
	busy := mustIssue(t, svc, IssueRequest{ClientName: "daily", IdleTimeout: 30 * 24 * time.Hour})
	// Sixty days of daily use: every request refreshes the busy token.
	for day := 0; day < 60; day++ {
		c.Advance(24 * time.Hour)
		if _, err := authenticate(svc, busy.Secret); err != nil {
			t.Fatalf("day %d: busy token rejected: %v", day, err)
		}
	}
	_, err := authenticate(svc, idle.Secret)
	if reason := rejectedBecause(t, err); !strings.Contains(reason, "unused") {
		t.Fatalf("reason = %q", reason)
	}
	// Status is reported the same way in the listing.
	tokens, err := svc.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	statuses := map[string]string{}
	for _, tok := range tokens {
		statuses[tok.ClientName] = tok.Status(c.Now())
	}
	if statuses["forgotten"] != StatusIdleExpired || statuses["daily"] != StatusActive {
		t.Fatalf("statuses = %v", statuses)
	}
}

func TestIdleTokensDefaultToThirtyDays(t *testing.T) {
	svc, c := newTestService(t)
	tok := mustIssue(t, svc, IssueRequest{ClientName: "default"})
	if tok.Token.IdleTimeout != DefaultIdleTimeout || DefaultIdleTimeout != 30*24*time.Hour {
		t.Fatalf("idle = %v", tok.Token.IdleTimeout)
	}
	c.Advance(DefaultIdleTimeout - time.Minute)
	if _, err := authenticate(svc, tok.Secret); err != nil {
		t.Fatalf("one minute before the idle limit: %v", err)
	}
	c.Advance(DefaultIdleTimeout + time.Minute)
	if _, err := authenticate(svc, tok.Secret); err == nil {
		t.Fatal("token survived a full idle window after its last use")
	}
}

func TestAHardTTLEndsATokenHoweverOftenItIsUsed(t *testing.T) {
	svc, c := newTestService(t)
	tok := mustIssue(t, svc, IssueRequest{ClientName: "bounded", TTL: 48 * time.Hour})
	for i := 0; i < 47; i++ {
		c.Advance(time.Hour)
		if _, err := authenticate(svc, tok.Secret); err != nil {
			t.Fatalf("hour %d: %v", i, err)
		}
	}
	c.Advance(2 * time.Hour)
	_, err := authenticate(svc, tok.Secret)
	if reason := rejectedBecause(t, err); !strings.Contains(reason, "expired") {
		t.Fatalf("reason = %q", reason)
	}
}

func TestUsageTouchesAreThrottled(t *testing.T) {
	store := NewMemoryStore()
	c := &clock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	svc := NewService(store, c.Now, nil)
	tok := mustIssue(t, svc, IssueRequest{ClientName: "chatty"})
	for i := 0; i < 50; i++ {
		c.Advance(time.Second)
		if _, err := authenticate(svc, tok.Secret); err != nil {
			t.Fatal(err)
		}
	}
	if store.touches > 1 {
		t.Fatalf("50 requests in 50s wrote last_used_at %d times", store.touches)
	}
}

func TestWrongSecretsAndForeignTokensAreUnknown(t *testing.T) {
	svc, _ := newTestService(t)
	tok := mustIssue(t, svc, IssueRequest{ClientName: "real"})
	forged := "pdw_" + tok.Token.ID + "_" + strings.Repeat("A", 43)
	for _, raw := range []string{forged, "pdw_nope_nope", "not-a-pdw-token", "", "pdw_", "pdwb_" + tok.Token.ID + "_x"} {
		if _, err := authenticate(svc, raw); !errors.Is(err, pdwauth.ErrUnknownToken) {
			t.Fatalf("%q: err = %v, want ErrUnknownToken", raw, err)
		}
	}
}

func TestIssuedTokenSecretsAreNotStored(t *testing.T) {
	store := NewMemoryStore()
	svc := NewService(store, nil, nil)
	tok := mustIssue(t, svc, IssueRequest{ClientName: "hashed"})
	for _, row := range store.rows {
		if strings.Contains(row.hash, tok.Secret) || strings.Contains(row.hash, strings.TrimPrefix(tok.Secret, "pdw_"+tok.Token.ID+"_")) {
			t.Fatal("store holds the plaintext secret")
		}
	}
}

func TestIssueValidatesItsRequest(t *testing.T) {
	svc, _ := newTestService(t)
	for _, req := range []IssueRequest{
		{ClientName: ""},
		{ClientName: "bad:name"},
		{ClientName: "x", Scope: pdwauth.ScopeRoot},
		{ClientName: "x", IdleTimeout: time.Minute},
		{ClientName: "x", IdleTimeout: 400 * 24 * time.Hour},
		{ClientName: "x", TTL: -time.Hour},
	} {
		if _, err := svc.Issue(context.Background(), req); err == nil {
			t.Fatalf("%+v: issued, want a validation error", req)
		}
	}
}

func TestABootstrapCodeRedeemsExactlyOnceForAShortLivedAgentToken(t *testing.T) {
	svc, c := newTestService(t)
	code, err := svc.CreateBootstrap(context.Background(), BootstrapRequest{ClientName: "cowork", CreatedBy: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(code.Code, "pdwb_") {
		t.Fatalf("code = %q", code.Code)
	}
	if code.TokenTTL != DefaultAgentTTL || DefaultAgentTTL != 24*time.Hour {
		t.Fatalf("token ttl = %v", code.TokenTTL)
	}
	// A bootstrap code is not itself a bearer credential.
	if _, err := authenticate(svc, code.Code); !errors.Is(err, pdwauth.ErrUnknownToken) {
		t.Fatalf("bootstrap code authenticated as a bearer: %v", err)
	}
	issued, err := svc.Redeem(context.Background(), code.Code)
	if err != nil {
		t.Fatalf("redeem: %v", err)
	}
	if issued.Token.Scope != pdwauth.ScopeAgent || issued.Token.ClientName != "cowork" || issued.Token.CreatedBy != "claude" {
		t.Fatalf("issued = %+v", issued.Token)
	}
	if want := c.Now().Add(24 * time.Hour); !issued.Token.ExpiresAt.Equal(want) {
		t.Fatalf("expires_at = %v, want %v", issued.Token.ExpiresAt, want)
	}
	if _, err := svc.Redeem(context.Background(), code.Code); err == nil {
		t.Fatal("a bootstrap code redeemed twice")
	} else if reason := rejectedBecause(t, err); !strings.Contains(reason, "already") {
		t.Fatalf("reason = %q", reason)
	}
	p, err := authenticate(svc, issued.Secret)
	if err != nil || p.Scope != pdwauth.ScopeAgent {
		t.Fatalf("agent token: %+v %v", p, err)
	}
	// The agent token stops working at its hard expiry, even in constant use.
	for i := 0; i < 23; i++ {
		c.Advance(time.Hour)
		if _, err := authenticate(svc, issued.Secret); err != nil {
			t.Fatalf("hour %d: %v", i, err)
		}
	}
	c.Advance(90 * time.Minute)
	_, err = authenticate(svc, issued.Secret)
	if reason := rejectedBecause(t, err); !strings.Contains(reason, "cli_authorize") {
		t.Fatalf("an expired agent token should say how to get a new one: %q", reason)
	}
}

func TestABootstrapCodeExpiresUnredeemed(t *testing.T) {
	svc, c := newTestService(t)
	code, err := svc.CreateBootstrap(context.Background(), BootstrapRequest{ClientName: "late"})
	if err != nil {
		t.Fatal(err)
	}
	c.Advance(BootstrapCodeLifetime + time.Second)
	if _, err := svc.Redeem(context.Background(), code.Code); err == nil {
		t.Fatal("expired code redeemed")
	} else if reason := rejectedBecause(t, err); !strings.Contains(reason, "expired") {
		t.Fatalf("reason = %q", reason)
	}
}

func TestConcurrentRedemptionsMintOneToken(t *testing.T) {
	svc, _ := newTestService(t)
	code, err := svc.CreateBootstrap(context.Background(), BootstrapRequest{ClientName: "race"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Redeem(context.Background(), code.Code); err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d redemptions succeeded, want 1", wins)
	}
}

func TestBootstrapTTLIsBounded(t *testing.T) {
	svc, _ := newTestService(t)
	if _, err := svc.CreateBootstrap(context.Background(), BootstrapRequest{ClientName: "x", TokenTTL: 8 * 24 * time.Hour}); err == nil {
		t.Fatal("an eight-day agent token was allowed")
	}
	if _, err := svc.CreateBootstrap(context.Background(), BootstrapRequest{ClientName: "x", TokenTTL: MaxAgentTTL}); err != nil {
		t.Fatalf("max ttl refused: %v", err)
	}
}

func TestRevokingAnUnknownTokenSaysSo(t *testing.T) {
	svc, _ := newTestService(t)
	if err := svc.Revoke(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}
