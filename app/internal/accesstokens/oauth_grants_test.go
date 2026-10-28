package accesstokens

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// The token service is the grant store the OAuth flow uses.
var _ pdwauth.OAuthGrants = (*Service)(nil)

func TestOAuthGrantLifecycle(t *testing.T) {
	svc, c := newTestService(t)
	ctx := context.Background()
	id, err := svc.CreateOAuthGrant(ctx, "claude")
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CheckOAuthGrant(ctx, id); err != nil {
		t.Fatalf("fresh grant rejected: %v", err)
	}
	listed, _ := svc.List(ctx)
	if len(listed) != 1 || listed[0].Kind != KindOAuth || listed[0].ClientName != "claude" || listed[0].IdleTimeout != OAuthGrantIdleTimeout {
		t.Fatalf("listing = %+v", listed)
	}

	// The cached answer must not outlive a revoke.
	if err := svc.Revoke(ctx, id); err != nil {
		t.Fatal(err)
	}
	if reason := rejectedBecause(t, svc.CheckOAuthGrant(ctx, id)); !strings.Contains(reason, "revoked") {
		t.Fatalf("reason = %q", reason)
	}
	c.Advance(time.Hour)
	if err := svc.CheckOAuthGrant(ctx, id); err == nil {
		t.Fatal("revoked grant accepted later")
	}
	if !errors.Is(svc.CheckOAuthGrant(ctx, "nosuchgrant00000"), pdwauth.ErrUnknownToken) {
		t.Fatal("unknown grant must be ErrUnknownToken")
	}
}

func TestOAuthGrantLapsesAfterNinetyIdleDays(t *testing.T) {
	svc, c := newTestService(t)
	ctx := context.Background()
	busy, _ := svc.CreateOAuthGrant(ctx, "busy")
	idle, _ := svc.CreateOAuthGrant(ctx, "idle")
	for day := 0; day < 100; day += 10 {
		c.Advance(10 * 24 * time.Hour)
		if err := svc.CheckOAuthGrant(ctx, busy); err != nil {
			t.Fatalf("grant in use rejected on day %d: %v", day, err)
		}
	}
	if reason := rejectedBecause(t, svc.CheckOAuthGrant(ctx, idle)); !strings.Contains(reason, "90d idle limit") {
		t.Fatalf("reason = %q", reason)
	}
}

func TestEnsureOAuthGrantIsIdempotentAndHonoursARevoke(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	const id = "legacygrant00001"
	for i := 0; i < 2; i++ {
		if err := svc.EnsureOAuthGrant(ctx, id, "claude"); err != nil {
			t.Fatalf("ensure %d: %v", i, err)
		}
	}
	if listed, _ := svc.List(ctx); len(listed) != 1 {
		t.Fatalf("ensure twice made %d rows", len(listed))
	}
	if err := svc.Revoke(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := svc.EnsureOAuthGrant(ctx, id, "claude"); err == nil {
		t.Fatal("ensure revived a revoked grant")
	}
}

func TestOAuthGrantsAreNeverStaticBearers(t *testing.T) {
	svc, _ := newTestService(t)
	id, _ := svc.CreateOAuthGrant(context.Background(), "claude")
	// Whatever secret might be guessed, the row's kind refuses it.
	if _, err := authenticate(svc, accessPrefix+id+"_anything"); !errors.Is(err, pdwauth.ErrUnknownToken) {
		t.Fatalf("grant authenticated as a static bearer: %v", err)
	}
	if err := svc.CheckOAuthGrant(context.Background(), mustIssue(t, svc, IssueRequest{ClientName: "cli"}).Token.ID); !errors.Is(err, pdwauth.ErrUnknownToken) {
		t.Fatalf("issued access token accepted as a grant: %v", err)
	}
}

func TestDisabledOAuthGrantsAnswerUnavailableNotUnknown(t *testing.T) {
	svc, _ := newTestService(t)
	svc.DisableOAuthGrants(ErrGrantStoreUnavailable)
	ctx := context.Background()
	if _, err := svc.CreateOAuthGrant(ctx, "claude"); !errors.Is(err, ErrGrantStoreUnavailable) {
		t.Fatalf("create = %v", err)
	}
	for name, err := range map[string]error{
		"check":  svc.CheckOAuthGrant(ctx, "anygrant00000000"),
		"ensure": svc.EnsureOAuthGrant(ctx, "anygrant00000000", "claude"),
	} {
		var rejected *pdwauth.TokenRejectedError
		if !errors.Is(err, ErrGrantStoreUnavailable) || errors.Is(err, pdwauth.ErrUnknownToken) || errors.As(err, &rejected) {
			t.Fatalf("%s = %v, want unavailable", name, err)
		}
	}
}
