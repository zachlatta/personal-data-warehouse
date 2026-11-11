package accesstokens

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

func TestExternalFeedCredentialLifecycle(t *testing.T) {
	svc, clock := newTestService(t)
	issued := mustIssue(t, svc, IssueRequest{ClientName: "consumer", Scope: pdwauth.ScopeVoiceFeed, TTL: time.Hour})
	_, hash, _ := svc.store.Get(context.Background(), issued.Token.ID)
	if strings.Contains(issued.Secret, hash) || len(hash) != 64 {
		t.Fatal("secret must be stored hashed")
	}
	p, err := authenticate(svc, issued.Secret)
	if err != nil || !p.Allows(pdwauth.VoiceFeedPath) {
		t.Fatalf("feed denied: %v", err)
	}
	for _, path := range []string{"/api/tools", "/api/auth/tokens", "/api/connections", pdwauth.VoiceFeedPath + "/other"} {
		if p.Allows(path) {
			t.Fatalf("feed key allowed %s", path)
		}
	}
	clock.Advance(time.Second)
	if err := svc.RecordFeedUse(context.Background(), p.TokenID); err != nil {
		t.Fatal(err)
	}
	row, _, _ := svc.store.Get(context.Background(), p.TokenID)
	if row.RequestCount != 1 || !row.LastUsedAt.Equal(clock.Now()) {
		t.Fatalf("usage: %+v", row)
	}
	data, _ := json.Marshal(viewOf(row, clock.Now()))
	if strings.Contains(string(data), issued.Secret) || strings.Contains(string(data), hash) {
		t.Fatal("listing leaked credential")
	}
	clock.Advance(time.Hour)
	if _, err := authenticate(svc, issued.Secret); err == nil {
		t.Fatal("expired key accepted")
	}
	second := mustIssue(t, svc, IssueRequest{ClientName: "consumer", Scope: pdwauth.ScopeVoiceFeed})
	if err := svc.Revoke(context.Background(), second.Token.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := authenticate(svc, second.Secret); err == nil {
		t.Fatal("revoked key accepted")
	}
}
