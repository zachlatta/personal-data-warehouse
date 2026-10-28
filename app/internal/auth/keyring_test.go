package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	oldSecret = "old-master-secret-0123456789abcdef"
	newSecret = "new-master-secret-fedcba9876543210"
)

func TestKeyIDsAreStableDistinctAndRevealNothing(t *testing.T) {
	a, b := NewKeyring([]byte(oldSecret)), NewKeyring([]byte(newSecret))
	if a.CurrentKeyID() != NewKeyring([]byte(oldSecret)).CurrentKeyID() {
		t.Fatal("a key id must be a pure function of the secret")
	}
	if a.CurrentKeyID() == b.CurrentKeyID() {
		t.Fatal("two secrets share a key id")
	}
	if strings.Contains(oldSecret, a.CurrentKeyID()) || strings.Contains(a.CurrentKeyID(), ".") {
		t.Fatalf("key id %q leaks the secret or collides with the token separator", a.CurrentKeyID())
	}
	k := a.current()
	if hmac.Equal(k.sub[purposeOAuthToken], k.sub[purposeObjectDownload]) || hmac.Equal(k.sub[purposeOAuthToken], k.secret) {
		t.Fatal("purposes must get distinct subkeys, none equal to the root")
	}
}

func TestRotationWindowAcceptsTheOldKeyAndSignsWithTheNew(t *testing.T) {
	before, beforeGrants, beforeMux := newGrantTestService(t, NewKeyring([]byte(oldSecret)))
	clientID := registerClient(t, beforeMux)
	pair := connect(t, beforeMux, clientID, oldSecret)
	if !strings.HasPrefix(pair.AccessToken, before.keys.CurrentKeyID()+".") {
		t.Fatalf("token %q does not name its key", pair.AccessToken)
	}
	legacyAccess := legacySign(t, oldSecret, signedPayload{Type: "access", ClientID: clientID, ClientName: "claude-test", Scope: "query", Nonce: "a", Exp: 1000 + 3600, Iat: 900})

	// The window: new secret current, old one previous.
	during := NewServiceWithKeyring(NewKeyring([]byte(newSecret), []byte(oldSecret)), func() time.Time { return time.Unix(1000, 0) })
	during.SetOAuthGrants(beforeGrants)
	duringMux := http.NewServeMux()
	during.RegisterHandlers(duringMux, "https://mcp.example.com")
	for name, token := range map[string]string{"old-key access": pair.AccessToken, "pre-key-id access": legacyAccess} {
		if _, err := during.VerifyBearer(context.Background(), token); err != nil {
			t.Fatalf("%s token rejected during the window: %v", name, err)
		}
	}
	refreshed := decodePair(t, refresh(duringMux, clientID, pair.RefreshToken))
	if !strings.HasPrefix(refreshed.AccessToken, during.keys.CurrentKeyID()+".") || !strings.HasPrefix(refreshed.RefreshToken, during.keys.CurrentKeyID()+".") {
		t.Fatal("tokens minted during the window must be signed by the new key")
	}
	// The consent page takes only the new secret.
	rec := postForm(duringMux, "/oauth/authorize", url.Values{
		"client_id": {clientID}, "redirect_uri": {testRedirect}, "response_type": {"code"},
		"code_challenge": {s256Challenge("verifier")}, "code_challenge_method": {"S256"},
		"secret_token": {oldSecret}, "client_name": {"claude-test"},
	})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("consent with the previous secret = %d, want 401", rec.Code)
	}

	// After the window: only what was re-signed survives.
	after := NewServiceWithKeyring(NewKeyring([]byte(newSecret)), func() time.Time { return time.Unix(1000, 0) })
	after.SetOAuthGrants(beforeGrants)
	if _, err := after.VerifyBearer(context.Background(), pair.AccessToken); err == nil {
		t.Fatal("old-key token verifies after the old key left the ring")
	}
	if _, err := after.VerifyBearer(context.Background(), refreshed.AccessToken); err != nil {
		t.Fatalf("token re-signed during the window rejected after it: %v", err)
	}
}

func TestDownloadLinksNameTheirKeyAndOldLinksStillVerify(t *testing.T) {
	now := func() time.Time { return time.Unix(1000, 0) }
	exp := time.Unix(2000, 0)
	old := NewServiceWithKeyring(NewKeyring([]byte(oldSecret)), now)
	sig := old.SignObjectDownload("file-1", "acct", exp)
	if !strings.HasPrefix(sig, old.keys.CurrentKeyID()+".") {
		t.Fatalf("download signature %q does not name its key", sig)
	}
	// A link from before key ids: HMAC under the raw secret.
	mac := hmac.New(sha256.New, []byte(oldSecret))
	mac.Write([]byte(objectDownloadKind + "\nfile-1\nacct\n" + strconv.FormatInt(exp.Unix(), 10)))
	legacySig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	during := NewServiceWithKeyring(NewKeyring([]byte(newSecret), []byte(oldSecret)), now)
	after := NewServiceWithKeyring(NewKeyring([]byte(newSecret)), now)
	for name, s := range map[string]string{"key-id": sig, "pre-key-id": legacySig} {
		if err := old.VerifyObjectDownload("file-1", "acct", "2000", s); err != nil {
			t.Fatalf("%s link rejected by its own key: %v", name, err)
		}
		if err := during.VerifyObjectDownload("file-1", "acct", "2000", s); err != nil {
			t.Fatalf("%s link rejected during the window: %v", name, err)
		}
		if err := after.VerifyObjectDownload("file-1", "acct", "2000", s); err == nil {
			t.Fatalf("%s link verifies after its key left the ring", name)
		}
		if err := during.VerifyObjectDownload("file-2", "acct", "2000", s); err == nil {
			t.Fatalf("%s link verifies for another file", name)
		}
	}
	if err := during.VerifyObjectDownload("file-1", "acct", "2000", "bogus."+strings.SplitN(sig, ".", 2)[1]); err == nil {
		t.Fatal("unknown key id accepted")
	}
}

// The timeline media signer is a one-key Service built from
// PDW_TIMELINE_MEDIA_SIGNING_KEY, signing links another app serves.
func TestMediaSigningKeyLinksVerifyOnTheAppHoldingThatKey(t *testing.T) {
	now := func() time.Time { return time.Unix(1000, 0) }
	media := NewService([]byte(oldSecret), now)
	sig := media.SignObjectDownload("file-1", "", time.Unix(2000, 0))
	for name, ring := range map[string]*Keyring{
		"current":  NewKeyring([]byte(oldSecret)),
		"previous": NewKeyring([]byte(newSecret), []byte(oldSecret)),
	} {
		if err := NewServiceWithKeyring(ring, now).VerifyObjectDownload("file-1", "", "2000", sig); err != nil {
			t.Fatalf("media link rejected where its key is %s: %v", name, err)
		}
	}
}

func TestUploadsSignedWithEitherSecretVerifyDuringTheWindow(t *testing.T) {
	now := func() time.Time { return time.Unix(1000, 0) }
	exp := time.Unix(2000, 0)
	oldClient := NewService([]byte(oldSecret), now)
	sig := oldClient.SignObjectUpload("/ingest/x", "sha", exp)
	// The wire format uploaders (including ingest_client.py) produce.
	mac := hmac.New(sha256.New, []byte(oldSecret))
	mac.Write([]byte("object-upload\n/ingest/x\nsha\n2000"))
	if sig != base64.RawURLEncoding.EncodeToString(mac.Sum(nil)) {
		t.Fatal("upload signature format changed; every uploader signs this exact MAC")
	}
	during := NewServiceWithKeyring(NewKeyring([]byte(newSecret), []byte(oldSecret)), now)
	if err := during.VerifyObjectUpload("/ingest/x", "sha", "2000", sig); err != nil {
		t.Fatalf("old-secret upload rejected during the window: %v", err)
	}
	if err := during.VerifyObjectUpload("/ingest/x", "sha", "2000", NewService([]byte(newSecret), now).SignObjectUpload("/ingest/x", "sha", exp)); err != nil {
		t.Fatalf("new-secret upload rejected: %v", err)
	}
	if err := NewService([]byte(newSecret), now).VerifyObjectUpload("/ingest/x", "sha", "2000", sig); err == nil {
		t.Fatal("old-secret upload verifies after the window")
	}
}

func TestStaticBearerAcceptsThePreviousSecretOnlyDuringTheWindow(t *testing.T) {
	during := NewServiceWithKeyring(NewKeyring([]byte(newSecret), []byte(oldSecret)), time.Now)
	after := NewServiceWithKeyring(NewKeyring([]byte(newSecret)), time.Now)
	status := func(svc *Service, secret string) int {
		h := svc.RequireStaticBearer()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if p, ok := PrincipalFromContext(r.Context()); !ok || p.Scope != ScopeRoot {
				t.Errorf("principal = %+v, want root", p)
			}
		}))
		req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
		req.Header.Set("Authorization", "Bearer cli:"+secret)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := status(during, oldSecret); code != http.StatusOK {
		t.Fatalf("previous secret during the window = %d", code)
	}
	if code := status(during, newSecret); code != http.StatusOK {
		t.Fatalf("current secret during the window = %d", code)
	}
	if code := status(after, oldSecret); code != http.StatusUnauthorized {
		t.Fatalf("previous secret after the window = %d, want 401", code)
	}
}
