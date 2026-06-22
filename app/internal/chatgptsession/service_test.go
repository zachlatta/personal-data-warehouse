package chatgptsession

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

const testSecret = "0123456789abcdef0123456789abcdef"

var testNow = time.Unix(1_700_000_000, 0).UTC()

func testSigner() *pdwauth.Service {
	return pdwauth.NewService([]byte(testSecret), func() time.Time { return testNow })
}

type fakeStore struct {
	calls []map[string]string
	err   error
}

func (f *fakeStore) Upsert(_ context.Context, account, sessionKey, sessionToken, sourceBrowser string, now time.Time) (Ack, error) {
	if f.err != nil {
		return Ack{}, f.err
	}
	f.calls = append(f.calls, map[string]string{
		"account": account, "session_key": sessionKey, "token": sessionToken, "browser": sourceBrowser,
	})
	sum := sha256.Sum256([]byte(sessionToken))
	return Ack{Account: account, SessionKey: sessionKey, SourceBrowser: sourceBrowser, TokenSHA256: hex.EncodeToString(sum[:]), UpdatedAt: now}, nil
}

func signedTarget(body []byte) string {
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	exp := testNow.Add(time.Hour)
	q := url.Values{}
	q.Set("content_sha256", sha)
	q.Set("exp", strconv.FormatInt(exp.Unix(), 10))
	q.Set("sig", testSigner().SignObjectUpload(Endpoint, sha, exp))
	return Endpoint + "?" + q.Encode()
}

func post(t *testing.T, svc *Service, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(string(body)))
	svc.Handler().ServeHTTP(rec, req)
	return rec
}

func newService(store Store) *Service {
	return NewService(store, testSigner(), func() time.Time { return testNow }, slog.Default())
}

func TestPublishStoresSession(t *testing.T) {
	store := &fakeStore{}
	svc := newService(store)
	body := []byte(`{"account":"user@example.com","session_key":"default","session_token":"__Secure-next-auth.session-token=abc","source_browser":"Google Chrome"}`)

	rec := post(t, svc, signedTarget(body), body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body %q", rec.Code, rec.Body.String())
	}
	if len(store.calls) != 1 {
		t.Fatalf("expected 1 upsert, got %d", len(store.calls))
	}
	if store.calls[0]["account"] != "user@example.com" || store.calls[0]["browser"] != "Google Chrome" {
		t.Fatalf("unexpected upsert args: %v", store.calls[0])
	}
	var ack Ack
	if err := json.Unmarshal(rec.Body.Bytes(), &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.TokenSHA256 == "" {
		t.Fatal("expected token_sha256 in ack")
	}
	if strings.Contains(rec.Body.String(), "session_token") {
		t.Fatal("ack must not echo the session token")
	}
}

func TestBadSignatureRejected(t *testing.T) {
	svc := newService(&fakeStore{})
	body := []byte(`{"account":"a","session_token":"t"}`)
	target := signedTarget(body)
	target = strings.Replace(target, "sig=", "sig=bogus", 1)
	rec := post(t, svc, target, body)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestBodyShaMismatchRejected(t *testing.T) {
	svc := newService(&fakeStore{})
	body := []byte(`{"account":"a","session_token":"t"}`)
	target := signedTarget(body)
	rec := post(t, svc, target, []byte(`{"account":"a","session_token":"DIFFERENT"}`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestMissingFieldsRejected(t *testing.T) {
	svc := newService(&fakeStore{})
	body := []byte(`{"account":"a"}`) // no session_token
	rec := post(t, svc, signedTarget(body), body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestNonPostRejected(t *testing.T) {
	svc := newService(&fakeStore{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, Endpoint, nil)
	svc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestDefaultsSessionKey(t *testing.T) {
	store := &fakeStore{}
	svc := newService(store)
	body := []byte(`{"account":"a","session_token":"t"}`)
	rec := post(t, svc, signedTarget(body), body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if store.calls[0]["session_key"] != "default" {
		t.Fatalf("session_key = %q, want default", store.calls[0]["session_key"])
	}
}
