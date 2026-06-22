package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

type fakeClaudeDesktopCredentialStore struct {
	calls int
	cred  claudeDesktopCredential
	err   error
}

func (f *fakeClaudeDesktopCredentialStore) upsert(_ context.Context, cred claudeDesktopCredential) error {
	f.calls++
	f.cred = cred
	return f.err
}

func credentialTestService(store *fakeClaudeDesktopCredentialStore) *credentialIngestService {
	return &credentialIngestService{
		store:    store,
		signer:   objectsTestSigner(),
		maxBytes: 1024,
		timeout:  time.Second,
		logger:   slog.Default(),
	}
}

func postCredential(t *testing.T, svc *credentialIngestService, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	svc.handler().ServeHTTP(rec, req)
	return rec
}

func TestCredentialIngestStoresSignedCredential(t *testing.T) {
	store := &fakeClaudeDesktopCredentialStore{}
	svc := credentialTestService(store)
	body, err := json.Marshal(claudeDesktopCredential{
		Account:    " account@example.com ",
		SessionKey: " sk-ant-sid02-secret ",
		OrgID:      " org-1 ",
		CapturedAt: "2026-06-22T18:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	target := signedIngestTarget(claudeDesktopCredentialEndpoint, body, url.Values{})

	rec := postCredential(t, svc, target, body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if store.calls != 1 {
		t.Fatalf("upsert calls = %d, want 1", store.calls)
	}
	if store.cred.Account != "account@example.com" {
		t.Fatalf("account = %q", store.cred.Account)
	}
	if store.cred.SessionKey != "sk-ant-sid02-secret" {
		t.Fatalf("session key = %q", store.cred.SessionKey)
	}
	if store.cred.OrgID != "org-1" {
		t.Fatalf("org id = %q", store.cred.OrgID)
	}
}

func TestCredentialIngestRejectsBodyShaMismatch(t *testing.T) {
	store := &fakeClaudeDesktopCredentialStore{}
	svc := credentialTestService(store)
	signedBody := []byte(`{"account":"account@example.com","session_key":"sk-ant-sid02-secret","org_id":"org-1"}`)
	target := signedIngestTarget(claudeDesktopCredentialEndpoint, signedBody, url.Values{})

	rec := postCredential(t, svc, target, []byte(`{"account":"account@example.com","session_key":"different","org_id":"org-1"}`))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if store.calls != 0 {
		t.Fatalf("upsert calls = %d, want 0", store.calls)
	}
}
