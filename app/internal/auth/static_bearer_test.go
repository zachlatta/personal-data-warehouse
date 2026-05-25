package auth

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newStaticBearerService(secret string) *Service {
	return NewService([]byte(secret), func() time.Time { return time.Unix(0, 0) })
}

func TestRequireStaticBearerAcceptsMatchingToken(t *testing.T) {
	svc := newStaticBearerService("test-secret-token-at-least-32-chars-long")
	var seenClient string
	handler := svc.RequireStaticBearer()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenClient = ClientNameFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
	req = req.WithContext(WithClientNameHolder(req.Context()))
	req.Header.Set("Authorization", "Bearer codex:test-secret-token-at-least-32-chars-long")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("body = %q", rec.Body.String())
	}
	if seenClient != "codex" {
		t.Fatalf("ClientNameFromContext = %q, want %q", seenClient, "codex")
	}
	if got := ClientNameFromContext(req.Context()); got != "codex" {
		t.Fatalf("client name not persisted on request context after handler: %q", got)
	}
}

func TestRequireStaticBearerRejectsMissingClientName(t *testing.T) {
	svc := newStaticBearerService("test-secret-token-at-least-32-chars-long")
	handler := svc.RequireStaticBearer()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run when client name is missing")
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
	req.Header.Set("Authorization", "Bearer test-secret-token-at-least-32-chars-long")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "client_name") {
		t.Fatalf("body should mention client_name format, got: %q", body)
	}
}

func TestRequireStaticBearerRejectsEmptyClientName(t *testing.T) {
	svc := newStaticBearerService("test-secret-token-at-least-32-chars-long")
	handler := svc.RequireStaticBearer()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run when client name is empty")
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
	// Leading colon — empty name, matching token.
	req.Header.Set("Authorization", "Bearer :test-secret-token-at-least-32-chars-long")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestRequireStaticBearerSplitsOnFirstColonOnly(t *testing.T) {
	// Tokens containing colons stay intact: only the first ':' separates name
	// from token, so the operator can use any high-entropy secret.
	secret := "tok:en:with:colons-padding-to-32-chars-minimum"
	svc := newStaticBearerService(secret)
	handler := svc.RequireStaticBearer()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
	req.Header.Set("Authorization", "Bearer claude:"+secret)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestRequireStaticBearerRejectsMissingHeader(t *testing.T) {
	svc := newStaticBearerService("test-secret-token-at-least-32-chars-long")
	handler := svc.RequireStaticBearer()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run without bearer")
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer") {
		t.Fatalf("WWW-Authenticate = %q, want Bearer challenge", got)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "missing bearer") {
		t.Fatalf("body = %q", body)
	}
}

func TestRequireStaticBearerRejectsWrongToken(t *testing.T) {
	svc := newStaticBearerService("right-secret-token-at-least-32-chars-long")
	handler := svc.RequireStaticBearer()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run with wrong bearer")
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
	req.Header.Set("Authorization", "Bearer codex:wrong-secret-token-at-least-32-chars-long-x")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestRequireStaticBearerRejectsNonBearerScheme(t *testing.T) {
	svc := newStaticBearerService("right-secret-token-at-least-32-chars-long")
	handler := svc.RequireStaticBearer()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run for non-Bearer scheme")
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestRequireStaticBearerHandlesEmptySecretAsServerError(t *testing.T) {
	svc := newStaticBearerService("")
	handler := svc.RequireStaticBearer()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run when secret is unset")
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
	req.Header.Set("Authorization", "Bearer codex:anything")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 when secret unset", rec.Code)
	}
}
