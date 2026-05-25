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
	handler := svc.RequireStaticBearer()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
	req.Header.Set("Authorization", "Bearer test-secret-token-at-least-32-chars-long")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("body = %q", rec.Body.String())
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
	req.Header.Set("Authorization", "Bearer wrong-secret-token-at-least-32-chars-long-x")
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
	req.Header.Set("Authorization", "Bearer anything")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 when secret unset", rec.Code)
	}
}
