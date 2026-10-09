package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testMaster = "test-secret-token-at-least-32-chars-long"

type fakeTokens struct {
	principals map[string]Principal
	rejections map[string]error
	calls      int
}

func (f *fakeTokens) AuthenticateToken(_ context.Context, raw string) (Principal, error) {
	f.calls++
	if err, ok := f.rejections[raw]; ok {
		return Principal{}, err
	}
	if p, ok := f.principals[raw]; ok {
		return p, nil
	}
	return Principal{}, ErrUnknownToken
}

func serveStatic(t *testing.T, svc *Service, path, authz string) (*httptest.ResponseRecorder, Principal, bool) {
	t.Helper()
	var seen Principal
	var ran bool
	handler := svc.RequireStaticBearer()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran = true
		seen, _ = PrincipalFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req = req.WithContext(WithClientNameHolder(req.Context()))
	req.Header.Set("Authorization", authz)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec, seen, ran
}

func TestMasterSecretIsTheRootPrincipal(t *testing.T) {
	svc := newStaticBearerService(testMaster)
	svc.SetTokenAuthenticator(&fakeTokens{})
	rec, p, _ := serveStatic(t, svc, "/api/auth/tokens", "Bearer codex:"+testMaster)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if p.Scope != ScopeRoot || p.ClientName != "codex" || p.TokenID != "" {
		t.Fatalf("principal = %+v", p)
	}
}

func TestIssuedTokensAreAcceptedBesideTheMasterSecret(t *testing.T) {
	svc := newStaticBearerService(testMaster)
	tokens := &fakeTokens{principals: map[string]Principal{
		"pdw_aaaa_one": {Scope: ScopeFull, TokenID: "aaaa", ClientName: "ios"},
		"pdw_bbbb_two": {Scope: ScopeFull, TokenID: "bbbb", ClientName: "laptop"},
	}}
	svc.SetTokenAuthenticator(tokens)
	for raw, id := range map[string]string{"pdw_aaaa_one": "aaaa", "pdw_bbbb_two": "bbbb"} {
		rec, p, _ := serveStatic(t, svc, "/api/mutation-requests", "Bearer phone:"+raw)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, body = %s", raw, rec.Code, rec.Body.String())
		}
		if p.TokenID != id || p.Scope != ScopeFull || p.ClientName != "phone" {
			t.Fatalf("%s: principal = %+v", raw, p)
		}
	}
	// The master secret never reaches the store: it is compared in constant
	// time first, so a database outage cannot lock the owner out.
	tokens.calls = 0
	if rec, _, _ := serveStatic(t, svc, "/api/tools", "Bearer codex:"+testMaster); rec.Code != http.StatusOK {
		t.Fatalf("master status = %d", rec.Code)
	}
	if tokens.calls != 0 {
		t.Fatalf("master secret consulted the token store %d times", tokens.calls)
	}
}

func TestARejectedIssuedTokenSaysWhy(t *testing.T) {
	svc := newStaticBearerService(testMaster)
	svc.SetTokenAuthenticator(&fakeTokens{rejections: map[string]error{
		"pdw_cccc_idle": &TokenRejectedError{Reason: "access token expired: unused for 30 days"},
	}})
	rec, _, ran := serveStatic(t, svc, "/api/tools", "Bearer codex:pdw_cccc_idle")
	if ran {
		t.Fatal("handler ran for a rejected token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "unused for 30 days") {
		t.Fatalf("body = %q", rec.Body.String())
	}
	if !strings.Contains(rec.Header().Get("WWW-Authenticate"), "invalid_token") {
		t.Fatalf("WWW-Authenticate = %q", rec.Header().Get("WWW-Authenticate"))
	}
}

func TestATokenStoreOutageIsNotAnAuthenticationFailure(t *testing.T) {
	svc := newStaticBearerService(testMaster)
	svc.SetTokenAuthenticator(&fakeTokens{rejections: map[string]error{
		"pdw_dddd_x": errors.New("connection refused"),
	}})
	rec, _, ran := serveStatic(t, svc, "/api/tools", "Bearer codex:pdw_dddd_x")
	if ran || rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d ran = %v", rec.Code, ran)
	}
	if strings.Contains(rec.Body.String(), "connection refused") {
		t.Fatalf("store error leaked to the client: %q", rec.Body.String())
	}
}

func TestUnknownTokenWithoutAStoreIsInvalid(t *testing.T) {
	svc := newStaticBearerService(testMaster)
	rec, _, ran := serveStatic(t, svc, "/api/tools", "Bearer codex:pdw_eeee_nope")
	if ran || rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d ran = %v", rec.Code, ran)
	}
}

func TestAgentScopeReachesOnlyTheToolsAPI(t *testing.T) {
	svc := newStaticBearerService(testMaster)
	svc.SetTokenAuthenticator(&fakeTokens{principals: map[string]Principal{
		"pdw_ffff_agent": {Scope: ScopeAgent, TokenID: "ffff", ClientName: "cowork"},
	}})
	for _, path := range []string{"/api/tools", "/api/tools/sql"} {
		if rec, _, _ := serveStatic(t, svc, path, "Bearer cowork:pdw_ffff_agent"); rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d", path, rec.Code)
		}
	}
	for _, path := range []string{"/api/mutation-requests/abc/approve", "/api/auth/tokens", "/api/connections", "/api/tools-evil", "/api/timeline"} {
		rec, _, ran := serveStatic(t, svc, path, "Bearer cowork:pdw_ffff_agent")
		if ran || rec.Code != http.StatusForbidden {
			t.Fatalf("%s: status = %d ran = %v", path, rec.Code, ran)
		}
		if !strings.Contains(rec.Body.String(), "/api/tools") {
			t.Fatalf("%s: body should name the allowed surface: %q", path, rec.Body.String())
		}
	}
}

func TestRequireRootAdmitsOnlyTheMasterSecret(t *testing.T) {
	svc := newStaticBearerService(testMaster)
	svc.SetTokenAuthenticator(&fakeTokens{principals: map[string]Principal{
		"pdw_gggg_full": {Scope: ScopeFull, TokenID: "gggg", ClientName: "ios"},
	}})
	handler := svc.RequireStaticBearer()(RequireRoot(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))
	for authz, want := range map[string]int{
		"Bearer codex:" + testMaster:  http.StatusNoContent,
		"Bearer codex:pdw_gggg_full": http.StatusForbidden,
	} {
		req := httptest.NewRequest(http.MethodGet, "/api/auth/tokens", nil)
		req = req.WithContext(WithClientNameHolder(req.Context()))
		req.Header.Set("Authorization", authz)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Fatalf("%s: status = %d, want %d", authz, rec.Code, want)
		}
	}
}
