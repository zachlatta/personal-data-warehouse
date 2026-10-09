package accesstokens

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

const httpMaster = "test-secret-token-at-least-32-chars-long"

func newHTTPFixture(t *testing.T) (*Service, *clock, http.Handler) {
	t.Helper()
	svc, c := newTestService(t)
	authSvc := pdwauth.NewService([]byte(httpMaster), c.Now)
	authSvc.SetTokenAuthenticator(svc)
	mux := http.NewServeMux()
	Register(mux, svc, authSvc.RequireStaticBearer(), "https://pdw.example.test/")
	// Every request goes through a client-name holder, as in the real server.
	return svc, c, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(pdwauth.WithClientNameHolder(r.Context())))
	})
}

func do(t *testing.T, h http.Handler, method, path, bearer, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestTheTokenAPIIssuesListsAndRevokesUnderTheMasterSecret(t *testing.T) {
	_, _, h := newHTTPFixture(t)
	rec := do(t, h, http.MethodPost, TokensPath, "codex:"+httpMaster, `{"client_name":"ios","label":"phone","idle_timeout":"14d"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var created struct {
		Token string    `json:"token"`
		Info  TokenView `json:"info"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(created.Token, "pdw_") || created.Info.IdleTimeout != "14d" || created.Info.CreatedBy != "codex" || created.Info.Status != StatusActive {
		t.Fatalf("created = %+v", created)
	}

	// The new token works on the API surfaces.
	if rec := do(t, h, http.MethodPost, BootstrapRedeemPath, "", `{"code":"x"}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("garbage code: %d", rec.Code)
	}

	rec = do(t, h, http.MethodGet, TokensPath, "codex:"+httpMaster, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), created.Token) || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("listing leaks a secret: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), created.Info.ID) {
		t.Fatalf("listing misses the token: %s", rec.Body.String())
	}

	// An issued token cannot administer tokens.
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		if rec := do(t, h, method, TokensPath, "ios:"+created.Token, `{"client_name":"escalate"}`); rec.Code != http.StatusForbidden {
			t.Fatalf("%s with an issued token: %d", method, rec.Code)
		}
	}

	rec = do(t, h, http.MethodDelete, TokensPath+"/"+created.Info.ID, "codex:"+httpMaster, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("revoke: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h, http.MethodGet, TokensPath, "ios:"+created.Token, ""); rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "revoked") {
		t.Fatalf("revoked token: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h, http.MethodDelete, TokensPath+"/nope", "codex:"+httpMaster, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("revoke unknown: %d", rec.Code)
	}
}

func TestTheTokenAPIRejectsABadRequest(t *testing.T) {
	_, _, h := newHTTPFixture(t)
	for _, body := range []string{`{`, `{"client_name":""}`, `{"client_name":"x","idle_timeout":"forever"}`, `{"client_name":"x","scope":"root"}`} {
		if rec := do(t, h, http.MethodPost, TokensPath, "codex:"+httpMaster, body); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
}

func TestRedeemingACodeOverHTTPReturnsAnAgentTokenOnce(t *testing.T) {
	svc, c, h := newHTTPFixture(t)
	code, err := svc.CreateBootstrap(t.Context(), BootstrapRequest{ClientName: "cowork", CreatedBy: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	rec := do(t, h, http.MethodPost, BootstrapRedeemPath, "", `{"code":"`+code.Code+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("redeem: %d %s", rec.Code, rec.Body.String())
	}
	var got RedeemResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.BaseURL != "https://pdw.example.test" || got.ClientName != "cowork" || got.Scope != "agent" || !strings.HasPrefix(got.Token, "pdw_") {
		t.Fatalf("redeem = %+v", got)
	}
	if want := c.Now().Add(DefaultAgentTTL); !got.ExpiresAt.Equal(want) {
		t.Fatalf("expires_at = %v, want %v", got.ExpiresAt, want)
	}
	if rec := do(t, h, http.MethodPost, BootstrapRedeemPath, "", `{"code":"`+code.Code+`"}`); rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "already used") {
		t.Fatalf("second redeem: %d %s", rec.Code, rec.Body.String())
	}
	// The agent token cannot reach the token API.
	if rec := do(t, h, http.MethodGet, TokensPath, "cowork:"+got.Token, ""); rec.Code != http.StatusForbidden {
		t.Fatalf("agent on token API: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, BootstrapRedeemPath, "", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET redeem: %d", rec.Code)
	}
}

func TestTheBootstrapScriptHoldsNoSecretAndLogsIn(t *testing.T) {
	_, _, h := newHTTPFixture(t)
	rec := do(t, h, http.MethodGet, BootstrapScriptPath, "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("script: %d", rec.Code)
	}
	script := rec.Body.String()
	for _, want := range []string{"#!/bin/sh", "'https://pdw.example.test'", "login --bootstrap", `"$base_url/cli/install.sh"`, "PDW_INSTALL_DIR"} {
		if !strings.Contains(script, want) {
			t.Fatalf("script lacks %q:\n%s", want, script)
		}
	}
	// GitHub refuses many agent sandboxes' egress, so the install goes
	// through the app's release proxy (internal/clirelease) only.
	if strings.Contains(script, "github") {
		t.Fatalf("script reaches GitHub directly:\n%s", script)
	}
	if strings.Contains(script, httpMaster) {
		t.Fatal("script embeds the master secret")
	}
}

func TestBootstrapCommandQuotesItsArguments(t *testing.T) {
	cmd := BootstrapCommand("https://pdw.example.test/", "pdwb_abc_d-e")
	if cmd != "curl -fsSL 'https://pdw.example.test/cli/bootstrap.sh' | sh -s -- 'pdwb_abc_d-e'" {
		t.Fatalf("command = %q", cmd)
	}
}

func TestDurationsAcceptDays(t *testing.T) {
	for in, want := range map[string]time.Duration{"30d": 30 * 24 * time.Hour, "12h": 12 * time.Hour, "": 0, "1d12h": 36 * time.Hour} {
		got, err := ParseDuration(in)
		if err != nil || got != want {
			t.Fatalf("%q = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := ParseDuration("-1d"); err == nil {
		t.Fatal("negative accepted")
	}
}
