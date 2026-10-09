package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/accesstokens"
	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
)

const cliTestMaster = "cli-test-master-secret-at-least-32-chars"

// tokenServer is the real token API and redemption endpoint, plus a stand-in
// /api/tools behind the same static bearer, so the CLI is tested against the
// server's actual auth rather than a mock of it.
func tokenServer(t *testing.T) (*httptest.Server, *accesstokens.Service) {
	t.Helper()
	svc := accesstokens.NewService(accesstokens.NewMemoryStore(), time.Now, nil)
	authSvc := pdwauth.NewService([]byte(cliTestMaster), time.Now)
	authSvc.SetTokenAuthenticator(svc)
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		accesstokensMux(svc, authSvc, srv.URL).ServeHTTP(w, r)
	})
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(pdwauth.WithClientNameHolder(r.Context())))
	}))
	t.Cleanup(srv.Close)
	return srv, svc
}

func accesstokensMux(svc *accesstokens.Service, authSvc *pdwauth.Service, baseURL string) http.Handler {
	mux := http.NewServeMux()
	accesstokens.Register(mux, svc, authSvc.RequireStaticBearer(), baseURL)
	tools := authSvc.RequireStaticBearer()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"name":"search","title":"Search","description":"d","input_schema":{}}]}`))
	}))
	mux.Handle("/api/tools", tools)
	mux.Handle("/api/tools/", tools)
	return mux
}

func TestLoginBootstrapRedeemsACodeAndSavesAShortLivedToken(t *testing.T) {
	srv, svc := tokenServer(t)
	code, err := svc.CreateBootstrap(context.Background(), accesstokens.BootstrapRequest{ClientName: "cowork", CreatedBy: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	var stdout, stderr bytes.Buffer
	exit := run([]string{"login", "--bootstrap", code.Code, "--base-url", srv.URL}, strings.NewReader(""), &stdout, &stderr, envWithHome(home))
	if exit != 0 {
		t.Fatalf("exit = %d, stderr = %s", exit, stderr.String())
	}
	cfg, err := cliconfig.Load(configPathFor(t, home))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BaseURL != srv.URL || cfg.ClientName != "cowork" || !strings.HasPrefix(cfg.Token, "pdw_") || cfg.TokenExpiresAt == "" {
		t.Fatalf("config = %+v", cfg)
	}
	if strings.Contains(stdout.String(), cfg.Token) || strings.Contains(stdout.String(), code.Code) {
		t.Fatalf("login echoed a credential: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "expires") {
		t.Fatalf("login should say when the token expires: %s", stdout.String())
	}

	// The saved CLI works against the tools API.
	stdout.Reset()
	stderr.Reset()
	if exit := run([]string{"list"}, strings.NewReader(""), &stdout, &stderr, envWithHome(home)); exit != 0 {
		t.Fatalf("pdw list with the bootstrapped token: %d %s", exit, stderr.String())
	}

	// The code was single use.
	other := t.TempDir()
	stderr.Reset()
	if exit := run([]string{"login", "--bootstrap", code.Code, "--base-url", srv.URL}, strings.NewReader(""), &stdout, &stderr, envWithHome(other)); exit == 0 {
		t.Fatal("a second login redeemed the same code")
	}
	if !strings.Contains(stderr.String(), "already used") {
		t.Fatalf("stderr = %s", stderr.String())
	}
}

func TestLoginBootstrapWillNotReplaceALongLivedLogin(t *testing.T) {
	srv, svc := tokenServer(t)
	home := t.TempDir()
	path := configPathFor(t, home)
	if err := cliconfig.Save(path, cliconfig.Config{BaseURL: srv.URL, Token: cliTestMaster, ClientName: "laptop"}); err != nil {
		t.Fatal(err)
	}
	code, err := svc.CreateBootstrap(context.Background(), accesstokens.BootstrapRequest{ClientName: "cowork"})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if exit := run([]string{"login", "--bootstrap", code.Code, "--base-url", srv.URL}, strings.NewReader(""), &stdout, &stderr, envWithHome(home)); exit == 0 {
		t.Fatal("bootstrap replaced an existing long-lived login")
	}
	if !strings.Contains(stderr.String(), "--force") {
		t.Fatalf("stderr = %s", stderr.String())
	}
	if cfg, _ := cliconfig.Load(path); cfg.Token != cliTestMaster {
		t.Fatal("the existing login was modified")
	}
	// Refusing must not burn the code: it still works with --force.
	stderr.Reset()
	if exit := run([]string{"login", "--bootstrap", code.Code, "--base-url", srv.URL, "--force"}, strings.NewReader(""), &stdout, &stderr, envWithHome(home)); exit != 0 {
		t.Fatalf("--force: %d %s", exit, stderr.String())
	}
}

func TestTokenCommandsIssueListAndRevoke(t *testing.T) {
	srv, _ := tokenServer(t)
	env := envWithHome(t.TempDir(), map[string]string{"PDW_API_URL": srv.URL, "PDW_SECRET_TOKEN": cliTestMaster, "PDW_CLIENT_NAME": "codex"})
	var stdout, stderr bytes.Buffer
	if exit := run([]string{"token", "create", "--name", "ios", "--label", "phone", "--idle", "14d"}, strings.NewReader(""), &stdout, &stderr, env); exit != 0 {
		t.Fatalf("create: %d %s", exit, stderr.String())
	}
	var secret string
	for _, field := range strings.Fields(stdout.String()) {
		if strings.HasPrefix(field, "pdw_") {
			secret = field
		}
	}
	if secret == "" {
		t.Fatalf("create printed no token: %s", stdout.String())
	}
	id := strings.SplitN(strings.TrimPrefix(secret, "pdw_"), "_", 2)[0]

	stdout.Reset()
	if exit := run([]string{"token", "list"}, strings.NewReader(""), &stdout, &stderr, env); exit != 0 {
		t.Fatalf("list: %d %s", exit, stderr.String())
	}
	if !strings.Contains(stdout.String(), id) || !strings.Contains(stdout.String(), "ios") || !strings.Contains(stdout.String(), "active") || strings.Contains(stdout.String(), secret) {
		t.Fatalf("list = %s", stdout.String())
	}

	stdout.Reset()
	if exit := run([]string{"token", "list", "--output", "json"}, strings.NewReader(""), &stdout, &stderr, env); exit != 0 {
		t.Fatalf("list json: %d %s", exit, stderr.String())
	}
	var listed struct {
		Tokens []accesstokens.TokenView `json:"tokens"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &listed); err != nil || len(listed.Tokens) != 1 || listed.Tokens[0].IdleTimeout != "14d" {
		t.Fatalf("json list = %s (%v)", stdout.String(), err)
	}

	// The issued token works as the CLI's credential.
	issuedEnv := envWithHome(t.TempDir(), map[string]string{"PDW_API_URL": srv.URL, "PDW_SECRET_TOKEN": secret, "PDW_CLIENT_NAME": "ios"})
	if exit := run([]string{"list"}, strings.NewReader(""), &stdout, &stderr, issuedEnv); exit != 0 {
		t.Fatalf("issued token on list: %s", stderr.String())
	}
	// ...but cannot administer tokens.
	stderr.Reset()
	if exit := run([]string{"token", "list"}, strings.NewReader(""), &stdout, &stderr, issuedEnv); exit == 0 || !strings.Contains(stderr.String(), "master secret") {
		t.Fatalf("issued token listed tokens: %s", stderr.String())
	}

	stdout.Reset()
	if exit := run([]string{"token", "revoke", id}, strings.NewReader(""), &stdout, &stderr, env); exit != 0 {
		t.Fatalf("revoke: %d %s", exit, stderr.String())
	}
	stderr.Reset()
	if exit := run([]string{"list"}, strings.NewReader(""), &stdout, &stderr, issuedEnv); exit == 0 || !strings.Contains(stderr.String(), "revoked") {
		t.Fatalf("revoked token still works: %s", stderr.String())
	}
}

func TestTokenCommandRejectsUnknownSubcommands(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := envWithHome(t.TempDir(), map[string]string{"PDW_API_URL": "http://127.0.0.1:1", "PDW_SECRET_TOKEN": cliTestMaster})
	if exit := run([]string{"token", "mint"}, strings.NewReader(""), &stdout, &stderr, env); exit != 2 {
		t.Fatalf("exit = %d", exit)
	}
}
