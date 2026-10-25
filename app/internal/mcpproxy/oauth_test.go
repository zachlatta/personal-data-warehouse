package mcpproxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestOAuthWebFlowBindsBrowserUsesPKCEAndConsumesState(t *testing.T) {
	var endpoint, challenge string
	exchanges := 0
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/mcp":
			w.Header().Set("WWW-Authenticate", `Bearer resource_metadata="`+endpoint+`/.well-known/oauth-protected-resource"`)
			w.WriteHeader(401)
		case "/.well-known/oauth-protected-resource":
			json.NewEncoder(w).Encode(map[string]any{"resource": endpoint + "/mcp", "authorization_servers": []string{endpoint}, "scopes_supported": []string{"tools"}})
		case "/.well-known/oauth-authorization-server":
			json.NewEncoder(w).Encode(map[string]any{"issuer": endpoint, "authorization_endpoint": endpoint + "/authorize", "token_endpoint": endpoint + "/token", "registration_endpoint": endpoint + "/register", "jwks_uri": endpoint + "/jwks", "response_types_supported": []string{"code"}, "code_challenge_methods_supported": []string{"S256"}})
		case "/register":
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{"client_id": "pdw-client", "token_endpoint_auth_method": "none", "redirect_uris": []string{"https://pdw.example/connections/oauth/callback"}})
		case "/token":
			exchanges++
			r.ParseForm()
			if r.Form.Get("resource") != endpoint+"/mcp" || r.Form.Get("code") != "code" || pkceChallenge(r.Form.Get("code_verifier")) != challenge {
				t.Error("missing resource or PKCE binding")
			}
			json.NewEncoder(w).Encode(map[string]any{"access_token": "secret-token", "refresh_token": "secret-refresh", "token_type": "Bearer", "expires_in": 3600})
		default:
			w.WriteHeader(404)
		}
	}))
	defer remote.Close()
	endpoint = remote.URL
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	store.Update(context.Background(), "skills", func(c *record) error { c.Name = "skills"; c.URL = endpoint + "/mcp"; c.Enabled = true; return nil })
	authURL, browser, err := service.BeginOAuth(context.Background(), "skills")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(authURL)
	q := parsed.Query()
	challenge = q.Get("code_challenge")
	if q.Get("code_challenge_method") != "S256" || challenge == "" || q.Get("resource") != endpoint+"/mcp" {
		t.Fatal(q)
	}
	state := q.Get("state")
	if _, err = service.CompleteOAuth(context.Background(), state, "code", "wrong-browser", ""); err == nil {
		t.Fatal("accepted other browser")
	}
	if exchanges != 0 {
		t.Fatal("exchanged before validating browser")
	}
	id, err := service.CompleteOAuth(context.Background(), state, "code", browser, "")
	if err != nil || id != "skills" {
		t.Fatalf("complete: %s %v", id, err)
	}
	if _, err = service.CompleteOAuth(context.Background(), state, "code", browser, ""); err == nil {
		t.Fatal("state replay")
	}
	if exchanges != 1 {
		t.Fatalf("exchanges %d", exchanges)
	}
	records, _ := store.List(context.Background())
	if records[0].Token.RefreshToken != "secret-refresh" {
		t.Fatal("refresh token not persisted")
	}
}

func TestExpiredOAuthStateIsRejected(t *testing.T) {
	store := newMemoryStore()
	service := New(store, "https://pdw.example", nil)
	store.Update(context.Background(), "tasks", func(c *record) error {
		c.Pending = &pendingAuth{State: "tasks.state", Browser: "browser", Expires: time.Now().Add(-time.Minute)}
		return nil
	})
	if _, err := service.CompleteOAuth(context.Background(), "tasks.state", "code", "browser", ""); err == nil {
		t.Fatal("expired state accepted")
	}
}

func TestManagementAPIRequiresAuthAndNeverReturnsCredentials(t *testing.T) {
	store := newMemoryStore()
	service := New(store, "https://pdw.example", nil)
	mux := http.NewServeMux()
	protect := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "admin" {
				w.WriteHeader(401)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	service.Register(mux, protect)
	for _, method := range []string{"GET", "POST"} {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(method, "/api/connections", nil))
		if rr.Code != 401 {
			t.Fatalf("unprotected: %d", rr.Code)
		}
	}
	req := httptest.NewRequest("POST", "/api/connections", strings.NewReader(`{"name":"skills","url":"https://skills.example/mcp","token":"secret-token","all_clients":true}`))
	req.Header.Set("Authorization", "admin")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != 201 {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}
	req = httptest.NewRequest("GET", "/api/connections", nil)
	req.Header.Set("Authorization", "admin")
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != 200 || strings.Contains(rr.Body.String(), "secret-token") {
		t.Fatalf("list: %d %s", rr.Code, rr.Body.String())
	}
}

func TestOAuthCallbackCannotAuthenticateWithoutBrowserCookie(t *testing.T) {
	store := newMemoryStore()
	service := New(store, "https://pdw.example", nil)
	store.Update(context.Background(), "skills", func(c *record) error {
		c.Pending = &pendingAuth{State: "skills.state", Browser: "correct-browser", Expires: time.Now().Add(time.Minute)}
		return nil
	})
	rr := httptest.NewRecorder()
	service.callback(rr, httptest.NewRequest("GET", callbackPath+"?state=skills.state&code=stolen-code", nil))
	if rr.Code != http.StatusSeeOther || rr.Header().Get("Location") != "/connections?auth=failed" {
		t.Fatalf("callback: %d %s", rr.Code, rr.Header().Get("Location"))
	}
	rows, _ := store.List(context.Background())
	if rows[0].Token.AccessToken != "" || rows[0].Pending == nil {
		t.Fatal("unbound callback modified authorization")
	}
}

func TestConcurrentManagementChangesFailClosed(t *testing.T) {
	store := newMemoryStore()
	service := New(store, "https://pdw.example", nil)
	store.Update(context.Background(), "tasks", func(c *record) error { c.Enabled = true; return nil })
	mux := http.NewServeMux()
	service.Register(mux, func(h http.Handler) http.Handler { return h })
	for _, status := range []int{200, 409} {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest("POST", "/api/connections/tasks/disable", strings.NewReader(`{"version":1}`)))
		if rr.Code != status {
			t.Fatalf("wanted %d, got %d: %s", status, rr.Code, rr.Body.String())
		}
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("POST", "/api/connections/tasks/remove", strings.NewReader(`{"version":2}`)))
	if rr.Code != 200 {
		t.Fatal(rr.Body.String())
	}
	rows, _ := store.List(context.Background())
	if len(rows) != 0 {
		t.Fatal("removed connection still listed")
	}
}

func TestConnectionNamesCannotCreateNamespaceCollisions(t *testing.T) {
	service := New(newMemoryStore(), "https://pdw.example", nil)
	for _, name := range []string{"a_", "a__b", "Upper", "a/b", ""} {
		body, _ := json.Marshal(map[string]any{"name": name, "url": "https://example.com/mcp"})
		rr := httptest.NewRecorder()
		service.connections(rr, httptest.NewRequest("POST", "/api/connections", strings.NewReader(string(body))))
		if rr.Code != 400 {
			t.Fatalf("ambiguous connection name accepted: %q", name)
		}
	}
}

func TestAppOAuthStartLinkBindsOneBrowserAndReturnsToApp(t *testing.T) {
	var endpoint string
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/mcp":
			w.Header().Set("WWW-Authenticate", `Bearer resource_metadata="`+endpoint+`/.well-known/oauth-protected-resource"`)
			w.WriteHeader(401)
		case "/.well-known/oauth-protected-resource":
			json.NewEncoder(w).Encode(map[string]any{"resource": endpoint + "/mcp", "authorization_servers": []string{endpoint}})
		case "/.well-known/oauth-authorization-server":
			json.NewEncoder(w).Encode(map[string]any{"issuer": endpoint, "authorization_endpoint": endpoint + "/authorize", "token_endpoint": endpoint + "/token", "registration_endpoint": endpoint + "/register", "response_types_supported": []string{"code"}, "code_challenge_methods_supported": []string{"S256"}})
		case "/register":
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{"client_id": "pdw-client", "token_endpoint_auth_method": "none"})
		case "/token":
			json.NewEncoder(w).Encode(map[string]any{"access_token": "secret-token", "token_type": "Bearer", "expires_in": 3600})
		default:
			w.WriteHeader(404)
		}
	}))
	defer remote.Close()
	endpoint = remote.URL
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	mux := http.NewServeMux()
	service.Register(mux, func(next http.Handler) http.Handler { return next })
	store.Update(context.Background(), "tasks", func(c *record) error { c.Name = "tasks"; c.URL = endpoint + "/mcp"; c.Enabled = true; return nil })

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("POST", "/api/connections/tasks/authorize?return=app", nil))
	var begun map[string]string
	json.Unmarshal(rr.Body.Bytes(), &begun)
	if rr.Code != 200 || len(rr.Result().Cookies()) != 0 || !strings.HasPrefix(begun["start_url"], "https://pdw.example"+startPath+"?h=tasks.") {
		t.Fatalf("authorize: %d %s cookies=%d", rr.Code, rr.Body.String(), len(rr.Result().Cookies()))
	}
	start, _ := url.Parse(begun["start_url"])

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", start.RequestURI(), nil))
	cookies := rr.Result().Cookies()
	if rr.Code != http.StatusSeeOther || !strings.HasPrefix(rr.Header().Get("Location"), endpoint+"/authorize?") || len(cookies) != 1 || cookies[0].Value == "" {
		t.Fatalf("start: %d %s", rr.Code, rr.Header().Get("Location"))
	}
	state, _ := url.Parse(rr.Header().Get("Location"))

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", start.RequestURI(), nil))
	if rr.Code != http.StatusSeeOther || rr.Header().Get("Location") != appReturn+"?auth=failed" || len(rr.Result().Cookies()) != 0 {
		t.Fatalf("start link replay: %d %s", rr.Code, rr.Header().Get("Location"))
	}

	req := httptest.NewRequest("GET", callbackPath+"?state="+url.QueryEscape(state.Query().Get("state"))+"&code=code", nil)
	req.AddCookie(cookies[0])
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	// Discovery fails against this stub; the token is what matters here.
	if rr.Code != http.StatusSeeOther || !strings.HasPrefix(rr.Header().Get("Location"), appReturn+"?auth=") || strings.HasSuffix(rr.Header().Get("Location"), "=failed") {
		t.Fatalf("callback: %d %s", rr.Code, rr.Header().Get("Location"))
	}
	rows, _ := store.List(context.Background())
	if rows[0].Token.AccessToken != "secret-token" || rows[0].Pending != nil {
		t.Fatal("app flow did not authorize")
	}
}

// registrationUpstream is an OAuth-protected MCP server whose dynamic client
// registration endpoint answers with the given status and body.
func registrationUpstream(t *testing.T, status int, body map[string]any) *httptest.Server {
	t.Helper()
	var endpoint string
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/mcp":
			w.WriteHeader(405)
		case "/.well-known/oauth-protected-resource/mcp":
			json.NewEncoder(w).Encode(map[string]any{"resource": endpoint + "/mcp", "authorization_servers": []string{endpoint + "/mcp"}, "scopes_supported": []string{"credit-card"}})
		case "/.well-known/oauth-authorization-server/mcp":
			json.NewEncoder(w).Encode(map[string]any{"issuer": endpoint + "/mcp", "authorization_endpoint": endpoint + "/authorize", "token_endpoint": endpoint + "/token", "registration_endpoint": endpoint + "/register", "response_types_supported": []string{"code"}, "code_challenge_methods_supported": []string{"S256"}, "token_endpoint_auth_methods_supported": []string{"none"}})
		case "/register":
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(body)
		default:
			w.WriteHeader(404)
		}
	}))
	endpoint = remote.URL
	t.Cleanup(remote.Close)
	return remote
}

// Robinhood's banking MCP answers a successful registration with 200 OK, not
// the 201 Created RFC 7591 names; a client_id in a 2xx is a registration.
func TestOAuthAcceptsAClientRegistrationAnsweredWithOK(t *testing.T) {
	remote := registrationUpstream(t, http.StatusOK, map[string]any{"client_id": "issued-client", "token_endpoint_auth_method": "none", "redirect_uris": []string{"https://pdw.example/connections/oauth/callback"}})
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	store.Update(context.Background(), "card", func(c *record) error { c.Name = "card"; c.URL = remote.URL + "/mcp"; c.Enabled = true; return nil })
	authURL, _, err := service.BeginOAuth(context.Background(), "card")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(authURL)
	if parsed.Query().Get("client_id") != "issued-client" {
		t.Fatalf("auth URL %s", authURL)
	}
}

func TestOAuthRegistrationFailureNamesTheUpstreamReason(t *testing.T) {
	remote := registrationUpstream(t, http.StatusBadRequest, map[string]any{"error": "invalid_redirect_uri", "error_description": "redirect host not allowed"})
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	store.Update(context.Background(), "card", func(c *record) error { c.Name = "card"; c.URL = remote.URL + "/mcp"; c.Enabled = true; return nil })
	_, _, err := service.BeginOAuth(context.Background(), "card")
	if err == nil || !strings.Contains(err.Error(), "invalid_redirect_uri") || !strings.Contains(err.Error(), "redirect host not allowed") {
		t.Fatalf("error %v", err)
	}
}

// loopbackUpstream is an OAuth MCP server that, like Robinhood's banking MCP,
// only accepts a loopback redirect. It records what PDW registered and sent.
type loopbackUpstream struct {
	*httptest.Server
	registered []string
	challenge  string
	exchanges  int
}

func newLoopbackUpstream(t *testing.T) *loopbackUpstream {
	t.Helper()
	u := &loopbackUpstream{}
	u.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		endpoint := u.URL
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/mcp":
			w.WriteHeader(405)
		case "/.well-known/oauth-protected-resource/mcp":
			json.NewEncoder(w).Encode(map[string]any{"resource": endpoint + "/mcp", "authorization_servers": []string{endpoint + "/mcp"}, "scopes_supported": []string{"credit-card"}})
		case "/.well-known/oauth-authorization-server/mcp":
			json.NewEncoder(w).Encode(map[string]any{"issuer": endpoint + "/mcp", "authorization_endpoint": endpoint + "/authorize", "token_endpoint": endpoint + "/token", "registration_endpoint": endpoint + "/register", "response_types_supported": []string{"code"}, "code_challenge_methods_supported": []string{"S256"}})
		case "/register":
			var body struct {
				RedirectURIs []string `json:"redirect_uris"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			u.registered = body.RedirectURIs
			json.NewEncoder(w).Encode(map[string]any{"client_id": "shared-client", "token_endpoint_auth_method": "none", "redirect_uris": body.RedirectURIs})
		case "/token":
			u.exchanges++
			r.ParseForm()
			if r.Form.Get("redirect_uri") != loopbackRedirectURL || r.Form.Get("code") != "code" || pkceChallenge(r.Form.Get("code_verifier")) != u.challenge {
				w.WriteHeader(400)
				json.NewEncoder(w).Encode(map[string]any{"error": "invalid_grant"})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"access_token": "secret-token", "refresh_token": "secret-refresh", "token_type": "Bearer", "expires_in": 3600})
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(u.Close)
	return u
}

func loopbackService(t *testing.T, upstream *loopbackUpstream) (*memoryStore, *http.ServeMux) {
	t.Helper()
	store := newMemoryStore()
	service := New(store, "https://pdw.example", upstream.Client())
	mux := http.NewServeMux()
	service.Register(mux, func(next http.Handler) http.Handler { return next })
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("POST", "/api/connections", strings.NewReader(`{"name":"card","url":"`+upstream.URL+`/mcp","loopback":true}`)))
	if rr.Code != 201 || !strings.Contains(rr.Body.String(), `"loopback":true`) {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}
	return store, mux
}

func post(mux *http.ServeMux, path, body string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("POST", path, strings.NewReader(body)))
	return rr
}

func TestLoopbackConnectionCompletesFromThePastedRedirect(t *testing.T) {
	upstream := newLoopbackUpstream(t)
	store, mux := loopbackService(t, upstream)

	rr := post(mux, "/api/connections/card/authorize", "")
	var begun map[string]string
	json.Unmarshal(rr.Body.Bytes(), &begun)
	authURL, err := url.Parse(begun["authorization_url"])
	if rr.Code != 200 || err != nil {
		t.Fatalf("authorize: %d %s", rr.Code, rr.Body.String())
	}
	q := authURL.Query()
	if q.Get("redirect_uri") != loopbackRedirectURL || len(upstream.registered) != 1 || upstream.registered[0] != loopbackRedirectURL {
		t.Fatalf("redirect %q registered %v", q.Get("redirect_uri"), upstream.registered)
	}
	upstream.challenge = q.Get("code_challenge")
	pasted := loopbackRedirectURL + "?code=code&state=" + url.QueryEscape(q.Get("state")) + "&iss=" + url.QueryEscape(upstream.URL+"/mcp")

	if rr = post(mux, "/api/connections/card/complete", `{"url":"`+loopbackRedirectURL+`?code=code&state=card.forged"}`); rr.Code != 400 || upstream.exchanges != 0 {
		t.Fatalf("forged state: %d %s exchanges=%d", rr.Code, rr.Body.String(), upstream.exchanges)
	}
	if rr = post(mux, "/api/connections/card/complete", `{"url":"`+pasted+`"}`); rr.Code != 200 {
		t.Fatalf("complete: %d %s", rr.Code, rr.Body.String())
	}
	if rr = post(mux, "/api/connections/card/complete", `{"url":"`+pasted+`"}`); rr.Code != 400 || upstream.exchanges != 1 {
		t.Fatalf("replay: %d exchanges=%d", rr.Code, upstream.exchanges)
	}
	records, _ := store.List(context.Background())
	// Tool discovery fails against this stub; the stored token is what matters.
	if records[0].Token.AccessToken != "secret-token" || records[0].Token.RefreshToken != "secret-refresh" || records[0].Pending != nil {
		t.Fatal("pasted redirect did not authorize")
	}
}

func TestLoopbackPastedDenialIsReportedAndConsumesTheFlow(t *testing.T) {
	upstream := newLoopbackUpstream(t)
	store, mux := loopbackService(t, upstream)
	rr := post(mux, "/api/connections/card/authorize", "")
	var begun map[string]string
	json.Unmarshal(rr.Body.Bytes(), &begun)
	authURL, _ := url.Parse(begun["authorization_url"])
	state := authURL.Query().Get("state")
	rr = post(mux, "/api/connections/card/complete", `{"url":"`+loopbackRedirectURL+`?error=access_denied&state=`+url.QueryEscape(state)+`"}`)
	if rr.Code != 400 || !strings.Contains(rr.Body.String(), "denied") {
		t.Fatalf("denial: %d %s", rr.Code, rr.Body.String())
	}
	records, _ := store.List(context.Background())
	if records[0].Status != "authorization_denied" || records[0].Pending != nil || upstream.exchanges != 0 {
		t.Fatalf("status %s", records[0].Status)
	}
}

// Pasting is a loopback-only path: a web-callback connection stays bound to
// the browser cookie that started its flow.
func TestPastedCompletionIsRefusedForAWebCallbackConnection(t *testing.T) {
	var endpoint string
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/oauth-protected-resource":
			json.NewEncoder(w).Encode(map[string]any{"resource": endpoint + "/mcp", "authorization_servers": []string{endpoint}})
		case "/.well-known/oauth-authorization-server":
			json.NewEncoder(w).Encode(map[string]any{"issuer": endpoint, "authorization_endpoint": endpoint + "/authorize", "token_endpoint": endpoint + "/token", "registration_endpoint": endpoint + "/register", "response_types_supported": []string{"code"}, "code_challenge_methods_supported": []string{"S256"}})
		case "/register":
			json.NewEncoder(w).Encode(map[string]any{"client_id": "pdw-client", "token_endpoint_auth_method": "none"})
		case "/token":
			t.Error("exchanged a pasted code for a web-callback connection")
		default:
			w.WriteHeader(404)
		}
	}))
	defer remote.Close()
	endpoint = remote.URL
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	mux := http.NewServeMux()
	service.Register(mux, func(next http.Handler) http.Handler { return next })
	store.Update(context.Background(), "tasks", func(c *record) error { c.Name = "tasks"; c.URL = endpoint + "/mcp"; c.Enabled = true; return nil })
	authURL, _, err := service.BeginOAuth(context.Background(), "tasks")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(authURL)
	rr := post(mux, "/api/connections/tasks/complete", `{"url":"https://pdw.example/connections/oauth/callback?code=code&state=`+url.QueryEscape(parsed.Query().Get("state"))+`"}`)
	if rr.Code != 400 {
		t.Fatalf("complete: %d %s", rr.Code, rr.Body.String())
	}
}

func TestLoopbackConnectionRefusesThePhoneAppFlow(t *testing.T) {
	upstream := newLoopbackUpstream(t)
	_, mux := loopbackService(t, upstream)
	rr := post(mux, "/api/connections/card/authorize?return=app", "")
	if rr.Code != 400 || !strings.Contains(rr.Body.String(), "web") {
		t.Fatalf("app authorize: %d %s", rr.Code, rr.Body.String())
	}
}
