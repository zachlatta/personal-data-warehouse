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
