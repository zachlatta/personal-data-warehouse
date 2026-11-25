package server

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
)

// newMCPTestServer mirrors newMuxAPITestServer but keeps the auth clock on
// real time: the MCP SDK's bearer middleware compares token expiry against
// the wall clock, so a frozen epoch clock mints tokens it rejects as expired.
func newMCPTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	runner := fakeRunner{results: map[string]query.RawResult{}}
	authSvc := pdwauth.NewService([]byte(muxAPITestSecret), time.Now)
	cfg := config.Config{
		Addr:          ":0",
		BaseURL:       "http://example.test",
		SecretToken:   muxAPITestSecret,
		MaxRows:       100,
		MaxFieldChars: 1000,
	}
	mux := NewMux(cfg, authSvc, runner)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// oauthAccessToken runs the full dynamic-registration OAuth flow against the
// mux and returns a bearer access token, the same way the Claude connector
// obtains one.
func oauthAccessToken(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	client := srv.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	regBody := `{"redirect_uris":["https://claude.ai/api/mcp/auth_callback"],"token_endpoint_auth_method":"none"}`
	regResp, err := client.Post(srv.URL+"/oauth/register", "application/json", strings.NewReader(regBody))
	if err != nil {
		t.Fatal(err)
	}
	defer regResp.Body.Close()
	if regResp.StatusCode != http.StatusCreated {
		t.Fatalf("register status = %d", regResp.StatusCode)
	}
	var reg struct {
		ClientID string `json:"client_id"`
	}
	if err := json.NewDecoder(regResp.Body).Decode(&reg); err != nil {
		t.Fatal(err)
	}

	verifier := "test-code-verifier"
	sum := sha256.Sum256([]byte(verifier))
	form := url.Values{
		"client_id":             {reg.ClientID},
		"redirect_uri":          {"https://claude.ai/api/mcp/auth_callback"},
		"response_type":         {"code"},
		"code_challenge":        {base64.RawURLEncoding.EncodeToString(sum[:])},
		"code_challenge_method": {"S256"},
		"secret_token":          {muxAPITestSecret},
		"client_name":           {"claude-test"},
	}
	authResp, err := client.Post(srv.URL+"/oauth/authorize", "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, authResp.Body)
	authResp.Body.Close()
	if authResp.StatusCode != http.StatusFound {
		t.Fatalf("authorize status = %d", authResp.StatusCode)
	}
	redirect, err := url.Parse(authResp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	code := redirect.Query().Get("code")
	if code == "" {
		t.Fatalf("authorize redirect missing code: %q", authResp.Header.Get("Location"))
	}

	tokenForm := url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {reg.ClientID},
		"code":          {code},
		"redirect_uri":  {"https://claude.ai/api/mcp/auth_callback"},
		"code_verifier": {verifier},
	}
	tokenResp, err := client.Post(srv.URL+"/oauth/token", "application/x-www-form-urlencoded", strings.NewReader(tokenForm.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer tokenResp.Body.Close()
	if tokenResp.StatusCode != http.StatusOK {
		t.Fatalf("token status = %d", tokenResp.StatusCode)
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(tokenResp.Body).Decode(&token); err != nil {
		t.Fatal(err)
	}
	if token.AccessToken == "" {
		t.Fatal("empty access token")
	}
	return token.AccessToken
}

// The Claude connector reuses its Mcp-Session-Id across long gaps, so after a
// redeploy (or 30 idle minutes) the stateful handler answered "session not
// found" 404 and the connector's tools never loaded for that conversation.
// The /mcp endpoint must therefore serve requests statelessly: any session id,
// no prior initialize handshake required.
func TestMCPServesToolsListForUnknownSessionID(t *testing.T) {
	srv := newMCPTestServer(t)
	token := oauthAccessToken(t, srv)

	body := `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-06-18")
	req.Header.Set("Mcp-Session-Id", "stale-session-from-before-a-redeploy")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tools/list with stale session id status = %d body=%s", resp.StatusCode, raw)
	}
	var rpc struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &rpc); err != nil {
		t.Fatalf("decoding tools/list response %s: %v", raw, err)
	}
	if len(rpc.Result.Tools) == 0 {
		t.Fatalf("tools/list returned no tools: %s", raw)
	}
}

// TestMCPInitializeAcceptsJSONMediaTypeParameters pins the headers a client may
// put on its first MCP request. Gemini signed in over OAuth and then had its
// initialize refused with 415 because its Content-Type carried a charset
// parameter, which the MCP SDK compared literally until v1.6.0. Media types are
// case-insensitive and may carry parameters (RFC 9110 §8.3.1), so every
// spelling of application/json must reach the handler, while a non-JSON or
// unparseable Content-Type, a missing bearer and a foreign Origin still fail.
func TestMCPInitializeAcceptsJSONMediaTypeParameters(t *testing.T) {
	srv := newMCPTestServer(t)
	token := oauthAccessToken(t, srv)
	const body = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`
	const accept = "application/json, text/event-stream"

	for _, tc := range []struct {
		name        string
		contentType string
		accept      string
		origin      string
		bearer      string
		want        int
	}{
		{name: "JSON", contentType: "application/json", bearer: token, want: http.StatusOK},
		{name: "JSON with charset", contentType: "application/json; charset=UTF-8", bearer: token, want: http.StatusOK},
		{name: "JSON with spacing and casing", contentType: "Application/JSON ;charset=utf-8", bearer: token, want: http.StatusOK},
		{name: "Accept with parameters and q-values", contentType: "application/json", accept: "application/json;q=0.9, text/event-stream;q=0.8", bearer: token, want: http.StatusOK},
		{name: "Accept wildcard", contentType: "application/json", accept: "*/*", bearer: token, want: http.StatusOK},
		{name: "Accept without event stream", contentType: "application/json", accept: "application/json", bearer: token, want: http.StatusBadRequest},
		{name: "missing content type", bearer: token, want: http.StatusUnsupportedMediaType},
		{name: "plain text", contentType: "text/plain", bearer: token, want: http.StatusUnsupportedMediaType},
		{name: "JSON look-alike type", contentType: "application/json-seq", bearer: token, want: http.StatusUnsupportedMediaType},
		{name: "malformed parameters", contentType: "application/json; charset", bearer: token, want: http.StatusUnsupportedMediaType},
		{name: "missing bearer", contentType: "application/json; charset=UTF-8", want: http.StatusUnauthorized},
		{name: "cross-origin", contentType: "application/json; charset=UTF-8", origin: "https://other.example", bearer: token, want: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Accept", accept)
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			if tc.bearer != "" {
				req.Header.Set("Authorization", "Bearer "+tc.bearer)
			}
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tc.want {
				t.Fatalf("initialize status = %d, want %d: %s", resp.StatusCode, tc.want, raw)
			}
			if tc.want != http.StatusOK {
				return
			}
			var rpc struct {
				Result struct {
					ProtocolVersion string `json:"protocolVersion"`
				} `json:"result"`
			}
			if err := json.Unmarshal(raw, &rpc); err != nil || rpc.Result.ProtocolVersion != "2025-11-25" {
				t.Fatalf("invalid initialize response: %s (decode error: %v)", raw, err)
			}
		})
	}
}

func TestMCPConnectionsAdminAPIRejectsDownstreamOAuthTokens(t *testing.T) {
	srv := newMCPTestServer(t)
	token := oauthAccessToken(t, srv)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/connections", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("downstream token reached admin API: %d", response.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/api/connections", nil)
	req.Header.Set("Authorization", "Bearer web:"+muxAPITestSecret)
	response, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("missing database should fail explicitly, not use volatile storage: %d", response.StatusCode)
	}
}
