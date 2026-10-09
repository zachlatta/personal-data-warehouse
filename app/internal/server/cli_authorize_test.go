package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/accesstokens"
)

func mcpCallTool(t *testing.T, srv *httptest.Server, bearer, name string, args map[string]any) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]any{"name": name, "arguments": args},
	})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-06-18")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tools/call %s: %d %s", name, resp.StatusCode, raw)
	}
	return string(raw)
}

func apiRequest(t *testing.T, srv *httptest.Server, method, path, bearer, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

var bootstrapCommandPattern = regexp.MustCompile(`curl -fsSL '([^']+)' \| sh -s -- '(pdwb_[^']+)'`)

// The whole cli_authorize flow at the HTTP surface: an MCP session asks for a
// command, the command's code is redeemed once for an agent token, and that
// token reaches the CLI's API and nothing else.
func TestCLIAuthorizeBootstrapsAnAgentScopedCLIToken(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	srv := newMCPTestServer(t)
	oauth := oauthAccessToken(t, srv)

	raw := mcpCallTool(t, srv, oauth, "cli_authorize", map[string]any{"client_name": "cowork", "lifetime_hours": 2})
	match := bootstrapCommandPattern.FindStringSubmatch(raw)
	if match == nil {
		t.Fatalf("no bootstrap command in %s", raw)
	}
	scriptURL, code := match[1], match[2]
	if scriptURL != "http://example.test/cli/bootstrap.sh" {
		t.Fatalf("script url = %q", scriptURL)
	}
	if strings.Contains(logs.String(), code) {
		t.Fatal("the bootstrap code reached the logs")
	}

	// The script itself is public and carries no credential.
	status, script := apiRequest(t, srv, http.MethodGet, "/cli/bootstrap.sh", "", "")
	if status != http.StatusOK || !strings.Contains(script, "login --bootstrap") || strings.Contains(script, muxAPITestSecret) {
		t.Fatalf("script: %d\n%s", status, script)
	}

	status, body := apiRequest(t, srv, http.MethodPost, accesstokens.BootstrapRedeemPath, "", `{"code":"`+code+`"}`)
	if status != http.StatusOK {
		t.Fatalf("redeem: %d %s", status, body)
	}
	var redeemed accesstokens.RedeemResponse
	if err := json.Unmarshal([]byte(body), &redeemed); err != nil {
		t.Fatal(err)
	}
	if redeemed.Scope != "agent" || redeemed.ClientName != "cowork" {
		t.Fatalf("redeemed = %+v", redeemed)
	}
	if strings.Contains(logs.String(), redeemed.Token) {
		t.Fatal("the minted token reached the logs")
	}
	if status, body := apiRequest(t, srv, http.MethodPost, accesstokens.BootstrapRedeemPath, "", `{"code":"`+code+`"}`); status != http.StatusUnauthorized {
		t.Fatalf("second redeem: %d %s", status, body)
	}

	bearer := "cowork:" + redeemed.Token
	status, body = apiRequest(t, srv, http.MethodGet, "/api/tools", bearer, "")
	if status != http.StatusOK {
		t.Fatalf("agent token on /api/tools: %d %s", status, body)
	}
	if strings.Contains(body, `"cli_authorize"`) {
		t.Fatal("cli_authorize is listed on the CLI surface; it is MCP-only")
	}
	for _, path := range []string{"/api/push/categories", accesstokens.TokensPath, "/api/connections", "/api/push/register"} {
		if status, _ := apiRequest(t, srv, http.MethodGet, path, bearer, ""); status != http.StatusForbidden {
			t.Fatalf("agent token on %s: %d", path, status)
		}
	}

	// The MCP session's OAuth token is not a static bearer for the token API.
	if status, _ := apiRequest(t, srv, http.MethodGet, accesstokens.TokensPath, oauth, ""); status != http.StatusUnauthorized {
		t.Fatalf("oauth token on token API: %d", status)
	}
	// The master secret lists the code and the token it minted.
	status, body = apiRequest(t, srv, http.MethodGet, accesstokens.TokensPath, "codex:"+muxAPITestSecret, "")
	if status != http.StatusOK || !strings.Contains(body, redeemed.TokenID) || !strings.Contains(body, `"redeemed"`) {
		t.Fatalf("list: %d %s", status, body)
	}
	if !strings.Contains(body, `"created_by":"claude-test"`) {
		t.Fatalf("the minted token should name the MCP client that asked for it: %s", body)
	}
}

func TestIssuedFullTokensWorkBesideTheMasterSecret(t *testing.T) {
	srv := newMCPTestServer(t)
	status, body := apiRequest(t, srv, http.MethodPost, accesstokens.TokensPath, "codex:"+muxAPITestSecret, `{"client_name":"ios"}`)
	if status != http.StatusCreated {
		t.Fatalf("create: %d %s", status, body)
	}
	var created struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal([]byte(body), &created)
	for _, bearer := range []string{"ios:" + created.Token, "codex:" + muxAPITestSecret} {
		if status, body := apiRequest(t, srv, http.MethodGet, "/api/tools", bearer, ""); status != http.StatusOK {
			t.Fatalf("%s on /api/tools: %d %s", bearer[:4], status, body)
		}
		if status, body := apiRequest(t, srv, http.MethodGet, "/api/push/categories", bearer, ""); status == http.StatusUnauthorized || status == http.StatusForbidden {
			t.Fatalf("%s on push: %d %s", bearer[:4], status, body)
		}
	}
}
