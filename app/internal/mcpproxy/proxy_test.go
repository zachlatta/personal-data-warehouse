package mcpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/zachlatta/personal-data-warehouse/app/internal/api"
	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
	"golang.org/x/oauth2"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestURLPolicy(t *testing.T) {
	for _, raw := range []string{"http://example.com/mcp", "https://user:secret@example.com/mcp", "file:///tmp/x", "https://example.com/mcp#token", "https://example.com/mcp?token=secret"} {
		if validateResourceURL(raw) == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	if err := validateResourceURL("https://example.com/mcp"); err != nil {
		t.Fatal(err)
	}
	for _, ip := range []string{"127.0.0.1", "::1", "10.1.2.3", "169.254.169.254", "100.100.100.200", "::ffff:127.0.0.1", "192.0.2.1"} {
		if publicIP(ip) {
			t.Errorf("accepted private/reserved address %s", ip)
		}
	}
}

func TestEncryptedRecords(t *testing.T) {
	box := newCipher("test-secret")
	raw, err := seal(box, "skills", []byte("upstream-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(raw, "upstream-secret") {
		t.Fatal("plaintext persisted")
	}
	if _, err := unseal(box, "tasks", raw); err == nil {
		t.Fatal("ciphertext can be moved between connections")
	}
	if _, err := unseal(newCipher("wrong-key"), "skills", raw); err == nil {
		t.Fatal("wrong key accepted")
	}
	decoded, err := unseal(box, "skills", raw)
	if err != nil || string(decoded) != "upstream-secret" {
		t.Fatalf("round trip: %s %v", decoded, err)
	}
}

func TestProxyPreservesResultsAndDoesNotForwardPDWCredentials(t *testing.T) {
	upstream := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "1"}, nil)
	upstream.AddTool(&mcp.Tool{Name: "echo", Description: "Echo", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`)}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(req.Params.Arguments)}, &mcp.ImageContent{Data: []byte("image"), MIMEType: "image/png"}}, StructuredContent: map[string]any{"ok": true}, IsError: true}, nil
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return upstream }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer upstream-secret" {
			t.Errorf("incorrect upstream credential")
		}
		handler.ServeHTTP(w, r)
	}))
	defer remote.Close()
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	ctx := context.Background()
	err := store.Update(ctx, "skills", func(c *record) error {
		c.Name = "skills"
		c.URL = remote.URL
		c.Enabled = true
		c.AllClients = true
		c.Token.AccessToken = "upstream-secret"
		c.Token.TokenType = "Bearer"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Refresh(ctx, "skills"); err != nil {
		t.Fatal(err)
	}
	ts, err := service.Tools(ctx)
	if err != nil || len(ts) != 1 {
		t.Fatalf("tools: %d %v", len(ts), err)
	}
	if ts[0].Name() != "skills__echo" {
		t.Fatal(ts[0].Name())
	}
	out, isErr, err := ts[0].Invoke(ctx, json.RawMessage(`{"text":"hi"}`))
	if err != nil || !isErr {
		t.Fatalf("invoke: %v %v", isErr, err)
	}
	result := out.(*mcp.CallToolResult)
	if len(result.Content) != 2 || result.StructuredContent == nil {
		t.Fatalf("lost content: %+v", result)
	}
	if err := store.Update(ctx, "skills", func(c *record) error { c.Enabled = false; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ts[0].Invoke(ctx, json.RawMessage(`{}`)); err == nil {
		t.Fatal("stale tool bypassed disabled connection")
	}
	ts, err = service.Tools(ctx)
	if err != nil || len(ts) != 0 {
		t.Fatalf("disabled tools: %d %v", len(ts), err)
	}
}

func TestAccessIsCheckedAtDiscoveryAndInvocation(t *testing.T) {
	store := newMemoryStore()
	service := New(store, "https://pdw.example", nil)
	store.Update(context.Background(), "tasks", func(c *record) error {
		c.Name = "tasks"
		c.Enabled = true
		c.Clients = []string{"allowed"}
		c.Tools = []*mcp.Tool{{Name: "write", InputSchema: map[string]any{"type": "object"}}}
		return nil
	})
	ctx := pdwauth.WithClientNameHolder(context.Background())
	pdwauth.SetClientName(ctx, "denied")
	defs, err := service.Tools(ctx)
	if err != nil || len(defs) != 0 {
		t.Fatalf("leaked tools: %d %v", len(defs), err)
	}
	pdwauth.SetClientName(ctx, "allowed")
	defs, err = service.Tools(ctx)
	if err != nil || len(defs) != 1 {
		t.Fatal("allowed client missing tool")
	}
	pdwauth.SetClientName(ctx, "denied")
	if _, _, err = defs[0].Invoke(ctx, json.RawMessage(`{}`)); err == nil {
		t.Fatal("stale tool bypassed access policy")
	}
}

func TestSafeClientBlocksLoopbackAndCredentialRedirects(t *testing.T) {
	local := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("request reached loopback") }))
	defer local.Close()
	if _, err := safeClient().Get(local.URL); err == nil {
		t.Fatal("SSRF to loopback succeeded")
	}
	called := false
	transport := bearerTransport{endpoint: "https://example.com/mcp", token: "secret", base: roundTripFunc(func(r *http.Request) (*http.Response, error) { called = true; return nil, nil })}
	req, _ := http.NewRequest("POST", "https://attacker.example/mcp", nil)
	if _, err := transport.RoundTrip(req); err == nil || called {
		t.Fatal("credential redirected")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRefreshTokenIsSavedBeforeAnUpstreamFailure(t *testing.T) {
	var endpoint string
	refreshes := 0
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			refreshes++
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"access_token":"rotated-access","refresh_token":"rotated-refresh","token_type":"Bearer","expires_in":3600}`))
			return
		}
		w.WriteHeader(503)
	}))
	defer remote.Close()
	endpoint = remote.URL
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	store.Update(context.Background(), "tasks", func(c *record) error {
		c.URL = endpoint + "/mcp"
		c.Enabled = true
		c.AllClients = true
		c.OAuth = oauth2.Config{ClientID: "client", Endpoint: oauth2.Endpoint{TokenURL: endpoint + "/token", AuthStyle: oauth2.AuthStyleInParams}}
		c.Token = oauth2.Token{AccessToken: "expired", RefreshToken: "old-refresh", Expiry: time.Now().Add(-time.Hour)}
		c.Tools = []*mcp.Tool{{Name: "write", InputSchema: map[string]any{"type": "object"}}}
		return nil
	})
	defs, _ := service.Tools(context.Background())
	if _, _, err := defs[0].Invoke(context.Background(), json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected upstream failure")
	}
	records, _ := store.List(context.Background())
	if records[0].Token.RefreshToken != "rotated-refresh" || refreshes != 1 {
		t.Fatal("rotated credentials lost on upstream failure")
	}
}

func TestHTTPAndMCPExposeTheSameUpstreamResult(t *testing.T) {
	upstream := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "1"}, nil)
	upstream.AddTool(&mcp.Tool{Name: "read", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "sensitive-result"}}, StructuredContent: map[string]any{"ok": false}, IsError: true}, nil
	})
	remote := httptest.NewTLSServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return upstream }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	defer remote.Close()
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	store.Update(context.Background(), "hc", func(c *record) error { c.URL = remote.URL; c.Enabled = true; c.AllClients = true; return nil })
	if err := service.Refresh(context.Background(), "hc"); err != nil {
		t.Fatal(err)
	}
	defs, _ := service.Tools(context.Background())
	registry := tool.NewRegistry(defs)
	var logs bytes.Buffer
	apiServer := httptest.NewServer(api.NewHandler(registry, slog.New(slog.NewTextHandler(&logs, nil))))
	defer apiServer.Close()
	resp, err := apiServer.Client().Post(apiServer.URL+"/api/tools/hc__read", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var cliResult struct {
		Data mcp.CallToolResult `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cliResult); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !cliResult.Data.IsError || len(cliResult.Data.Content) != 1 || cliResult.Data.StructuredContent == nil {
		t.Fatalf("CLI lost result: %+v", cliResult)
	}
	if strings.Contains(logs.String(), "sensitive-result") {
		t.Fatal("upstream body logged")
	}
	downstream := mcp.NewServer(&mcp.Implementation{Name: "pdw", Version: "1"}, nil)
	for _, def := range defs {
		def.RegisterMCP(downstream, tool.Hooks{})
	}
	mcpServer := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return downstream }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	defer mcpServer.Close()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: mcpServer.URL, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	listed, err := session.ListTools(context.Background(), nil)
	if err != nil || len(listed.Tools) != 1 || listed.Tools[0].Name != "hc__read" {
		t.Fatalf("MCP discovery: %+v %v", listed, err)
	}
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "hc__read", Arguments: map[string]any{}})
	if err != nil || !result.IsError || len(result.Content) != 1 || result.StructuredContent == nil {
		t.Fatalf("MCP result: %+v %v", result, err)
	}
}

func TestDiscoveryFailureKeepsPreviousToolSnapshot(t *testing.T) {
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer remote.Close()
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	store.Update(context.Background(), "hc", func(c *record) error {
		c.URL = remote.URL
		c.Enabled = true
		c.AllClients = true
		c.Tools = []*mcp.Tool{{Name: "query", InputSchema: map[string]any{"type": "object"}}}
		return nil
	})
	if err := service.Refresh(context.Background(), "hc"); err == nil {
		t.Fatal("reported successful refresh during outage")
	}
	defs, err := service.Tools(context.Background())
	if err != nil || len(defs) != 1 || defs[0].Name() != "hc__query" {
		t.Fatal("failed refresh erased the previous snapshot")
	}
	rows, _ := store.List(context.Background())
	if rows[0].Status != "connection_failed" {
		t.Fatal("outage not recorded")
	}
}

func TestInvalidRemoteSchemasCannotPanicThePDWServer(t *testing.T) {
	for _, schema := range []any{nil, "bad", map[string]any{"type": "string"}, map[string]any{"properties": map[string]any{}}} {
		if validSchema(schema) {
			t.Fatalf("accepted invalid schema: %+v", schema)
		}
	}
	if !validSchema(map[string]any{"type": "object", "x-vendor": "extension"}) {
		t.Fatal("valid extensible schema rejected")
	}
}

func TestUnchangedReadsDoNotRewriteConnectionCredentials(t *testing.T) {
	store := newMemoryStore()
	ctx := context.Background()
	store.Update(ctx, "skills", func(c *record) error { c.Token.AccessToken = "token"; return nil })
	before, _ := store.List(ctx)
	if err := store.Update(ctx, "skills", func(c *record) error { return nil }); err != nil {
		t.Fatal(err)
	}
	after, _ := store.List(ctx)
	if before[0].Version != after[0].Version {
		t.Fatal("a read invalidated the web form version")
	}
}
