package mcpproxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The proxy opens a new upstream session per call, so the trace id is the only
// thing that lets an upstream tell one agent session's calls apart from the
// next. It must reach the upstream on every request of the call.
func TestProxyForwardsTraceIDUpstream(t *testing.T) {
	upstream := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "1"}, nil)
	upstream.AddTool(&mcp.Tool{Name: "echo", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return upstream }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	var mu sync.Mutex
	var seen []string
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get(TraceHeader))
		mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	defer remote.Close()
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	ctx := context.Background()
	if err := store.Update(ctx, "skills", func(c *record) error {
		c.Name, c.URL, c.Enabled, c.AllClients = "skills", remote.URL, true, true
		c.Token.AccessToken, c.Token.TokenType = "upstream-secret", "Bearer"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Refresh(ctx, "skills"); err != nil {
		t.Fatal(err)
	}
	ts, err := service.Tools(ctx)
	if err != nil || len(ts) != 1 {
		t.Fatalf("tools: %d %v", len(ts), err)
	}

	mu.Lock()
	seen = nil
	mu.Unlock()
	if _, _, err := ts[0].Invoke(ContextWithTraceID(ctx, "porygon-claude-123-abcd"), json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) == 0 {
		t.Fatal("no upstream requests recorded")
	}
	for _, h := range got {
		if h != "porygon-claude-123-abcd" {
			t.Fatalf("upstream %s headers = %q, want the caller's trace on every request", TraceHeader, got)
		}
	}

	// Without a trace the header is absent rather than empty or invented.
	mu.Lock()
	seen = nil
	mu.Unlock()
	if _, _, err := ts[0].Invoke(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, h := range seen {
		if h != "" {
			t.Fatalf("untraced call forwarded %q", h)
		}
	}
}

func TestTraceMiddlewareReadsHeaderThenTraceparent(t *testing.T) {
	var got string
	h := TraceMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = TraceIDFromContext(r.Context())
	}))
	for _, tc := range []struct {
		headers map[string]string
		want    string
	}{
		{map[string]string{TraceHeader: "crobat-codex-1-ff"}, "crobat-codex-1-ff"},
		{map[string]string{TraceHeader: "bad id/with\nstuff"}, "bad_id_with_stuff"},
		{map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}, "4bf92f3577b34da6a3ce929d0e0e4736"},
		{map[string]string{TraceHeader: "wins", "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}, "wins"},
		{map[string]string{"traceparent": "garbage"}, ""},
		{nil, ""},
	} {
		got = "unset"
		req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader("{}"))
		for k, v := range tc.headers {
			req.Header.Set(k, v)
		}
		h.ServeHTTP(httptest.NewRecorder(), req)
		if got != tc.want {
			t.Fatalf("headers %v: trace = %q, want %q", tc.headers, got, tc.want)
		}
	}
	if long := SanitizeTraceID(strings.Repeat("a", 200)); len(long) != 96 {
		t.Fatalf("trace id not bounded: %d", len(long))
	}
}
