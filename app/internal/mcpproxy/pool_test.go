package mcpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// poolUpstream is a skills-like upstream that counts initialize requests and
// tool executions. A stateful one can be made to forget its sessions, as a
// redeploy does.
type poolUpstream struct {
	t        *testing.T
	inits    atomic.Int64
	calls    atomic.Int64
	mu       sync.Mutex
	handler  http.Handler
	stateful bool
	server   *httptest.Server
	service  *Service
	store    *memoryStore
}

func newPoolUpstream(t *testing.T, stateful bool) *poolUpstream {
	u := &poolUpstream{t: t, stateful: stateful}
	u.restart()
	u.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		var msg struct{ Method string }
		if json.Unmarshal(body, &msg) == nil && msg.Method == "initialize" {
			u.inits.Add(1)
		}
		u.mu.Lock()
		h := u.handler
		u.mu.Unlock()
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(u.server.Close)
	u.store = newMemoryStore()
	u.service = New(u.store, "https://pdw.example", u.server.Client())
	ctx := context.Background()
	if err := u.store.Update(ctx, "skills", func(c *record) error {
		c.Name, c.URL, c.Enabled, c.AllClients = "skills", u.server.URL, true, true
		c.Token.AccessToken, c.Token.TokenType = "token-1", "Bearer"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := u.service.Refresh(ctx, "skills"); err != nil {
		t.Fatal(err)
	}
	u.inits.Store(0)
	return u
}

// restart replaces the upstream server, dropping every session it knew.
func (u *poolUpstream) restart() {
	server := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "1"}, nil)
	server.AddTool(&mcp.Tool{Name: "read", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		u.calls.Add(1)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
	})
	u.mu.Lock()
	u.handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: !u.stateful, JSONResponse: true})
	u.mu.Unlock()
}

func (u *poolUpstream) call(ctx context.Context) {
	u.t.Helper()
	ts, err := u.service.Tools(ctx)
	if err != nil || len(ts) != 1 {
		u.t.Fatalf("tools: %d %v", len(ts), err)
	}
	if _, isErr, err := ts[0].Invoke(ctx, json.RawMessage(`{}`)); err != nil || isErr {
		u.t.Fatalf("invoke: %v %v", isErr, err)
	}
}

func TestPooledSessionIsReusedWithinOneTraceOnly(t *testing.T) {
	u := newPoolUpstream(t, false)
	a := ContextWithTraceID(context.Background(), "trace-a")
	b := ContextWithTraceID(context.Background(), "trace-b")
	u.call(a)
	u.call(a)
	u.call(a)
	if n := u.inits.Load(); n != 1 {
		t.Fatalf("three calls in one trace opened %d sessions, want 1", n)
	}
	u.call(b)
	if n := u.inits.Load(); n != 2 {
		t.Fatalf("a second trace shared a session: %d initializations", n)
	}
	// Untraced calls are never pooled.
	u.call(context.Background())
	u.call(context.Background())
	if n := u.inits.Load(); n != 4 {
		t.Fatalf("untraced calls opened %d sessions in total, want one each", n)
	}
	if n := u.calls.Load(); n != 6 {
		t.Fatalf("tool ran %d times for 6 calls", n)
	}
}

func TestPooledSessionForgottenByUpstreamIsRetriedOnce(t *testing.T) {
	u := newPoolUpstream(t, true)
	ctx := ContextWithTraceID(context.Background(), "trace-a")
	u.call(ctx)
	u.restart() // a redeploy: the pooled session id is now unknown
	u.call(ctx)
	if n := u.calls.Load(); n != 2 {
		t.Fatalf("tool ran %d times for 2 calls, want exactly once each", n)
	}
	if n := u.inits.Load(); n != 2 {
		t.Fatalf("expected a new session after the restart, got %d initializations", n)
	}
}

func TestRotatedTokenOpensANewPooledSession(t *testing.T) {
	u := newPoolUpstream(t, false)
	ctx := ContextWithTraceID(context.Background(), "trace-a")
	u.call(ctx)
	if err := u.store.Update(ctx, "skills", func(c *record) error { c.Token.AccessToken = "token-2"; return nil }); err != nil {
		t.Fatal(err)
	}
	u.call(ctx)
	if n := u.inits.Load(); n != 2 {
		t.Fatalf("a rotated token reused the old session: %d initializations", n)
	}
}

func TestPoolEvictsIdleSessions(t *testing.T) {
	u := newPoolUpstream(t, false)
	now := poolNow()
	poolNow = func() time.Time { return now }
	t.Cleanup(func() { poolNow = time.Now })
	ctx := ContextWithTraceID(context.Background(), "trace-a")
	u.call(ctx)
	now = now.Add(poolIdleTimeout + time.Second)
	u.call(ctx)
	if n := u.inits.Load(); n != 2 {
		t.Fatalf("an idle session was reused after the idle timeout: %d initializations", n)
	}
	u.service.pool.mu.Lock()
	n := len(u.service.pool.sessions)
	u.service.pool.mu.Unlock()
	if n != 1 {
		t.Fatalf("pool holds %d sessions, want 1", n)
	}
}
