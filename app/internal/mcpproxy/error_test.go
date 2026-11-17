package mcpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUpstreamFailureCauseIsSanitizedAndLogged(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(old)
	token := "pdw_" + strings.Repeat("a", 16) + "_" + strings.Repeat("B", 40)
	for _, readOnly := range []bool{true, false} {
		err := upstreamFailure(context.Background(), "example", "query", readOnly, "known-secret", errors.New("bad gateway https://example.invalid/mcp?token=hidden "+token+" known-secret"))
		for _, text := range []string{err.Error(), logs.String()} {
			if !strings.Contains(text, "bad gateway") || strings.Contains(text, token) || strings.Contains(text, "hidden") || strings.Contains(text, "known-secret") {
				t.Fatal(text)
			}
		}
		if readOnly && strings.Contains(err.Error(), "write may") {
			t.Fatal(err)
		}
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "connection=example") || !strings.Contains(logs.String(), "tool=query") {
		t.Fatal(logs.String())
	}
}

func TestFailingUpstreamIncludesCause(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(old)
	upstream := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "1"}, nil)
	token := "pdw_" + strings.Repeat("a", 16) + "_" + strings.Repeat("B", 40)
	upstream.AddTool(&mcp.Tool{Name: "query", InputSchema: json.RawMessage(`{"type":"object"}`), Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return nil, errors.New("upstream unavailable " + token)
	})
	remote := httptest.NewTLSServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return upstream }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	defer remote.Close()
	store := newMemoryStore()
	svc := New(store, "https://pdw.example", remote.Client())
	ctx := context.Background()
	if err := store.Update(ctx, "example", func(c *record) error {
		c.Name = "example"
		c.URL = remote.URL
		c.Enabled = true
		c.AllClients = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Refresh(ctx, "example"); err != nil {
		t.Fatal(err)
	}
	ts, err := svc.Tools(ctx)
	if err != nil || len(ts) != 1 {
		t.Fatal(err)
	}
	_, _, err = ts[0].Invoke(ctx, json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("upstream failure disappeared")
	}
	for _, output := range []string{err.Error(), logs.String()} {
		if !strings.Contains(output, "upstream unavailable") || strings.Contains(output, token) || strings.Contains(output, "write may") {
			t.Fatal(output)
		}
	}
}
