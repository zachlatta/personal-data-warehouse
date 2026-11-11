package mcpproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestDiscoveryPreservesAdminVersion(t *testing.T) {
	testDiscoveryPreservesAdminVersion(t, newMemoryStore())
}

// Shared with the Postgres integration test run by the canonical Python suite.
func testDiscoveryPreservesAdminVersion(t *testing.T, store Store) {
	upstream := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "1"}, nil)
	add := func(description string) {
		upstream.AddTool(&mcp.Tool{Name: "example", Description: description, InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			t.Error("discovery invoked a tool")
			return nil, nil
		})
	}
	add("before")
	remote := httptest.NewTLSServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return upstream }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	defer remote.Close()
	svc := New(store, "https://pdw.example", remote.Client())
	ctx := context.Background()
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("tools_changed=%v", changed), func(t *testing.T) {
			if err := store.Update(ctx, "editing", func(c *record) error {
				*c = record{Name: "editing", URL: remote.URL, Enabled: true, Version: c.Version}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := svc.Refresh(ctx, "editing"); err != nil {
				t.Fatal(err)
			}
			read := func() record {
				t.Helper()
				records, err := store.List(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, c := range records {
					if c.Name == "editing" {
						return c
					}
				}
				t.Fatal("connection missing")
				return record{}
			}
			before := read()
			if changed {
				add("after")
			}
			if err := svc.RefreshAll(ctx); err != nil {
				t.Fatal(err)
			}
			after := read()
			if !after.LastDiscoveredAt.After(before.LastDiscoveredAt) {
				t.Fatal("discovery success not recorded")
			}
			if changed && after.Tools[0].Description != "after" {
				t.Fatal("changed tool not saved")
			}
			response := httptest.NewRecorder()
			svc.connection(response, httptest.NewRequest(http.MethodPost, "/api/connections/editing/disable", strings.NewReader(fmt.Sprintf(`{"version":%d}`, before.Version))))
			if response.Code != 200 {
				t.Fatalf("pre-sweep edit rejected: version %d -> %d, HTTP %d: %s", before.Version, after.Version, response.Code, response.Body.String())
			}
			stale := httptest.NewRecorder()
			svc.connection(stale, httptest.NewRequest(http.MethodPost, "/api/connections/editing/enable", strings.NewReader(fmt.Sprintf(`{"version":%d}`, before.Version))))
			if stale.Code != 409 {
				t.Fatal("stale configuration edit was accepted")
			}
			before = read()
			if err := store.Update(ctx, "editing", func(*record) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if after = read(); after.Version != before.Version || after.UpdatedAt != before.UpdatedAt {
				t.Fatal("no-op update changed the record")
			}
		})
	}
}
