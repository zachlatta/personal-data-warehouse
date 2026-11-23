package mcpproxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

// 26 sessions in the week to 2026-11-23 sent intelligence__query {"sql": ...}
// and got an opaque upstream http 500, although PDW holds the tool's schema
// (additionalProperties false, property `query`).
func TestAnUnknownArgumentIsRejectedFromTheCachedSchemaWithoutCallingUpstream(t *testing.T) {
	calls := 0
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(500)
	}))
	defer remote.Close()
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	store.Update(context.Background(), "intelligence", func(c *record) error {
		c.URL = remote.URL + "/mcp"
		c.Enabled = true
		c.AllClients = true
		c.Tools = []*mcp.Tool{
			{Name: "query", InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []any{"query"},
				"properties":           map[string]any{"query": map[string]any{"type": "string"}, "limit": map[string]any{"type": "integer"}},
			}},
			{Name: "open", InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"query": map[string]any{"type": "string"}},
			}},
		}
		return nil
	})
	defs, _ := service.Tools(context.Background())
	byName := map[string]tool.Tool{}
	for _, def := range defs {
		byName[def.Name()] = def
	}

	_, _, err := byName["intelligence__query"].Invoke(context.Background(), json.RawMessage(`{"sql":"SELECT 1"}`))
	var invalid *tool.InvalidInputError
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %v, want an InvalidInputError", err)
	}
	want := `unknown field "sql"; intelligence__query takes: query (required), limit`
	if invalid.Message != want {
		t.Fatalf("message = %q, want %q", invalid.Message, want)
	}
	if calls != 0 {
		t.Fatalf("upstream was called %d times for arguments PDW could reject", calls)
	}

	// A schema that allows extra keys is the upstream's to judge.
	if _, _, err := byName["intelligence__open"].Invoke(context.Background(), json.RawMessage(`{"sql":"x"}`)); errors.As(err, &invalid) {
		t.Fatalf("rejected an argument the schema allows: %v", err)
	}
}
