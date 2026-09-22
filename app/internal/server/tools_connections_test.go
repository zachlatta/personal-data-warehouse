package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

// fakeProxiedTool stands in for an mcpproxy tool: named <connection>__<tool>,
// answering with an upstream-shaped CallToolResult.
type fakeProxiedTool struct {
	name, description string
	schema            map[string]any
	calls             []json.RawMessage
}

func (t *fakeProxiedTool) Name() string           { return t.name }
func (t *fakeProxiedTool) Title() string          { return "" }
func (t *fakeProxiedTool) Description() string    { return t.description }
func (t *fakeProxiedTool) Surfaces() tool.Surface { return tool.SurfaceAll }
func (t *fakeProxiedTool) RawInputSchema() any    { return t.schema }
func (t *fakeProxiedTool) InputSchema() (*jsonschema.Schema, error) {
	return &jsonschema.Schema{Type: "object"}, nil
}
func (t *fakeProxiedTool) Invoke(_ context.Context, input json.RawMessage) (any, bool, error) {
	t.calls = append(t.calls, input)
	if strings.Contains(string(input), "boom") {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "Input validation error: 'schema_name' is a required property"}}, IsError: true}, true, nil
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "upstream says hi"}}}, false, nil
}
func (t *fakeProxiedTool) RegisterMCP(server *mcp.Server, hooks tool.Hooks) {
	server.AddTool(&mcp.Tool{Name: t.name, Description: t.description, InputSchema: t.schema}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		out, _, err := t.Invoke(ctx, req.Params.Arguments)
		if err != nil {
			return nil, err
		}
		return out.(*mcp.CallToolResult), nil
	})
}

func connectMCP(t *testing.T, registry *tool.Registry) *mcp.ClientSession {
	t.Helper()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	srv := newMCPServerFromRegistry(registry, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Run(ctx, serverTransport) }()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func toolNames(t *testing.T, session *mcp.ClientSession) map[string]*mcp.Tool {
	t.Helper()
	listed, err := session.ListTools(context.Background(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]*mcp.Tool{}
	for _, tl := range listed.Tools {
		names[tl.Name] = tl
	}
	return names
}

func TestMCPListsConnectionToolsBehindTwoToolsNotFlat(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"schema_name": map[string]any{"type": "string"}}, "required": []any{"schema_name"}}
	upstream := &fakeProxiedTool{name: "hcdw__list_columns", description: "[hcdw] List all columns for a table. Paginated.", schema: schema}
	other := &fakeProxiedTool{name: "skills__skill_read", description: "[skills] Read one skill.", schema: map[string]any{"type": "object"}}
	registry, _ := buildRegistry(fakeRunner{results: map[string]query.RawResult{}}, query.Options{MaxRows: 5, MaxFieldChars: 100}, nil, slog.Default(), upstream, other)
	session := connectMCP(t, registry)
	ctx := context.Background()

	names := toolNames(t, session)
	if _, flat := names["hcdw__list_columns"]; flat {
		t.Fatal("proxied tools must not be listed flat on MCP (153 KB of definitions per session)")
	}
	for _, want := range []string{"connections", "connection_call", "search", "query", "readme"} {
		if _, ok := names[want]; !ok {
			t.Fatalf("%s missing from tools/list: %v", want, names)
		}
	}

	// Discovery: every connection, then one connection's tools, then one
	// tool's schema.
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "connections", Arguments: map[string]any{}})
	if err != nil || result.IsError {
		t.Fatalf("connections: %v %#v", err, result)
	}
	text := compactText(t, result)
	if !strings.Contains(text, `"name":"hcdw"`) || !strings.Contains(text, `"name":"skills"`) || strings.Contains(text, "list_columns") {
		t.Fatalf("top level should name connections only: %s", text)
	}
	result, _ = session.CallTool(ctx, &mcp.CallToolParams{Name: "connections", Arguments: map[string]any{"connection": "hcdw"}})
	text = compactText(t, result)
	if !strings.Contains(text, `"name":"hcdw__list_columns"`) || !strings.Contains(text, `"summary":"List all columns for a table."`) || strings.Contains(text, "schema_name") {
		t.Fatalf("a connection listing carries names and summaries, not schemas: %s", text)
	}
	result, _ = session.CallTool(ctx, &mcp.CallToolParams{Name: "connections", Arguments: map[string]any{"tool": "hcdw__list_columns"}})
	text = compactText(t, result)
	if !strings.Contains(text, `"required":["schema_name"]`) {
		t.Fatalf("a tool lookup carries the input schema: %s", text)
	}
	result, _ = session.CallTool(ctx, &mcp.CallToolParams{Name: "connections", Arguments: map[string]any{"tool": "hcdw__nope"}})
	if !result.IsError || !strings.Contains(result.Content[0].(*mcp.TextContent).Text, "hcdw, skills") {
		t.Fatalf("an unknown tool names the connections: %#v", result)
	}

	// Invocation passes the upstream result through, error flag included.
	result, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "connection_call", Arguments: map[string]any{"tool": "hcdw__list_columns", "arguments": map[string]any{"schema_name": "x"}}})
	if err != nil || result.IsError || result.Content[0].(*mcp.TextContent).Text != "upstream says hi" {
		t.Fatalf("connection_call: %v %#v", err, result)
	}
	if len(upstream.calls) != 1 || string(upstream.calls[0]) != `{"schema_name":"x"}` {
		t.Fatalf("upstream received %v", upstream.calls)
	}
	result, _ = session.CallTool(ctx, &mcp.CallToolParams{Name: "connection_call", Arguments: map[string]any{"tool": "hcdw__list_columns", "arguments": map[string]any{"schema_name": "boom"}}})
	if !result.IsError || !strings.Contains(result.Content[0].(*mcp.TextContent).Text, "required property") {
		t.Fatalf("upstream errors pass through as errors: %#v", result)
	}

	// The escape hatch restores the flat listing.
	t.Setenv(connectionToolsEnv, "1")
	flat := toolNames(t, connectMCP(t, registry))
	if _, ok := flat["hcdw__list_columns"]; !ok {
		t.Fatalf("%s=1 should list proxied tools flat", connectionToolsEnv)
	}
	if _, ok := flat["connections"]; ok {
		t.Fatal("the flat listing has no connections tool")
	}
}

// compactText re-encodes a JSON text result without whitespace so tests can
// match on key/value pairs regardless of the SDK's indentation.
func compactText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	raw := result.Content[0].(*mcp.TextContent).Text
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatalf("not JSON: %s", raw)
	}
	out, _ := json.Marshal(v)
	return string(out)
}
