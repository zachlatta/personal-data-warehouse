package server

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/guide"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
)

// The guide is what an MCP agent reads before its first real call, so it
// must come back as prose (one text block, no JSON envelope), name the MCP
// tools rather than the CLI commands, and answer an unknown topic with the
// list instead of an empty page.
func TestReadmeToolReturnsTheMCPGuideAsPlainText(t *testing.T) {
	srv := NewMCPServer(fakeRunner{results: map[string]query.RawResult{}}, query.Options{MaxRows: 5, MaxFieldChars: 100})
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Run(ctx, serverTransport) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "readme-test", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	call := func(args map[string]any) *mcp.CallToolResult {
		t.Helper()
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "readme", Arguments: args})
		if err != nil {
			t.Fatalf("readme %v: %v", args, err)
		}
		return res
	}
	text := func(res *mcp.CallToolResult) string {
		t.Helper()
		if len(res.Content) != 1 {
			t.Fatalf("expected one text block, got %d", len(res.Content))
		}
		tc, ok := res.Content[0].(*mcp.TextContent)
		if !ok {
			t.Fatalf("expected TextContent, got %T", res.Content[0])
		}
		return tc.Text
	}

	main := call(map[string]any{})
	if main.IsError {
		t.Fatalf("main guide is an error: %s", text(main))
	}
	body := text(main)
	if strings.HasPrefix(strings.TrimSpace(body), "{") {
		t.Fatalf("guide came back as JSON, not prose:\n%s", body[:200])
	}
	want, _ := guide.Render(guide.SurfaceMCP, "")
	if body != want {
		t.Fatal("MCP readme differs from guide.Render(SurfaceMCP)")
	}
	for _, tool := range []string{"`search`", "`query`", "`describe_table`", "`readme`"} {
		if !strings.Contains(body, tool) {
			t.Fatalf("MCP guide does not name %s", tool)
		}
	}
	if strings.Count(body, "pdw search") > 1 {
		t.Fatal("MCP guide teaches the CLI spelling")
	}

	topic := call(map[string]any{"topic": "finance"})
	if topic.IsError || !strings.Contains(text(topic), "marts_finance.net_worth") {
		t.Fatalf("finance topic missing: %s", text(topic))
	}

	unknown := call(map[string]any{"topic": "nope"})
	if !unknown.IsError {
		t.Fatal("unknown topic should be an error")
	}
	if msg := text(unknown); !strings.Contains(msg, "agent-sessions") || strings.Contains(msg, "ingest") {
		t.Fatalf("unknown-topic error should list the MCP topics (and not the CLI-only one): %s", msg)
	}

	ingest := call(map[string]any{"topic": "ingest"})
	if !ingest.IsError {
		t.Fatal("ingest is CLI-only and must not render over MCP")
	}
}

func TestServerInstructionsOpenWithTheReadmeTool(t *testing.T) {
	if !strings.HasPrefix(serverInstructions, "Call the readme tool first") {
		t.Fatalf("instructions should send an agent to readme before anything else: %q", serverInstructions[:80])
	}
	for _, name := range guide.TopicNames(guide.SurfaceMCP) {
		if !strings.Contains(readmeDescription, name) {
			t.Fatalf("readme description omits topic %q", name)
		}
	}
}
