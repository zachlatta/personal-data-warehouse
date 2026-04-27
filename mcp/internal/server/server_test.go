package server

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/mcp/internal/query"
)

type fakeRunner struct {
	results map[string]query.RawResult
}

func (f fakeRunner) Query(_ context.Context, sql string, maxRows int) (query.RawResult, error) {
	result := f.results[sql]
	if maxRows > 0 && len(result.Rows) > maxRows {
		result.Rows = result.Rows[:maxRows]
	}
	return result, nil
}

func TestMCPServerExposesSchemaOverviewTool(t *testing.T) {
	runner := fakeRunner{results: map[string]query.RawResult{
		"SELECT currentDatabase() AS database": {
			Columns: []string{"database"},
			Rows:    []map[string]any{{"database": "default"}},
		},
		"SHOW TABLES": {
			Columns: []string{"name"},
			Rows:    []map[string]any{{"name": "gmail_messages"}},
		},
		"DESCRIBE TABLE `gmail_messages`": {
			Columns: []string{"name", "type", "default_type", "default_expression", "comment"},
			Rows:    []map[string]any{{"name": "subject", "type": "String"}},
		},
		"SELECT substring(toString(`subject`), 1, 15) AS `subject`, length(toString(`subject`)) AS `__pdw_preview_len_0` FROM `gmail_messages` LIMIT 3": {
			Columns: []string{"subject", "__pdw_preview_len_0"},
			Rows:    []map[string]any{{"subject": "hello", "__pdw_preview_len_0": 5}},
		},
	}}
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	srv := NewMCPServer(runner, query.Options{MaxRows: 5, MaxFieldChars: 100})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.Run(ctx, serverTransport)
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect failed: %v", err)
	}
	defer session.Close()

	tools, err := session.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools failed: %v", err)
	}
	var found bool
	for _, tool := range tools.Tools {
		if tool.Name == "schema_overview" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("schema_overview tool not listed: %#v", tools.Tools)
	}

	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "schema_overview", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool failed: %v", err)
	}
	if result.IsError {
		t.Fatalf("schema_overview returned error: %#v", result.Content)
	}
	if len(result.Content) != 1 {
		t.Fatalf("content length = %d", len(result.Content))
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content type = %T", result.Content[0])
	}
	if !strings.Contains(text.Text, "# default.gmail_messages") || !strings.Contains(text.Text, "subject\nhello") {
		t.Fatalf("unexpected schema overview text: %q", text.Text)
	}

	cancel()
	<-serverErr
}
