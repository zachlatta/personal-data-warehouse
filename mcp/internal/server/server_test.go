package server

import (
	"context"
	"encoding/json"
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
		"SELECT subject FROM gmail_messages LIMIT 1": {
			Columns: []string{"subject"},
			Rows:    []map[string]any{{"subject": "hello"}},
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
	found := map[string]bool{}
	for _, tool := range tools.Tools {
		found[tool.Name] = true
		if tool.Name == "query" || tool.Name == "get_field" || tool.Name == "get_rows" || tool.Name == "grep_rows" {
			if !strings.Contains(tool.Description, "Do NOT compute substring offsets in SQL") {
				t.Fatalf("%s description does not steer away from substring SQL: %q", tool.Name, tool.Description)
			}
		}
	}
	for _, name := range []string{"query", "get_rows", "get_field", "grep_rows", "schema_overview"} {
		if !found[name] {
			t.Fatalf("%s tool not listed: %#v", name, tools.Tools)
		}
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

	queryResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{
		"sql":          []string{"SELECT subject FROM gmail_messages LIMIT 1"},
		"preview_rows": 1,
		"format":       "csv",
	}})
	if err != nil {
		t.Fatalf("query CallTool failed: %v", err)
	}
	queryText := queryResult.Content[0].(*mcp.TextContent).Text
	var queryPayload struct {
		Results []struct {
			QueryID     string   `json:"query_id"`
			TotalRows   int      `json:"total_rows"`
			ColumnNames []string `json:"column_names"`
			Preview     string   `json:"preview"`
			Error       string   `json:"error"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(queryText), &queryPayload); err != nil {
		t.Fatalf("query response was not JSON: %v\n%s", err, queryText)
	}
	if queryPayload.Results[0].QueryID == "" || queryPayload.Results[0].Preview != "subject\nhello" {
		t.Fatalf("unexpected query payload: %#v", queryPayload)
	}
	fieldResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "get_field", Arguments: map[string]any{
		"query_id": queryPayload.Results[0].QueryID,
		"row":      0,
		"column":   "subject",
		"length":   20,
	}})
	if err != nil {
		t.Fatalf("get_field CallTool failed: %v", err)
	}
	fieldText := fieldResult.Content[0].(*mcp.TextContent).Text
	if !strings.Contains(fieldText, `"value": "hello"`) || strings.Contains(fieldText, "substring(") {
		t.Fatalf("unexpected get_field response: %s", fieldText)
	}

	cancel()
	<-serverErr
}
