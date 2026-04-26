package server

import (
	"context"
	"net/http"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	pdwauth "github.com/zachlatta/personal-data-warehouse/mcp/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/config"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/query"
)

type queryInput struct {
	SQL []string `json:"sql" jsonschema:"array of read-only ClickHouse SQL strings to run"`
}

const serverInstructions = "Read-only ClickHouse warehouse for Zach's personal data. Contains synced Gmail mail and attachment text for configured mailboxes, Slack workspace messages/files/users, and calendar data when present. Use for questions about those datasets; query ClickHouse SQL only."

func NewMCPServer(runner query.Runner, opts query.Options) *mcp.Server {
	svc := query.NewService(runner, opts)
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "personal-data-warehouse",
		Version: "0.1.0",
	}, &mcp.ServerOptions{Instructions: serverInstructions})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "query",
		Title:       "Query ClickHouse",
		Description: "Run read-only SQL queries against the personal ClickHouse data warehouse. Each statement returns CSV text. Results are row-limited and long fields are truncated.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input queryInput) (*mcp.CallToolResult, any, error) {
		resp := svc.Execute(ctx, input.SQL)
		content := make([]mcp.Content, 0, len(resp.Results))
		for _, result := range resp.Results {
			content = append(content, &mcp.TextContent{Text: result.CSV})
			if !result.Truncated.Empty() {
				content = append(content, &mcp.TextContent{Text: result.Truncated.CSV()})
			}
		}
		return &mcp.CallToolResult{Content: content}, nil, nil
	})
	return server
}

func NewMux(cfg config.Config, authSvc *pdwauth.Service, runner query.Runner) http.Handler {
	mux := http.NewServeMux()
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = "http://localhost" + cfg.Addr
	}
	authSvc.RegisterHandlers(mux, baseURL)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("Personal Data Warehouse MCP server\nMCP endpoint: /mcp\n"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mcpServer := NewMCPServer(runner, query.Options{MaxRows: cfg.MaxRows, MaxFieldChars: cfg.MaxFieldChars})
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return mcpServer }, &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
	})
	protected := authSvc.RequireBearer(strings.TrimRight(baseURL, "/") + "/.well-known/oauth-protected-resource")(mcpHandler)
	mux.Handle("/mcp", protected)
	return mux
}
