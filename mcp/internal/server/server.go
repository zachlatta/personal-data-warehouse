package server

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	pdwauth "github.com/zachlatta/personal-data-warehouse/mcp/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/config"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/query"
)

type queryInput struct {
	SQL []string `json:"sql" jsonschema:"array of read-only ClickHouse SQL strings to run"`
}

type schemaOverviewInput struct{}

const serverInstructions = "Read-only ClickHouse warehouse for Zach's personal data. Contains synced Gmail mail and attachment text for configured mailboxes, Slack workspace messages/files/users, and calendar data when present. Use for questions about those datasets; query ClickHouse SQL only."

func NewMCPServer(runner query.Runner, opts query.Options) *mcp.Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	serverLogger := logger.With("component", "server")
	svc := query.NewService(runner, opts)
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "personal-data-warehouse",
		Version: "0.1.0",
	}, &mcp.ServerOptions{Instructions: serverInstructions})
	serverLogger.Info("registering MCP tools")
	mcp.AddTool(server, &mcp.Tool{
		Name:        "query",
		Title:       "Query ClickHouse",
		Description: "Run read-only SQL queries against the personal ClickHouse data warehouse. Each statement returns CSV text. Results are row-limited and long fields are truncated.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input queryInput) (*mcp.CallToolResult, any, error) {
		serverLogger.InfoContext(ctx, "MCP tool called", "tool", "query", "statements", len(input.SQL))
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
	mcp.AddTool(server, &mcp.Tool{
		Name:        "schema_overview",
		Title:       "Schema Overview",
		Description: "Return a CSV overview of tables and columns in the default ClickHouse database using SHOW TABLES and DESCRIBE TABLE. Does not read system metadata tables.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input schemaOverviewInput) (*mcp.CallToolResult, any, error) {
		serverLogger.InfoContext(ctx, "MCP tool called", "tool", "schema_overview")
		resp := svc.SchemaOverview(ctx)
		content := make([]mcp.Content, 0, len(resp.Results))
		isError := false
		for _, result := range resp.Results {
			content = append(content, &mcp.TextContent{Text: result.CSV})
			if !result.Truncated.Empty() {
				content = append(content, &mcp.TextContent{Text: result.Truncated.CSV()})
			}
			if result.Error != "" {
				isError = true
			}
		}
		return &mcp.CallToolResult{
			Content: content,
			IsError: isError,
		}, nil, nil
	})
	return server
}

func NewMux(cfg config.Config, authSvc *pdwauth.Service, runner query.Runner) http.Handler {
	logger := slog.Default().With("component", "http")
	mux := http.NewServeMux()
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = "http://localhost" + cfg.Addr
	}
	logger.Info("registering HTTP handlers", "base_url", baseURL)
	authSvc.RegisterHandlers(mux, baseURL)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			logger.WarnContext(r.Context(), "unknown route", "method", r.Method, "path", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("Personal Data Warehouse MCP server\nMCP endpoint: /mcp\n"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mcpServer := NewMCPServer(runner, query.Options{MaxRows: cfg.MaxRows, MaxFieldChars: cfg.MaxFieldChars, Logger: slog.Default()})
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return mcpServer }, &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
	})
	protected := authSvc.RequireBearer(strings.TrimRight(baseURL, "/") + "/.well-known/oauth-protected-resource")(mcpHandler)
	mux.Handle("/mcp", protected)
	return logRequests(logger, mux)
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += n
	return n, err
}

func (w *statusResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *statusResponseWriter) Flush() {
	flusher, ok := w.ResponseWriter.(http.Flusher)
	if ok {
		flusher.Flush()
	}
}

func logRequests(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rec := &statusResponseWriter{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		logger.InfoContext(r.Context(), "HTTP request completed", "method", r.Method, "path", r.URL.Path, "status", status, "bytes", rec.bytes, "duration", time.Since(started))
	})
}
