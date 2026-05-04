package server

import (
	"context"
	"encoding/json"
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
	SQL         []string `json:"sql" jsonschema:"array of read-only ClickHouse SQL strings to run"`
	PreviewRows int      `json:"preview_rows,omitempty" jsonschema:"number of initial rows to preview per statement, default 20"`
	Format      string   `json:"format,omitempty" jsonschema:"preview format: csv, json, or ndjson; default csv"`
}

type schemaOverviewInput struct{}

type getRowsInput struct {
	QueryID string `json:"query_id" jsonschema:"query_id returned by query"`
	Offset  int    `json:"offset,omitempty" jsonschema:"zero-based row offset, default 0"`
	Limit   int    `json:"limit,omitempty" jsonschema:"number of rows to return, default 50"`
	Format  string `json:"format,omitempty" jsonschema:"optional override: csv, json, or ndjson; default is the original query format"`
}

type getFieldInput struct {
	QueryID string `json:"query_id" jsonschema:"query_id returned by query"`
	Row     int    `json:"row" jsonschema:"zero-based row index in the cached result"`
	Column  string `json:"column" jsonschema:"column name to read"`
	Offset  int    `json:"offset,omitempty" jsonschema:"zero-based character offset, default 0"`
	Length  int    `json:"length,omitempty" jsonschema:"number of characters to return, default 50000 and capped by MCP_GET_FIELD_MAX_CHARS"`
}

type grepRowsInput struct {
	QueryID      string   `json:"query_id" jsonschema:"query_id returned by query"`
	Pattern      string   `json:"pattern" jsonschema:"case-insensitive regex pattern to search"`
	Columns      []string `json:"columns,omitempty" jsonschema:"optional list of columns to search; default all columns"`
	Limit        int      `json:"limit,omitempty" jsonschema:"maximum matches to return, default 100"`
	ContextChars int      `json:"context_chars,omitempty" jsonschema:"characters of context around each match, default 200"`
}

type debugCacheInput struct{}

const serverInstructions = "Read-only ClickHouse warehouse for Zach's personal data. Contains synced Gmail mail and attachment text for configured mailboxes, Slack workspace messages/files/users, and calendar data when present. Use for questions about those datasets; query ClickHouse SQL only."

const queryDescription = "Execute read-only ClickHouse SQL, cache the full result under query_id, and return a preview. Example: query({\"sql\":[\"SELECT id, transcript FROM voice_memo_transcripts WHERE id='abc'\"],\"preview_rows\":1,\"format\":\"csv\"}) returns query_id plus a truncated preview; then call get_field(query_id,row=0,column=\"transcript\",offset=0,length=200000) to read the full transcript. Do NOT compute substring offsets in SQL. Use get_field for long fields. Related tools: get_rows pages cached rows, grep_rows searches cached rows, schema_overview lists tables."

const getRowsDescription = "Return a row slice from a cached query result without re-executing SQL. Example: get_rows({\"query_id\":\"abc123\",\"offset\":50,\"limit\":25}) returns rows 50-74 in the query's original format unless format is overridden. Do NOT compute substring offsets in SQL. Use get_field for long fields. Related tools: query creates query_id, get_field reads a long cell, grep_rows searches cached rows."

const getFieldDescription = "Return a raw character chunk from one cached cell, which is the right way to read transcripts, email bodies, attachment text, or any long text column end-to-end. Example: after query returns query_id for SELECT id, transcript FROM voice_memo_transcripts LIMIT 1, call get_field({\"query_id\":\"abc123\",\"row\":0,\"column\":\"transcript\",\"offset\":0,\"length\":200000}) to retrieve the full transcript when eof=true. Do NOT compute substring offsets in SQL. Related tools: query creates query_id, get_rows pages rows, grep_rows finds text before fetching a field."

const grepRowsDescription = "Regex-search cached query rows without re-executing SQL and return match context. Example: grep_rows({\"query_id\":\"abc123\",\"pattern\":\"weighted projects\",\"columns\":[\"transcript\"],\"limit\":20}) finds where that phrase appears across cached transcripts. Do NOT compute substring offsets in SQL. Use get_field to read the matching long field. Related tools: query creates query_id, get_rows pages rows."

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
		Description: queryDescription,
	}, func(ctx context.Context, req *mcp.CallToolRequest, input queryInput) (*mcp.CallToolResult, any, error) {
		serverLogger.InfoContext(ctx, "MCP tool called", "tool", "query", "statements", len(input.SQL))
		resp := svc.Execute(ctx, input.SQL, input.PreviewRows, input.Format)
		return jsonToolResult(resp, queryResponseHasError(resp)), nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_rows",
		Title:       "Get Cached Rows",
		Description: getRowsDescription,
	}, func(ctx context.Context, req *mcp.CallToolRequest, input getRowsInput) (*mcp.CallToolResult, any, error) {
		serverLogger.InfoContext(ctx, "MCP tool called", "tool", "get_rows", "query_id", input.QueryID, "offset", input.Offset, "limit", input.Limit)
		resp := svc.GetRows(input.QueryID, input.Offset, input.Limit, input.Format)
		return jsonToolResult(resp, resp.Error != ""), nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_field",
		Title:       "Get Cached Field",
		Description: getFieldDescription,
	}, func(ctx context.Context, req *mcp.CallToolRequest, input getFieldInput) (*mcp.CallToolResult, any, error) {
		serverLogger.InfoContext(ctx, "MCP tool called", "tool", "get_field", "query_id", input.QueryID, "row", input.Row, "column", input.Column, "offset", input.Offset, "length", input.Length)
		resp := svc.GetField(input.QueryID, input.Row, input.Column, input.Offset, input.Length)
		return jsonToolResult(resp, resp.Error != ""), nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "grep_rows",
		Title:       "Grep Cached Rows",
		Description: grepRowsDescription,
	}, func(ctx context.Context, req *mcp.CallToolRequest, input grepRowsInput) (*mcp.CallToolResult, any, error) {
		serverLogger.InfoContext(ctx, "MCP tool called", "tool", "grep_rows", "query_id", input.QueryID, "columns", len(input.Columns), "limit", input.Limit)
		resp := svc.GrepRows(input.QueryID, input.Pattern, input.Columns, input.Limit, input.ContextChars)
		return jsonToolResult(resp, resp.Error != ""), nil, nil
	})
	if opts.DebugCacheTool {
		mcp.AddTool(server, &mcp.Tool{
			Name:        "_debug_cache_status",
			Title:       "Debug Query Cache Status",
			Description: "Return live cached query_ids, ages, and total cache size for debugging. Example: _debug_cache_status({}) shows which query handles are still valid. Do NOT compute substring offsets in SQL. Use get_field for long fields. Related tools: query, get_rows, grep_rows.",
		}, func(ctx context.Context, req *mcp.CallToolRequest, input debugCacheInput) (*mcp.CallToolResult, any, error) {
			serverLogger.InfoContext(ctx, "MCP tool called", "tool", "_debug_cache_status")
			return jsonToolResult(svc.DebugCacheStatus(), false), nil, nil
		})
	}
	mcp.AddTool(server, &mcp.Tool{
		Name:        "schema_overview",
		Title:       "Schema Overview",
		Description: "List tables and columns in the default ClickHouse database with compact samples. Example: schema_overview({}) returns one text section per table. Do NOT compute substring offsets in SQL. Use query to create a query_id, then get_field for long fields. Related tools: query, get_rows, get_field, grep_rows.",
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

	mcpServer := NewMCPServer(runner, query.Options{MaxRows: cfg.MaxRows, MaxFieldChars: cfg.MaxFieldChars, QueryCacheMaxBytes: cfg.QueryCacheMaxBytes, GetFieldMaxChars: cfg.GetFieldMaxChars, QueryCacheTTL: cfg.QueryCacheTTL, DebugCacheTool: cfg.DebugCacheTool, Logger: slog.Default()})
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return mcpServer }, &mcp.StreamableHTTPOptions{
		JSONResponse:   true,
		Logger:         slog.Default().With("component", "mcp_streamable"),
		SessionTimeout: 30 * time.Minute,
	})
	protected := authSvc.RequireBearer(strings.TrimRight(baseURL, "/") + "/.well-known/oauth-protected-resource")(mcpHandler)
	mux.Handle("/mcp", protected)
	return logRequests(logger, mux)
}

func jsonToolResult(value any, isError bool) *mcp.CallToolResult {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: `{"error":"failed to encode tool response"}`}},
			IsError: true,
		}
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		IsError: isError,
	}
}

func queryResponseHasError(resp query.QueryResponse) bool {
	for _, result := range resp.Results {
		if result.Error != "" {
			return true
		}
	}
	return false
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
