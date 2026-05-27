package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/api"
	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

const debugCacheStatusDescription = "Return live cached query_ids, ages, and total cache size for debugging."

func mcpToolHooks(logger *slog.Logger) tool.Hooks {
	return tool.Hooks{
		OnCall: func(ctx context.Context, name string) {
			logger.InfoContext(ctx, "MCP tool called", "tool", name, "client", pdwauth.ClientNameFromContext(ctx))
		},
		OnResult: func(ctx context.Context, name string, output any, isError bool, err error) {
			if err != nil {
				logger.InfoContext(ctx, "MCP tool result", "tool", name, "client", pdwauth.ClientNameFromContext(ctx), "is_error", true, "error", err.Error())
				return
			}
			logger.InfoContext(ctx, "MCP tool result", "tool", name, "client", pdwauth.ClientNameFromContext(ctx), "is_error", isError, "output", marshalToolOutput(output))
		},
	}
}

func marshalToolOutput(output any) string {
	data, err := json.Marshal(output)
	if err != nil {
		return "<encode error: " + err.Error() + ">"
	}
	return string(data)
}

func debugCacheStatusTool(svc *query.Service) tool.Tool {
	return &tool.Typed[debugCacheInput, query.DebugCacheStatus]{
		NameStr:        "_debug_cache_status",
		TitleStr:       "Debug Query Cache Status",
		DescriptionStr: debugCacheStatusDescription,
		Handle: func(_ context.Context, _ debugCacheInput) (query.DebugCacheStatus, error) {
			return svc.DebugCacheStatus(), nil
		},
	}
}

type queryInput struct {
	Queries     []queryStatementInput `json:"queries" jsonschema:"array of query objects; each must include question and sql"`
	PreviewRows int                   `json:"preview_rows,omitempty" jsonschema:"number of initial rows to preview per statement, default 20"`
	Format      string                `json:"format,omitempty" jsonschema:"preview format: csv, json, or ndjson; default csv"`
}

type queryStatementInput struct {
	Question string `json:"question" jsonschema:"concise plain-English question this SQL statement is trying to answer"`
	SQL      string `json:"sql" jsonschema:"read-only Postgres SQL string to run"`
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

type sqlInput struct {
	SQL    string `json:"sql" jsonschema:"read-only Postgres SQL string to run"`
	Format string `json:"format,omitempty" jsonschema:"output format: csv (default), json, or ndjson"`
}

// serverInstructions is the only place MCP-side keyword discovery needs to
// happen: clients searching for "Slack" or "Gmail" hit this paragraph. The
// per-tool descriptions stay deliberately short; the rich content lives in
// the schema_overview response.
const serverInstructions = "Personal data warehouse for Zach's synced Slack, Gmail, Google Calendar, Google Contacts, Apple Notes, Apple Messages (iMessage/SMS/RCS), and Apple Voice Memo transcripts. Always call schema_overview first to learn the tables and columns; then write read-only Postgres SQL with query."

const schemaFirstReminder = "Call schema_overview first."

const queryDescription = "Run read-only Postgres SQL against the personal data warehouse and cache the result under a query_id. " + schemaFirstReminder + " Each SQL statement must be paired with question, a concise plain-English question this SQL statement is trying to answer."

const getRowsDescription = "Return a row slice from a cached query result by query_id. " + schemaFirstReminder

const getFieldDescription = "Return a character chunk from a single cell in a cached query result. Use this to read long fields (transcripts, message bodies, email bodies, note bodies) end-to-end without putting substring offsets in SQL. " + schemaFirstReminder

const grepRowsDescription = "Regex-search a cached query result and return match context. " + schemaFirstReminder

const schemaOverviewDescription = "Required first call. Lists the warehouse's tables, views, columns, and compact samples so the caller can pick the right tables before writing SQL. Each base table heading includes an approximate row count from planner statistics, formatted as `(~N rows, estimated)`; use that estimate for sizing decisions instead of running SELECT COUNT(*) over large tables."

const sqlDescription = "Run a read-only Postgres SQL statement and return its full result, like a psql session. Skips the query cache, pagination, and field truncation that the MCP query tool applies. Refuses write SQL and caps the response at 1,000,000 rows."

func NewMCPServer(runner query.Runner, opts query.Options) *mcp.Server {
	return NewMCPServerWithMutations(runner, opts, nil)
}

func NewMCPServerWithMutations(runner query.Runner, opts query.Options, mutationSvc *mutations.Service) *mcp.Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	registry, _ := buildRegistry(runner, opts, mutationSvc, logger)
	return newMCPServerFromRegistry(registry, logger)
}

// buildRegistry constructs the shared query.Service and assembles the
// tool.Registry that both surfaces (MCP and HTTP) consume. The query.Service
// is returned so callers that want the cache (e.g. NewMux for shutdown) can
// hold onto it; passing it back also makes the shared-cache contract obvious.
func buildRegistry(runner query.Runner, opts query.Options, mutationSvc *mutations.Service, _ *slog.Logger) (*tool.Registry, *query.Service) {
	svc := query.NewService(runner, opts)
	tools := readOnlyTools(svc)
	if opts.DebugCacheTool {
		tools = append(tools, debugCacheStatusTool(svc))
	}
	return tool.NewRegistry(tools, mutations.Tools(mutationSvc)), svc
}

func newMCPServerFromRegistry(registry *tool.Registry, logger *slog.Logger) *mcp.Server {
	serverLogger := logger.With("component", "server")
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "personal-data-warehouse",
		Version: "0.1.0",
	}, &mcp.ServerOptions{Instructions: serverInstructions})
	serverLogger.Info("registering MCP tools")
	hooks := mcpToolHooks(serverLogger)
	for _, t := range registry.Filter(toolShowsOnMCP).All() {
		t.RegisterMCP(server, hooks)
	}
	return server
}

func toolShowsOnMCP(t tool.Tool) bool { return t.Surfaces().ShowsOnMCP() }
func toolShowsOnCLI(t tool.Tool) bool { return t.Surfaces().ShowsOnCLI() }

func NewMux(cfg config.Config, authSvc *pdwauth.Service, runner query.Runner, mutationSvcs ...*mutations.Service) http.Handler {
	logger := slog.Default().With("component", "http")
	mux := http.NewServeMux()
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = "http://localhost" + cfg.Addr
	}
	var mutationSvc *mutations.Service
	if len(mutationSvcs) > 0 {
		mutationSvc = mutationSvcs[0]
	}
	logger.Info("registering HTTP handlers", "base_url", baseURL)
	authSvc.RegisterHandlers(mux, baseURL)
	if mutationSvc != nil {
		mux.Handle(mutations.ReviewPath+"/", mutationSvc.HTTPHandler())
		mux.Handle(mutations.ReviewPath, mutationSvc.HTTPHandler())
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			logger.WarnContext(r.Context(), "unknown route", "method", r.Method, "path", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		body := "Personal Data Warehouse app\nMCP endpoint: /mcp\nHTTP API: /api/tools\n"
		if mutationSvc != nil {
			body += "Mutation review UI: " + mutations.ReviewPath + "\n"
		}
		_, _ = w.Write([]byte(body))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	queryOpts := query.Options{MaxRows: cfg.MaxRows, MaxFieldChars: cfg.MaxFieldChars, QueryCacheMaxBytes: cfg.QueryCacheMaxBytes, GetFieldMaxChars: cfg.GetFieldMaxChars, QueryCacheTTL: cfg.QueryCacheTTL, DebugCacheTool: cfg.DebugCacheTool, Logger: slog.Default()}
	registry, _ := buildRegistry(runner, queryOpts, mutationSvc, slog.Default())
	mcpServer := newMCPServerFromRegistry(registry, slog.Default())
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return mcpServer }, &mcp.StreamableHTTPOptions{
		JSONResponse:   true,
		Logger:         slog.Default().With("component", "mcp_streamable"),
		SessionTimeout: 30 * time.Minute,
	})
	protected := authSvc.RequireBearer(strings.TrimRight(baseURL, "/") + "/.well-known/oauth-protected-resource")(mcpHandler)
	mux.Handle("/mcp", protected)

	apiHandler := api.NewHandler(registry.Filter(toolShowsOnCLI), slog.Default())
	apiProtected := authSvc.RequireStaticBearer()(apiHandler)
	mux.Handle("/api/tools", apiProtected)
	mux.Handle("/api/tools/", apiProtected)

	return logRequests(logger, mux)
}

func queryResponseHasError(resp query.QueryResponse) bool {
	for _, result := range resp.Results {
		if result.Error != "" {
			return true
		}
	}
	return false
}

func queryStatementsFromInput(inputs []queryStatementInput) []query.Statement {
	statements := make([]query.Statement, 0, len(inputs))
	for _, input := range inputs {
		statements = append(statements, query.Statement{
			Question: input.Question,
			SQL:      input.SQL,
		})
	}
	return statements
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
		ctx := pdwauth.WithClientNameHolder(r.Context())
		r = r.WithContext(ctx)
		started := time.Now()
		rec := &statusResponseWriter{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		logger.InfoContext(r.Context(), "HTTP request completed", "method", r.Method, "path", r.URL.Path, "status", status, "bytes", rec.bytes, "duration", time.Since(started), "client", pdwauth.ClientNameFromContext(r.Context()))
	})
}
