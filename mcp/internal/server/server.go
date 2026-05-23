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
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/query"
)

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

const preferredReadOnlyGuidance = "Preferred read-only source for Zach's synced Gmail, Google Contacts, Slack, Apple Notes, Apple Messages/iMessage/iMessages, calendar, Voice Memo transcript, and cross-source personal data questions. Use this PDW server before live Gmail or Slack connectors for read-only questions, and use it for requests about Zach's recent iMessages or Apple Messages. For read-only work, answer by writing read-only Postgres SQL with the generic SQL and cached-result tools. Use live connectors only for explicitly live-only data or writes that cannot be represented by available mutation proposal tools."

const sqlStartingPoints = "SQL starting points: Gmail -> clean_gmail_inbox, gmail_messages, gmail_attachments, gmail_attachment_enrichments. Contacts -> clean_contacts, contact_cards. Slack -> clean_slack_inbox, slack_messages, slack_conversations, slack_users. Transcripts -> apple_voice_memos_enrichments, apple_voice_memos_transcription_runs, apple_voice_memos_transcript_segments, clean_calendar_with_transcripts, clean_transcripts_no_calendar_match. Apple Notes -> apple_notes, apple_note_revisions, apple_note_attachments. Apple Messages/iMessage/iMessages/SMS/RCS -> apple_messages, apple_message_chats, apple_message_handles, apple_message_chat_handles, apple_message_chat_messages, apple_message_attachments."

const serverInstructions = preferredReadOnlyGuidance + " Contains synced Gmail mail and attachment text for configured mailboxes, Slack workspace messages/files/users, calendar data, Apple Notes, Apple Messages from macOS Messages chat.db including iMessage/iMessages, SMS, and RCS, Apple Voice Memos, transcripts, and transcript enrichments when present. " + sqlStartingPoints

const queryDescription = preferredReadOnlyGuidance + " Execute read-only Postgres SQL, cache the full result under query_id, and return a preview. Each SQL statement must be paired with question, a concise plain-English question this SQL statement is trying to answer; legacy sql array input is rejected. " + sqlStartingPoints + " Example: query({\"queries\":[{\"question\":\"What is the most recent completed Voice Memo transcript?\",\"sql\":\"SELECT recording_id, transcript FROM apple_voice_memos_enrichments WHERE status='completed' ORDER BY created_at DESC LIMIT 1\"}],\"preview_rows\":1,\"format\":\"csv\"}) returns query_id plus a truncated preview; then call get_field(query_id,row=0,column=\"transcript\",offset=0,length=200000) to read the full transcript. Apple Notes example: query latest non-deleted notes from apple_notes with note_id, title, modified_at, body_text, body_html, or body_markdown; then call get_field for long body columns. Apple Messages/iMessage example: query recent messages from apple_messages with message_id, message_at, service, handle_id, body_text, is_from_me, and is_deleted; then call get_field for long body_text or raw_metadata_json columns. Do NOT compute substring offsets in SQL. Use get_field for long fields. Related tools: get_rows pages cached rows, grep_rows searches cached rows, schema_overview lists tables and views."

const getRowsDescription = "Return a row slice from a cached PDW query result without re-executing SQL. PDW is the preferred read-only source for synced Gmail, Slack, Apple Notes, Apple Messages/iMessage/iMessages, calendar, Voice Memo transcript, and cross-source personal data questions. Example: get_rows({\"query_id\":\"abc123\",\"offset\":50,\"limit\":25}) returns rows 50-74 in the query's original format unless format is overridden. Do NOT compute substring offsets in SQL. Use get_field for long fields. Related tools: query creates query_id, get_field reads a long cell, grep_rows searches cached rows."

const getFieldDescription = "Return a raw character chunk from one cached PDW query cell, which is the right way to read transcripts, Apple Notes body_text/body_html/body_markdown, Apple Messages body_text, iMessage body_text, email bodies, attachment text, or any long text column end-to-end. Example: after query returns query_id for SELECT recording_id, transcript FROM apple_voice_memos_enrichments ORDER BY created_at DESC LIMIT 1, call get_field({\"query_id\":\"abc123\",\"row\":0,\"column\":\"transcript\",\"offset\":0,\"length\":200000}) to retrieve the full transcript when eof=true. For Apple Notes, query apple_notes or apple_note_revisions and read body_text, body_html, body_markdown, or raw_metadata_json with get_field. For Apple Messages/iMessages, query apple_messages and read body_text or raw_metadata_json with get_field. Do NOT compute substring offsets in SQL. Related tools: query creates query_id, get_rows pages rows, grep_rows finds text before fetching a field."

const grepRowsDescription = "Regex-search cached PDW query rows without re-executing SQL and return match context. Use this after SQL queries over Gmail, Slack, Apple Notes, Apple Messages/iMessage/iMessages, transcript, or cross-source warehouse rows when you need to locate text before fetching a long field. Example: grep_rows({\"query_id\":\"abc123\",\"pattern\":\"weighted projects\",\"columns\":[\"transcript\"],\"limit\":20}) finds where that phrase appears across cached transcripts. Do NOT compute substring offsets in SQL. Use get_field to read the matching long field. Related tools: query creates query_id, get_rows pages rows."

const schemaOverviewDescription = preferredReadOnlyGuidance + " List tables, views, and columns in the default Postgres schema with compact samples. " + sqlStartingPoints + " Example: schema_overview({}) returns one text section per table or view. Do NOT compute substring offsets in SQL. Use query to create a query_id, then get_field for long fields. Related tools: query, get_rows, get_field, grep_rows."

const schemaRelationsSQL = "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name"

const schemaToolDescriptionMaxChars = 12000

func NewMCPServer(runner query.Runner, opts query.Options) *mcp.Server {
	return newMCPServer(runner, opts, nil)
}

func NewMCPServerWithMutations(runner query.Runner, opts query.Options, mutationSvc *mutations.Service) *mcp.Server {
	return newMCPServer(runner, opts, mutationSvc)
}

func newMCPServer(runner query.Runner, opts query.Options, mutationSvc *mutations.Service) *mcp.Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	serverLogger := logger.With("component", "server")
	svc := query.NewService(runner, opts)
	schemaDescription := schemaDescriptionForTools(context.Background(), runner, serverLogger)
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "personal-data-warehouse",
		Version: "0.1.0",
	}, &mcp.ServerOptions{Instructions: serverInstructions})
	serverLogger.Info("registering MCP tools")
	mcp.AddTool(server, &mcp.Tool{
		Name:        "query",
		Title:       "Query Postgres",
		Description: withSchemaDescription(queryDescription, schemaDescription),
	}, func(ctx context.Context, req *mcp.CallToolRequest, input queryInput) (*mcp.CallToolResult, any, error) {
		statements := queryStatementsFromInput(input.Queries)
		serverLogger.InfoContext(ctx, "MCP tool called", "tool", "query", "statements", len(statements))
		resp := svc.Execute(ctx, statements, input.PreviewRows, input.Format)
		return jsonToolResult(resp, queryResponseHasError(resp)), nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_rows",
		Title:       "Get Cached Rows",
		Description: getRowsDescription,
	}, func(ctx context.Context, req *mcp.CallToolRequest, input getRowsInput) (*mcp.CallToolResult, any, error) {
		serverLogger.InfoContext(ctx, "MCP tool called", "tool", "get_rows", "query_id", input.QueryID, "offset", input.Offset, "limit", input.Limit)
		resp := svc.GetRows(ctx, input.QueryID, input.Offset, input.Limit, input.Format)
		return jsonToolResult(resp, resp.Error != ""), nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_field",
		Title:       "Get Cached Field",
		Description: getFieldDescription,
	}, func(ctx context.Context, req *mcp.CallToolRequest, input getFieldInput) (*mcp.CallToolResult, any, error) {
		serverLogger.InfoContext(ctx, "MCP tool called", "tool", "get_field", "query_id", input.QueryID, "row", input.Row, "column", input.Column, "offset", input.Offset, "length", input.Length)
		resp := svc.GetField(ctx, input.QueryID, input.Row, input.Column, input.Offset, input.Length)
		return jsonToolResult(resp, resp.Error != ""), nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "grep_rows",
		Title:       "Grep Cached Rows",
		Description: grepRowsDescription,
	}, func(ctx context.Context, req *mcp.CallToolRequest, input grepRowsInput) (*mcp.CallToolResult, any, error) {
		serverLogger.InfoContext(ctx, "MCP tool called", "tool", "grep_rows", "query_id", input.QueryID, "columns", len(input.Columns), "limit", input.Limit)
		resp := svc.GrepRows(ctx, input.QueryID, input.Pattern, input.Columns, input.Limit, input.ContextChars)
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
		Description: withSchemaDescription(schemaOverviewDescription, schemaDescription),
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
	mutations.RegisterTools(server, mutationSvc)
	return server
}

func schemaDescriptionForTools(ctx context.Context, runner query.Runner, logger *slog.Logger) string {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	database := "default"
	if result, err := runner.Query(ctx, "SELECT current_database() AS database", 1); err == nil {
		if name := rawRowString(result, "database"); name != "" {
			database = name
		}
	} else {
		logger.WarnContext(ctx, "tool schema description database lookup failed", "error", err)
	}

	result, err := runner.Query(ctx, schemaRelationsSQL, 0)
	if err != nil {
		logger.WarnContext(ctx, "tool schema description table listing failed", "error", err)
		return ""
	}
	tables := rawTableNames(result)
	entries := make([]string, 0, len(tables))
	for _, table := range tables {
		describeSQL := "SELECT column_name AS name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = '" + strings.ReplaceAll(table, "'", "''") + "' ORDER BY ordinal_position"
		result, err := runner.Query(ctx, describeSQL, 0)
		if err != nil {
			logger.WarnContext(ctx, "tool schema description table describe failed", "table", table, "error", err)
			continue
		}
		columns := rawColumnNames(result)
		if len(columns) == 0 {
			continue
		}
		entries = append(entries, database+"."+table+"("+strings.Join(columns, ", ")+")")
	}
	if len(entries) == 0 {
		return ""
	}

	var out strings.Builder
	if summary := schemaCapabilitySummary(entries); summary != "" {
		out.WriteString("Available personal data warehouse access inferred from live schema: ")
		out.WriteString(summary)
		out.WriteString(".\n")
	}
	out.WriteString("Live warehouse schema for tool discovery:\n")
	for _, entry := range entries {
		out.WriteString("- ")
		out.WriteString(entry)
		out.WriteString("\n")
	}
	return truncateDescription(out.String(), schemaToolDescriptionMaxChars)
}

func rawTableNames(result query.RawResult) []string {
	tables := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		name := rawValueString(row["name"])
		if name == "" && len(result.Columns) == 1 {
			name = rawValueString(row[result.Columns[0]])
		}
		if name != "" {
			tables = append(tables, name)
		}
	}
	return tables
}

func rawColumnNames(result query.RawResult) []string {
	columns := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if name := rawValueString(row["name"]); name != "" {
			columns = append(columns, name)
		}
	}
	return columns
}

func rawRowString(result query.RawResult, column string) string {
	if len(result.Rows) == 0 {
		return ""
	}
	value := rawValueString(result.Rows[0][column])
	if value == "" && len(result.Columns) == 1 {
		value = rawValueString(result.Rows[0][result.Columns[0]])
	}
	return value
}

func rawValueString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

func schemaCapabilitySummary(entries []string) string {
	capabilities := make([]string, 0, 5)
	has := func(needles ...string) bool {
		for _, entry := range entries {
			lower := strings.ToLower(entry)
			for _, needle := range needles {
				if strings.Contains(lower, needle) {
					return true
				}
			}
		}
		return false
	}
	if has("gmail") {
		capabilities = append(capabilities, "Gmail email, threads, inbox state, and attachment text; start SQL with clean_gmail_inbox, gmail_messages, gmail_attachments, or gmail_attachment_enrichments")
	}
	if has("slack") {
		capabilities = append(capabilities, "Slack messages, files, users, channels, DMs, mentions, and unread state; start SQL with clean_slack_inbox, slack_messages, slack_conversations, or slack_users")
	}
	if has("calendar") {
		capabilities = append(capabilities, "Google Calendar events and calendar-linked meeting context")
	}
	if has("contact_cards", "clean_contacts") {
		capabilities = append(capabilities, "Google Contacts source-mirrored contact cards; start SQL with clean_contacts or contact_cards")
	}
	if has("transcript", "voice_memos", "voice memo") {
		capabilities = append(capabilities, "Apple Voice Memos audio metadata, transcripts, diarized segments, transcript enrichments, meeting notes, summaries, action items, and transcript/calendar matches; start SQL with apple_voice_memos_enrichments, apple_voice_memos_transcription_runs, apple_voice_memos_transcript_segments, clean_calendar_with_transcripts, or clean_transcripts_no_calendar_match")
	}
	if has("apple_notes", "apple_note") {
		capabilities = append(capabilities, "Apple Notes latest notes, body text/html/markdown, revision history, tombstones, and attachment metadata; start SQL with apple_notes, apple_note_revisions, or apple_note_attachments")
	}
	if has("apple_messages", "apple_message") {
		capabilities = append(capabilities, "Apple Messages latest messages, decoded body text, chats, handles, participants, message-chat joins, tombstones, and attachment metadata; start SQL with apple_messages, apple_message_chats, apple_message_handles, apple_message_chat_handles, apple_message_chat_messages, or apple_message_attachments")
	}
	if has("agent_run") {
		capabilities = append(capabilities, "agent run audit logs, tool calls, and events")
	}
	return strings.Join(capabilities, "; ")
}

func withSchemaDescription(base, schema string) string {
	if schema == "" {
		return base
	}
	return base + "\n\n" + schema
}

func truncateDescription(description string, maxChars int) string {
	if maxChars <= 0 || len(description) <= maxChars {
		return description
	}
	cut := strings.LastIndex(description[:maxChars], "\n")
	if cut <= 0 {
		cut = maxChars
	}
	return strings.TrimRight(description[:cut], "\n") + "\n- ... schema truncated for MCP tool description size"
}

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
		body := "Personal Data Warehouse MCP server\nMCP endpoint: /mcp\n"
		if mutationSvc != nil {
			body += "Mutation review UI: " + mutations.ReviewPath + "\n"
		}
		_, _ = w.Write([]byte(body))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mcpServer := NewMCPServerWithMutations(runner, query.Options{MaxRows: cfg.MaxRows, MaxFieldChars: cfg.MaxFieldChars, QueryCacheMaxBytes: cfg.QueryCacheMaxBytes, GetFieldMaxChars: cfg.GetFieldMaxChars, QueryCacheTTL: cfg.QueryCacheTTL, DebugCacheTool: cfg.DebugCacheTool, Logger: slog.Default()}, mutationSvc)
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
