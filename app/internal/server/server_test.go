package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
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

func queryableSchemaArraySQL() string {
	schemas := warehouse.QueryableSchemas()
	quoted := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		quoted = append(quoted, warehouse.SQLString(schema))
	}
	return "ARRAY[" + strings.Join(quoted, ",") + "]"
}

func describeColumnsSQL(schema, table string) string {
	return "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type " +
		"FROM pg_attribute a " +
		"JOIN pg_class c ON c.oid = a.attrelid " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE n.nspname = '" + strings.ReplaceAll(schema, "'", "''") + "' AND c.relname = '" + strings.ReplaceAll(table, "'", "''") + "' " +
		"AND a.attnum > 0 AND NOT a.attisdropped " +
		"ORDER BY a.attnum"
}

type fakeMutationStore struct {
	request mutations.Request
}

func (s fakeMutationStore) CreateRequest(context.Context, mutations.CreateRequestInput) (mutations.Request, error) {
	return s.request, nil
}

func (s fakeMutationStore) ListRequests(context.Context, mutations.RequestFilter) ([]mutations.Request, error) {
	return nil, nil
}

func (s fakeMutationStore) GetRequest(context.Context, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
}

func (s fakeMutationStore) UpdateGmailEmailMutation(context.Context, string, string, mutations.UpdateGmailEmailMutationInput, string) (mutations.Mutation, error) {
	return mutations.Mutation{}, mutations.ErrNotFound
}

func (s fakeMutationStore) RemoveMutation(context.Context, string, string, string) (mutations.Mutation, error) {
	return mutations.Mutation{}, mutations.ErrNotFound
}

func (s fakeMutationStore) ApproveRequest(context.Context, string, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
}

func (s fakeMutationStore) RejectRequest(context.Context, string, string, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
}

func (s fakeMutationStore) SupersedeRequest(context.Context, string, string, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
}

type recordingMutationStore struct {
	request mutations.Request
	input   mutations.CreateRequestInput
}

func (s *recordingMutationStore) CreateRequest(_ context.Context, input mutations.CreateRequestInput) (mutations.Request, error) {
	s.input = input
	return s.request, nil
}

func (s *recordingMutationStore) ListRequests(context.Context, mutations.RequestFilter) ([]mutations.Request, error) {
	return nil, nil
}

func (s *recordingMutationStore) GetRequest(context.Context, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
}

func (s *recordingMutationStore) UpdateGmailEmailMutation(context.Context, string, string, mutations.UpdateGmailEmailMutationInput, string) (mutations.Mutation, error) {
	return mutations.Mutation{}, mutations.ErrNotFound
}

func (s *recordingMutationStore) RemoveMutation(context.Context, string, string, string) (mutations.Mutation, error) {
	return mutations.Mutation{}, mutations.ErrNotFound
}

func (s *recordingMutationStore) ApproveRequest(context.Context, string, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
}

func (s *recordingMutationStore) RejectRequest(context.Context, string, string, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
}

func (s *recordingMutationStore) SupersedeRequest(context.Context, string, string, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
}

// TestServerInstructionsCarryDiscoveryKeywords pins the keywords MCP clients
// search for when looking up "Slack", "Gmail", etc. Since tool descriptions
// are intentionally short, the server-level Instructions string is the only
// place this discovery happens; it must not regress.
func TestServerInstructionsCarryDiscoveryKeywords(t *testing.T) {
	for _, kw := range []string{
		"Slack",
		"Gmail",
		"Google Calendar",
		"Google Contacts",
		"Apple Notes",
		"Apple Messages",
		"iMessage",
		"Voice Memo",
		"schema_overview",
	} {
		if !strings.Contains(serverInstructions, kw) {
			t.Fatalf("serverInstructions missing discovery keyword %q: %s", kw, serverInstructions)
		}
	}
}

func TestServerInstructionsTellAgentsToSearchBeforeSchemaDiscovery(t *testing.T) {
	for _, want := range []string{"text, topic, person", "search first", "no schema discovery", "timeline.context"} {
		if !strings.Contains(strings.ToLower(serverInstructions), strings.ToLower(want)) {
			t.Fatalf("serverInstructions missing %q: %s", want, serverInstructions)
		}
	}
}

func TestSearchToolIsAvailableToMCPAndCLI(t *testing.T) {
	tool := searchTool(query.NewService(fakeRunner{}, query.Options{}))
	if !tool.Surfaces().ShowsOnMCP() || !tool.Surfaces().ShowsOnCLI() {
		t.Fatalf("search surfaces = %v; want MCP and CLI", tool.Surfaces())
	}
}

func TestMCPServerRegistersMutationProposalToolsWhenConfigured(t *testing.T) {
	runner := fakeRunner{results: map[string]query.RawResult{}}
	mutationSvc := mutations.NewService(fakeMutationStore{request: mutations.Request{
		ID:     "req-123",
		Status: "pending_review",
		Mutations: []mutations.Mutation{{
			ID: "mut-123",
		}},
	}}, mutations.Config{BaseURL: "https://mcp.example.test"})
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	srv := NewMCPServerWithMutations(runner, query.Options{MaxRows: 5, MaxFieldChars: 100}, mutationSvc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = srv.Run(ctx, serverTransport)
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
	}
	for _, name := range []string{"query", "propose_mutation", "propose_mutation_help"} {
		if !found[name] {
			t.Fatalf("%s tool not listed: %#v", name, tools.Tools)
		}
	}

	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "propose_mutation", Arguments: map[string]any{
		"title":  "Archive stale mail",
		"reason": "clear stale mail",
		"mutations": []map[string]any{{
			"type":       "gmail.archive_threads",
			"account":    "zach@example.test",
			"thread_ids": []string{"thread-1"},
		}},
	}})
	if err != nil {
		t.Fatalf("CallTool failed: %v", err)
	}
	if result.IsError {
		t.Fatalf("propose_mutation returned error: %#v", result.Content)
	}
	text := result.Content[0].(*mcp.TextContent).Text
	if !strings.Contains(text, `"request_id": "req-123"`) || !strings.Contains(text, `"approval_url": "https://mcp.example.test/mutation-review/requests/req-123"`) {
		t.Fatalf("unexpected mutation proposal response: %q", text)
	}
}

func TestMCPServerProposeGmailSendEmailAcceptsVariants(t *testing.T) {
	runner := fakeRunner{results: map[string]query.RawResult{}}
	store := &recordingMutationStore{request: mutations.Request{
		ID:     "req-variants",
		Status: "pending_review",
		Mutations: []mutations.Mutation{{
			ID: "mut-variants",
		}},
	}}
	mutationSvc := mutations.NewService(store, mutations.Config{
		BaseURL:       "https://mcp.example.test",
		GmailAccounts: []string{"zach@example.test"},
	})
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	srv := NewMCPServerWithMutations(runner, query.Options{MaxRows: 5, MaxFieldChars: 100}, mutationSvc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = srv.Run(ctx, serverTransport)
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect failed: %v", err)
	}
	defer session.Close()

	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "propose_mutation", Arguments: map[string]any{
		"title":  "Send reply",
		"reason": "review alternate replies",
		"mutations": []map[string]any{{
			"type":    "gmail.send_email",
			"account": "zach@example.test",
			"message": map[string]any{
				"to":        []string{"zach@example.test"},
				"subject":   "Base subject",
				"body_text": "Base body",
			},
			"variants": []map[string]any{{
				"title":     "Direct Reply",
				"body_text": "Direct body",
			}, {
				"title":     "Softer Ask",
				"subject":   "Softer subject",
				"body_text": "Softer body",
			}},
		}},
	}})
	if err != nil {
		t.Fatalf("CallTool failed: %v", err)
	}
	if result.IsError {
		t.Fatalf("propose_mutation returned error: %#v", result.Content)
	}
	if len(store.input.Mutations) != 1 {
		t.Fatalf("expected one mutation, got %#v", store.input.Mutations)
	}
	variants := store.input.Mutations[0].EmailVariants
	if len(variants) != 2 {
		t.Fatalf("expected two email variants, got %#v", variants)
	}
	if variants[0].Title != "Direct Reply" || variants[1].Title != "Softer Ask" {
		t.Fatalf("unexpected variant titles: %#v", variants)
	}
	if variants[1].Subject != "Softer subject" || variants[1].BodyText != "Softer body" {
		t.Fatalf("unexpected second variant: %#v", variants[1])
	}
}

// TestMCPHandlerErrorWrapsAsIsError pins the MCP error envelope produced when
// a tool handler returns a non-nil error: the result body is the indented
// {"error":"..."} map and IsError=true. This is the path mutation tools take
// when service validation rejects an input.
func TestMCPHandlerErrorWrapsAsIsError(t *testing.T) {
	runner := fakeRunner{results: map[string]query.RawResult{}}
	mutationSvc := mutations.NewService(fakeMutationStore{request: mutations.Request{ID: "ignored"}}, mutations.Config{BaseURL: "https://mcp.example.test"})
	srv := NewMCPServerWithMutations(runner, query.Options{MaxRows: 5, MaxFieldChars: 100}, mutationSvc)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Run(ctx, serverTransport) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect failed: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	// propose_mutation with a malformed mutation triggers mutationInputFromMap
	// -> validation error, which Typed.Handle propagates as a non-nil err,
	// exercising tool.mcpErrorResult.
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "propose_mutation", Arguments: map[string]any{
		"title":     "broken",
		"reason":    "test error path",
		"mutations": []map[string]any{{"kind": "totally_unknown_kind"}},
	}})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !result.IsError {
		t.Fatalf("expected IsError=true; content=%#v", result.Content)
	}
	if len(result.Content) != 1 {
		t.Fatalf("expected one content block, got %d", len(result.Content))
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content type = %T", result.Content[0])
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(text.Text), &body); err != nil {
		t.Fatalf("error body was not JSON: %v\n%s", err, text.Text)
	}
	if body["error"] == "" {
		t.Fatalf("error body missing \"error\" field: %s", text.Text)
	}
	// Indented format must match what mutationToolResult emitted pre-refactor.
	if !strings.Contains(text.Text, "\n  \"error\":") {
		t.Fatalf("error envelope is not indented as before: %q", text.Text)
	}
}

func listToolNames(t *testing.T, srv *mcp.Server) map[string]bool {
	t.Helper()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Run(ctx, serverTransport) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect failed: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	tools, err := session.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools failed: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range tools.Tools {
		names[tool.Name] = true
	}
	return names
}

func callToolText(t *testing.T, srv *mcp.Server, name string, args map[string]any) string {
	t.Helper()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Run(ctx, serverTransport) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect failed: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool(%s) failed: %v", name, err)
	}
	if result.IsError {
		t.Fatalf("%s returned IsError: %#v", name, result.Content)
	}
	if len(result.Content) == 0 {
		t.Fatalf("%s returned no content", name)
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("%s first content type = %T", name, result.Content[0])
	}
	return text.Text
}

func TestMCPServerExposesSchemaOverviewTool(t *testing.T) {
	runner := fakeRunner{results: map[string]query.RawResult{
		"SELECT current_database() AS database": {
			Columns: []string{"database"},
			Rows:    []map[string]any{{"database": "default"}},
		},
		"SELECT table_schema AS schema, table_name AS name FROM information_schema.tables WHERE table_schema = ANY(" + queryableSchemaArraySQL() + ") AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_schema, table_name": {
			Columns: []string{"schema", "name"},
			Rows: []map[string]any{
				{"schema": "base_apple_messages", "name": "messages"},
				{"schema": "base_apple_notes", "name": "notes"},
				{"schema": "derived_voice_memos", "name": "enrichments"},
				{"schema": "marts_inbox", "name": "gmail_threads"},
				{"schema": "base_gmail", "name": "messages"},
			},
		},
		describeColumnsSQL("base_apple_messages", "messages"): {
			Columns: []string{"name", "type"},
			Rows: []map[string]any{
				{"name": "message_id", "type": "text"},
				{"name": "message_at", "type": "timestamp with time zone"},
				{"name": "service", "type": "text"},
				{"name": "handle_id", "type": "text"},
				{"name": "body_text", "type": "text"},
				{"name": "is_from_me", "type": "bigint"},
				{"name": "is_deleted", "type": "bigint"},
			},
		},
		describeColumnsSQL("base_apple_notes", "notes"): {
			Columns: []string{"name", "type"},
			Rows: []map[string]any{
				{"name": "note_id", "type": "text"},
				{"name": "title", "type": "text"},
				{"name": "modified_at", "type": "timestamp with time zone"},
				{"name": "body_text", "type": "text"},
				{"name": "body_html", "type": "text"},
				{"name": "is_deleted", "type": "bigint"},
			},
		},
		describeColumnsSQL("marts_inbox", "gmail_threads"): {
			Columns: []string{"name", "type"},
			Rows:    []map[string]any{{"name": "thread_id", "type": "text"}, {"name": "latest_subject", "type": "text"}},
		},
		describeColumnsSQL("base_gmail", "messages"): {
			Columns: []string{"name", "type"},
			Rows:    []map[string]any{{"name": "subject", "type": "text"}},
		},
		describeColumnsSQL("derived_voice_memos", "enrichments"): {
			Columns: []string{"name", "type"},
			Rows:    []map[string]any{{"name": "transcript", "type": "text"}, {"name": "summary", "type": "text"}},
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
		// Tool descriptions are intentionally short and must point callers at the
		// two-step discovery path: schema_overview for the relations, then
		// describe_table for a relation's columns. Those two tools' own
		// descriptions don't need the reminder.
		if tool.Name == "query" {
			if !strings.Contains(tool.Description, "Call schema_overview first") ||
				!strings.Contains(tool.Description, "describe_table") {
				t.Fatalf("%s description should point callers at schema_overview then describe_table: %q", tool.Name, tool.Description)
			}
		}
	}
	for _, name := range []string{"query", "search", "schema_overview", "describe_table"} {
		if !found[name] {
			t.Fatalf("%s tool not listed: %#v", name, tools.Tools)
		}
	}
	for _, retired := range []string{"get_rows", "get_field", "grep_rows"} {
		if found[retired] {
			t.Fatalf("retired cursor helper %s is still listed: %#v", retired, tools.Tools)
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
	// The overview lists relations grouped by schema; per-relation columns moved
	// to describe_table, so the view appears as a line under its schema heading
	// rather than as its own column catalog.
	if !strings.Contains(text.Text, "# marts_inbox (1 relation)") || !strings.Contains(text.Text, "marts_inbox.gmail_threads") {
		t.Fatalf("schema overview did not list the marts view: %q", text.Text)
	}
	if strings.Contains(text.Text, "thread_id (text),latest_subject (text)") {
		t.Fatalf("schema overview should not carry per-relation columns: %q", text.Text)
	}
	// The overview must still name every relation, with its curated event-time
	// column, so a caller knows what exists and what to filter on before it
	// reaches for describe_table.
	for _, want := range []string{
		"base_gmail.messages",
		"base_apple_notes.notes",
		"time: modified_at",
		"base_apple_messages.messages",
		"time: message_at",
	} {
		if !strings.Contains(text.Text, want) {
			t.Fatalf("schema overview missing %q: %q", want, text.Text)
		}
	}
	// No sampled data rows, and no per-relation column catalogs.
	if strings.Contains(text.Text, "thread-1,hello inbox") || strings.Contains(text.Text, "hello message") {
		t.Fatalf("schema overview should not include sampled row values: %q", text.Text)
	}
	if strings.Contains(text.Text, "is_from_me (bigint)") {
		t.Fatalf("schema overview should not carry per-relation columns: %q", text.Text)
	}

	queryResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{
		"queries": []map[string]any{
			{
				"question": "What is one recent Gmail subject?",
				"sql":      "SELECT subject FROM gmail_messages LIMIT 1",
			},
		},
		"format": "csv",
	}})
	if err != nil {
		t.Fatalf("query CallTool failed: %v", err)
	}
	queryText := queryResult.Content[0].(*mcp.TextContent).Text
	var queryPayload struct {
		Results []struct {
			TotalRows   int      `json:"total_rows"`
			ColumnNames []string `json:"column_names"`
			Rows        string   `json:"rows"`
			Error       string   `json:"error"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(queryText), &queryPayload); err != nil {
		t.Fatalf("query response was not JSON: %v\n%s", err, queryText)
	}
	if queryPayload.Results[0].Rows != "subject\nhello" {
		t.Fatalf("unexpected query payload: %#v", queryPayload)
	}

	legacyResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{
		"sql": []string{"SELECT subject FROM gmail_messages LIMIT 1"},
	}})
	if err != nil {
		t.Fatalf("legacy query CallTool failed: %v", err)
	}
	legacyText := legacyResult.Content[0].(*mcp.TextContent).Text
	if !legacyResult.IsError || legacyText == "" {
		t.Fatalf("legacy sql-only query was not rejected: %q", legacyText)
	}

	blankQuestionResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{
		"queries": []map[string]any{
			{"question": " ", "sql": "SELECT subject FROM gmail_messages LIMIT 1"},
		},
	}})
	if err != nil {
		t.Fatalf("blank question query CallTool failed: %v", err)
	}
	var blankQuestionPayload struct {
		Results []struct {
			Error string `json:"error"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(blankQuestionResult.Content[0].(*mcp.TextContent).Text), &blankQuestionPayload); err != nil {
		t.Fatalf("blank question query response was not JSON: %v", err)
	}
	if !blankQuestionResult.IsError || !strings.Contains(blankQuestionPayload.Results[0].Error, "queries[0].question") {
		t.Fatalf("blank query question was not rejected: %#v", blankQuestionPayload)
	}

	cancel()
	<-serverErr
}

func TestMCPQueryAcceptsStringifiedQueriesArgument(t *testing.T) {
	runner := fakeRunner{results: map[string]query.RawResult{
		"SELECT 1 AS n": {
			Columns: []string{"n"},
			Rows:    []map[string]any{{"n": 1}},
		},
		"SELECT 2 AS n": {
			Columns: []string{"n"},
			Rows:    []map[string]any{{"n": 2}},
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

	stringifiedQueries := `[{"question":"ping","sql":"SELECT 1 AS n"}]`
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{
		"queries": stringifiedQueries,
		"format":  "csv",
	}})
	if err != nil {
		t.Fatalf("query CallTool failed: %v", err)
	}
	text := result.Content[0].(*mcp.TextContent).Text
	var payload struct {
		Results []struct {
			Rows      string `json:"rows"`
			TotalRows int    `json:"total_rows"`
			Error     string `json:"error"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		t.Fatalf("query response was not JSON: %v\n%s", err, text)
	}
	if result.IsError ||
		len(payload.Results) != 1 ||
		payload.Results[0].Rows != "n\n1" ||
		payload.Results[0].TotalRows != 1 ||
		payload.Results[0].Error != "" {
		t.Fatalf("unexpected query response for stringified queries: isError=%v payload=%#v text=%s", result.IsError, payload, text)
	}

	stringifiedMultiQueries := `[{"question":"one","sql":"SELECT 1 AS n"},{"question":"two","sql":"SELECT 2 AS n"}]`
	multiResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{
		"queries": stringifiedMultiQueries,
		"format":  "csv",
	}})
	if err != nil {
		t.Fatalf("multi-query CallTool failed: %v", err)
	}
	multiText := multiResult.Content[0].(*mcp.TextContent).Text
	var multiPayload struct {
		Results []struct {
			Rows  string `json:"rows"`
			Error string `json:"error"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(multiText), &multiPayload); err != nil {
		t.Fatalf("multi-query response was not JSON: %v\n%s", err, multiText)
	}
	if multiResult.IsError ||
		len(multiPayload.Results) != 2 ||
		multiPayload.Results[0].Rows != "n\n1" ||
		multiPayload.Results[1].Rows != "n\n2" ||
		multiPayload.Results[0].Error != "" ||
		multiPayload.Results[1].Error != "" {
		t.Fatalf("unexpected multi-query response: isError=%v payload=%#v text=%s", multiResult.IsError, multiPayload, multiText)
	}

	nullResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{
		"queries": nil,
	}})
	if err != nil {
		t.Fatalf("null queries CallTool failed: %v", err)
	}
	nullText := nullResult.Content[0].(*mcp.TextContent).Text
	if !nullResult.IsError || !strings.Contains(nullText, "queries must contain at least one") {
		t.Fatalf("queries:null should reach query validation, got isError=%v text=%s", nullResult.IsError, nullText)
	}

	cancel()
	<-serverErr
}

func TestMcpToolHooksOnResultLogsFullOutputOnSuccess(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	hooks := mcpToolHooks(logger)

	hooks.OnResult(context.Background(), "echo", map[string]any{"value": "hi", "n": 2}, false, nil)

	line := buf.String()
	if !strings.Contains(line, `"msg":"MCP tool result"`) {
		t.Fatalf("log missing msg: %s", line)
	}
	if !strings.Contains(line, `"tool":"echo"`) {
		t.Fatalf("log missing tool name: %s", line)
	}
	if !strings.Contains(line, `"is_error":false`) {
		t.Fatalf("log missing is_error=false: %s", line)
	}
	// Full output must be present, JSON-encoded as a string field.
	if !strings.Contains(line, `\"value\":\"hi\"`) || !strings.Contains(line, `\"n\":2`) {
		t.Fatalf("log missing full output payload: %s", line)
	}
}

func TestMcpToolHooksOnResultLogsHandlerError(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	hooks := mcpToolHooks(logger)

	hooks.OnResult(context.Background(), "echo", nil, true, errors.New("boom"))

	line := buf.String()
	if !strings.Contains(line, `"msg":"MCP tool result"`) {
		t.Fatalf("log missing msg: %s", line)
	}
	if !strings.Contains(line, `"is_error":true`) {
		t.Fatalf("log missing is_error=true: %s", line)
	}
	if !strings.Contains(line, `"error":"boom"`) {
		t.Fatalf("log missing error field: %s", line)
	}
	if strings.Contains(line, `"output"`) {
		t.Fatalf("log must not include output on handler error: %s", line)
	}
}

func TestMcpToolHooksOnResultLogsSoftError(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	hooks := mcpToolHooks(logger)

	hooks.OnResult(context.Background(), "soft", map[string]any{"partial": "yes"}, true, nil)

	line := buf.String()
	if !strings.Contains(line, `"is_error":true`) {
		t.Fatalf("soft IsError must log is_error=true: %s", line)
	}
	if !strings.Contains(line, `\"partial\":\"yes\"`) {
		t.Fatalf("soft IsError must still log full payload: %s", line)
	}
}

func TestServerInstructionsNameTheTimelineTiersAndLayerOrder(t *testing.T) {
	// Measured over 60 days: only 7.4% of agent sessions started at search or
	// the timeline, 43% opened with schema_overview, 18% opened by guessing a
	// relation that does not exist, the MCP search tool was called zero times,
	// and 7 sessions total used any priority literal. The instructions never
	// mentioned the timeline as the entry point, never mentioned the layer
	// order, and never mentioned the tiers -- so an agent had no way to know.
	lowered := strings.ToLower(serverInstructions)
	for _, want := range []string{
		"start at the timeline",
		"timeline.events",
		"priority tier",
	} {
		if !strings.Contains(lowered, want) {
			t.Fatalf("serverInstructions must say %q: %s", want, serverInstructions)
		}
	}
	// Every tier, with what it means -- a bare list of five words is not usable.
	for _, tier := range query.SearchPriorities {
		if tier == "unclassified" {
			continue // the not-yet-synced bucket; not something to scope to
		}
		if !strings.Contains(lowered, tier+" =") {
			t.Fatalf("serverInstructions must define the %q tier: %s", tier, serverInstructions)
		}
	}
	// The layer order, in order.
	timeline := strings.Index(lowered, "timeline (the event stream)")
	marts := strings.Index(lowered, "marts_*")
	base := strings.Index(lowered, "base_*")
	if timeline < 0 || marts < 0 || base < 0 || !(timeline < marts && marts < base) {
		t.Fatalf("serverInstructions must give the timeline -> marts_* -> base_* order: %s", serverInstructions)
	}
}

func TestSearchDescriptionTellsAgentsAboutPriorityScoping(t *testing.T) {
	// The search tool is where an agent decides whether to scope, so the tiers
	// have to be named here too -- not only in the input schema, which many
	// clients never surface to the model.
	lowered := strings.ToLower(searchDescription)
	for _, want := range []string{"priorities", "self", "direct", "noise", "priority"} {
		if !strings.Contains(lowered, want) {
			t.Fatalf("searchDescription must mention %q: %s", want, searchDescription)
		}
	}
}

func TestHighTrafficSQLDescriptionsPutPriorityGuidanceAtTheDecisionPoint(t *testing.T) {
	for name, description := range map[string]string{
		"sql":             sqlDescription,
		"schema_overview": schemaOverviewDescription,
	} {
		lowered := strings.ToLower(description)
		for _, want := range []string{"timeline.events", "priority", "self", "direct", "cc"} {
			if !strings.Contains(lowered, want) {
				t.Fatalf("%s description must mention %q: %s", name, want, description)
			}
		}
	}
}
