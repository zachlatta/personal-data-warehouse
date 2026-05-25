package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
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

func (s fakeMutationStore) ApproveRequest(context.Context, string, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
}

func (s fakeMutationStore) RejectRequest(context.Context, string, string, string) (mutations.Request, error) {
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

func (s *recordingMutationStore) ApproveRequest(context.Context, string, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
}

func (s *recordingMutationStore) RejectRequest(context.Context, string, string, string) (mutations.Request, error) {
	return mutations.Request{}, mutations.ErrNotFound
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
	for _, name := range []string{"query", "propose_gmail_archive_threads", "propose_gmail_unarchive_threads", "propose_gmail_send_email", "propose_contact_mutations", "propose_mutation_request"} {
		if !found[name] {
			t.Fatalf("%s tool not listed: %#v", name, tools.Tools)
		}
	}

	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "propose_gmail_archive_threads", Arguments: map[string]any{
		"account":    "zach@example.test",
		"thread_ids": []string{"thread-1"},
		"reason":     "clear stale mail",
	}})
	if err != nil {
		t.Fatalf("CallTool failed: %v", err)
	}
	if result.IsError {
		t.Fatalf("propose_gmail_archive_threads returned error: %#v", result.Content)
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

	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "propose_gmail_send_email", Arguments: map[string]any{
		"account":   "zach@example.test",
		"to":        []string{"zach@example.test"},
		"subject":   "Base subject",
		"body_text": "Base body",
		"reason":    "review alternate replies",
		"variants": []map[string]any{{
			"title":     "Direct Reply",
			"body_text": "Direct body",
		}, {
			"title":     "Softer Ask",
			"subject":   "Softer subject",
			"body_text": "Softer body",
		}},
	}})
	if err != nil {
		t.Fatalf("CallTool failed: %v", err)
	}
	if result.IsError {
		t.Fatalf("propose_gmail_send_email returned error: %#v", result.Content)
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

func TestMCPServerExposesDebugCacheStatusOnlyWhenEnabled(t *testing.T) {
	runner := fakeRunner{results: map[string]query.RawResult{}}

	// Off by default.
	disabledSrv := NewMCPServer(runner, query.Options{MaxRows: 5, MaxFieldChars: 100})
	disabledNames := listToolNames(t, disabledSrv)
	if disabledNames["_debug_cache_status"] {
		t.Fatalf("_debug_cache_status must not be listed when DebugCacheTool is false: %v", disabledNames)
	}

	// On when enabled, returns a JSON DebugCacheStatus document.
	enabledSrv := NewMCPServer(runner, query.Options{MaxRows: 5, MaxFieldChars: 100, DebugCacheTool: true})
	enabledNames := listToolNames(t, enabledSrv)
	if !enabledNames["_debug_cache_status"] {
		t.Fatalf("_debug_cache_status must be listed when DebugCacheTool is true: %v", enabledNames)
	}

	text := callToolText(t, enabledSrv, "_debug_cache_status", map[string]any{})
	var status struct {
		TotalBytes int64 `json:"total_bytes"`
		MaxBytes   int64 `json:"max_bytes"`
		TTLSeconds int64 `json:"ttl_seconds"`
		Queries    []any `json:"queries"`
	}
	if err := json.Unmarshal([]byte(text), &status); err != nil {
		t.Fatalf("_debug_cache_status response was not JSON: %v\n%s", err, text)
	}
	if status.Queries == nil {
		t.Fatalf("expected queries field to be present (empty array), got nil: %s", text)
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
		"SELECT table_name AS name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name": {
			Columns: []string{"name"},
			Rows: []map[string]any{
				{"name": "apple_messages"},
				{"name": "apple_notes"},
				{"name": "apple_voice_memos_enrichments"},
				{"name": "clean_gmail_inbox"},
				{"name": "gmail_messages"},
			},
		},
		"SELECT column_name AS name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'apple_messages' ORDER BY ordinal_position": {
			Columns: []string{"name"},
			Rows: []map[string]any{
				{"name": "message_id"},
				{"name": "message_at"},
				{"name": "service"},
				{"name": "handle_id"},
				{"name": "body_text"},
				{"name": "is_from_me"},
				{"name": "is_deleted"},
			},
		},
		"SELECT column_name AS name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'apple_notes' ORDER BY ordinal_position": {
			Columns: []string{"name"},
			Rows: []map[string]any{
				{"name": "note_id"},
				{"name": "title"},
				{"name": "modified_at"},
				{"name": "body_text"},
				{"name": "body_html"},
				{"name": "is_deleted"},
			},
		},
		"SELECT column_name AS name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'clean_gmail_inbox' ORDER BY ordinal_position": {
			Columns: []string{"name"},
			Rows:    []map[string]any{{"name": "thread_id"}, {"name": "latest_subject"}},
		},
		"SELECT column_name AS name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'gmail_messages' ORDER BY ordinal_position": {
			Columns: []string{"name"},
			Rows:    []map[string]any{{"name": "subject"}},
		},
		"SELECT column_name AS name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'apple_voice_memos_enrichments' ORDER BY ordinal_position": {
			Columns: []string{"name"},
			Rows:    []map[string]any{{"name": "transcript"}, {"name": "summary"}},
		},
		"SELECT substring(\"thread_id\"::text from 1 for 15) AS \"thread_id\", char_length(\"thread_id\"::text) AS \"__pdw_preview_len_0\", substring(\"latest_subject\"::text from 1 for 15) AS \"latest_subject\", char_length(\"latest_subject\"::text) AS \"__pdw_preview_len_1\" FROM \"clean_gmail_inbox\" LIMIT 3": {
			Columns: []string{"thread_id", "__pdw_preview_len_0", "latest_subject", "__pdw_preview_len_1"},
			Rows:    []map[string]any{{"thread_id": "thread-1", "__pdw_preview_len_0": 8, "latest_subject": "hello inbox", "__pdw_preview_len_1": 11}},
		},
		"SELECT substring(\"subject\"::text from 1 for 15) AS \"subject\", char_length(\"subject\"::text) AS \"__pdw_preview_len_0\" FROM \"gmail_messages\" LIMIT 3": {
			Columns: []string{"subject", "__pdw_preview_len_0"},
			Rows:    []map[string]any{{"subject": "hello", "__pdw_preview_len_0": 5}},
		},
		"SELECT substring(\"transcript\"::text from 1 for 15) AS \"transcript\", char_length(\"transcript\"::text) AS \"__pdw_preview_len_0\", substring(\"summary\"::text from 1 for 15) AS \"summary\", char_length(\"summary\"::text) AS \"__pdw_preview_len_1\" FROM \"apple_voice_memos_enrichments\" LIMIT 3": {
			Columns: []string{"transcript", "__pdw_preview_len_0", "summary", "__pdw_preview_len_1"},
			Rows:    []map[string]any{{"transcript": "meeting words", "__pdw_preview_len_0": 13, "summary": "recap", "__pdw_preview_len_1": 5}},
		},
		"SELECT substring(\"message_id\"::text from 1 for 15) AS \"message_id\", char_length(\"message_id\"::text) AS \"__pdw_preview_len_0\", substring(\"message_at\"::text from 1 for 15) AS \"message_at\", char_length(\"message_at\"::text) AS \"__pdw_preview_len_1\", substring(\"service\"::text from 1 for 15) AS \"service\", char_length(\"service\"::text) AS \"__pdw_preview_len_2\", substring(\"handle_id\"::text from 1 for 15) AS \"handle_id\", char_length(\"handle_id\"::text) AS \"__pdw_preview_len_3\", substring(\"body_text\"::text from 1 for 15) AS \"body_text\", char_length(\"body_text\"::text) AS \"__pdw_preview_len_4\", substring(\"is_from_me\"::text from 1 for 15) AS \"is_from_me\", char_length(\"is_from_me\"::text) AS \"__pdw_preview_len_5\", substring(\"is_deleted\"::text from 1 for 15) AS \"is_deleted\", char_length(\"is_deleted\"::text) AS \"__pdw_preview_len_6\" FROM \"apple_messages\" LIMIT 3": {
			Columns: []string{"message_id", "__pdw_preview_len_0", "message_at", "__pdw_preview_len_1", "service", "__pdw_preview_len_2", "handle_id", "__pdw_preview_len_3", "body_text", "__pdw_preview_len_4", "is_from_me", "__pdw_preview_len_5", "is_deleted", "__pdw_preview_len_6"},
			Rows: []map[string]any{{
				"message_id":          "message-1",
				"__pdw_preview_len_0": 9,
				"message_at":          "2026-05-21T12",
				"__pdw_preview_len_1": 13,
				"service":             "iMessage",
				"__pdw_preview_len_2": 8,
				"handle_id":           "1",
				"__pdw_preview_len_3": 1,
				"body_text":           "hello message",
				"__pdw_preview_len_4": 13,
				"is_from_me":          "0",
				"__pdw_preview_len_5": 1,
				"is_deleted":          "0",
				"__pdw_preview_len_6": 1,
			}},
		},
		"SELECT substring(\"note_id\"::text from 1 for 15) AS \"note_id\", char_length(\"note_id\"::text) AS \"__pdw_preview_len_0\", substring(\"title\"::text from 1 for 15) AS \"title\", char_length(\"title\"::text) AS \"__pdw_preview_len_1\", substring(\"modified_at\"::text from 1 for 15) AS \"modified_at\", char_length(\"modified_at\"::text) AS \"__pdw_preview_len_2\", substring(\"body_text\"::text from 1 for 15) AS \"body_text\", char_length(\"body_text\"::text) AS \"__pdw_preview_len_3\", substring(\"body_html\"::text from 1 for 15) AS \"body_html\", char_length(\"body_html\"::text) AS \"__pdw_preview_len_4\", substring(\"is_deleted\"::text from 1 for 15) AS \"is_deleted\", char_length(\"is_deleted\"::text) AS \"__pdw_preview_len_5\" FROM \"apple_notes\" LIMIT 3": {
			Columns: []string{"note_id", "__pdw_preview_len_0", "title", "__pdw_preview_len_1", "modified_at", "__pdw_preview_len_2", "body_text", "__pdw_preview_len_3", "body_html", "__pdw_preview_len_4", "is_deleted", "__pdw_preview_len_5"},
			Rows: []map[string]any{{
				"note_id":             "note-1",
				"__pdw_preview_len_0": 6,
				"title":               "Trip plan",
				"__pdw_preview_len_1": 9,
				"modified_at":         "2026-05-21T12",
				"__pdw_preview_len_2": 13,
				"body_text":           "Pack charger",
				"__pdw_preview_len_3": 12,
				"body_html":           "<p>Pack charger",
				"__pdw_preview_len_4": 15,
				"is_deleted":          "0",
				"__pdw_preview_len_5": 1,
			}},
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
		if tool.Name == "query" || tool.Name == "schema_overview" {
			if !strings.Contains(tool.Description, "Preferred read-only source") {
				t.Fatalf("%s description does not steer agents to PDW first: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "Use this PDW server before live Gmail or Slack connectors for read-only questions") {
				t.Fatalf("%s description does not prefer PDW over live connectors for reads: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "clean_gmail_inbox") || !strings.Contains(tool.Description, "clean_slack_inbox") {
				t.Fatalf("%s description does not include clean-view SQL starting points: %q", tool.Name, tool.Description)
			}
			if strings.Contains(tool.Description, "voice_memo_transcripts") {
				t.Fatalf("%s description still references stale transcript table: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "apple_voice_memos_enrichments") {
				t.Fatalf("%s description does not include live transcript table: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "Apple Notes") || !strings.Contains(tool.Description, "apple_notes") {
				t.Fatalf("%s description does not include Apple Notes SQL starting points: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "Apple Messages") || !strings.Contains(tool.Description, "apple_messages") {
				t.Fatalf("%s description does not include Apple Messages SQL starting points: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "iMessage") || !strings.Contains(tool.Description, "iMessages") {
				t.Fatalf("%s description does not include iMessage aliases for connector discovery: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "default.clean_gmail_inbox(thread_id, latest_subject)") {
				t.Fatalf("%s description does not include clean view schema for discovery: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "default.gmail_messages(subject)") {
				t.Fatalf("%s description does not include schema for discovery: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "Apple Voice Memos audio metadata, transcripts") {
				t.Fatalf("%s description does not include inferred transcript access: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "default.apple_voice_memos_enrichments(transcript, summary)") {
				t.Fatalf("%s description does not include transcript schema for discovery: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "default.apple_notes(note_id, title, modified_at, body_text, body_html, is_deleted)") {
				t.Fatalf("%s description does not include Apple Notes schema for discovery: %q", tool.Name, tool.Description)
			}
			if !strings.Contains(tool.Description, "default.apple_messages(message_id, message_at, service, handle_id, body_text, is_from_me, is_deleted)") {
				t.Fatalf("%s description does not include Apple Messages schema for discovery: %q", tool.Name, tool.Description)
			}
		}
		if tool.Name == "get_field" {
			if !strings.Contains(tool.Description, "Apple Notes body_text/body_html/body_markdown") || !strings.Contains(tool.Description, "Apple Messages body_text") {
				t.Fatalf("%s description does not steer agents to get_field for Apple local-data bodies: %q", tool.Name, tool.Description)
			}
		}
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
	if !strings.Contains(text.Text, "# default.clean_gmail_inbox") || !strings.Contains(text.Text, "thread_id,latest_subject\nthread-1,hello inbox") {
		t.Fatalf("schema overview did not include clean view: %q", text.Text)
	}
	if !strings.Contains(text.Text, "# default.gmail_messages") || !strings.Contains(text.Text, "subject\nhello") {
		t.Fatalf("unexpected schema overview text: %q", text.Text)
	}
	if !strings.Contains(text.Text, "# default.apple_notes") || !strings.Contains(text.Text, "note_id,title,modified_at,body_text,body_html,is_deleted") {
		t.Fatalf("schema overview did not include Apple Notes table: %q", text.Text)
	}
	if !strings.Contains(text.Text, "# default.apple_messages") || !strings.Contains(text.Text, "message_id,message_at,service,handle_id,body_text,is_from_me,is_deleted") {
		t.Fatalf("schema overview did not include Apple Messages table: %q", text.Text)
	}

	queryResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{
		"queries": []map[string]any{
			{
				"question": "What is one recent Gmail subject?",
				"sql":      "SELECT subject FROM gmail_messages LIMIT 1",
			},
		},
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
