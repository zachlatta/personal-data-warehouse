package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
)

const muxAPITestSecret = "test-secret-token-at-least-32-chars-x"

// apiSnapshotTools lists tool names whose API-exposed input schemas we pin
// against the testdata/schemas/{name}.input_schema.json goldens. It mirrors
// mcpSnapshotTools but swaps in the CLI-only tools (and drops MCP-only ones)
// for the core read/query and mutation proposal schemas snapshotted here.
var apiSnapshotTools = []string{
	"search",
	"schema_overview",
	"describe_table",
	"sql",
	"propose_mutation",
	"propose_mutation_help",
}

// mcpOnlyToolNames are exposed on MCP but must NOT appear on the HTTP API.
var mcpOnlyToolNames = []string{"query", "get_rows", "get_field", "grep_rows"}

// cliOnlyToolNames are exposed on the HTTP API but must NOT appear on MCP.
var cliOnlyToolNames = []string{"sql"}

func newMuxAPITestServer(t *testing.T) *httptest.Server {
	t.Helper()
	runner := fakeRunner{results: map[string]query.RawResult{
		"SELECT 1 AS n": {Columns: []string{"n"}, Rows: []map[string]any{{"n": int64(1)}, {"n": int64(2)}, {"n": int64(3)}}},
	}}
	authSvc := pdwauth.NewService([]byte(muxAPITestSecret), func() time.Time { return time.Unix(0, 0) })
	cfg := config.Config{
		Addr:          ":0",
		BaseURL:       "http://example.test",
		SecretToken:   muxAPITestSecret,
		MaxRows:       100,
		MaxFieldChars: 1000,
	}
	mutationSvc := mutations.NewService(fakeMutationStore{request: mutations.Request{ID: "mux-fixture"}}, mutations.Config{BaseURL: "http://example.test"})
	mux := NewMux(cfg, authSvc, runner, mutationSvc)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestAPIRequiresBearer(t *testing.T) {
	srv := newMuxAPITestServer(t)
	resp, err := http.Get(srv.URL + "/api/tools")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestRootShowsGitSHA(t *testing.T) {
	t.Setenv("PDW_GIT_SHA", "test-sha")
	runner := fakeRunner{results: map[string]query.RawResult{}}
	authSvc := pdwauth.NewService([]byte(muxAPITestSecret), func() time.Time { return time.Unix(0, 0) })
	cfg := config.Config{Addr: ":0", SecretToken: muxAPITestSecret}
	mux := NewMux(cfg, authSvc, runner)

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	mux.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d", resp.Code)
	}
	body := resp.Body.String()
	if !strings.Contains(body, "Git SHA: test-sha") {
		t.Fatalf("body missing git SHA: %s", body)
	}
}

func TestHealthzReturnsNoContent(t *testing.T) {
	runner := fakeRunner{results: map[string]query.RawResult{}}
	authSvc := pdwauth.NewService([]byte(muxAPITestSecret), func() time.Time { return time.Unix(0, 0) })
	cfg := config.Config{Addr: ":0", SecretToken: muxAPITestSecret}
	mux := NewMux(cfg, authSvc, runner)

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	mux.ServeHTTP(resp, req)

	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d", resp.Code)
	}
	if resp.Body.Len() != 0 {
		t.Fatalf("body = %q", resp.Body.String())
	}
}

func TestAPIListsTools(t *testing.T) {
	srv := newMuxAPITestServer(t)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/api/tools", nil)
	req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
	var payload struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	names := map[string]bool{}
	for _, e := range payload.Data {
		names[e.Name] = true
	}
	for _, required := range []string{"schema_overview", "search", "sql"} {
		if !names[required] {
			t.Fatalf("API tool list missing %q: %#v", required, names)
		}
	}
	for _, hidden := range mcpOnlyToolNames {
		if names[hidden] {
			t.Fatalf("MCP-only tool %q must not be exposed on the API: %#v", hidden, names)
		}
	}
}

func TestAPIRunsSQL(t *testing.T) {
	// Pins the CLI-only sql tool: a single read-only SQL
	// statement goes in, the full result comes back without a query_id
	// (no caching) and without field truncation.
	srv := newMuxAPITestServer(t)
	body := `{"question":"How many rows?","sql":"SELECT 1 AS n","format":"csv"}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/tools/sql", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST sql: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
	var envelope struct {
		Data struct {
			Question    string   `json:"question"`
			SQL         string   `json:"sql"`
			Format      string   `json:"format"`
			ColumnNames []string `json:"column_names"`
			TotalRows   int      `json:"total_rows"`
			Rows        string   `json:"rows"`
			Error       string   `json:"error"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if envelope.Data.Error != "" {
		t.Fatalf("sql error: %s", envelope.Data.Error)
	}
	if envelope.Data.Question != "How many rows?" || envelope.Data.TotalRows != 3 || envelope.Data.Format != "csv" {
		t.Fatalf("unexpected response: %#v", envelope.Data)
	}
	if envelope.Data.Rows != "n\n1\n2\n3" {
		t.Fatalf("rows body = %q", envelope.Data.Rows)
	}
}

// searchCapableRunner extends fakeRunner with the parameterized-query
// capability the search tool needs, recording each statement and its bind
// args. Every parameterized statement returns the same canned hit so tests
// can assert on the response shape without restating the search SQL here.
type searchCapableRunner struct {
	fakeRunner
	mu         sync.Mutex
	statements []string
	args       [][]any
}

func (r *searchCapableRunner) QueryArgs(_ context.Context, statement string, args []any, _ int) (query.RawResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statements = append(r.statements, statement)
	r.args = append(r.args, args)
	return query.RawResult{
		Columns: []string{"source", "who", "text"},
		Rows:    []map[string]any{{"source": "slack", "who": "zach", "text": "offer letter attached"}},
	}, nil
}

func TestAPIRunsSearchAndReportsKeywordFallback(t *testing.T) {
	// Without SEARCH_EMBEDDINGS_* config the default hybrid mode must still
	// answer: keyword retrieval runs and the response says why.
	runner := &searchCapableRunner{}
	authSvc := pdwauth.NewService([]byte(muxAPITestSecret), func() time.Time { return time.Unix(0, 0) })
	cfg := config.Config{
		Addr:          ":0",
		BaseURL:       "http://example.test",
		SecretToken:   muxAPITestSecret,
		MaxRows:       100,
		MaxFieldChars: 1000,
	}
	mux := NewMux(cfg, authSvc, runner)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	body := `{"query":"offer letter","sources":["slack"],"since":"2026-03-01"}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/tools/search", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST search: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
	var envelope struct {
		Data struct {
			Query          string           `json:"query"`
			Mode           string           `json:"mode"`
			FallbackReason string           `json:"fallback_reason"`
			TotalRows      int              `json:"total_rows"`
			Rows           []map[string]any `json:"rows"`
			Error          string           `json:"error"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if envelope.Data.Error != "" {
		t.Fatalf("search error: %s", envelope.Data.Error)
	}
	if envelope.Data.Mode != "keyword" || !strings.Contains(envelope.Data.FallbackReason, "embeddings unconfigured") {
		t.Fatalf("mode = %q fallback_reason = %q", envelope.Data.Mode, envelope.Data.FallbackReason)
	}
	if envelope.Data.TotalRows != 1 || len(envelope.Data.Rows) != 1 || envelope.Data.Rows[0]["text"] != "offer letter attached" {
		t.Fatalf("unexpected rows: %#v", envelope.Data)
	}
	if len(runner.statements) != 1 || !strings.Contains(runner.statements[0], "timeline.search_text(") {
		t.Fatalf("statements = %#v", runner.statements)
	}
	args := runner.args[0]
	if len(args) != 4 || args[0] != "offer letter" || args[3] != "2026-03-01" {
		t.Fatalf("args = %#v", args)
	}
}

func TestAPISearchRequiresQuery(t *testing.T) {
	srv := newMuxAPITestServer(t)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/tools/search", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST search: %v", err)
	}
	defer resp.Body.Close()
	var envelope struct {
		Data struct {
			Error string `json:"error"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(envelope.Data.Error, "query must be") {
		t.Fatalf("error = %q", envelope.Data.Error)
	}
}

func TestAPIRejectsMCPOnlyTools(t *testing.T) {
	srv := newMuxAPITestServer(t)
	for _, name := range mcpOnlyToolNames {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/tools/"+name, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("MCP-only tool %q was reachable from the API: status=%d", name, resp.StatusCode)
		}
	}
}

func TestAPIInputSchemasMatchGolden(t *testing.T) {
	// API tool listings must return the pinned JSON Schema for every API
	// tool whose input schema we snapshot.
	srv := newMuxAPITestServer(t)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/tools", nil)
	req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	var payload struct {
		Data []struct {
			Name        string         `json:"name"`
			InputSchema map[string]any `json:"input_schema"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	apiByName := map[string]map[string]any{}
	for _, e := range payload.Data {
		apiByName[e.Name] = e.InputSchema
	}
	for _, name := range apiSnapshotTools {
		gotAPI, ok := apiByName[name]
		if !ok {
			t.Fatalf("API listing missing %q", name)
		}
		path := filepath.Join("testdata", "schemas", name+".input_schema.json")
		gotJSON, err := json.MarshalIndent(gotAPI, "", "  ")
		if err != nil {
			t.Fatalf("marshal API schema for %q: %v", name, err)
		}
		gotWithNL := append(gotJSON, '\n')
		if *updateSchemaGoldens {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			if err := os.WriteFile(path, gotWithNL, 0o644); err != nil {
				t.Fatalf("write golden for %q: %v", name, err)
			}
			continue
		}
		goldenBytes, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read golden for %q: %v (run with -update to create it)", name, err)
		}
		if string(gotWithNL) != string(goldenBytes) {
			t.Fatalf("API input_schema for %q diverges from golden\n--- want ---\n%s\n--- got ---\n%s", name, goldenBytes, gotWithNL)
		}
	}
}

func TestAPIUnknownToolReturns404(t *testing.T) {
	srv := newMuxAPITestServer(t)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/tools/no_such_tool", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
