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
	"schema_overview",
	"query_full_result",
	"propose_mutation",
	"propose_mutation_help",
}

// mcpOnlyToolNames are exposed on MCP but must NOT appear on the HTTP API.
var mcpOnlyToolNames = []string{"query", "get_rows", "get_field", "grep_rows"}

// cliOnlyToolNames are exposed on the HTTP API but must NOT appear on MCP.
var cliOnlyToolNames = []string{"query_full_result"}

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
	for _, required := range []string{"schema_overview", "query_full_result"} {
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

func TestAPIRunsQueryFullResult(t *testing.T) {
	// Pins the CLI-only query_full_result tool: a single read-only SQL
	// statement goes in, the full result comes back without a query_id
	// (no caching) and without field truncation.
	srv := newMuxAPITestServer(t)
	body := `{"sql":"SELECT 1 AS n","format":"csv"}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/tools/query_full_result", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST query_full_result: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
	var envelope struct {
		Data struct {
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
		t.Fatalf("query_full_result error: %s", envelope.Data.Error)
	}
	if envelope.Data.TotalRows != 3 || envelope.Data.Format != "csv" {
		t.Fatalf("unexpected response: %#v", envelope.Data)
	}
	if envelope.Data.Rows != "n\n1\n2\n3" {
		t.Fatalf("rows body = %q", envelope.Data.Rows)
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
