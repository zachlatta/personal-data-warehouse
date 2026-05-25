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
	for _, required := range []string{"query", "get_rows", "get_field", "grep_rows", "schema_overview"} {
		if !names[required] {
			t.Fatalf("API tool list missing %q: %#v", required, names)
		}
	}
}

func TestAPIQueryAndGetRowsShareCache(t *testing.T) {
	// Pins PR 4's shared-cache contract: a query_id minted on the API is
	// fetchable from the API (and over MCP, since both surfaces consume the
	// same tool.Registry which is constructed once per process and holds
	// the only query.Service).
	srv := newMuxAPITestServer(t)
	queryBody := `{"queries":[{"question":"how many","sql":"SELECT 1 AS n"}],"preview_rows":1,"format":"csv"}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/tools/query", strings.NewReader(queryBody))
	req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("query status = %d body = %s", resp.StatusCode, body)
	}
	var queryEnvelope struct {
		Data struct {
			Results []struct {
				QueryID string `json:"query_id"`
			} `json:"results"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&queryEnvelope); err != nil {
		t.Fatalf("decode query: %v", err)
	}
	if len(queryEnvelope.Data.Results) != 1 || queryEnvelope.Data.Results[0].QueryID == "" {
		t.Fatalf("missing query_id: %#v", queryEnvelope)
	}
	queryID := queryEnvelope.Data.Results[0].QueryID

	rowsBody := `{"query_id":"` + queryID + `","offset":1,"limit":2}`
	rowsReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/tools/get_rows", strings.NewReader(rowsBody))
	rowsReq.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	rowsReq.Header.Set("Content-Type", "application/json")
	rowsResp, err := http.DefaultClient.Do(rowsReq)
	if err != nil {
		t.Fatalf("POST get_rows: %v", err)
	}
	defer rowsResp.Body.Close()
	if rowsResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(rowsResp.Body)
		t.Fatalf("get_rows status = %d body = %s", rowsResp.StatusCode, body)
	}
	var rowsEnvelope struct {
		Data struct {
			QueryID string `json:"query_id"`
			Error   string `json:"error"`
		} `json:"data"`
	}
	if err := json.NewDecoder(rowsResp.Body).Decode(&rowsEnvelope); err != nil {
		t.Fatalf("decode get_rows: %v", err)
	}
	if rowsEnvelope.Data.Error != "" {
		t.Fatalf("get_rows reported error against the same query_id, cache not shared: %s", rowsEnvelope.Data.Error)
	}
	if rowsEnvelope.Data.QueryID != queryID {
		t.Fatalf("get_rows returned different query_id: got %q want %q", rowsEnvelope.Data.QueryID, queryID)
	}
}

func TestAPIInputSchemasMatchMCPGolden(t *testing.T) {
	// API tool listings must return the exact same JSON Schema as MCP shows
	// for every tool. If the SDK changes how it reflects schemas, or if the
	// tool package's InputSchema() helper diverges from mcp.AddTool's
	// internal derivation, this test fails before a CLI sees the drift.
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
	for _, name := range snapshotTools {
		gotAPI, ok := apiByName[name]
		if !ok {
			t.Fatalf("API listing missing %q", name)
		}
		goldenBytes, err := os.ReadFile(filepath.Join("testdata", "schemas", name+".input_schema.json"))
		if err != nil {
			t.Fatalf("read golden for %q: %v", name, err)
		}
		var golden map[string]any
		if err := json.Unmarshal(goldenBytes, &golden); err != nil {
			t.Fatalf("decode golden %q: %v", name, err)
		}
		gotJSON, _ := json.Marshal(gotAPI)
		wantJSON, _ := json.Marshal(golden)
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("API input_schema for %q diverges from MCP golden\n  api:    %s\n  golden: %s", name, gotJSON, wantJSON)
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
