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
	"github.com/zachlatta/personal-data-warehouse/app/internal/push"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
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
	if len(args) != 5 || args[0] != "offer letter" || args[3] != "2026-03-01" {
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

// postAPITool is the shared "call a tool over the HTTP API" helper the
// unknown-field tests use; it returns the status and the raw body so a test can
// assert on the error envelope rather than only on decoded data.
func postAPITool(t *testing.T, runner query.Runner, name, body string) (int, string) {
	t.Helper()
	authSvc := pdwauth.NewService([]byte(muxAPITestSecret), func() time.Time { return time.Unix(0, 0) })
	cfg := config.Config{
		Addr:          ":0",
		BaseURL:       "http://example.test",
		SecretToken:   muxAPITestSecret,
		MaxRows:       100,
		MaxFieldChars: 1000,
	}
	srv := httptest.NewServer(NewMux(cfg, authSvc, runner))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/tools/"+name, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", name, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func TestAPIRejectsUnknownToolFields(t *testing.T) {
	// The worst failure mode in the system: the HTTP path used to json.Unmarshal
	// and drop anything it did not recognize, so {"query":"x","priority":"self"}
	// returned 200 with UNFILTERED results. An agent that guessed the singular
	// field name got a confident, authoritative-looking answer to a question it
	// had never asked, and nothing downstream could tell. The MCP path already
	// validated against the same schema (additionalProperties:false); this is
	// the HTTP surface reaching parity.
	runner := &searchCapableRunner{}
	status, body := postAPITool(t, runner, "search", `{"query":"offer letter","priority":"self"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s, want 400", status, body)
	}
	if !strings.Contains(body, `unknown field \"priority\"`) {
		t.Fatalf("error must name the unknown field; body = %s", body)
	}
	// Naming the alternatives is the point: the caller is a model that guessed,
	// and the correct key is one character away from the one it tried.
	if !strings.Contains(body, "priorities") || !strings.Contains(body, "max_results") {
		t.Fatalf("error must list the valid fields; body = %s", body)
	}
	if len(runner.statements) != 0 {
		t.Fatalf("rejected input must not reach the database; statements = %#v", runner.statements)
	}
}

func TestAPIAcceptsKnownToolFields(t *testing.T) {
	// The guard must not become a wall: the documented fields still work, and
	// omitted optional fields are still omitted rather than rejected.
	runner := &searchCapableRunner{}
	status, body := postAPITool(t, runner, "search",
		`{"query":"offer letter","priorities":["self","direct"],"mode":"keyword"}`)
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %s, want 200", status, body)
	}
	if len(runner.args) != 1 {
		t.Fatalf("args = %#v", runner.args)
	}
	tiers, ok := runner.args[0][4].([]string)
	if !ok || len(tiers) != 2 || tiers[0] != "self" || tiers[1] != "direct" {
		t.Fatalf("priorities must reach the SQL call; args = %#v", runner.args[0])
	}
}

func TestAPIRejectsUnknownPriorityTier(t *testing.T) {
	// Mirrors the `sources` contract: an unknown token errors and names the
	// valid set, rather than being dropped into a search of everything.
	runner := &searchCapableRunner{}
	status, body := postAPITool(t, runner, "search", `{"query":"offer letter","priorities":["urgent"]}`)
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %s", status, body)
	}
	if !strings.Contains(body, `unknown priority \"urgent\"`) {
		t.Fatalf("body = %s", body)
	}
	if !strings.Contains(body, "self, direct, cc, noise, background") {
		t.Fatalf("error must list the valid tiers; body = %s", body)
	}
	// unclassified stays accepted -- scoping to it is how a classification
	// outage is found -- but the error must not present it as a sixth tier.
	if !strings.Contains(body, "sentinel") {
		t.Fatalf("error must mark unclassified as a fail-loud sentinel, not a tier; body = %s", body)
	}
	if len(runner.statements) != 0 {
		t.Fatalf("invalid tier must not reach the database; statements = %#v", runner.statements)
	}
}

func TestPushAndMutationAPIRoutesRequireBearer(t *testing.T) {
	srv := newMuxAPITestServer(t)
	for _, path := range []string{"/api/push/register", "/api/push/test", "/api/mutations/requests"} {
		resp, err := http.Post(srv.URL+path, "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s without a bearer returned %d, want 401", path, resp.StatusCode)
		}
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/push/register", strings.NewReader(`{"expo_push_token":"ExponentPushToken[t]","device_name":"sim","platform":"ios"}`))
	req.Header.Set("Authorization", "Bearer iphone:"+muxAPITestSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("register with a bearer returned %d: %s", resp.StatusCode, body)
	}
	var registered struct {
		Device struct {
			ClientName string `json:"client_name"`
		} `json:"device"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&registered)
	if registered.Device.ClientName != "iphone" {
		t.Fatalf("bearer client name not attributed to the device: %+v", registered)
	}
	list, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/mutations/requests", nil)
	list.Header.Set("Authorization", "Bearer iphone:"+muxAPITestSecret)
	listResp, err := http.DefaultClient.Do(list)
	if err != nil {
		t.Fatal(err)
	}
	listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("mutation list with a bearer returned %d", listResp.StatusCode)
	}
}

func TestMutationNotificationShape(t *testing.T) {
	n := mutationNotification(mutations.Request{ID: "r1", Title: "Send reply", Reason: "asked for it", MutationCount: 2})
	if n.Title != "2 mutations to review: Send reply" || n.Body != "asked for it" {
		t.Fatalf("unexpected notification %+v", n)
	}
	if n.Route != "/mutations/r1" || n.Data["request_id"] != "r1" {
		t.Fatalf("route or request id missing: %+v", n)
	}
	single := mutationNotification(mutations.Request{ID: "r2", Title: "Archive", Mutations: []mutations.Mutation{{}}})
	if single.Title != "Mutation to review: Archive" || single.Body == "" {
		t.Fatalf("single-mutation shape wrong: %+v", single)
	}
}

func TestMutationNotificationIsActionableFromTheLockScreen(t *testing.T) {
	n := mutationNotification(mutations.Request{ID: "r1", Title: "Send reply", Reason: "asked for it", MutationCount: 2})
	if n.Category != push.CategoryMutationReview {
		t.Fatalf("a review alert must carry the mutation_review category so Approve/Deny buttons appear: %+v", n)
	}
	if n.Route != "/mutations/r1" || n.ThreadID == "" || n.InterruptionLevel != push.InterruptionTimeSensitive {
		t.Fatalf("route/thread/interruption not set: %+v", n)
	}
	if err := n.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestNotifyToolIsExposedAndValidates(t *testing.T) {
	srv := newMuxAPITestServer(t)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/tools/notify", strings.NewReader(`{"title":"Hi","category":"nope"}`))
	req.Header.Set("Authorization", "Bearer cli:"+muxAPITestSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "category") {
		t.Fatalf("invalid category should be a 400 naming the field, got %d: %s", resp.StatusCode, body)
	}
	// With no devices registered a valid notification reports zero sent
	// rather than failing: an empty registry is a fact, not an error.
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/api/tools/notify", strings.NewReader(`{"title":"Hi","body":"there","image_url":"https://example.com/a.png","route":"/timeline"}`))
	req.Header.Set("Authorization", "Bearer cli:"+muxAPITestSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"devices":0`) {
		t.Fatalf("valid notify with no devices: %d %s", resp.StatusCode, body)
	}
	// This deployment has no object store, so a storage id is refused
	// with a reason rather than turned into a link nothing would serve.
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/api/tools/notify", strings.NewReader(`{"title":"Hi","image_storage_file_id":"file-123"}`))
	req.Header.Set("Authorization", "Bearer cli:"+muxAPITestSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "object storage") {
		t.Fatalf("storage id without an object store should be a 400 saying so: %d %s", resp.StatusCode, body)
	}
}

func TestNotifyToolSignsAStorageImageLink(t *testing.T) {
	store := push.NewMemoryStore()
	notifier := push.NewNotifier(store, fakePushSender{}, nil, nil)
	signer := pdwauth.NewService([]byte("secret"), func() time.Time { return time.Unix(0, 0) })
	now := func() time.Time { return time.Unix(1_700_000_000, 0) }
	tl := notifyTool(notifier, signer, "https://example.test/", time.Hour, true, now).(*tool.Typed[notifyInput, notifyOutput])
	out, err := tl.Handle(context.Background(), notifyInput{Title: "Hi", ImageStorageFileID: "file-123"})
	if err != nil {
		t.Fatal(err)
	}
	want := "https://example.test/objects/file-123?exp=1700003600&sig="
	if !strings.HasPrefix(out.ImageURL, want) {
		t.Fatalf("image_url = %q, want prefix %q", out.ImageURL, want)
	}
	if _, err := tl.Handle(context.Background(), notifyInput{Title: "Hi", ImageStorageFileID: "f", ImageURL: "https://x/y.png"}); err == nil {
		t.Fatal("both image fields at once must be rejected")
	}
}

type fakePushSender struct{}

func (fakePushSender) Send(_ context.Context, messages []push.Message) ([]push.Ticket, error) {
	tickets := make([]push.Ticket, len(messages))
	for i := range tickets {
		tickets[i].Status = "ok"
	}
	return tickets, nil
}
