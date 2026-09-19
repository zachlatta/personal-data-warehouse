package plaid

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
)

// --- config ------------------------------------------------------------------

func envOf(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func baseEnv() map[string]string {
	return map[string]string{
		"PLAID_ACCOUNT":         "owner@example.com",
		"PLAID_CLIENT_ID":       "client-id",
		"PLAID_SECRET":          "secret-value",
		"POSTGRES_DATABASE_URL": "postgres://user:pw@localhost/db",
	}
}

func TestLoadConfigDefaultsAndEnvMapping(t *testing.T) {
	cfg, err := LoadConfig(envOf(baseEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Environment != "development" || cfg.EffectiveBaseURL() != "https://development.plaid.com" {
		t.Fatalf("default env: %+v", cfg)
	}
	if strings.Join(cfg.Products, ",") != "transactions,investments,liabilities" || cfg.CountryCodes[0] != "US" {
		t.Fatalf("defaults: %+v", cfg)
	}
	if cfg.TransactionsLookbackDay != 730 || cfg.RequestTimeoutSeconds != 30 || cfg.ClientName != DefaultClientName || cfg.Language != "en" {
		t.Fatalf("defaults: %+v", cfg)
	}
	env := baseEnv()
	env["PLAID_ENV"] = "Production"
	env["PLAID_PRODUCTS"] = "Transactions, liabilities"
	env["PLAID_COUNTRY_CODES"] = "us,ca"
	env["PLAID_BASE_URL"] = "https://plaid.example/"
	cfg, err = LoadConfig(envOf(env))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Environment != "production" || cfg.EffectiveBaseURL() != "https://plaid.example" {
		t.Fatalf("env mapping: %+v", cfg)
	}
	if strings.Join(cfg.Products, ",") != "transactions,liabilities" || strings.Join(cfg.CountryCodes, ",") != "US,CA" {
		t.Fatalf("csv parsing: %+v", cfg)
	}
	delete(env, "PLAID_BASE_URL")
	cfg, _ = LoadConfig(envOf(env))
	if cfg.EffectiveBaseURL() != "https://production.plaid.com" {
		t.Fatalf("production base url: %s", cfg.EffectiveBaseURL())
	}
}

func TestLoadConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]string)
		want   string
	}{
		{"missing creds", func(m map[string]string) { delete(m, "PLAID_CLIENT_ID"); delete(m, "PLAID_SECRET") }, "PLAID_CLIENT_ID, PLAID_SECRET must be set"},
		{"bad env", func(m map[string]string) { m["PLAID_ENV"] = "staging" }, "PLAID_ENV must be one of"},
		{"bad product", func(m map[string]string) { m["PLAID_PRODUCTS"] = "transactions,auth" }, "read-only products"},
		{"lookback too long", func(m map[string]string) { m["PLAID_TRANSACTIONS_LOOKBACK_DAYS"] = "731" }, "at most 730"},
		{"lookback too short", func(m map[string]string) { m["PLAID_TRANSACTIONS_LOOKBACK_DAYS"] = "0" }, "at least 1"},
		{"timeout", func(m map[string]string) { m["PLAID_REQUEST_TIMEOUT_SECONDS"] = "0" }, "at least 1"},
		{"no database", func(m map[string]string) { delete(m, "POSTGRES_DATABASE_URL") }, "POSTGRES_DATABASE_URL must be set"},
	}
	for _, tc := range cases {
		env := baseEnv()
		tc.mutate(env)
		_, err := LoadConfig(envOf(env))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.name, err, tc.want)
		}
	}
}

func TestAccountFallsBackThroughTheSharedAccountChain(t *testing.T) {
	env := baseEnv()
	delete(env, "PLAID_ACCOUNT")
	env["GMAIL_ACCOUNTS"] = "first@example.com,second@example.com"
	cfg, err := LoadConfig(envOf(env))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Account != "first@example.com" {
		t.Fatalf("account = %q", cfg.Account)
	}
}

func TestWithDotenvLayersTheRepoFileUnderTheEnvironment(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("PLAID_SECRET=from-file\nPLAID_ENV='sandbox'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	getenv := WithDotenv(envOf(map[string]string{"PDW_INGEST_PROJECT_DIR": nested, "PLAID_SECRET": "from-env"}))
	if getenv("PLAID_SECRET") != "from-env" {
		t.Fatalf("environment must win: %q", getenv("PLAID_SECRET"))
	}
	if getenv("PLAID_ENV") != "sandbox" {
		t.Fatalf("dotenv fallback: %q", getenv("PLAID_ENV"))
	}
}

// --- client ------------------------------------------------------------------

type recordedRequest struct {
	path string
	body map[string]any
}

func plaidServer(t *testing.T, respond func(path string) (int, string)) (*Client, *[]recordedRequest) {
	t.Helper()
	var requests []recordedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected request shape: %s %s", r.Method, r.Header.Get("Content-Type"))
		}
		raw, _ := io.ReadAll(r.Body)
		body := map[string]any{}
		_ = json.Unmarshal(raw, &body)
		requests = append(requests, recordedRequest{path: r.URL.Path, body: body})
		status, payload := respond(r.URL.Path)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, payload)
	}))
	t.Cleanup(server.Close)
	cfg := Config{
		Account:                 "owner@example.com",
		ClientID:                "client-id",
		Secret:                  "secret-value",
		Environment:             "sandbox",
		Products:                []string{"transactions", "investments", "liabilities"},
		CountryCodes:            []string{"US"},
		ClientName:              "PDW",
		Language:                "en",
		BaseURL:                 server.URL,
		RequestTimeoutSeconds:   5,
		TransactionsLookbackDay: 730,
	}
	return NewClient(cfg, nil), &requests
}

func TestCreateLinkTokenRequestShapes(t *testing.T) {
	client, requests := plaidServer(t, func(string) (int, string) { return 200, `{"link_token":"link-sandbox-1","request_id":"r"}` })
	response, err := client.CreateLinkToken("", "")
	if err != nil {
		t.Fatal(err)
	}
	if response["link_token"] != "link-sandbox-1" {
		t.Fatalf("response: %v", response)
	}
	req := (*requests)[0]
	if req.path != "/link/token/create" {
		t.Fatalf("path %s", req.path)
	}
	body := req.body
	if body["client_id"] != "client-id" || body["secret"] != "secret-value" || body["client_name"] != "PDW" || body["language"] != "en" {
		t.Fatalf("body: %v", body)
	}
	user := body["user"].(map[string]any)
	if user["client_user_id"] != ClientUserID("owner@example.com") || strings.Contains(user["client_user_id"].(string), "@") {
		t.Fatalf("client_user_id must be an opaque hash: %v", user)
	}
	if products := body["products"].([]any); len(products) != 1 || products[0] != "transactions" {
		t.Fatalf("products: %v", body["products"])
	}
	if additional := body["additional_consented_products"].([]any); len(additional) != 2 || additional[0] != "investments" || additional[1] != "liabilities" {
		t.Fatalf("additional_consented_products: %v", body["additional_consented_products"])
	}
	if tx := body["transactions"].(map[string]any); tx["days_requested"] != float64(730) {
		t.Fatalf("transactions: %v", body["transactions"])
	}
	if _, present := body["access_token"]; present {
		t.Fatal("fresh link must not carry an access token")
	}

	_, err = client.CreateLinkToken("other@example.com", "access-existing")
	if err != nil {
		t.Fatal(err)
	}
	body = (*requests)[1].body
	if body["access_token"] != "access-existing" {
		t.Fatalf("update body: %v", body)
	}
	if update := body["update"].(map[string]any); update["account_selection_enabled"] != true {
		t.Fatalf("update body: %v", body)
	}
	for _, key := range []string{"products", "additional_consented_products", "transactions"} {
		if _, present := body[key]; present {
			t.Fatalf("update mode must not initialize products: %s present", key)
		}
	}
	if body["user"].(map[string]any)["client_user_id"] != ClientUserID("other@example.com") {
		t.Fatalf("update mode user: %v", body["user"])
	}
}

func TestSimpleEndpointsAndErrorHandling(t *testing.T) {
	client, requests := plaidServer(t, func(path string) (int, string) {
		switch path {
		case "/item/public_token/exchange":
			return 200, `{"access_token":"access-1","item_id":"item-1"}`
		case "/accounts/get":
			return 200, `{"accounts":[{"account_id":"a"}]}`
		case "/item/remove":
			return 400, `{"error_code":"ITEM_NOT_FOUND","error_message":"The Item you requested cannot be found"}`
		case "/item/get":
			return 200, `{"error_code":"INVALID_ACCESS_TOKEN","display_message":null,"error_message":"bad token"}`
		case "/nonjson":
			return 200, `<html>`
		case "/list":
			return 200, `[1,2]`
		}
		return 500, `oops`
	})
	if _, err := client.ExchangePublicToken("public-1"); err != nil {
		t.Fatal(err)
	}
	if (*requests)[0].body["public_token"] != "public-1" {
		t.Fatalf("exchange body: %v", (*requests)[0].body)
	}
	if _, err := client.AccountsGet("access-1"); err != nil {
		t.Fatal(err)
	}
	if (*requests)[1].body["access_token"] != "access-1" {
		t.Fatalf("accounts/get body: %v", (*requests)[1].body)
	}
	_, err := client.ItemRemove("access-1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Message != "ITEM_NOT_FOUND: The Item you requested cannot be found" {
		t.Fatalf("item/remove error: %v", err)
	}
	if (*requests)[2].path != "/item/remove" || (*requests)[2].body["access_token"] != "access-1" {
		t.Fatalf("item/remove request: %+v", (*requests)[2])
	}
	_, err = client.ItemGet("access-1")
	if !errors.As(err, &apiErr) || apiErr.Message != "INVALID_ACCESS_TOKEN: bad token" {
		t.Fatalf("200 with error_code must fail: %v", err)
	}
	if _, err = client.post("/nonjson", nil); err == nil || !strings.Contains(err.Error(), "non-JSON response") {
		t.Fatalf("non-JSON: %v", err)
	}
	if _, err = client.post("/list", nil); err == nil || !strings.Contains(err.Error(), "unexpected response shape") {
		t.Fatalf("list: %v", err)
	}
	if _, err = client.post("/boom", nil); err == nil || err.Error() != "Plaid HTTP 500" {
		t.Fatalf("http 500: %v", err)
	}
}

func TestErrorCodeAndRedact(t *testing.T) {
	cases := map[string]string{
		"ITEM_NOT_FOUND: gone":      "ITEM_NOT_FOUND",
		"RATE_LIMIT_EXCEEDED: slow": "RATE_LIMIT_EXCEEDED",
		"connection refused":        "",
		"error: lower case":         "",
		": nothing":                 "",
		"404: numbers":              "",
	}
	for message, want := range cases {
		if got := ErrorCode(message); got != want {
			t.Errorf("ErrorCode(%q) = %q, want %q", message, got, want)
		}
	}
	if got := Redact("token access-1 and link-2 and access-1", "access-1", "", "link-2"); got != "token [redacted] and [redacted] and [redacted]" {
		t.Fatalf("Redact: %q", got)
	}
}

// --- resolve -----------------------------------------------------------------

func linkedItem(itemID, institution string) LinkedItem {
	return LinkedItem{Account: "zach@example.com", ItemID: itemID, AccessToken: "access-token-secret", InstitutionID: "ins_1", InstitutionName: institution}
}

func TestResolveItemAcceptsAnUnambiguousPrefix(t *testing.T) {
	items := []LinkedItem{linkedItem("item-oldest", "Example Bank"), linkedItem("item-newer", "Example Bank")}
	if got, _ := ResolveItem(items, "item-oldest"); got.ItemID != "item-oldest" {
		t.Fatalf("exact: %+v", got)
	}
	if got, _ := ResolveItem(items, "item-old"); got.ItemID != "item-oldest" {
		t.Fatalf("prefix: %+v", got)
	}
}

func TestResolveItemRefusesUnknownAmbiguousAndEmpty(t *testing.T) {
	items := []LinkedItem{linkedItem("item-aa", ""), linkedItem("item-ab", "")}
	if _, err := ResolveItem(items, "nope"); err == nil || !strings.Contains(err.Error(), "no linked Plaid item") {
		t.Fatalf("unknown: %v", err)
	}
	_, err := ResolveItem(items, "item-a")
	if err == nil || !strings.Contains(err.Error(), "matches 2 linked Plaid items: item-aa, item-ab") {
		t.Fatalf("ambiguous: %v", err)
	}
	if _, err := ResolveItem(items, "  "); err == nil || err.Error() != "an item id is required" {
		t.Fatalf("empty: %v", err)
	}
}

// --- link server ---------------------------------------------------------------

func startServer(t *testing.T, mode LinkMode) *LinkServer {
	t.Helper()
	server, err := NewLinkServer(mode, "link-token", "PDW", "127.0.0.1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	return server
}

func postJSON(t *testing.T, url string, payload string) (int, map[string]any) {
	t.Helper()
	response, err := http.Post(url, "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	body := map[string]any{}
	_ = json.Unmarshal(raw, &body)
	return response.StatusCode, body
}

type waitOutcome struct {
	result LinkResult
	err    error
}

func waitAsync(server *LinkServer) chan waitOutcome {
	ch := make(chan waitOutcome, 1)
	go func() {
		result, err := server.WaitForResult()
		ch <- waitOutcome{result, err}
	}()
	return ch
}

func awaitOutcome(t *testing.T, ch chan waitOutcome) waitOutcome {
	t.Helper()
	select {
	case outcome := <-ch:
		return outcome
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForResult did not return")
		return waitOutcome{}
	}
}

func TestLinkServerReceivesPublicTokenWithoutQueryLeak(t *testing.T) {
	server := startServer(t, ModeLink)
	if !strings.HasPrefix(server.URL(), "http://127.0.0.1:") || server.Port == 0 {
		t.Fatalf("url %s", server.URL())
	}
	done := waitAsync(server)
	status, body := postJSON(t, server.URL()+"exchange?state="+server.StateToken,
		`{"public_token":"public-token","metadata":{"institution":{"institution_id":"ins_1","name":"Example Bank"}}}`)
	if status != 200 || body["ok"] != true {
		t.Fatalf("response %d %v", status, body)
	}
	outcome := awaitOutcome(t, done)
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if outcome.result != (LinkResult{PublicToken: "public-token", InstitutionID: "ins_1", InstitutionName: "Example Bank"}) {
		t.Fatalf("result %+v", outcome.result)
	}
}

func TestLinkServerErrorTerminatesWithActionableMessage(t *testing.T) {
	server := startServer(t, ModeLink)
	done := waitAsync(server)
	status, body := postJSON(t, server.URL()+"exchange?state="+server.StateToken, `{"error":"institution login was canceled link-token"}`)
	if status != 200 || body["ok"] != false || body["error"] != "institution login was canceled [redacted]" {
		t.Fatalf("response %d %v", status, body)
	}
	outcome := awaitOutcome(t, done)
	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "institution login was canceled") {
		t.Fatalf("err %v", outcome.err)
	}
	if server.Result() != nil {
		t.Fatal("no result expected")
	}
}

func TestLinkServerPagesAndUnknownPaths(t *testing.T) {
	server := startServer(t, ModeLink)
	get := func(path string) (int, string) {
		response, err := http.Get(server.URL() + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(raw)
	}
	status, page := get("")
	if status != 200 || !strings.Contains(page, `token: "link-token"`) || !strings.Contains(page, "PDW Plaid Link") {
		t.Fatalf("page: %d %s", status, page)
	}
	if !strings.Contains(page, server.StateToken) {
		t.Fatal("page must carry the state token")
	}
	if status, body := get("done"); status != 200 || !strings.Contains(body, "Plaid Link complete") {
		t.Fatalf("done: %d %s", status, body)
	}
	if status, _ := get("nope"); status != 404 {
		t.Fatalf("404 expected, got %d", status)
	}
	if status, _ := postJSON(t, server.URL()+"other?state="+server.StateToken, `{}`); status != 404 {
		t.Fatalf("POST other: %d", status)
	}
}

func TestLinkPageResumesOAuthRedirectAndMarksSuccess(t *testing.T) {
	page := LinkPage("link-token", "PDW <x>", "state-token")
	for _, want := range []string{
		"oauth_state_id", "receivedRedirectUri = window.location.href", "window.location.href",
		"Plaid Link exited before an account was linked", "fetch('/exchange?state='", "success: true",
		"PDW &lt;x&gt; Plaid Link", `"state-token"`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("page missing %q", want)
		}
	}
}

func TestUpdateCallbackRequiresExplicitSuccessAndValidState(t *testing.T) {
	cases := []struct {
		payload string
		success bool
	}{
		{`{"success": true, "public_token": ""}`, true},
		{`{"success": true, "public_token": null}`, true},
		{`{"success": false}`, false},
		{`{"error": "canceled"}`, false},
		{`{"success": true, "error": "failed"}`, false},
		{`{}`, false},
	}
	for _, tc := range cases {
		server := startServer(t, ModeUpdate)
		done := waitAsync(server)
		if status, _ := postJSON(t, server.URL()+"exchange?state=wrong", tc.payload); status != 403 {
			t.Fatalf("%s: wrong state got %d", tc.payload, status)
		}
		if server.Result() != nil {
			t.Fatalf("%s: wrong state must record nothing", tc.payload)
		}
		_, body := postJSON(t, server.URL()+"exchange?state="+server.StateToken, tc.payload)
		if body["ok"] != tc.success {
			t.Fatalf("%s: ok=%v want %v", tc.payload, body["ok"], tc.success)
		}
		outcome := awaitOutcome(t, done)
		if (outcome.err == nil) != tc.success {
			t.Fatalf("%s: err=%v", tc.payload, outcome.err)
		}
		if !tc.success && outcome.err.Error() != "Plaid update canceled or failed; existing Item was kept." {
			t.Fatalf("%s: err=%v", tc.payload, outcome.err)
		}
		server.Close()
	}
}

func TestNewLinkCallbackRejectsEmptyPublicToken(t *testing.T) {
	server := startServer(t, ModeLink)
	done := waitAsync(server)
	_, body := postJSON(t, server.URL()+"exchange?state="+server.StateToken, `{"success": true, "public_token": ""}`)
	if body["ok"] != false || body["error"] != "Plaid Link did not return a public token" {
		t.Fatalf("body %v", body)
	}
	if outcome := awaitOutcome(t, done); outcome.err == nil || server.Result() != nil {
		t.Fatalf("expected failure, got %+v", outcome)
	}
}

// --- fakes ---------------------------------------------------------------------

type fakeStore struct {
	items       []LinkedItem
	accounts    []ItemAccount
	counts      map[string]int64
	deleted     [][2]string
	upserted    []LinkedItem
	closed      int
	ensureErr   error
	loadItemErr error
}

func newFakeStore(items ...LinkedItem) *fakeStore {
	return &fakeStore{
		items: items,
		accounts: []ItemAccount{{
			AccountID: "acc-1", Name: "Rewards Card", Mask: "4242", Type: "credit", Subtype: "credit card", CurrentBalance: 100, IsRemoved: 0,
		}},
		counts: map[string]int64{"plaid_accounts": 2, "plaid_transactions": 12, "plaid_items": 1},
	}
}

func (s *fakeStore) EnsurePlaidTables(context.Context) error { return s.ensureErr }
func (s *fakeStore) LoadItemTokens(context.Context) ([]LinkedItem, error) {
	return append([]LinkedItem(nil), s.items...), s.loadItemErr
}
func (s *fakeStore) UpsertItemToken(_ context.Context, item LinkedItem, _ time.Time) error {
	s.upserted = append(s.upserted, item)
	return nil
}
func (s *fakeStore) LoadItemAccounts(context.Context, string, string) ([]ItemAccount, error) {
	return append([]ItemAccount(nil), s.accounts...), nil
}
func (s *fakeStore) CountItemRows(context.Context, string, string) (map[string]int64, error) {
	out := map[string]int64{}
	for k, v := range s.counts {
		out[k] = v
	}
	return out, nil
}
func (s *fakeStore) DeleteItem(_ context.Context, account, itemID string) (map[string]int64, error) {
	s.deleted = append(s.deleted, [2]string{account, itemID})
	return s.CountItemRows(context.Background(), account, itemID)
}
func (s *fakeStore) Close() error { s.closed++; return nil }

type fakeAPI struct {
	linkToken     string
	createErr     error
	accounts      []any
	accountsErr   error
	exchange      map[string]any
	exchangeErr   error
	removeErr     error
	removed       []string
	createCalls   [][2]string
	exchangeCalls []string
	accountsCalls []string
}

func (a *fakeAPI) CreateLinkToken(account, accessToken string) (map[string]any, error) {
	a.createCalls = append(a.createCalls, [2]string{account, accessToken})
	if a.createErr != nil {
		return nil, a.createErr
	}
	return map[string]any{"link_token": a.linkToken}, nil
}
func (a *fakeAPI) ExchangePublicToken(publicToken string) (map[string]any, error) {
	a.exchangeCalls = append(a.exchangeCalls, publicToken)
	return a.exchange, a.exchangeErr
}
func (a *fakeAPI) AccountsGet(accessToken string) (map[string]any, error) {
	a.accountsCalls = append(a.accountsCalls, accessToken)
	if a.accountsErr != nil {
		return nil, a.accountsErr
	}
	return map[string]any{"accounts": a.accounts}, nil
}
func (a *fakeAPI) ItemRemove(accessToken string) (map[string]any, error) {
	a.removed = append(a.removed, accessToken)
	if a.removeErr != nil {
		return nil, a.removeErr
	}
	return map[string]any{"request_id": "req-1"}, nil
}

// --- unlink --------------------------------------------------------------------

func TestUnlinkRevokesAtPlaidThenDeletesTheItemsRows(t *testing.T) {
	store, client := newFakeStore(), &fakeAPI{}
	var out bytes.Buffer
	code, err := UnlinkItem(context.Background(), store, client, linkedItem("item-old", "Example Bank"), func(string) bool { return true }, &out, false, false)
	if err != nil || code != 0 {
		t.Fatalf("code %d err %v", code, err)
	}
	if len(client.removed) != 1 || client.removed[0] != "access-token-secret" {
		t.Fatalf("removed %v", client.removed)
	}
	if len(store.deleted) != 1 || store.deleted[0] != [2]string{"zach@example.com", "item-old"} {
		t.Fatalf("deleted %v", store.deleted)
	}
	printed := out.String()
	for _, want := range []string{"Example Bank", "account 4242 Rewards Card (credit/credit card) balance 100.0", "plaid_transactions=12", "Revoked at Plaid.", "Deleted: plaid_accounts=2 plaid_items=1 plaid_transactions=12"} {
		if !strings.Contains(printed, want) {
			t.Errorf("output missing %q:\n%s", want, printed)
		}
	}
	if strings.Contains(printed, "access-token-secret") {
		t.Fatal("the access token is a credential, never an output")
	}
}

func TestUnlinkDryRunAndDeclinedPromptTouchNothing(t *testing.T) {
	store, client := newFakeStore(), &fakeAPI{}
	var out bytes.Buffer
	code, _ := UnlinkItem(context.Background(), store, client, linkedItem("item-old", ""), func(string) bool { return true }, &out, true, false)
	if code != 0 || len(client.removed) != 0 || len(store.deleted) != 0 || !strings.Contains(strings.ToLower(out.String()), "dry run") {
		t.Fatalf("dry run: code %d out %q", code, out.String())
	}
	if !strings.Contains(out.String(), "(ins_1)") {
		t.Fatalf("institution id fallback: %q", out.String())
	}
	out.Reset()
	code, _ = UnlinkItem(context.Background(), store, client, linkedItem("item-old", ""), func(string) bool { return false }, &out, false, false)
	if code != 1 || len(client.removed) != 0 || len(store.deleted) != 0 || !strings.Contains(out.String(), "Aborted") {
		t.Fatalf("declined: code %d out %q", code, out.String())
	}
}

func TestUnlinkProceedsWhenPlaidHasAlreadyForgottenTheItem(t *testing.T) {
	store := newFakeStore()
	client := &fakeAPI{removeErr: &APIError{Message: "ITEM_NOT_FOUND: The Item you requested cannot be found"}}
	var out bytes.Buffer
	code, _ := UnlinkItem(context.Background(), store, client, linkedItem("item-old", ""), func(string) bool { return true }, &out, false, false)
	if code != 0 || len(store.deleted) != 1 || !strings.Contains(out.String(), "ITEM_NOT_FOUND") {
		t.Fatalf("code %d deleted %v out %q", code, store.deleted, out.String())
	}
}

func TestUnlinkKeepsTheRowsWhenPlaidFailsForAnyOtherReason(t *testing.T) {
	store := newFakeStore()
	client := &fakeAPI{removeErr: &APIError{Message: "RATE_LIMIT_EXCEEDED: too many requests access-token-secret"}}
	var out bytes.Buffer
	code, _ := UnlinkItem(context.Background(), store, client, linkedItem("item-old", ""), func(string) bool { return true }, &out, false, false)
	if code != 1 || len(store.deleted) != 0 {
		t.Fatalf("code %d deleted %v", code, store.deleted)
	}
	if !strings.Contains(out.String(), "Plaid refused to remove the item: RATE_LIMIT_EXCEEDED") || strings.Contains(out.String(), "access-token-secret") {
		t.Fatalf("out %q", out.String())
	}
}

func TestUnlinkCanSkipThePlaidCall(t *testing.T) {
	store, client := newFakeStore(), &fakeAPI{}
	var out bytes.Buffer
	code, _ := UnlinkItem(context.Background(), store, client, linkedItem("item-old", ""), func(string) bool { return true }, &out, false, true)
	if code != 0 || len(client.removed) != 0 || len(store.deleted) != 1 || !strings.Contains(out.String(), "--skip-remote") {
		t.Fatalf("code %d removed %v deleted %v", code, client.removed, store.deleted)
	}
}

func TestConfirmOnStdin(t *testing.T) {
	var out bytes.Buffer
	if !confirmOnStdin(strings.NewReader("Yes\n"), &out)("Really?") {
		t.Fatal("yes must confirm")
	}
	if !strings.Contains(out.String(), "Really? [y/N] ") {
		t.Fatalf("prompt %q", out.String())
	}
	if confirmOnStdin(strings.NewReader(""), &out)("Really?") {
		t.Fatal("EOF must decline")
	}
	if confirmOnStdin(strings.NewReader("no\n"), &out)("Really?") {
		t.Fatal("no must decline")
	}
}

// --- argument parsing and Run ---------------------------------------------------

func TestParseArgs(t *testing.T) {
	opts, err := ParseArgs([]string{"update", "item-ex", "--host", "0.0.0.0", "--port=8765", "--no-browser"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Command != "update" || opts.ItemID != "item-ex" || opts.Host != "0.0.0.0" || opts.Port != 8765 || !opts.NoBrowser {
		t.Fatalf("%+v", opts)
	}
	opts, err = ParseArgs([]string{"unlink", "item-1", "--yes", "--dry-run", "--skip-remote"})
	if err != nil || !opts.Yes || !opts.DryRun || !opts.SkipRemote || opts.ItemID != "item-1" {
		t.Fatalf("%+v %v", opts, err)
	}
	if opts, _ := ParseArgs([]string{"link"}); opts.Host != "127.0.0.1" || opts.Port != 0 {
		t.Fatalf("link defaults %+v", opts)
	}
	for _, bad := range [][]string{
		{"update"}, {"unlink"}, {"bogus"}, {"link", "--yes"}, {"unlink", "a", "--port", "1"},
		{"link", "--port", "x"}, {"items", "extra"}, {"link", "--host"},
	} {
		if _, err := ParseArgs(bad); err == nil {
			t.Errorf("%v should fail to parse", bad)
		}
	}
}

func TestRunUsageErrorsHelpAndSyncRefusal(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"update"}, strings.NewReader(""), &stdout, &stderr, envOf(baseEnv()), ingestclient.Config{})
	if code != 2 || !strings.Contains(stderr.String(), "required: item_id") {
		t.Fatalf("code %d stderr %q", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"--help"}, nil, &stdout, &stderr, envOf(baseEnv()), ingestclient.Config{}); code != 0 || !strings.Contains(stdout.String(), "{link,update,sync,items,unlink}") {
		t.Fatalf("help: %d %q", code, stdout.String())
	}
	stdout.Reset()
	if code := Run([]string{"unlink", "-h"}, nil, &stdout, &stderr, envOf(baseEnv()), ingestclient.Config{}); code != 0 || !strings.Contains(stdout.String(), "--skip-remote") {
		t.Fatalf("unlink help: %d %q", code, stdout.String())
	}
	stderr.Reset()
	if code := Run([]string{"sync"}, nil, &stdout, &stderr, envOf(baseEnv()), ingestclient.Config{}); code != 1 || !strings.Contains(stderr.String(), "plaid_finance_sync") || !strings.Contains(stderr.String(), "30 minutes") {
		t.Fatalf("sync: %d %q", code, stderr.String())
	}
	stderr.Reset()
	if code := Run(nil, nil, &stdout, &stderr, envOf(baseEnv()), ingestclient.Config{}); code != 2 || !strings.Contains(stderr.String(), "plaid_finance_sync") {
		t.Fatalf("bare: %d %q", code, stderr.String())
	}
}

func TestRunReportsMissingConfigurationBeforeTouchingAnything(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runWithDeps(Options{Command: "items"}, nil, &stdout, &stderr, envOf(map[string]string{}), nil, nil)
	if code != 1 || !strings.Contains(stderr.String(), "PLAID_ACCOUNT, PLAID_CLIENT_ID, PLAID_SECRET must be set") {
		t.Fatalf("code %d stderr %q", code, stderr.String())
	}
}

func runCommand(t *testing.T, store *fakeStore, client *fakeAPI, stdin string, args ...string) (int, string, string) {
	t.Helper()
	opts, err := ParseArgs(args)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := runWithDeps(opts, strings.NewReader(stdin), &stdout, &stderr, envOf(baseEnv()),
		func(Config) (Store, error) { return store, nil },
		func(Config) API { return client },
	)
	if store.closed != 1 {
		t.Fatalf("store must be closed exactly once, got %d", store.closed)
	}
	return code, stdout.String(), stderr.String()
}

func TestItemsListsLinkedItemsWithRowCounts(t *testing.T) {
	code, out, _ := runCommand(t, newFakeStore(), &fakeAPI{}, "", "items")
	if code != 0 || !strings.Contains(out, "No linked Plaid items") {
		t.Fatalf("%d %q", code, out)
	}
	code, out, _ = runCommand(t, newFakeStore(linkedItem("item-1", "Example Bank"), linkedItem("item-2", "")), &fakeAPI{}, "", "items")
	if code != 0 {
		t.Fatalf("code %d", code)
	}
	if !strings.Contains(out, "item-1  Example Bank  accounts=2 transactions=12\n") || !strings.Contains(out, "item-2  ins_1  accounts=2") {
		t.Fatalf("out %q", out)
	}
	if strings.Contains(out, "access-token-secret") {
		t.Fatal("token leaked")
	}
}

func TestUnlinkCommandResolvesPrefixesAndPrompts(t *testing.T) {
	store := newFakeStore(linkedItem("item-aa", ""), linkedItem("item-ab", ""))
	code, _, stderr := runCommand(t, store, &fakeAPI{}, "", "unlink", "item-a")
	if code != 2 || !strings.Contains(stderr, "matches 2 linked Plaid items") || !strings.Contains(stderr, "plaid items") {
		t.Fatalf("%d %q", code, stderr)
	}
	store = newFakeStore(linkedItem("item-aa", ""), linkedItem("item-ab", ""))
	code, out, _ := runCommand(t, store, &fakeAPI{}, "n\n", "unlink", "item-aa")
	if code != 1 || len(store.deleted) != 0 || !strings.Contains(out, "[y/N]") {
		t.Fatalf("declined: %d %q", code, out)
	}
	store = newFakeStore(linkedItem("item-aa", ""), linkedItem("item-ab", ""))
	code, out, _ = runCommand(t, store, &fakeAPI{}, "", "unlink", "item-ab", "--yes")
	if code != 0 || len(store.deleted) != 1 || store.deleted[0][1] != "item-ab" || strings.Contains(out, "[y/N]") {
		t.Fatalf("--yes: %d %v %q", code, store.deleted, out)
	}
}

// runLinkFlow runs link/update against the real local server and answers the
// callback with payload once the page is up.
func runLinkFlow(t *testing.T, store *fakeStore, client *fakeAPI, payload string, args ...string) (int, string, string, bool) {
	t.Helper()
	opts, err := ParseArgs(args)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(envOf(baseEnv()))
	if err != nil {
		t.Fatal(err)
	}
	servers := make(chan *LinkServer, 1)
	browserOpened := false
	var stdout, stderr bytes.Buffer
	flow := &linkFlow{
		cfg: cfg, store: store, client: client, stdout: &stdout, stderr: &stderr,
		host: opts.Host, port: opts.Port, noBrowser: opts.NoBrowser,
		update: opts.Command == "update", updateItem: opts.ItemID,
		now:  func() time.Time { return time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC) },
		open: func(string) error { browserOpened = true; return nil },
		newServer: func(mode LinkMode, linkToken, clientName, host string, port int) (*LinkServer, error) {
			server, err := NewLinkServer(mode, linkToken, clientName, host, port)
			if err == nil {
				servers <- server
			}
			return server, err
		},
	}
	codes := make(chan int, 1)
	go func() { codes <- flow.run(context.Background()) }()
	select {
	case server := <-servers:
		if payload != "" {
			select {
			case <-server.Started():
			case <-time.After(5 * time.Second):
				t.Fatal("link server never bound")
			}
			postJSON(t, server.URL()+"exchange?state="+server.StateToken, payload)
		}
	case code := <-codes:
		return code, stdout.String(), stderr.String(), browserOpened
	case <-time.After(5 * time.Second):
		t.Fatal("link server never started")
	}
	select {
	case code := <-codes:
		return code, stdout.String(), stderr.String(), browserOpened
	case <-time.After(5 * time.Second):
		t.Fatal("flow did not finish")
	}
	return 0, "", "", false
}

func TestUpdatePreservesIdentityAndCredentialWithoutSync(t *testing.T) {
	item := linkedItem("item-existing", "Example Bank")
	for _, accounts := range [][]any{{}, {map[string]any{"account_id": "account-existing"}}} {
		for _, noBrowser := range []bool{true, false} {
			store := newFakeStore(item)
			client := &fakeAPI{linkToken: "link-secret-token", accounts: accounts}
			args := []string{"update", "item-ex", "--host", "127.0.0.1", "--port", "0"}
			if noBrowser {
				args = append(args, "--no-browser")
			}
			code, out, errOut, opened := runLinkFlow(t, store, client, `{"success": true}`, args...)
			wantCode := 0
			if len(accounts) == 0 {
				wantCode = 1
			}
			if code != wantCode {
				t.Fatalf("accounts=%v code %d out %q err %q", accounts, code, out, errOut)
			}
			if len(client.createCalls) != 1 || client.createCalls[0] != [2]string{item.Account, item.AccessToken} {
				t.Fatalf("create calls %v", client.createCalls)
			}
			if len(client.accountsCalls) != 1 || client.accountsCalls[0] != item.AccessToken {
				t.Fatalf("accounts calls %v", client.accountsCalls)
			}
			if len(client.exchangeCalls) != 0 || len(client.removed) != 0 || len(store.upserted) != 0 || len(store.deleted) != 0 {
				t.Fatal("update must not exchange, remove, upsert or delete")
			}
			if opened == noBrowser {
				t.Fatalf("browser opened=%v with no-browser=%v", opened, noBrowser)
			}
			text := out + errOut
			if !strings.Contains(text, "Existing Plaid Item item-existing updated") || !strings.Contains(text, "accounts available: "+string(rune('0'+len(accounts)))) {
				t.Fatalf("text %q", text)
			}
			if strings.Contains(text, item.AccessToken) || strings.Contains(text, "link-secret-token") {
				t.Fatalf("credential leaked: %q", text)
			}
			if !strings.Contains(out, "Open this URL to authorize Plaid accounts:\nhttp://127.0.0.1:") {
				t.Fatalf("url line: %q", out)
			}
		}
	}
}

func TestUpdateRefusesUnknownOrAmbiguousItemBeforeLink(t *testing.T) {
	for _, needle := range []string{"missing", "item-", ""} {
		store := newFakeStore(linkedItem("item-existing", ""), linkedItem("item-other", ""))
		client := &fakeAPI{linkToken: "lt"}
		code, _, errOut, _ := runLinkFlow(t, store, client, "", "update", needle)
		if code != 2 || len(client.createCalls) != 0 || !strings.Contains(errOut, "plaid items") {
			t.Fatalf("%q: code %d err %q calls %v", needle, code, errOut, client.createCalls)
		}
	}
}

func TestUpdateFailureNeverReplacesOrDeletesItemOrLeaksTokens(t *testing.T) {
	item := linkedItem("item-existing", "")
	leaky := errors.New("provider echoed access-token-secret link-secret-token")
	for _, stage := range []string{"create_link_token", "callback", "accounts_get"} {
		store := newFakeStore(item)
		client := &fakeAPI{linkToken: "link-secret-token", accounts: []any{"x"}}
		payload := `{"success": true}`
		switch stage {
		case "create_link_token":
			client.createErr = leaky
			payload = ""
		case "callback":
			payload = `{"error": "provider echoed link-secret-token"}`
		case "accounts_get":
			client.accountsErr = leaky
		}
		code, out, errOut, _ := runLinkFlow(t, store, client, payload, "update", item.ItemID, "--no-browser")
		if code != 1 {
			t.Fatalf("%s: code %d", stage, code)
		}
		if len(client.exchangeCalls) != 0 || len(client.removed) != 0 || len(store.upserted) != 0 || len(store.deleted) != 0 {
			t.Fatalf("%s: side effects", stage)
		}
		text := out + errOut
		if strings.Contains(text, "access-token-secret") || strings.Contains(text, "link-secret-token") {
			t.Fatalf("%s: leaked: %q", stage, text)
		}
		if stage == "accounts_get" && !strings.Contains(errOut, "Account availability could not be verified") {
			t.Fatalf("%s: %q", stage, errOut)
		}
		if stage != "accounts_get" && !strings.Contains(errOut, "Plaid Link failed or was canceled; no Item was replaced or deleted.") {
			t.Fatalf("%s: %q", stage, errOut)
		}
	}
}

func TestNewLinkExchangesAndPersistsOnlyTheNewItem(t *testing.T) {
	store := newFakeStore(linkedItem("item-existing", ""))
	client := &fakeAPI{linkToken: "lt", exchange: map[string]any{"item_id": "item-new", "access_token": "access-new"}}
	code, out, errOut, opened := runLinkFlow(t, store, client, `{"public_token":"public-1","metadata":{"institution":{"institution_id":"ins_9","name":"New Bank"}}}`, "link", "--no-browser")
	if code != 0 {
		t.Fatalf("code %d out %q err %q", code, out, errOut)
	}
	if opened {
		t.Fatal("--no-browser must not open a browser")
	}
	if len(client.createCalls) != 1 || client.createCalls[0] != [2]string{"owner@example.com", ""} {
		t.Fatalf("create calls %v", client.createCalls)
	}
	if len(client.exchangeCalls) != 1 || client.exchangeCalls[0] != "public-1" || len(client.accountsCalls) != 0 {
		t.Fatalf("exchange %v accounts %v", client.exchangeCalls, client.accountsCalls)
	}
	if len(store.upserted) != 1 || store.upserted[0] != (LinkedItem{Account: "owner@example.com", ItemID: "item-new", AccessToken: "access-new", InstitutionID: "ins_9", InstitutionName: "New Bank"}) {
		t.Fatalf("upserted %+v", store.upserted)
	}
	if !strings.Contains(out, "Plaid institution linked successfully.") || strings.Contains(out+errOut, "access-new") {
		t.Fatalf("out %q err %q", out, errOut)
	}
}

func TestNewLinkCanceledLeavesNothingBehind(t *testing.T) {
	store := newFakeStore()
	client := &fakeAPI{linkToken: "lt"}
	code, _, errOut, _ := runLinkFlow(t, store, client, `{"error":"institution login was canceled"}`, "link", "--no-browser")
	if code != 1 || len(store.upserted) != 0 || len(client.exchangeCalls) != 0 || !strings.Contains(errOut, "no Item was replaced or deleted") {
		t.Fatalf("code %d err %q", code, errOut)
	}
}
