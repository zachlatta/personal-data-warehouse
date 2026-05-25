package cliclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
)

const testToken = "test-secret-token-at-least-32-chars-x"

func newServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func newClient(t *testing.T, baseURL string) *cliclient.Client {
	t.Helper()
	c, err := cliclient.New(baseURL, "pdw-cli-test", testToken)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestNewValidatesArguments(t *testing.T) {
	cases := []struct {
		name       string
		baseURL    string
		clientName string
		token      string
		wantErr    string
	}{
		{"missing base url", "", "pdw-cli", testToken, "base url"},
		{"missing client name", "http://x", "", testToken, "client name"},
		{"client name with colon", "http://x", "a:b", testToken, "client name"},
		{"missing token", "http://x", "pdw-cli", "", "token"},
		{"bad scheme", "ftp://x", "pdw-cli", testToken, "scheme"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cliclient.New(tc.baseURL, tc.clientName, tc.token)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestListToolsParsesResponse(t *testing.T) {
	var gotAuth string
	var gotPath string
	srv := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		_, _ = io.WriteString(w, `{"data":[
			{"name":"query","title":"Query","description":"run SQL","input_schema":{"type":"object","properties":{"sql":{"type":"string"}}}},
			{"name":"schema_overview","title":"Schema","description":"overview","input_schema":{"type":"object"}}
		]}`)
	})
	tools, err := newClient(t, srv.URL).ListTools(context.Background())
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if gotPath != "/api/tools" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer pdw-cli-test:"+testToken {
		t.Fatalf("auth header = %q", gotAuth)
	}
	if len(tools) != 2 || tools[0].Name != "query" || tools[1].Name != "schema_overview" {
		t.Fatalf("tools = %#v", tools)
	}
	if tools[0].Title != "Query" || tools[0].Description != "run SQL" {
		t.Fatalf("first tool fields = %#v", tools[0])
	}
	if len(tools[0].InputSchema) == 0 || !strings.Contains(string(tools[0].InputSchema), "properties") {
		t.Fatalf("input_schema not preserved: %s", tools[0].InputSchema)
	}
}

func TestListToolsUnauthorizedReturnsTypedError(t *testing.T) {
	srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, "missing bearer\n")
	})
	_, err := newClient(t, srv.URL).ListTools(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *cliclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want *APIError", err)
	}
	if apiErr.Status != http.StatusUnauthorized || apiErr.Code != "unauthorized" {
		t.Fatalf("apiErr = %+v", apiErr)
	}
}

func TestCallToolPostsBodyAndReturnsData(t *testing.T) {
	var gotBody []byte
	var gotPath string
	var gotContentType string
	srv := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = io.WriteString(w, `{"data":{"results":[{"query_id":"abc","total_rows":1}]}}`)
	})
	out, err := newClient(t, srv.URL).CallTool(context.Background(), "query", json.RawMessage(`{"queries":[]}`))
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if gotPath != "/api/tools/query" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotContentType != "application/json" {
		t.Fatalf("content-type = %q", gotContentType)
	}
	if string(gotBody) != `{"queries":[]}` {
		t.Fatalf("body = %q", gotBody)
	}
	if !strings.Contains(string(out), `"query_id":"abc"`) {
		t.Fatalf("output = %s", out)
	}
}

func TestCallToolEmptyInputSendsEmptyBody(t *testing.T) {
	var gotBody []byte
	srv := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = io.WriteString(w, `{"data":{}}`)
	})
	_, err := newClient(t, srv.URL).CallTool(context.Background(), "schema_overview", nil)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if len(gotBody) != 0 {
		t.Fatalf("body = %q, want empty", gotBody)
	}
}

func TestCallToolRejectsEmptyName(t *testing.T) {
	srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("server should not be called")
	})
	_, err := newClient(t, srv.URL).CallTool(context.Background(), "", nil)
	if err == nil || !strings.Contains(err.Error(), "tool name") {
		t.Fatalf("err = %v", err)
	}
}

func TestCallToolNotFoundReturnsTypedError(t *testing.T) {
	srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":{"code":"tool_not_found","message":"no tool named foo"}}`)
	})
	_, err := newClient(t, srv.URL).CallTool(context.Background(), "foo", nil)
	var apiErr *cliclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotFound || apiErr.Code != "tool_not_found" {
		t.Fatalf("apiErr = %+v", apiErr)
	}
	if !strings.Contains(apiErr.Message, "no tool named foo") {
		t.Fatalf("message = %q", apiErr.Message)
	}
}

func TestCallToolBubblesToolError(t *testing.T) {
	srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, `{"error":{"code":"tool_error","message":"postgres unreachable"}}`)
	})
	_, err := newClient(t, srv.URL).CallTool(context.Background(), "query", json.RawMessage(`{}`))
	var apiErr *cliclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want *APIError", err)
	}
	if apiErr.Code != "tool_error" || !strings.Contains(apiErr.Message, "postgres unreachable") {
		t.Fatalf("apiErr = %+v", apiErr)
	}
}

func TestCallToolFallsBackWhenServerReturnsNonJSONError(t *testing.T) {
	srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "kapow\n")
	})
	_, err := newClient(t, srv.URL).CallTool(context.Background(), "query", json.RawMessage(`{}`))
	var apiErr *cliclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want *APIError", err)
	}
	if apiErr.Status != http.StatusInternalServerError || apiErr.Code != "http_error" {
		t.Fatalf("apiErr = %+v", apiErr)
	}
	if !strings.Contains(apiErr.Message, "kapow") {
		t.Fatalf("message = %q", apiErr.Message)
	}
}
