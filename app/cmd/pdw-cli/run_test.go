package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	pdwserver "github.com/zachlatta/personal-data-warehouse/app/internal/server"
)

const cliTestToken = "test-secret-token-at-least-32-chars-x"

type stubServer struct {
	*httptest.Server
	lastAuth        string
	lastPath        string
	lastMethod      string
	lastBody        []byte
	lastContentType string
}

func newStubServer(t *testing.T, handler func(http.ResponseWriter, *http.Request)) *stubServer {
	t.Helper()
	s := &stubServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.lastAuth = r.Header.Get("Authorization")
		s.lastPath = r.URL.Path
		s.lastMethod = r.Method
		s.lastContentType = r.Header.Get("Content-Type")
		s.lastBody, _ = io.ReadAll(r.Body)
		handler(w, r)
	}))
	t.Cleanup(s.Server.Close)
	return s
}

type cliIntegrationRunner struct{}

func (cliIntegrationRunner) Query(_ context.Context, sql string, _ int) (query.RawResult, error) {
	if sql != "SELECT 1 AS n" {
		return query.RawResult{}, fmt.Errorf("unexpected SQL: %s", sql)
	}
	return query.RawResult{
		Columns: []string{"n"},
		Rows: []map[string]any{
			{"n": int64(1)},
			{"n": int64(2)},
		},
	}, nil
}

func runCLI(t *testing.T, baseURL string, stdin string, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	env := map[string]string{
		"PDW_API_URL":      baseURL,
		"PDW_SECRET_TOKEN": cliTestToken,
		"PDW_CLIENT_NAME":  "pdw-cli-test",
	}
	code = run(args, strings.NewReader(stdin), &outBuf, &errBuf, func(k string) string { return env[k] })
	return outBuf.String(), errBuf.String(), code
}

func TestCallQueryFullResultAgainstRealMux(t *testing.T) {
	authSvc := pdwauth.NewService([]byte(cliTestToken), func() time.Time { return time.Unix(0, 0) })
	handler := pdwserver.NewMux(config.Config{
		Addr:          ":0",
		BaseURL:       "http://example.test",
		SecretToken:   cliTestToken,
		MaxRows:       10,
		MaxFieldChars: 10,
	}, authSvc, cliIntegrationRunner{})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	out, errOut, code := runCLI(t, srv.URL, "", "call", "query_full_result", "--data", `{"sql":"SELECT 1 AS n","format":"csv"}`)
	if code != 0 {
		t.Fatalf("expected zero exit, got %d (stderr=%s)", code, errOut)
	}
	for _, want := range []string{`"sql": "SELECT 1 AS n"`, `"format": "csv"`, `"total_rows": 2`, `"rows": "n\n1\n2"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("query_full_result CLI output missing %q:\n%s", want, out)
		}
	}
}

func TestHelpExitsZero(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("server should not be called")
	})
	out, _, code := runCLI(t, srv.URL, "", "help")
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	for _, want := range []string{"list", "describe", "call", "PDW_API_URL", "PDW_SECRET_TOKEN"} {
		if !strings.Contains(out, want) {
			t.Fatalf("help missing %q in output:\n%s", want, out)
		}
	}
}

func TestNoArgsRunsSchemaOverview(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"results":[{"sql":"SCHEMA","csv":"# default.slack_messages\nuser_id,conversation_id\nU09,C0\n"}]}}`)
	})
	out, errOut, code := runCLI(t, srv.URL, "")
	if code != 0 {
		t.Fatalf("expected zero exit, got %d (stderr=%s)", code, errOut)
	}
	if srv.lastPath != "/api/tools/schema_overview" || srv.lastMethod != http.MethodPost {
		t.Fatalf("expected POST /api/tools/schema_overview, got %s %s", srv.lastMethod, srv.lastPath)
	}
	if !strings.Contains(out, "default.slack_messages") || !strings.Contains(out, "user_id,conversation_id") {
		t.Fatalf("schema CSV not printed:\n%s", out)
	}
}

func TestSchemaCommandRunsSchemaOverview(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"results":[{"sql":"SCHEMA","csv":"# default.gmail_messages\nsubject\nhello\n"}]}}`)
	})
	out, errOut, code := runCLI(t, srv.URL, "", "schema")
	if code != 0 {
		t.Fatalf("expected zero exit, got %d (stderr=%s)", code, errOut)
	}
	if srv.lastPath != "/api/tools/schema_overview" || srv.lastMethod != http.MethodPost {
		t.Fatalf("expected POST /api/tools/schema_overview, got %s %s", srv.lastMethod, srv.lastPath)
	}
	if !strings.Contains(out, "default.gmail_messages") || !strings.Contains(out, "subject") {
		t.Fatalf("schema CSV not printed:\n%s", out)
	}
}

func TestSchemaCommandRejectsArguments(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("server should not be called")
	})
	_, errOut, code := runCLI(t, srv.URL, "", "schema", "extra")
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if !strings.Contains(errOut, "unexpected arguments") {
		t.Fatalf("stderr missing argument error: %s", errOut)
	}
}

func TestNoArgsWithoutConfigReportsMissingCredentials(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	code := run(nil, strings.NewReader(""), &outBuf, &errBuf, func(string) string { return "" })
	if code == 0 {
		t.Fatalf("expected non-zero exit when unconfigured")
	}
	if !strings.Contains(errBuf.String(), "PDW_API_URL") && !strings.Contains(errBuf.String(), "PDW_SECRET_TOKEN") {
		t.Fatalf("stderr should hint at missing credentials: %s", errBuf.String())
	}
}

func TestMissingEnvErrors(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	code := run([]string{"list"}, strings.NewReader(""), &outBuf, &errBuf, func(string) string { return "" })
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if !strings.Contains(errBuf.String(), "PDW_API_URL") && !strings.Contains(errBuf.String(), "PDW_SECRET_TOKEN") {
		t.Fatalf("stderr missing env hint: %s", errBuf.String())
	}
}

func TestListRendersToolTable(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[
			{"name":"query","title":"Query","description":"run read-only SQL against the warehouse","input_schema":{"type":"object"}},
			{"name":"schema_overview","title":"Schema Overview","description":"sample every table","input_schema":{"type":"object"}}
		]}`)
	})
	out, _, code := runCLI(t, srv.URL, "", "list")
	if code != 0 {
		t.Fatalf("exit code = %d, stdout=%s", code, out)
	}
	if !strings.Contains(out, "query") || !strings.Contains(out, "schema_overview") {
		t.Fatalf("listing missing tool names:\n%s", out)
	}
	if !strings.Contains(out, "run read-only SQL") {
		t.Fatalf("listing missing description:\n%s", out)
	}
	if srv.lastPath != "/api/tools" || srv.lastMethod != http.MethodGet {
		t.Fatalf("server got %s %s", srv.lastMethod, srv.lastPath)
	}
	wantAuth := "Bearer pdw-cli-test:" + cliTestToken
	if srv.lastAuth != wantAuth {
		t.Fatalf("auth = %q want %q", srv.lastAuth, wantAuth)
	}
}

func TestListJSONFlagEmitsRawArray(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"name":"query","title":"","description":"","input_schema":{"type":"object"}}]}`)
	})
	out, _, code := runCLI(t, srv.URL, "", "list", "--json")
	if code != 0 {
		t.Fatalf("exit code = %d, stdout=%s", code, out)
	}
	var arr []map[string]any
	if err := json.Unmarshal([]byte(out), &arr); err != nil {
		t.Fatalf("not JSON array: %v\n%s", err, out)
	}
	if len(arr) != 1 || arr[0]["name"] != "query" {
		t.Fatalf("unexpected json: %#v", arr)
	}
}

func TestDescribePrintsSchema(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[
			{"name":"query","title":"Query","description":"run SQL","input_schema":{"type":"object","properties":{"sql":{"type":"string"}}}},
			{"name":"schema_overview","title":"Schema","description":"overview","input_schema":{"type":"object"}}
		]}`)
	})
	out, _, code := runCLI(t, srv.URL, "", "describe", "query")
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	for _, want := range []string{"query", "Query", "run SQL", "properties", "sql"} {
		if !strings.Contains(out, want) {
			t.Fatalf("describe output missing %q:\n%s", want, out)
		}
	}
}

func TestDescribeUnknownToolExitsNonZero(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"name":"query","title":"","description":"","input_schema":{}}]}`)
	})
	_, errOut, code := runCLI(t, srv.URL, "", "describe", "missing")
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if !strings.Contains(errOut, "missing") {
		t.Fatalf("stderr missing tool name: %s", errOut)
	}
}

func TestCallPassesDataFlagAsBody(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"data":{"echo":%s}}`, `"hi"`)
	})
	out, _, code := runCLI(t, srv.URL, "", "call", "query", "--data", `{"sql":"SELECT 1"}`)
	if code != 0 {
		t.Fatalf("exit code = %d, out=%s", code, out)
	}
	if srv.lastPath != "/api/tools/query" || srv.lastMethod != http.MethodPost {
		t.Fatalf("server got %s %s", srv.lastMethod, srv.lastPath)
	}
	if string(srv.lastBody) != `{"sql":"SELECT 1"}` {
		t.Fatalf("body = %q", srv.lastBody)
	}
	if srv.lastContentType != "application/json" {
		t.Fatalf("content-type = %q", srv.lastContentType)
	}
	if !strings.Contains(out, `"echo": "hi"`) {
		t.Fatalf("output not pretty-printed JSON:\n%s", out)
	}
}

func TestCallReadsStdinWhenNoDataFlag(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"ok":true}}`)
	})
	out, _, code := runCLI(t, srv.URL, `  {"x":1}  `, "call", "query")
	if code != 0 {
		t.Fatalf("exit code = %d, out=%s", code, out)
	}
	if string(srv.lastBody) != `{"x":1}` {
		t.Fatalf("body = %q", srv.lastBody)
	}
}

func TestCallRejectsInvalidJSONInput(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("server should not be hit")
	})
	_, errOut, code := runCLI(t, srv.URL, "", "call", "query", "--data", `{nope`)
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if !strings.Contains(errOut, "invalid") {
		t.Fatalf("stderr missing reason: %s", errOut)
	}
}

func TestCallRequiresToolName(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {})
	_, errOut, code := runCLI(t, srv.URL, "", "call")
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if !strings.Contains(errOut, "tool name") {
		t.Fatalf("stderr missing reason: %s", errOut)
	}
}

func TestCallServerErrorReturnsNonZeroAndStructuredMessage(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, `{"error":{"code":"tool_error","message":"postgres unreachable"}}`)
	})
	_, errOut, code := runCLI(t, srv.URL, "", "call", "query", "--data", `{}`)
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if !strings.Contains(errOut, "tool_error") || !strings.Contains(errOut, "postgres unreachable") {
		t.Fatalf("stderr missing message: %s", errOut)
	}
}

func TestFlagsOverrideEnv(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[]}`)
	})
	var outBuf, errBuf bytes.Buffer
	code := run(
		[]string{"--base-url", srv.URL, "--token", cliTestToken, "--client", "override", "list"},
		strings.NewReader(""), &outBuf, &errBuf, func(string) string { return "" },
	)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, errBuf.String())
	}
	if srv.lastAuth != "Bearer override:"+cliTestToken {
		t.Fatalf("auth = %q", srv.lastAuth)
	}
}
