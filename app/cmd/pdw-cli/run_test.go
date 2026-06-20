package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

func TestSQLCommandDefaultOutputsNoteAndCSV(t *testing.T) {
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

	out, errOut, code := runCLI(t, srv.URL, "", "sql", "-q", "How many rows do we get?", "SELECT 1 AS n")
	if code != 0 {
		t.Fatalf("expected zero exit, got %d (stderr=%s)", code, errOut)
	}
	wantOut := sqlOutputHint + "\n" + "n\n1\n2\n"
	if out != wantOut {
		t.Fatalf("sql CLI output = %q, want %q", out, wantOut)
	}
	if strings.Contains(out, `"rows"`) || strings.Contains(out, `"total_rows"`) {
		t.Fatalf("sql CLI should print row output, not response metadata:\n%s", out)
	}
}

func TestSQLCommandExplicitJSONOmitsDefaultNote(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"question":"What is one?","sql":"SELECT 1 AS n","format":"json","rows":[{"n":1}],"total_rows":1}}`)
	})
	out, errOut, code := runCLI(t, srv.URL, "", "sql", "--output", "json", "-q", "What is one?", "SELECT 1 AS n")
	if code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, errOut)
	}
	if srv.lastPath != "/api/tools/sql" || srv.lastMethod != http.MethodPost {
		t.Fatalf("server got %s %s", srv.lastMethod, srv.lastPath)
	}
	var input map[string]string
	if err := json.Unmarshal(srv.lastBody, &input); err != nil {
		t.Fatalf("request body is not JSON: %v\n%s", err, srv.lastBody)
	}
	if input["sql"] != "SELECT 1 AS n" || input["format"] != "json" || input["question"] != "What is one?" {
		t.Fatalf("request input = %#v", input)
	}
	if strings.Contains(out, sqlOutputHint) {
		t.Fatalf("explicit output should not print default note:\n%s", out)
	}
	if !strings.Contains(out, "[\n  {\n    \"n\": 1\n  }\n]") {
		t.Fatalf("json rows were not pretty-printed:\n%s", out)
	}
}

func TestSQLCommandNDJSONFlagMapsToToolFormat(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"sql":"SELECT 1 AS n","format":"ndjson","rows":"{\"n\":1}\n{\"n\":2}","total_rows":2}}`)
	})
	out, errOut, code := runCLI(t, srv.URL, "", "sql", "--output", "nd-json", "-q", "Which rows come back?", "SELECT 1 AS n")
	if code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, errOut)
	}
	var input map[string]string
	if err := json.Unmarshal(srv.lastBody, &input); err != nil {
		t.Fatalf("request body is not JSON: %v\n%s", err, srv.lastBody)
	}
	if input["format"] != "ndjson" {
		t.Fatalf("format = %q, want ndjson", input["format"])
	}
	if out != "{\"n\":1}\n{\"n\":2}\n" {
		t.Fatalf("nd-json output = %q", out)
	}
}

func TestSQLCommandRejectsInvalidOutputFormat(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("server should not be hit")
	})
	_, errOut, code := runCLI(t, srv.URL, "", "sql", "--output", "table", "SELECT 1")
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if !strings.Contains(errOut, "invalid --output") {
		t.Fatalf("stderr missing invalid output message: %s", errOut)
	}
}

func TestSQLCommandReturnsDomainErrorsOnStderr(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"question":"Delete everything?","sql":"DELETE FROM gmail_messages","format":"csv","error":"query tool is read-only"}}`)
	})
	out, errOut, code := runCLI(t, srv.URL, "", "sql", "-q", "Delete everything?", "DELETE FROM gmail_messages")
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if out != "" {
		t.Fatalf("stdout = %q, want empty", out)
	}
	if !strings.Contains(errOut, "query tool is read-only") {
		t.Fatalf("stderr missing domain error: %s", errOut)
	}
}

// TestSQLCommandSQLOnlyPositionalSucceeds covers the headline ergonomics fix:
// `pdw sql "SELECT 1"` with no question runs, and the server still receives a
// populated intent field (the defaultSQLQuestion marker) so intent logging is
// preserved even when -q is omitted.
func TestSQLCommandSQLOnlyPositionalSucceeds(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"rows":"n\n1"}}`)
	})
	_, errOut, code := runCLI(t, srv.URL, "", "sql", "SELECT 1 AS n")
	if code != 0 {
		t.Fatalf("expected zero exit for SQL-only positional, got %d (stderr=%s)", code, errOut)
	}
	var input map[string]string
	if err := json.Unmarshal(srv.lastBody, &input); err != nil {
		t.Fatalf("body not JSON: %v\n%s", err, srv.lastBody)
	}
	if input["sql"] != "SELECT 1 AS n" {
		t.Fatalf("sql = %q", input["sql"])
	}
	if strings.TrimSpace(input["question"]) == "" {
		t.Fatalf("question must stay populated for server intent logging, got %q", input["question"])
	}
	if input["question"] != defaultSQLQuestion {
		t.Fatalf("omitted question should fall back to the default marker, got %q", input["question"])
	}
}

// TestSQLCommandQuestionFlagReachesServer verifies both -q and --question send
// the caller's intent through to the server unchanged.
func TestSQLCommandQuestionFlagReachesServer(t *testing.T) {
	for _, flagName := range []string{"-q", "--question"} {
		srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"data":{"rows":"n\n1"}}`)
		})
		_, errOut, code := runCLI(t, srv.URL, "", "sql", flagName, "How many?", "SELECT 1 AS n")
		if code != 0 {
			t.Fatalf("%s exit = %d, stderr=%s", flagName, code, errOut)
		}
		var input map[string]string
		if err := json.Unmarshal(srv.lastBody, &input); err != nil {
			t.Fatalf("%s body not JSON: %v\n%s", flagName, err, srv.lastBody)
		}
		if input["question"] != "How many?" || input["sql"] != "SELECT 1 AS n" {
			t.Fatalf("%s input = %#v", flagName, input)
		}
	}
}

// TestSQLCommandTooManyArgsErrorIsOneLineWithExample covers the old
// two-positional footgun: passing question + SQL positionally now fails with a
// single actionable line that points at -q, never the full usage blob.
func TestSQLCommandTooManyArgsErrorIsOneLineWithExample(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("server should not be hit")
	})
	_, errOut, code := runCLI(t, srv.URL, "", "sql", "What is one?", "SELECT 1")
	if code == 0 {
		t.Fatalf("expected non-zero exit for two positional args")
	}
	assertSQLErrorErgonomic(t, errOut)
	if !strings.Contains(errOut, "-q") {
		t.Fatalf("too-many-args error should point at -q: %s", errOut)
	}
	if !strings.Contains(errOut, sqlExample) {
		t.Fatalf("error should embed a copy-pasteable example: %s", errOut)
	}
}

// TestSQLCommandNoSQLErrorIsOneLineWithExample covers an empty invocation (no
// positional, empty stdin): one actionable line with a copy-pasteable example.
func TestSQLCommandNoSQLErrorIsOneLineWithExample(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("server should not be hit")
	})
	_, errOut, code := runCLI(t, srv.URL, "", "sql")
	if code == 0 {
		t.Fatalf("expected non-zero exit when no SQL is provided")
	}
	assertSQLErrorErgonomic(t, errOut)
	if !strings.Contains(errOut, sqlExample) || !strings.Contains(errOut, sqlStdinExample) {
		t.Fatalf("no-SQL error should embed copy-pasteable examples: %s", errOut)
	}
}

// TestSQLCommandFileAndPositionalConflict covers passing SQL both via --file
// and as a positional: a single actionable line, not the usage blob.
func TestSQLCommandFileAndPositionalConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.sql")
	if err := os.WriteFile(path, []byte("SELECT 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("server should not be hit")
	})
	_, errOut, code := runCLI(t, srv.URL, "", "sql", "--file", path, "SELECT 2")
	if code == 0 {
		t.Fatalf("expected non-zero exit when SQL comes from both --file and a positional")
	}
	assertSQLErrorErgonomic(t, errOut)
	if !strings.Contains(errOut, "--file") {
		t.Fatalf("conflict error should mention --file: %s", errOut)
	}
}

// assertSQLErrorErgonomic enforces the contract that sql errors are a single
// actionable line and never dump the full usage blob.
func assertSQLErrorErgonomic(t *testing.T, errOut string) {
	t.Helper()
	if strings.Contains(errOut, "USAGE") || strings.Contains(errOut, "COMMANDS") || strings.Contains(errOut, "EXAMPLES") {
		t.Fatalf("sql error must not dump the full usage blob:\n%s", errOut)
	}
	if lines := strings.Count(strings.TrimRight(errOut, "\n"), "\n"); lines != 0 {
		t.Fatalf("sql error should be a single line, got %d newlines:\n%s", lines, errOut)
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
	out, _, code := runCLI(t, srv.URL, "", "call", "get_rows", "--data", `{"sql":"SELECT 1"}`)
	if code != 0 {
		t.Fatalf("exit code = %d, out=%s", code, out)
	}
	if srv.lastPath != "/api/tools/get_rows" || srv.lastMethod != http.MethodPost {
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
	out, _, code := runCLI(t, srv.URL, `  {"x":1}  `, "call", "get_rows")
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
	_, errOut, code := runCLI(t, srv.URL, "", "call", "get_rows", "--data", `{nope`)
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
	_, errOut, code := runCLI(t, srv.URL, "", "call", "get_rows", "--data", `{}`)
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if !strings.Contains(errOut, "tool_error") || !strings.Contains(errOut, "postgres unreachable") {
		t.Fatalf("stderr missing message: %s", errOut)
	}
}

func TestCallRedirectsSQLToolsToSQLCommand(t *testing.T) {
	for _, name := range []string{"sql", "query"} {
		srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
			t.Fatalf("server should not be hit for call %s", name)
		})
		_, errOut, code := runCLI(t, srv.URL, "", "call", name, "--data", `{"sql":"SELECT 1"}`)
		if code == 0 {
			t.Fatalf("call %s should be rejected, not run", name)
		}
		if !strings.Contains(errOut, "pdw sql") {
			t.Fatalf("call %s should redirect to the sql command, got: %s", name, errOut)
		}
	}
}

func TestColumnsCommandUsesSQLTool(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"rows":"column_name,data_type,is_nullable\nid,text,NO"}}`)
	})
	out, errOut, code := runCLI(t, srv.URL, "", "columns", "gmail_messages")
	if code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, errOut)
	}
	if srv.lastPath != "/api/tools/sql" || srv.lastMethod != http.MethodPost {
		t.Fatalf("server got %s %s", srv.lastMethod, srv.lastPath)
	}
	var input map[string]string
	if err := json.Unmarshal(srv.lastBody, &input); err != nil {
		t.Fatalf("body not JSON: %v\n%s", err, srv.lastBody)
	}
	if !strings.Contains(input["sql"], "information_schema.columns") || !strings.Contains(input["sql"], "'gmail_messages'") {
		t.Fatalf("columns SQL = %q", input["sql"])
	}
	if !strings.Contains(out, "column_name,data_type,is_nullable") || !strings.Contains(out, "id,text,NO") {
		t.Fatalf("columns output:\n%s", out)
	}
}

func TestColumnsCommandRejectsBadIdentifier(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("server should not be hit")
	})
	_, errOut, code := runCLI(t, srv.URL, "", "columns", "gmail; DROP TABLE x")
	if code == 0 {
		t.Fatalf("expected non-zero exit for an injection-y table name")
	}
	if !strings.Contains(errOut, "bare identifier") {
		t.Fatalf("stderr = %s", errOut)
	}
}

func TestSQLCommandReadsSQLFromStdin(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"rows":"n\n1"}}`)
	})
	_, errOut, code := runCLI(t, srv.URL, "SELECT 1 AS n", "sql", "-q", "What is one?")
	if code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, errOut)
	}
	var input map[string]string
	if err := json.Unmarshal(srv.lastBody, &input); err != nil {
		t.Fatalf("body not JSON: %v\n%s", err, srv.lastBody)
	}
	if input["sql"] != "SELECT 1 AS n" || input["question"] != "What is one?" {
		t.Fatalf("input = %#v", input)
	}
}

// TestSQLCommandReadsMultiLineSQLFromStdin confirms piped multi-line SQL works
// without -q, exercising the stdin path agents use to dodge shell quoting.
func TestSQLCommandReadsMultiLineSQLFromStdin(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"rows":"n\n1"}}`)
	})
	multiLine := "SELECT 1 AS n\nFROM (VALUES (1)) AS t(x)\n"
	_, errOut, code := runCLI(t, srv.URL, multiLine, "sql")
	if code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, errOut)
	}
	var input map[string]string
	if err := json.Unmarshal(srv.lastBody, &input); err != nil {
		t.Fatalf("body not JSON: %v\n%s", err, srv.lastBody)
	}
	if input["sql"] != strings.TrimSpace(multiLine) {
		t.Fatalf("sql = %q", input["sql"])
	}
}

func TestSQLCommandReadsSQLFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.sql")
	if err := os.WriteFile(path, []byte("SELECT 1 AS n\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"rows":"n\n1"}}`)
	})
	_, errOut, code := runCLI(t, srv.URL, "", "sql", "--file", path, "-q", "What is one?")
	if code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, errOut)
	}
	var input map[string]string
	if err := json.Unmarshal(srv.lastBody, &input); err != nil {
		t.Fatalf("body not JSON: %v\n%s", err, srv.lastBody)
	}
	if input["sql"] != "SELECT 1 AS n" {
		t.Fatalf("sql = %q", input["sql"])
	}
}

func TestHelpFlagsExitZero(t *testing.T) {
	for _, arg := range []string{"--help", "-h"} {
		var outBuf, errBuf bytes.Buffer
		code := run([]string{arg}, strings.NewReader(""), &outBuf, &errBuf, func(string) string { return "" })
		if code != 0 {
			t.Fatalf("%s exit = %d (stderr=%s)", arg, code, errBuf.String())
		}
		if !strings.Contains(outBuf.String(), "USAGE") {
			t.Fatalf("%s did not print usage to stdout:\n%s", arg, outBuf.String())
		}
	}
}

func TestCallHelpFlagExitsZero(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("server should not be hit")
	})
	out, _, code := runCLI(t, srv.URL, "", "call", "--help")
	if code != 0 {
		t.Fatalf("call --help exit = %d", code)
	}
	if !strings.Contains(out, "USAGE") {
		t.Fatalf("call --help did not print usage:\n%s", out)
	}
}

func TestSubcommandHelpFlagsDoNotRequireConfig(t *testing.T) {
	for _, args := range [][]string{
		{"list", "--help"},
		{"describe", "--help"},
		{"call", "--help"},
		{"sql", "--help"},
		{"columns", "--help"},
		{"schema", "--help"},
	} {
		var outBuf, errBuf bytes.Buffer
		code := run(args, strings.NewReader(""), &outBuf, &errBuf, func(string) string { return "" })
		if code != 0 {
			t.Fatalf("%v exit = %d (stderr=%s)", args, code, errBuf.String())
		}
		if !strings.Contains(outBuf.String(), "USAGE") {
			t.Fatalf("%v did not print usage to stdout:\n%s", args, outBuf.String())
		}
	}
}

func TestCallUnknownToolSuggestsClosest(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/tools" {
			_, _ = io.WriteString(w, `{"data":[{"name":"get_rows","title":"","description":"","input_schema":{}},{"name":"schema_overview","title":"","description":"","input_schema":{}}]}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":{"code":"tool_not_found","message":"no tool named get_row"}}`)
	})
	_, errOut, code := runCLI(t, srv.URL, "", "call", "get_row", "--data", `{}`)
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if !strings.Contains(errOut, "did you mean") || !strings.Contains(errOut, "get_rows") {
		t.Fatalf("stderr should suggest the closest tool: %s", errOut)
	}
}

func TestCallAcceptsDataAliasFlags(t *testing.T) {
	for _, flagName := range []string{"--args", "--input", "--json"} {
		srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"data":{"ok":true}}`)
		})
		_, errOut, code := runCLI(t, srv.URL, "", "call", "get_rows", flagName, `{"x":1}`)
		if code != 0 {
			t.Fatalf("%s exit = %d, stderr=%s", flagName, code, errOut)
		}
		if string(srv.lastBody) != `{"x":1}` {
			t.Fatalf("%s body = %q", flagName, srv.lastBody)
		}
	}
}

func TestCallKeyValueArgsGivesHelpfulError(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("server should not be hit")
	})
	_, errOut, code := runCLI(t, srv.URL, "", "call", "get_rows", "question=hi", "sql=SELECT 1")
	if code == 0 {
		t.Fatalf("expected non-zero exit for key=value args")
	}
	if !strings.Contains(errOut, "--data") {
		t.Fatalf("stderr should steer to --data/JSON: %s", errOut)
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
