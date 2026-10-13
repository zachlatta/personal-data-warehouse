package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCallMCPErrorPreservesEnvelopeAndFails(t *testing.T) {
	for _, format := range []string{"json", "text", "structured"} {
		t.Run(format, func(t *testing.T) {
			calls := 0
			srv := newStubServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "POST" {
					// An argument error is answered with the schema inline,
					// which needs one listing call; a real session guessed
					// schema/table/schema_name across seven round trips.
					fmt.Fprint(w, `{"data":[{"name":"remote__list","title":"","description":"","input_schema":{"type":"object","properties":{"schema_name":{"type":"string"}},"required":["schema_name"]}}]}`)
					return
				}
				fmt.Fprint(w, `{"data":{"isError":true,"error":"also a top-level error","structuredContent":{"detail":"failure-detail"},"content":[{"type":"text","text":"Invalid arguments"},{"type":"image","data":"image-data"}]}}`)
			})
			out, diagnostics, code := runCLI(t, srv.URL, "", "call", "remote__list", "--output", format)
			if code != 1 || calls != 2 {
				t.Fatalf("code=%d calls=%d stderr=%s", code, calls, diagnostics)
			}
			if !strings.Contains(diagnostics, "Input schema for remote__list") || !strings.Contains(diagnostics, `"required":["schema_name"]`) {
				t.Fatalf("argument error should carry the input schema inline: %s", diagnostics)
			}
			preserved := out
			if format != "json" {
				preserved = diagnostics
			}
			if !strings.Contains(preserved, `"isError": true`) || !strings.Contains(preserved, "image-data") || !strings.Contains(preserved, "failure-detail") {
				t.Fatalf("lost envelope: %s", preserved)
			}
		})
	}
}

func TestCallMCPErrorWithoutArgumentWordingDoesNotDiscover(t *testing.T) {
	calls := 0
	srv := newStubServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" {
			t.Error("unexpected schema discovery")
		}
		fmt.Fprint(w, `{"data":{"isError":true,"content":[{"type":"text","text":"upstream exploded"}]}}`)
	})
	_, diagnostics, code := runCLI(t, srv.URL, "", "call", "remote__list")
	if code != 1 || calls != 1 || !strings.Contains(diagnostics, "pdw describe remote__list") {
		t.Fatalf("code=%d calls=%d stderr=%s", code, calls, diagnostics)
	}
}

func TestCallOutputFormats(t *testing.T) {
	for _, tc := range []struct {
		name, body, format, want, diagnostic string
		code                                 int
	}{
		{"text", `{"content":[{"type":"text","text":"%s first"},{"type":"text","text":"second"}]}`, "text", "%s first\nsecond\n", "", 0},
		{"mixed", `{"content":[{"type":"text","text":"hello"},{"type":"image","data":"x"}]}`, "text", "hello\n", "non-text", 0},
		{"structured", `{"structuredContent":{"count":2},"content":[]}`, "structured", "{\n  \"count\": 2\n}\n", "", 0},
		{"JSON text", `{"content":[{"type":"text","text":"{\"x\":1}"}]}`, "text", "{\"x\":1}\n", "", 0},
		{"null structured", `{"structuredContent":null,"content":[]}`, "structured", "", "already executed", 1},
		{"no guess", `{"content":[{"type":"text","text":"{\"x\":1}"}]}`, "structured", "", "already executed", 1},
		{"legacy", `{"ok":true}`, "json", "{\n  \"ok\": true\n}\n", "", 0},
		{"false", `{"isError":false,"content":[]}`, "text", "", "", 0},
		{"missing content", `{"ok":true}`, "text", "{\"ok\":true}\n", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) { calls++; fmt.Fprintf(w, `{"data":%s}`, tc.body) })
			out, diagnostics, code := runCLI(t, srv.URL, "", "call", "remote__list", "--output", tc.format)
			if tc.code == 1 && (!strings.Contains(diagnostics, "Raw result follows:") || !(strings.Contains(diagnostics, "content") || strings.Contains(diagnostics, "ok"))) {
				t.Fatalf("raw result missing: %s", diagnostics)
			}
			if code != tc.code || out != tc.want || !strings.Contains(diagnostics, tc.diagnostic) || calls != 1 {
				t.Fatalf("code=%d out=%q stderr=%q calls=%d", code, out, diagnostics, calls)
			}
		})
	}
}

func TestCallRejectsUnsupportedInputBeforeRequest(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--output", "yaml"}, "--output must be"},
		{[]string{"--data", "@" + missing}, "read --data file"},
		{[]string{"--data", "@"}, "--data @ needs a file path"},
		{[]string{"--data", "@" + bad}, "invalid JSON input"},
	} {
		srv := newStubServer(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected network request") })
		_, diagnostic, code := runCLI(t, srv.URL, "", append([]string{"call", "remote__list"}, tc.args...)...)
		if code != 2 || !strings.Contains(diagnostic, tc.want) {
			t.Fatalf("args=%v code=%d stderr=%s", tc.args, code, diagnostic)
		}
	}
}

// curl's `--data @file` / `--data @-` is what agents reach for first; on the
// week to 2026-10-12 the old rejection cost 17 retries. Zach approved
// accepting it on 2026-10-13.
func TestCallReadsDataFromAtFileAndAtStdin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.json")
	if err := os.WriteFile(path, []byte(`{"from":"file"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		args  []string
		stdin string
		want  string
	}{
		{"file", []string{"--data", "@" + path}, "", `{"from":"file"}`},
		{"file equals form", []string{"--data=@" + path}, "", `{"from":"file"}`},
		{"file via alias", []string{"--json", "@" + path}, "", `{"from":"file"}`},
		{"stdin", []string{"--data", "@-"}, `{"from":"stdin"}`, `{"from":"stdin"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, `{"data":{"isError":false,"content":[]}}`)
			})
			_, diagnostic, code := runCLI(t, srv.URL, tc.stdin, append([]string{"call", "remote__list"}, tc.args...)...)
			got := string(srv.lastBody)
			if code != 0 || !strings.Contains(got, tc.want) {
				t.Fatalf("code=%d body=%s stderr=%s", code, got, diagnostic)
			}
		})
	}
}

func TestCallValidationHTTPErrorHintsWithoutDiscoveryOrRetry(t *testing.T) {
	calls := 0
	srv := newStubServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost {
			t.Error("unexpected discovery request")
		}
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"code":"invalid_input","message":"schema_name is required"}}`)
	})
	_, diagnostic, code := runCLI(t, srv.URL, "", "call", "remote__list", "--data", `{}`)
	if code != 1 || calls != 1 || !strings.Contains(diagnostic, "pdw describe remote__list") || !strings.Contains(diagnostic, "schema_name is required") {
		t.Fatalf("code=%d calls=%d stderr=%s", code, calls, diagnostic)
	}
}

func TestCallDefaultJSONPreservesSuccessfulMCPResult(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"data":{"isError":false,"content":[{"type":"image","data":"image-data"}],"structuredContent":{"x":1}}}`)
	})
	out, diagnostic, code := runCLI(t, srv.URL, "", "call", "remote__list")
	if code != 0 || diagnostic != "" || !strings.Contains(out, "image-data") || !strings.Contains(out, "structuredContent") {
		t.Fatalf("code=%d out=%s stderr=%s", code, out, diagnostic)
	}
}

func TestCallOutputFlagBeforeToolName(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"data":{"content":[{"type":"text","text":"hello"}]}}`)
	})
	out, diagnostic, code := runCLI(t, srv.URL, "", "call", "--output", "text", "remote__list")
	if code != 0 || out != "hello\n" {
		t.Fatalf("code=%d out=%q stderr=%s", code, out, diagnostic)
	}
}

func TestCallHTTPFailureDoesNotSuggestSchemaForOtherErrors(t *testing.T) {
	for _, code := range []string{"unauthorized", "upstream_error", "internal_error"} {
		t.Run(code, func(t *testing.T) {
			srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
				fmt.Fprintf(w, `{"error":{"code":%q,"message":"upstream unavailable"}}`, code)
			})
			_, diagnostic, exit := runCLI(t, srv.URL, "", "call", "remote__list")
			if exit != 1 || strings.Contains(diagnostic, "pdw describe") {
				t.Fatalf("code=%d stderr=%s", exit, diagnostic)
			}
		})
	}
}

// An embedded text resource is text. The skills connection's skill_file answers
// with {"type":"resource","resource":{"text":...}}, and --output text used to
// print nothing for it, so agents fell back to JSON and parsed
// content[0].text — which a resource block does not have (KeyError 'text' in
// five sessions in the week to 2026-10-10).
func TestCallOutputTextPrintsEmbeddedTextResources(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"data":{"content":[{"type":"resource","resource":{"uri":"skills://file/x/1/a.md","mimeType":"text/markdown","text":"# A file"}},{"type":"resource","resource":{"uri":"skills://file/x/1/b.bin","blob":"AAAA"}}]}}`)
	})
	out, diagnostic, code := runCLI(t, srv.URL, "", "call", "--output", "text", "remote__list")
	if code != 0 || out != "# A file\n" {
		t.Fatalf("code=%d out=%q stderr=%s", code, out, diagnostic)
	}
	if !strings.Contains(diagnostic, "omitted 1 non-text content block") {
		t.Fatalf("a blob resource is still omitted loudly: stderr=%s", diagnostic)
	}
}
