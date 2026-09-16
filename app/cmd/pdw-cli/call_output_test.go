package main

import (
	"fmt"
	"net/http"
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
					t.Error("unexpected schema discovery")
				}
				fmt.Fprint(w, `{"data":{"isError":true,"error":"also a top-level error","structuredContent":{"detail":"failure-detail"},"content":[{"type":"text","text":"Invalid arguments"},{"type":"image","data":"image-data"}]}}`)
			})
			out, diagnostics, code := runCLI(t, srv.URL, "", "call", "remote__list", "--output", format)
			if code != 1 || calls != 1 {
				t.Fatalf("code=%d calls=%d stderr=%s", code, calls, diagnostics)
			}
			if !strings.Contains(diagnostics, "pdw describe remote__list") {
				t.Fatal(diagnostics)
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
		{"missing content", `{"ok":true}`, "text", "", "already executed", 1},
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
	for _, args := range [][]string{{"--data", "@file.json"}, {"--data", "@-"}, {"--output", "yaml"}} {
		srv := newStubServer(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected network request") })
		_, diagnostic, code := runCLI(t, srv.URL, "", append([]string{"call", "remote__list"}, args...)...)
		if code != 2 {
			t.Fatalf("code=%d stderr=%s", code, diagnostic)
		}
		if args[0] == "--data" && !strings.Contains(diagnostic, "pdw call remote__list < input.json") {
			t.Fatal(diagnostic)
		}
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
