package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunSendsSourceToServerAndFencesCall(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tools/run" {
			t.Errorf("path %s", r.URL.Path)
		}
		var input struct {
			Code string `json:"code"`
		}
		if e := json.NewDecoder(r.Body).Decode(&input); e != nil {
			t.Error(e)
		}
		if input.Code != "return 42;" {
			t.Errorf("source %q", input.Code)
		}
		called = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"value":42,"operations":[],"sources":[],"run_id":"test"}}`))
	}))
	defer server.Close()
	env := func(k string) string {
		switch k {
		case "PDW_API_URL":
			return server.URL
		case "PDW_SECRET_TOKEN":
			return "test-token"
		case "PDW_NO_AUTO_UPDATE":
			return "1"
		}
		return ""
	}
	var out, err bytes.Buffer
	code := run([]string{"run", "-"}, strings.NewReader("return 42;"), &out, &err, env)
	if code != 0 || !called || !strings.Contains(out.String(), `"value":42`) {
		t.Fatalf("%d %s %s", code, out.String(), err.String())
	}
	called = false
	out.Reset()
	err.Reset()
	code = run([]string{"call", "run"}, strings.NewReader(`{}`), &out, &err, env)
	if code != 2 || called || !strings.Contains(err.String(), "pdw run") {
		t.Fatalf("%d %s", code, err.String())
	}
}
