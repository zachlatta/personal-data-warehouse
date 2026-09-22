package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

const briefSearchBody = `{"data":{"query":"mini magazine","mode":"hybrid","priority_scope":"all","selected_priorities":[],"returned_priority_counts":{"direct":1},"total_rows":1,"rows":[{"source":"slack","priority":"direct","occurred_at":"2026-09-09T17:55:53.337969Z","who":"pat","title":"","text":"DM with pat pat: Hi Zach. Do you know when the mini-magazines are arriving in the warehouse? Specifically, some US leaders have club fairs happening soon and I would love to send them materials to help recruit members. It is kind of time sensitive since they happen at the beginning of the school year.","ref":"slack_message:zrl|T0266FRGM|D07HLL4GZ4L|1788976553.337969"}]}}`

func TestSearchDefaultsToOneLinePerHit(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, briefSearchBody) })
	out, errOut, code := runCLI(t, srv.URL, "", "search", "mini", "magazine")
	if code != 0 || errOut != "" {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
	var input struct {
		MaxResults int `json:"max_results"`
	}
	_ = json.Unmarshal(srv.lastBody, &input)
	if input.MaxResults != 10 {
		t.Fatalf("default max_results = %d, want 10", input.MaxResults)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var hit, ref string
	for i, line := range lines {
		if strings.HasPrefix(line, "1. ") {
			hit, ref = line, lines[i+1]
		}
	}
	if !strings.HasPrefix(hit, "1. slack · direct · 2026-09-09 17:55 · pat — DM with pat pat: Hi Zach.") {
		t.Fatalf("brief line = %q", hit)
	}
	if len(hit) > 260 {
		t.Fatalf("brief line is %d chars; the preview must be bounded: %q", len(hit), hit)
	}
	if strings.TrimSpace(ref) != "ref: slack_message:zrl|T0266FRGM|D07HLL4GZ4L|1788976553.337969" {
		t.Fatalf("ref line = %q", ref)
	}
	if strings.Contains(out, "school year") {
		t.Fatal("the default output printed the long preview")
	}
}

func TestSearchFullFlagPrintsTheLongPreview(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, briefSearchBody) })
	out, _, code := runCLI(t, srv.URL, "", "search", "--full", "mini", "magazine")
	if code != 0 || !strings.Contains(out, "school year") {
		t.Fatalf("--full should print the long preview: %s", out)
	}
}

func TestSearchRejectsUnknownFlagsAnywhereAndAcceptsLimit(t *testing.T) {
	hit := false
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) { hit = true; _, _ = io.WriteString(w, briefSearchBody) })
	_, errOut, code := runCLI(t, srv.URL, "", "search", "Howden", "--limitt", "15")
	if code != 2 || hit || !strings.Contains(errOut, "unknown flag --limitt") {
		t.Fatalf("a trailing unknown flag must be refused, not searched for: code=%d hit=%v stderr=%s", code, hit, errOut)
	}
	_, errOut, code = runCLI(t, srv.URL, "", "search", "Howden", "--limit", "15")
	if code != 0 {
		t.Fatalf("--limit should alias -n: %s", errOut)
	}
	var input struct {
		Query      string `json:"query"`
		MaxResults int    `json:"max_results"`
	}
	_ = json.Unmarshal(srv.lastBody, &input)
	if input.Query != "Howden" || input.MaxResults != 15 {
		t.Fatalf("input = %#v", input)
	}
}

func TestContextCommandNamesColumnsAndPrintsOneLinePerEvent(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"format":"json","total_rows":2,"rows":[{"adapter":"slack_message","event_id":"zrl|T|C|1.0","priority":"direct","event_ts":"2026-08-13T15:38:16.040079Z","actor":"sam","title":"","snippet":"fallout postcards? magazines and t-shirt?"},{"adapter":"slack_message","event_id":"zrl|T|C|2.0","priority":"direct","event_ts":"2026-08-13T15:41:08Z","actor":"lee","title":"","snippet":"are these the mini magazines perhaps?"}]}}`)
	})
	out, errOut, code := runCLI(t, srv.URL, "", "context", "slack_message:zrl|T|C|1.0", "--before", "2", "-a", "3")
	if code != 0 || errOut != "" {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
	if srv.lastPath != "/api/tools/sql" {
		t.Fatalf("context should run through the sql tool, got %s", srv.lastPath)
	}
	var input map[string]string
	_ = json.Unmarshal(srv.lastBody, &input)
	if input["sql"] != "SELECT adapter, event_id, priority, event_ts, actor, title, snippet FROM timeline.context('slack_message:zrl|T|C|1.0', 2, 3)" {
		t.Fatalf("sql = %q", input["sql"])
	}
	if input["format"] != "json" {
		t.Fatalf("format = %q", input["format"])
	}
	want := "Context: slack_message:zrl|T|C|1.0 (2 before, 3 after) — 2 events\n2026-08-13 15:38 · direct · sam — fallout postcards? magazines and t-shirt?\n2026-08-13 15:41 · direct · lee — are these the mini magazines perhaps?\n"
	if out != want {
		t.Fatalf("out = %q\nwant %q", out, want)
	}
	if _, errOut, code := runCLI(t, srv.URL, "", "context"); code != 2 || !strings.Contains(errOut, "exactly one ref") {
		t.Fatalf("missing ref: code=%d stderr=%s", code, errOut)
	}
	if _, errOut, code := runCLI(t, srv.URL, "", "context", "x'; DROP"); code != 2 || !strings.Contains(errOut, "ref must be") {
		t.Fatalf("quote in ref: code=%d stderr=%s", code, errOut)
	}
}

func TestSQLHintOnlyForCSVAndAfterRows(t *testing.T) {
	body := `{"data":{"sql":"SELECT 1","format":"%s","rows":%s,"total_rows":1,"hint":"(hint: add a priority predicate)"}}`
	var srv *stubServer
	srv = newStubServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(string(srv.lastBody), `"json"`) {
			_, _ = io.WriteString(w, strings.NewReplacer("%s", "json").Replace(`{"data":{"sql":"SELECT 1","format":"json","rows":[{"n":1}],"total_rows":1,"hint":"(hint: add a priority predicate)"}}`))
			return
		}
		_, _ = io.WriteString(w, strings.Replace(strings.Replace(body, "%s", "csv", 1), "%s", `"n\n1"`, 1))
	})
	out, errOut, code := runCLI(t, srv.URL, "", "sql", "--output", "json", "-q", "why", "SELECT 1")
	if code != 0 || errOut != "" {
		t.Fatalf("json output must carry no stderr hint (2>&1 | json.load broke in a real session): code=%d stderr=%q", code, errOut)
	}
	if !strings.HasPrefix(out, "[") {
		t.Fatalf("json stdout = %q", out)
	}
	out, errOut, code = runCLI(t, srv.URL, "", "sql", "--output", "text", "-q", "why", "SELECT 1")
	if code != 0 || out != "n\n1\n" || !strings.Contains(errOut, "priority predicate") {
		t.Fatalf("csv (text alias) should print rows then the hint on stderr: code=%d out=%q stderr=%q", code, out, errOut)
	}
}
