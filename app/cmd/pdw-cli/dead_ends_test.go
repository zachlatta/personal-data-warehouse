package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// Each case is a dead end real sessions hit in the week to 2026-10-12: the
// refusal must name the spelling that works, in the same line.
func TestUnknownFlagNamesTheFlagThatWorks(t *testing.T) {
	hit := false
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) { hit = true; _, _ = io.WriteString(w, briefSearchBody) })
	for _, tc := range []struct {
		args []string
		want string
	}{
		// --query: 5 calls in 5 sessions; the query is the positional argument.
		{[]string{"search", "--query", "Howden"}, "pdw search [flags] '<words>'"},
		{[]string{"search", "--table", "base_gmail.messages", "Howden"}, "--source"},
		// A near-miss spelling is answered with the closest real flag.
		{[]string{"search", "Howden", "--priorty", "self"}, "--priority"},
	} {
		hit = false
		_, errOut, code := runCLI(t, srv.URL, "", tc.args...)
		if code != 2 || hit {
			t.Fatalf("%v: code=%d hit=%v, want a refusal before any request", tc.args, code, hit)
		}
		if !strings.Contains(errOut, tc.want) {
			t.Fatalf("%v: stderr must name %q, got: %s", tc.args, tc.want, errOut)
		}
	}
}

func TestToolNamesTypedAsCommandsAreRedirected(t *testing.T) {
	for cmd, want := range map[string]string{
		// `pdw connections`: 4 sessions, the MCP tool's name.
		"connections": "pdw list",
		// `pdw mutation`: 2 sessions wanting the reviewed-write help.
		"mutation": "propose_mutation_help",
	} {
		_, errOut, code := runCLI(t, "http://127.0.0.1:1", "", cmd)
		if code != 2 || !strings.Contains(errOut, want) {
			t.Fatalf("pdw %s: code=%d, want a one-line redirect naming %q, got: %s", cmd, code, want, errOut)
		}
		if strings.Contains(errOut, "USAGE") {
			t.Fatalf("pdw %s: the redirect must not dump the global usage: %s", cmd, errOut)
		}
	}
}

const deadEndToolList = `{"data":[` +
	`{"name":"skills__skill_edit","title":"","description":"","input_schema":{}},` +
	`{"name":"skills__skill_write","title":"","description":"","input_schema":{}},` +
	`{"name":"skills__skill_file","title":"","description":"","input_schema":{}},` +
	`{"name":"tasks__query_tasks_sql","title":"","description":"","input_schema":{}},` +
	`{"name":"tasks__create_task","title":"","description":"","input_schema":{}},` +
	`{"name":"hack_club_data_warehouse__query","title":"","description":"","input_schema":{}},` +
	`{"name":"zrl_bot_email_control__get_thread","title":"","description":"","input_schema":{}},` +
	`{"name":"zrl_bot_email_control__search_messages","title":"","description":"","input_schema":{}},` +
	`{"name":"search","title":"","description":"","input_schema":{}}]}`

// `pdw describe` on an invented name: 20 calls in 18 sessions in the week to
// 2026-10-12, every one answered only "try 'pdw list'" (a 196-row listing).
func TestDescribeUnknownToolNamesItsConnectionsRealTools(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, deadEndToolList) })
	for name, want := range map[string][]string{
		"skills__skill_create":                {"skills__skill_write", "skills__skill_edit"},
		"tasks__query_tasks":                  {"tasks__query_tasks_sql"},
		"hack_club_data_warehouse__run_query": {"hack_club_data_warehouse__query"},
		// A misremembered connection name: 9 calls in the week to 2026-10-19
		// were answered only "`pdw list` lists every tool".
		"zrl-bot__get_thread": {"no zrl-bot connection", "zrl_bot_email_control__get_thread"},
		"hcdw__query":         {"hack_club_data_warehouse"},
		"timeline.context":    {"pdw context '<ref>'"},
		"base_gmail.messages": {"pdw columns"},
		"base_gmail":          {"pdw columns"},
	} {
		_, errOut, code := runCLI(t, srv.URL, "", "describe", name)
		if code == 0 {
			t.Fatalf("describe %s: expected non-zero exit", name)
		}
		for _, w := range want {
			if !strings.Contains(errOut, w) {
				t.Fatalf("describe %s: stderr must name %q, got: %s", name, w, errOut)
			}
		}
	}
}

// get_object with a search hit's ref: 8 sessions in the week to 2026-10-12
// (muse_file:, gmail_email:, drive_file:, photo: refs), each answered only
// with get_object's field list.
func TestGetObjectWithARefSaysHowToReadTheRecord(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected network request") })
	_, errOut, code := runCLI(t, srv.URL, "", "call", "get_object", "--data", `{"ref":"drive_file:zach@example.com|1abc"}`)
	if code != 2 {
		t.Fatalf("code = %d, want 2: %s", code, errOut)
	}
	for _, want := range []string{"storage_file_id", "pdw context", "source_table"} {
		if !strings.Contains(errOut, want) {
			t.Fatalf("stderr must name %q: %s", want, errOut)
		}
	}
}

// The timeline functions and the MCP-only connections tool, called through
// `pdw call`, are answered with the CLI command that does the job.
func TestCallOfTimelineFunctionsAndConnectionsRedirects(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected network request") })
	for tool, want := range map[string]string{
		"timeline__search_text":       "pdw search",
		"timeline__search_text_exact": "pdw search --mode exact",
		"connections":                 "pdw list",
	} {
		_, errOut, code := runCLI(t, srv.URL, "", "call", tool, "--data", `{}`)
		if code != 2 || !strings.Contains(errOut, want) {
			t.Fatalf("call %s: code=%d, want a redirect naming %q: %s", tool, code, want, errOut)
		}
	}
}

// --format was refused with the right spelling from 2026-10-12, and agents
// kept typing it anyway (10 refusals in 9 sessions over 2026-10-10..13): each
// was a failed SQL call that cost a turn. It is now simply --output.
func TestSQLAcceptsFormatAsOutput(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"rows":[{"x":1}]}}`)
	})
	for _, args := range [][]string{
		{"sql", "--format", "json", "-q", "why", "SELECT 1 AS x"},
		{"sql", "-q", "why", "SELECT 1 AS x", "--format=json"},
	} {
		out, errOut, code := runCLI(t, srv.URL, "", args...)
		if code != 0 {
			t.Fatalf("%v: code=%d stderr=%s", args, code, errOut)
		}
		var input struct {
			Format string `json:"format"`
		}
		_ = json.Unmarshal(srv.lastBody, &input)
		if input.Format != "json" || !strings.Contains(out, `"x": 1`) {
			t.Fatalf("%v: format=%q out=%s", args, input.Format, out)
		}
	}
}
