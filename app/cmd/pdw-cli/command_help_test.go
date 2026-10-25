package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// `pdw context --help` printed the global usage, so the --before/--after/--refs
// flags were undiscoverable from the command itself -- the same gap `pdw search
// --help` had before it got its own help.
func TestCommandsWithTheirOwnHelpPrintIt(t *testing.T) {
	for cmd, want := range map[string]string{
		"search":  "pdw search - hybrid search",
		"context": "pdw context - the conversation around a search hit",
		"token":   "usage: pdw token",
	} {
		out, _, code := runCLI(t, "http://127.0.0.1:1", "", cmd, "--help")
		if code != 0 || !strings.HasPrefix(out, want) {
			t.Fatalf("pdw %s --help: code=%d out=%.120q", cmd, code, out)
		}
	}
}

// A bad flag on context named `pdw search --help`.
func TestContextUnknownFlagNamesContext(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("an argument error must not reach the server")
	})
	_, stderr, code := runCLI(t, srv.URL, "", "context", "gmail_email:a|b", "--bogus")
	if code != 2 || !strings.Contains(stderr, "pdw context --help") || strings.Contains(stderr, "pdw search --help") {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
}

// --full, which agents reach for because search has it, used to be refused
// with directions to a hand-built SQL drill -- and the drill is where they
// invented agent_sessions.turns (14 sessions in the 30 days to 2026-10-13).
// It now asks the server for each event's whole text and prints it in blocks,
// with the drill statement for any event longer than the cap.
func TestContextFullPrintsEachEventsWholeText(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"ref":"agent_session_turn:codex|s|5","before":0,"after":1,"total_rows":2,"rows":[`+
			`{"adapter":"agent_session_turn","event_id":"codex|s|5","priority":"self","occurred_at":"2026-10-01T10:00:00Z","actor":"user","title":"","text":"line one\nline two of the whole prompt","text_chars":29},`+
			`{"adapter":"agent_session_turn","event_id":"codex|s|6","priority":"background","occurred_at":"2026-10-01T10:01:00Z","actor":"assistant","title":"","text":"a long answer","text_chars":20000,"drill_sql":"SELECT * FROM marts_ai_conversations.events WHERE event_uuid = 'u6' AND session_id = 's' AND source = 'codex'"}]}}`)
	})
	out, stderr, code := runCLI(t, srv.URL, "", "context", "agent_session_turn:codex|s|5", "--full", "-b", "0", "-a", "1")
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	var input struct {
		Full bool `json:"full"`
	}
	_ = json.Unmarshal(srv.lastBody, &input)
	if !input.Full {
		t.Fatalf("--full must ask the server for whole texts: %s", srv.lastBody)
	}
	for _, want := range []string{"line one\nline two of the whole prompt", "a long answer", "[truncated at 13 of 20000 characters; the rest: pdw sql \"SELECT * FROM marts_ai_conversations.events WHERE event_uuid = 'u6' AND session_id = 's' AND source = 'codex'\"]"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestGetIsAnsweredWithHowToReadAHit(t *testing.T) {
	for _, cmd := range []string{"get", "get-object", "get_object"} {
		_, stderr, code := runCLI(t, "http://127.0.0.1:1", "", cmd, "gmail_email:a|b")
		if code != 2 || !strings.Contains(stderr, "pdw context") || !strings.Contains(stderr, "--full") || !strings.Contains(stderr, "sql:") || !strings.Contains(stderr, "get_object") {
			t.Fatalf("pdw %s: code=%d stderr=%s", cmd, code, stderr)
		}
	}
}
