package main

import (
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

// A bad flag on context named `pdw search --help`, and --full -- which agents
// reach for because search has it -- got no answer at all. The context tool
// returns each event's stored preview (at most 500 characters); the full text
// is one hop away and the error says where.
func TestContextUnknownFlagNamesContextAndFullSaysWhereFullTextIs(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("an argument error must not reach the server")
	})
	_, stderr, code := runCLI(t, srv.URL, "", "context", "gmail_email:a|b", "--bogus")
	if code != 2 || !strings.Contains(stderr, "pdw context --help") || strings.Contains(stderr, "pdw search --help") {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	_, stderr, code = runCLI(t, srv.URL, "", "context", "agent_session_turn:claude_code|s|5", "--full")
	if code != 2 || !strings.Contains(stderr, "marts_ai_conversations.events") || !strings.Contains(stderr, "source_pk") {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
}
