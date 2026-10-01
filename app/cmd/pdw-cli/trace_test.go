package main

import (
	"errors"
	"strings"
	"testing"
)

func TestResolveTraceIDPrefersHarnessEnv(t *testing.T) {
	env := map[string]string{"PDW_TRACE_ID": "harness/one", "SKILLS_TRACE_ID": "older"}
	if got := resolveTraceID(func(k string) string { return env[k] }); got != "harness_one" {
		t.Fatalf("PDW_TRACE_ID: got %q", got)
	}
	delete(env, "PDW_TRACE_ID")
	if got := resolveTraceID(func(k string) string { return env[k] }); got != "older" {
		t.Fatalf("SKILLS_TRACE_ID fallback: got %q", got)
	}
	if got := resolveTraceID(func(k string) string { return map[string]string{"PDW_NO_TRACE": "1"}[k] }); got != "" {
		t.Fatalf("PDW_NO_TRACE: got %q", got)
	}
}

func TestSessionTraceIDSkipsShellsToTheAgent(t *testing.T) {
	tree := map[int]struct {
		ppid    int
		started string
		comm    string
	}{
		500: {400, "Wed Oct  1 10:00:01 2026", "/bin/zsh"},
		400: {300, "Wed Oct  1 10:00:00 2026", "-zsh"},
		300: {1, "Wed Oct  1 09:00:00 2026", "/opt/homebrew/bin/claude"},
	}
	info := func(pid int) (int, string, string, error) {
		p, ok := tree[pid]
		if !ok {
			return 0, "", "", errors.New("gone")
		}
		return p.ppid, p.started, p.comm, nil
	}
	a := sessionTraceID("porygon.local", 500, info)
	if !strings.HasPrefix(a, "porygon-claude-300-") {
		t.Fatalf("trace = %q, want the agent process", a)
	}
	// A second tool call is a different shell under the same agent: same trace.
	tree[600] = struct {
		ppid    int
		started string
		comm    string
	}{300, "Wed Oct  1 10:05:00 2026", "/bin/zsh"}
	if b := sessionTraceID("porygon.local", 600, info); b != a {
		t.Fatalf("second call trace = %q, want %q", b, a)
	}
	if got := sessionTraceID("porygon", 999, info); got != "" {
		t.Fatalf("unreadable tree should give no trace, got %q", got)
	}
}

func TestParsePSLine(t *testing.T) {
	ppid, started, comm, err := parsePSLine("  123 Wed Sep 24 13:41:02 2026 /usr/bin/node\n")
	if err != nil || ppid != 123 || started != "Wed Sep 24 13:41:02 2026" || comm != "/usr/bin/node" {
		t.Fatalf("got %d %q %q %v", ppid, started, comm, err)
	}
}
