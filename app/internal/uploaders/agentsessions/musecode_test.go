package agentsessions

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Muse (the Muse models' coding CLI) keeps one session.jsonl per
// session under <data>/muse/sessions/YYYY/MM/DD/<session-id>/, with child
// sessions nested at <parent>/subagent/<child-id>/session.jsonl. The session
// id is the directory name; a subagent transcript's own lines never name the
// parent session, so discovery reads it from the path.
func TestDiscoverMuseCodeFindsSessionsAndSubagentParents(t *testing.T) {
	root := t.TempDir()
	main := filepath.Join(root, "2026", "10", "13", "sess-main")
	sub := filepath.Join(main, "subagent", "sess-sub")
	for _, dir := range []string{main, sub} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path string) {
		if err := os.WriteFile(path, []byte("{}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(main, "session.jsonl"))
	write(filepath.Join(main, "session.peer-history.jsonl"))
	write(filepath.Join(sub, "session.jsonl"))

	files := Discover(Dirs{MuseCodeSessions: root})
	if len(files) != 2 {
		t.Fatalf("Discover = %v, want the two session.jsonl transcripts only", files)
	}
	byID := map[string]SessionFile{}
	for _, f := range files {
		if f.Tool != MuseCodeTool {
			t.Fatalf("tool = %q, want %q", f.Tool, MuseCodeTool)
		}
		byID[f.SessionID] = f
	}
	if byID["sess-main"].Session != nil {
		t.Fatalf("main session carries no session meta, got %v", byID["sess-main"].Session)
	}
	if got := byID["sess-sub"].Session["parent_session_id"]; got != "sess-main" {
		t.Fatalf("subagent parent_session_id = %v, want sess-main", got)
	}
}

func TestDirsFromEnvMuseCodeDefaultsAndOverrides(t *testing.T) {
	env := func(values map[string]string) func(string) string {
		return func(name string) string { return values[name] }
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	if got := DirsFromEnv(env(nil)).MuseCodeSessions; got != filepath.Join(home, ".local", "share", "muse", "sessions") {
		t.Fatalf("default = %q", got)
	}
	xdg := DirsFromEnv(env(map[string]string{"XDG_DATA_HOME": "/xdg/data"})).MuseCodeSessions
	if xdg != "/xdg/data/muse/sessions" {
		t.Fatalf("XDG default = %q", xdg)
	}
	custom := DirsFromEnv(env(map[string]string{
		"AGENT_SESSIONS_MUSE_CODE_SESSIONS_DIR": "/custom/sessions",
		"XDG_DATA_HOME":                         "/xdg/data",
	})).MuseCodeSessions
	if custom != "/custom/sessions" {
		t.Fatalf("override = %q", custom)
	}
	if MuseCodeTool == MuseTool || strings.Contains(MuseCodeTool, " ") {
		t.Fatalf("muse_code must stay distinct from Meta's muse: %q vs %q", MuseCodeTool, MuseTool)
	}
}
