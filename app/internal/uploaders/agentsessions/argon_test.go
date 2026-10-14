package agentsessions

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// Argon (Google's Antigravity CLI, `agy`) keeps one untruncated append-only
// transcript per session at
// ~/.gemini/antigravity-cli/brain/<session-id>/.system_generated/logs/transcript_full.jsonl,
// beside a truncated transcript.jsonl and per-step chunks/. Per-session
// metadata (workspace_uris, title, parent_conversation_id for subagents) lives
// outside the JSONL lines in conversation_summaries.db and annotations/<id>.pbtxt.
func TestDiscoverArgonFindsFullTranscriptsAndReadsSessionMeta(t *testing.T) {
	root := t.TempDir()
	brain := filepath.Join(root, "brain")
	mainLogs := filepath.Join(brain, "sess-main", ".system_generated", "logs")
	mainChunks := filepath.Join(mainLogs, "chunks")
	subLogs := filepath.Join(brain, "sess-sub", ".system_generated", "logs")
	plainLogs := filepath.Join(brain, "sess-plain", ".system_generated", "logs")
	annotations := filepath.Join(root, "annotations")
	for _, dir := range []string{mainChunks, subLogs, plainLogs, annotations} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, content string) {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(mainLogs, "transcript_full.jsonl"), "{}\n")
	write(filepath.Join(mainLogs, "transcript.jsonl"), "{}\n")
	write(filepath.Join(mainChunks, "0.jsonl"), "{}\n")
	write(filepath.Join(subLogs, "transcript_full.jsonl"), "{}\n")
	write(filepath.Join(plainLogs, "transcript_full.jsonl"), "{}\n")
	write(filepath.Join(annotations, "sess-main.pbtxt"), "title:\"Add Argon Ingester\"\n")

	dbPath := filepath.Join(root, "conversation_summaries.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE conversation_summaries (
			conversation_id TEXT PRIMARY KEY,
			title TEXT,
			workspace_uris TEXT,
			parent_conversation_id TEXT,
			nesting_depth INTEGER
		);
		INSERT INTO conversation_summaries VALUES
			('sess-main', '', '["file:///work/repo"]', '', 0),
			('sess-sub', 'Subtask', '["file:///work/repo"]', 'sess-main', 1);
	`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	files := Discover(Dirs{ArgonSessions: brain})
	if len(files) != 3 {
		t.Fatalf("Discover = %v, want the three transcript_full.jsonl transcripts only", files)
	}
	byID := map[string]SessionFile{}
	for _, f := range files {
		if f.Tool != ArgonTool {
			t.Fatalf("tool = %q, want %q", f.Tool, ArgonTool)
		}
		byID[f.SessionID] = f
	}
	if got := byID["sess-main"].Session["cwd"]; got != "/work/repo" {
		t.Fatalf("main cwd = %v, want /work/repo", got)
	}
	if got := byID["sess-main"].Session["title"]; got != "Add Argon Ingester" {
		t.Fatalf("main title = %v, want fallback from annotations pbtxt", got)
	}
	if got := byID["sess-sub"].Session["parent_session_id"]; got != "sess-main" {
		t.Fatalf("subagent parent_session_id = %v, want sess-main", got)
	}
	if got := byID["sess-sub"].Session["title"]; got != "Subtask" {
		t.Fatalf("subagent title = %v, want Subtask", got)
	}
	if byID["sess-plain"].Session != nil {
		t.Fatalf("plain session carries no session meta, got %v", byID["sess-plain"].Session)
	}
}

func TestDirsFromEnvArgonDefaultsAndOverrides(t *testing.T) {
	env := func(values map[string]string) func(string) string {
		return func(name string) string { return values[name] }
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	wantDefault := filepath.Join(home, ".gemini", "antigravity-cli", "brain")
	if got := DirsFromEnv(env(nil)).ArgonSessions; got != wantDefault {
		t.Fatalf("default = %q, want %q", got, wantDefault)
	}
	custom := DirsFromEnv(env(map[string]string{
		"AGENT_SESSIONS_ARGON_SESSIONS_DIR": "/custom/argon/brain",
	})).ArgonSessions
	if custom != "/custom/argon/brain" {
		t.Fatalf("override = %q", custom)
	}
}
