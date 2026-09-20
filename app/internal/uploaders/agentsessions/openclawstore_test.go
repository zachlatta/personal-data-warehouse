package agentsessions

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// OpenClaw 2026.9 moved transcripts out of "<sessionId>.jsonl" files and into
// the agent's SQLite store; the sessions directory now holds only archives.
func newOpenClawStore(t *testing.T, root string) (*sql.DB, Dirs) {
	t.Helper()
	sessions := filepath.Join(root, "agents", "main", "sessions")
	store := filepath.Join(root, "agents", "main", "agent", "openclaw-agent.sqlite")
	mustMkdir(t, sessions)
	mustMkdir(t, filepath.Dir(store))
	db, err := sql.Open("sqlite", store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		`CREATE TABLE transcript_events (session_id TEXT NOT NULL, seq INTEGER NOT NULL, event_json TEXT NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY (session_id, seq))`,
		`CREATE TABLE transcript_rewrite_watermarks (session_id TEXT NOT NULL PRIMARY KEY, generation TEXT NOT NULL, updated_at INTEGER NOT NULL)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	return db, DirsFromEnv(func(name string) string {
		if name == "AGENT_SESSIONS_OPENCLAW_SESSIONS_DIR" {
			return sessions
		}
		if name == "AGENT_SESSIONS_CLAUDE_PROJECTS_DIR" || name == "AGENT_SESSIONS_CODEX_SESSIONS_DIR" || name == "AGENT_SESSIONS_PI_SESSIONS_DIR" {
			return filepath.Join(root, "absent")
		}
		return ""
	})
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := osMkdirAll(path); err != nil {
		t.Fatal(err)
	}
}

func addEvent(t *testing.T, db *sql.DB, session string, seq int, json string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO transcript_events VALUES (?, ?, ?, 0)`, session, seq, json); err != nil {
		t.Fatal(err)
	}
}

func storeRunner(t *testing.T, dirs Dirs, uploads *fakeUploads, state *State) *Runner {
	r := newRunner(t, "", uploads, state)
	r.Dirs = dirs
	return r
}

func TestOpenClawStoreDefaultsBesideTheSessionsDirAndFollowsItsDisable(t *testing.T) {
	_, dirs := newOpenClawStore(t, t.TempDir())
	if filepath.Base(dirs.OpenClawStore) != "openclaw-agent.sqlite" || filepath.Base(filepath.Dir(dirs.OpenClawStore)) != "agent" {
		t.Fatalf("store = %q", dirs.OpenClawStore)
	}
	t.Setenv("AGENT_SESSIONS_OPENCLAW_SESSIONS_DIR", "")
	if got := DirsFromEnv(func(string) string { return "" }); got.OpenClawStore != "" {
		t.Fatalf("a host that disables OpenClaw must not read its store, got %q", got.OpenClawStore)
	}
}

func TestRunnerShipsOpenClawStoreEventsThenOnlyNewOnes(t *testing.T) {
	root := t.TempDir()
	db, dirs := newOpenClawStore(t, root)
	addEvent(t, db, "s1", 0, `{"type":"session","id":"s1"}`)
	addEvent(t, db, "s1", 1, `{"type":"message","id":"m1"}`)
	addEvent(t, db, "s2", 0, `{"type":"session","id":"s2"}`)
	state, err := OpenState(filepath.Join(root, "state.sqlite"), "z@x")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	uploads := &fakeUploads{}
	summary, err := storeRunner(t, dirs, uploads, state).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.LinesSelected != 3 || summary.FilesSeen != 2 || len(uploads.batches) != 1 {
		t.Fatalf("summary = %+v batches = %d", summary, len(uploads.batches))
	}
	record := uploads.batches[0][1]["record"].(map[string]any)
	if uploads.batches[0][1]["record_type"] != "openclaw_event" || record["session_id"] != "s1" || record["seq"].(float64) != 1 ||
		record["line"].(map[string]any)["id"] != "m1" {
		t.Fatalf("record = %v", uploads.batches[0][1])
	}

	addEvent(t, db, "s1", 2, `{"type":"message","id":"m2"}`)
	uploads = &fakeUploads{}
	summary, err = storeRunner(t, dirs, uploads, state).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.LinesSelected != 1 || summary.FilesWithNewLines != 1 {
		t.Fatalf("second run summary = %+v", summary)
	}
}

// A rewrite (compaction, rewind) reuses seq numbers for different events, so
// a seq cursor alone would skip them; the generation is part of the cursor.
func TestRunnerReshipsAnOpenClawSessionWhoseTranscriptWasRewritten(t *testing.T) {
	root := t.TempDir()
	db, dirs := newOpenClawStore(t, root)
	addEvent(t, db, "s1", 0, `{"type":"message","id":"old"}`)
	if _, err := db.Exec(`INSERT INTO transcript_rewrite_watermarks VALUES ('s1', 'g1', 0)`); err != nil {
		t.Fatal(err)
	}
	state, _ := OpenState(filepath.Join(root, "state.sqlite"), "z@x")
	defer state.Close()
	if _, err := storeRunner(t, dirs, &fakeUploads{}, state).Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE transcript_events SET event_json = '{"type":"message","id":"new"}'; UPDATE transcript_rewrite_watermarks SET generation = 'g2'`); err != nil {
		t.Fatal(err)
	}
	uploads := &fakeUploads{}
	summary, err := storeRunner(t, dirs, uploads, state).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.LinesSelected != 1 {
		t.Fatalf("rewritten session was not re-shipped: %+v", summary)
	}
}

func TestRunnerRespectsLimitAcrossOpenClawStoreEvents(t *testing.T) {
	root := t.TempDir()
	db, dirs := newOpenClawStore(t, root)
	for seq := 0; seq < 5; seq++ {
		addEvent(t, db, "s1", seq, `{"type":"message"}`)
	}
	state, _ := OpenState(filepath.Join(root, "state.sqlite"), "z@x")
	defer state.Close()
	runner := storeRunner(t, dirs, &fakeUploads{}, state)
	runner.Limit = 2
	summary, err := runner.Sync()
	if err != nil || summary.LinesSelected != 2 || !summary.LimitReached {
		t.Fatalf("summary = %+v err = %v", summary, err)
	}
	runner = storeRunner(t, dirs, &fakeUploads{}, state)
	summary, _ = runner.Sync()
	if summary.LinesSelected != 3 {
		t.Fatalf("remainder = %+v", summary)
	}
}

func osMkdirAll(path string) error { return os.MkdirAll(path, 0o755) }
