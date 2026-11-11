package agentsessions

import (
	"os"
	"path/filepath"
	"testing"
)

const manifestFixtureID = "0b5e8c1a-1111-4a2b-9c3d-000000000001"

// layoutManifestFixture puts testdata/muse_manifest_session.jsonl where the
// VM keeps a transcript, plus a sessions.json and a stray non-session .jsonl.
// The fixture is shared with tests/test_agent_sessions_drive_ingest.py, which
// proves the warehouse keeps exactly as many rows for it as the manifest says.
func layoutManifestFixture(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "muse_manifest_session.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	agents := t.TempDir()
	dir := filepath.Join(agents, "agent-"+manifestFixtureID, "sessions")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestFixtureID+".jsonl"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sessions.json"), []byte(`{"version":1,"sessions":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(agents, "agent-"+manifestFixtureID, "tool-output")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "calls.jsonl"), []byte("{\"a\":1}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return agents
}

func TestMuseManifestCountsTheRowsTheWarehouseKeeps(t *testing.T) {
	agents := layoutManifestFixture(t)
	entries := MuseManifest(agents)
	if len(entries) != 1 {
		t.Fatalf("entries = %+v", entries)
	}
	entry := entries[0]
	// header, #0, #1, #compaction-3, #line-10, #2, #3: the repeated item seq,
	// the repeated compaction, a seq written as "2" or 2.0 and an item with
	// no seq (which keys as #0) each collapse onto one row; the blank, the
	// non-JSON, the null and the array lines are skipped; the trailing
	// partial line waits for its newline.
	if entry.SessionID != manifestFixtureID || entry.AgentID != manifestFixtureID || entry.EventCount != 7 {
		t.Fatalf("entry = %+v", entry)
	}
	if entry.LastTimestamp != "1790624108.5" {
		t.Fatalf("last timestamp = %q", entry.LastTimestamp)
	}
	info, _ := os.Stat(filepath.Join(agents, "agent-"+manifestFixtureID, "sessions", manifestFixtureID+".jsonl"))
	if entry.SizeBytes != info.Size() || entry.ModifiedAt == "" {
		t.Fatalf("stat = %+v", entry)
	}
}

func TestMuseManifestListsExactlyTheSessionsTheUploaderShips(t *testing.T) {
	agents := layoutManifestFixture(t)
	uploads := &fakeUploads{}
	runner := newRunner(t, "", uploads, nil)
	runner.Dirs = Dirs{MuseAgents: agents}
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	shipped := map[string]bool{}
	lines := 0
	for _, batch := range uploads.batches {
		for _, envelope := range batch {
			record := envelope["record"].(map[string]any)
			shipped[record["session_id"].(string)] = true
			lines++
		}
	}
	entries := MuseManifest(agents)
	if len(shipped) != len(entries) || !shipped[entries[0].SessionID] {
		t.Fatalf("shipped %v, manifest %+v", shipped, entries)
	}
	// Eleven complete JSON-object lines leave the VM; the warehouse keys them
	// into the manifest's seven rows.
	if lines != 11 || summary.LinesSelected != 11 {
		t.Fatalf("lines shipped = %d (summary %+v)", lines, summary)
	}
}

func TestMuseManifestIsEmptyWithoutAnAgentsDirectory(t *testing.T) {
	if entries := MuseManifest(filepath.Join(t.TempDir(), "missing")); len(entries) != 0 {
		t.Fatalf("entries = %+v", entries)
	}
	if entries := MuseManifest(""); len(entries) != 0 {
		t.Fatalf("entries = %+v", entries)
	}
}

func TestPythonIntMatchesTheWarehouse(t *testing.T) {
	cases := []struct {
		in   any
		want int64
	}{
		{nil, 0}, {false, 0}, {true, 1}, {float64(7), 7}, {7.9, 7}, {-7.9, -7},
		{"12", 12}, {" 12 ", 12}, {"+3", 3}, {"1.5", 0}, {"x", 0}, {"", 0},
		{map[string]any{}, 0}, {[]any{1.0}, 0},
	}
	for _, c := range cases {
		if got := pythonInt(c.in); got != c.want {
			t.Errorf("pythonInt(%#v) = %d, want %d", c.in, got, c.want)
		}
	}
}
