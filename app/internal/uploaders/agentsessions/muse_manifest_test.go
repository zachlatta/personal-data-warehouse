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
	entries, _ := MuseManifest(agents, nil)
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
	entries, _ := MuseManifest(agents, nil)
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
	if entries, _ := MuseManifest(filepath.Join(t.TempDir(), "missing"), nil); len(entries) != 0 {
		t.Fatalf("entries = %+v", entries)
	}
	if entries, _ := MuseManifest("", nil); len(entries) != 0 {
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

type memoryManifestCache struct {
	entries map[string]struct {
		size, mtime int64
		counts      MuseManifestCounts
	}
}

func (c *memoryManifestCache) Lookup(path string, size, mtime int64) (MuseManifestCounts, bool) {
	e, ok := c.entries[path]
	if !ok || e.size != size || e.mtime != mtime {
		return MuseManifestCounts{}, false
	}
	return e.counts, true
}

func (c *memoryManifestCache) Store(path string, size, mtime int64, counts MuseManifestCounts) error {
	c.entries[path] = struct {
		size, mtime int64
		counts      MuseManifestCounts
	}{size, mtime, counts}
	return nil
}

func (c *memoryManifestCache) Retain(keep map[string]bool) error {
	for path := range c.entries {
		if !keep[path] {
			delete(c.entries, path)
		}
	}
	return nil
}

func TestMuseManifestReadsOnlyTranscriptsThatChanged(t *testing.T) {
	agents := layoutManifestFixture(t)
	other := "0b5e8c1a-1111-4a2b-9c3d-000000000002"
	otherPath := filepath.Join(agents, "agent-"+other, "sessions", other+".jsonl")
	os.MkdirAll(filepath.Dir(otherPath), 0o755)
	os.WriteFile(otherPath, []byte(`{"type":"session_header"}`+"\n"), 0o644)

	reads := map[string]int{}
	readMuseTranscript = func(path string) ([]byte, error) {
		reads[filepath.Base(path)]++
		return os.ReadFile(path)
	}
	t.Cleanup(func() { readMuseTranscript = os.ReadFile })
	cache := &memoryManifestCache{entries: map[string]struct {
		size, mtime int64
		counts      MuseManifestCounts
	}{}}

	first, err := MuseManifest(agents, cache)
	if err != nil || len(first) != 2 || first[0].EventCount != 7 || first[1].EventCount != 1 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	second, err := MuseManifest(agents, cache)
	if err != nil || len(reads) != 2 || reads[other+".jsonl"] != 1 || reads[manifestFixtureID+".jsonl"] != 1 {
		t.Fatalf("an unchanged run re-read transcripts: %v (%v)", reads, err)
	}
	if second[0] != first[0] || second[1] != first[1] {
		t.Fatalf("cached entries differ: %+v vs %+v", second, first)
	}

	// Appending a line re-reads that file alone and counts it.
	handle, _ := os.OpenFile(otherPath, os.O_APPEND|os.O_WRONLY, 0)
	handle.WriteString(`{"type":"item","seq":0}` + "\n")
	handle.Close()
	third, _ := MuseManifest(agents, cache)
	if reads[other+".jsonl"] != 2 || reads[manifestFixtureID+".jsonl"] != 1 || third[1].EventCount != 2 {
		t.Fatalf("reads=%v third=%+v", reads, third)
	}

	// A transcript that is gone leaves the cache.
	os.Remove(otherPath)
	if _, err := MuseManifest(agents, cache); err != nil || len(cache.entries) != 1 {
		t.Fatalf("cache = %v (%v)", cache.entries, err)
	}
}

func TestMuseManifestCountsASessionSplitAcrossFilesOverItsUnion(t *testing.T) {
	agents := t.TempDir()
	id := "0b5e8c1a-1111-4a2b-9c3d-000000000003"
	for i, agent := range []string{"a", "b"} {
		path := filepath.Join(agents, "agent-"+agent, "sessions", id+".jsonl")
		os.MkdirAll(filepath.Dir(path), 0o755)
		body := `{"type":"session_header"}` + "\n" + `{"type":"item","seq":0}` + "\n"
		if i == 1 {
			body += `{"type":"item","seq":1}` + "\n"
		}
		os.WriteFile(path, []byte(body), 0o644)
	}
	entries, err := MuseManifest(agents, &memoryManifestCache{entries: map[string]struct {
		size, mtime int64
		counts      MuseManifestCounts
	}{}})
	if err != nil || len(entries) != 1 || entries[0].EventCount != 3 {
		t.Fatalf("entries = %+v (%v)", entries, err)
	}
}

func TestMuseManifestTakesTheNewestTimestampOfASplitSession(t *testing.T) {
	agents := t.TempDir()
	id := "0b5e8c1a-1111-4a2b-9c3d-000000000004"
	// agent-a sorts first in the walk but holds the newer line (epoch
	// seconds for 2026-11-10T12:00:00Z); agent-b's ISO line is older.
	bodies := map[string]string{
		"a": `{"type":"item","seq":0,"created_at":1794312000}` + "\n",
		"b": `{"type":"item","seq":1,"created_at":"2026-11-10T09:00:00Z"}` + "\n",
	}
	for agent, body := range bodies {
		path := filepath.Join(agents, "agent-"+agent, "sessions", id+".jsonl")
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(body), 0o644)
	}
	entries, err := MuseManifest(agents, nil)
	if err != nil || len(entries) != 1 || entries[0].LastTimestamp != "1794312000" {
		t.Fatalf("entries = %+v (%v)", entries, err)
	}
}

func TestMuseStampAfterComparesTimesNotText(t *testing.T) {
	for _, c := range []struct {
		stamp, current string
		want           bool
	}{
		{"2026-11-10T10:00:00Z", "", true},
		{"", "2026-11-10T10:00:00Z", false},
		{"2026-11-10T10:00:00Z", "2026-11-10T09:00:00Z", true},
		{"2026-11-10T09:00:00Z", "2026-11-10T10:00:00Z", false},
		{"2026-11-10T09:00:00-05:00", "2026-11-10T10:00:00Z", true},
		{"1794312000", "2026-11-10T11:59:59Z", true},
		{"garbled", "2026-11-10T10:00:00Z", false},
		{"2026-11-10T10:00:00Z", "garbled", true},
	} {
		if got := museStampAfter(c.stamp, c.current); got != c.want {
			t.Errorf("museStampAfter(%q, %q) = %v, want %v", c.stamp, c.current, got, c.want)
		}
	}
}
