package muse

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/agentsessions"
)

func transcript(id string, items int) []byte {
	var b strings.Builder
	b.WriteString(`{"type":"session_header","session_id":"` + id + `","created_at":"2026-11-10T10:00:00Z"}` + "\n")
	for i := 0; i < items; i++ {
		b.WriteString(`{"type":"item","seq":` + string(rune('0'+i)) + `,"source":"runtime","item":{"type":"message","role":"user","text":"secret words"},"created_at":"2026-11-10T10:00:0` + string(rune('1'+i)) + `Z"}` + "\n")
	}
	return []byte(b.String())
}

func TestSessionManifestShardsBySessionIDAndCarriesNoText(t *testing.T) {
	home := t.TempDir()
	a := "0b5e8c1a-1111-4a2b-9c3d-000000000001"
	b := "0c5e8c1a-1111-4a2b-9c3d-000000000002"
	c := "Fa5e8c1a-1111-4a2b-9c3d-000000000003"
	write(t, home, "agents/agent-"+a+"/sessions/"+a+".jsonl", transcript(a, 2))
	write(t, home, "agents/agent-"+b+"/sessions/"+b+".jsonl", transcript(b, 1))
	write(t, home, "agents/agent-"+c+"/sessions/"+c+".jsonl", transcript(c, 3))
	files, err := SessionManifestFiles(home+"/agents", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Path != ".pdw/session-manifest/0.json" || files[1].Path != ".pdw/session-manifest/f.json" {
		t.Fatalf("files = %+v", files)
	}
	var shard struct {
		Format   string `json:"format"`
		Shard    string `json:"shard"`
		Sessions []struct {
			SessionID     string `json:"session_id"`
			EventCount    int64  `json:"event_count"`
			LastTimestamp string `json:"last_timestamp"`
		} `json:"sessions"`
	}
	if err := json.Unmarshal(files[0].Content, &shard); err != nil {
		t.Fatal(err)
	}
	if shard.Format != ManifestFormat || shard.Shard != "0" || len(shard.Sessions) != 2 ||
		shard.Sessions[0].SessionID != a || shard.Sessions[0].EventCount != 3 || shard.Sessions[0].LastTimestamp != "2026-11-10T10:00:02Z" ||
		shard.Sessions[1].SessionID != b || shard.Sessions[1].EventCount != 2 {
		t.Fatalf("shard 0 = %+v", shard)
	}
	for _, file := range files {
		if strings.Contains(string(file.Content), "secret words") {
			t.Fatalf("%s carries transcript text", file.Path)
		}
	}
}

func TestGeneratedRecordsShipOnChangeAtMostOncePerInterval(t *testing.T) {
	home := t.TempDir()
	write(t, home, "MEMORY.md", []byte("# MEMORY\n"))
	state := openState(t)
	transport := &fakeTransport{}
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	run := func(content string) WorkspaceSummary {
		t.Helper()
		ws := newWorkspace(t, home, transport, state)
		ws.Now = func() time.Time { return now }
		ws.Generated = []GeneratedFile{{Path: ".pdw/session-manifest/0.json", Content: []byte(content)}}
		ws.GeneratedMinInterval = time.Hour
		summary, err := ws.Sync()
		if err != nil {
			t.Fatal(err)
		}
		return summary
	}

	if s := run(`{"v":1}`); s.GeneratedSent != 1 {
		t.Fatalf("first run = %+v", s)
	}
	record := transport.records()[".pdw/session-manifest/0.json"]
	if record["content_text"] != `{"v":1}` || record["is_text"] != true || record["mime_type"] != "application/json" || record["modified_at"] != "2026-11-11T12:00:00+00:00" {
		t.Fatalf("record = %v", record)
	}
	batches := len(transport.batches)

	now = now.Add(5 * time.Minute)
	if s := run(`{"v":1}`); s.GeneratedSent != 0 || s.GeneratedDeferred != 0 || len(transport.batches) != batches {
		t.Fatalf("unchanged run = %+v", s)
	}
	if s := run(`{"v":2}`); s.GeneratedSent != 0 || s.GeneratedDeferred != 1 || len(transport.batches) != batches {
		t.Fatalf("changed inside the interval = %+v", s)
	}
	now = now.Add(time.Hour)
	if s := run(`{"v":2}`); s.GeneratedSent != 1 {
		t.Fatalf("changed after the interval = %+v", s)
	}
	if transport.records()[".pdw/session-manifest/0.json"]["content_text"] != `{"v":2}` {
		t.Fatalf("records = %v", transport.records())
	}

	// A shard with no sessions left is tombstoned like a deleted file.
	ws := newWorkspace(t, home, transport, state)
	ws.Now = func() time.Time { return now }
	summary, err := ws.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesDeleted != 1 || transport.records()[".pdw/session-manifest/0.json"]["deleted"] != true {
		t.Fatalf("tombstone summary = %+v", summary)
	}
}

func TestGeneratedRecordsMustLiveUnderTheGeneratedDir(t *testing.T) {
	home := t.TempDir()
	ws := newWorkspace(t, home, &fakeTransport{}, openState(t))
	ws.Generated = []GeneratedFile{{Path: "MEMORY.md", Content: []byte("x")}}
	if _, err := ws.Sync(); err == nil || !strings.Contains(err.Error(), ".pdw") {
		t.Fatalf("err = %v", err)
	}
}

func TestTheWalkNeverShipsARealFileUnderTheGeneratedDir(t *testing.T) {
	home := t.TempDir()
	write(t, home, ".pdw/session-manifest/0.json", []byte(`{"forged":true}`))
	transport := &fakeTransport{}
	if _, err := newWorkspace(t, home, transport, openState(t)).Sync(); err != nil {
		t.Fatal(err)
	}
	if _, ok := transport.records()[".pdw/session-manifest/0.json"]; ok {
		t.Fatalf("walked a file under .pdw: %v", transport.records())
	}
}

func TestSessionManifestSplitsAShardThatWouldNotFitInline(t *testing.T) {
	home := t.TempDir()
	ids := []string{
		"0a000000-1111-4a2b-9c3d-000000000001", "0a000000-1111-4a2b-9c3d-000000000002",
		"0b000000-1111-4a2b-9c3d-000000000003", "1c000000-1111-4a2b-9c3d-000000000004",
	}
	for _, id := range ids {
		write(t, home, "agents/agent-"+id+"/sessions/"+id+".jsonl", transcript(id, 1))
	}
	whole, err := SessionManifestFiles(home+"/agents", nil, 0)
	if err != nil || len(whole) != 2 {
		t.Fatalf("unsplit = %+v (%v)", whole, err)
	}
	// Room for about two sessions per shard: "0" (three sessions) splits into
	// "0a" and "0b"; "1" fits as it is.
	limit := int64(len(whole[1].Content)) + 300
	files, err := SessionManifestFiles(home+"/agents", nil, limit)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	listed := 0
	for _, file := range files {
		paths = append(paths, file.Path)
		if int64(len(file.Content)) > limit {
			t.Fatalf("%s is %d bytes, over %d", file.Path, len(file.Content), limit)
		}
		var shard manifestShard
		if err := json.Unmarshal(file.Content, &shard); err != nil {
			t.Fatal(err)
		}
		listed += len(shard.Sessions)
	}
	if strings.Join(paths, ",") != ".pdw/session-manifest/0a.json,.pdw/session-manifest/0b.json,.pdw/session-manifest/1.json" || listed != 4 {
		t.Fatalf("paths = %v, listed = %d", paths, listed)
	}
}

func TestAnOversizedGeneratedRecordIsRefusedNotUploaded(t *testing.T) {
	home := t.TempDir()
	transport := &fakeTransport{}
	ws := newWorkspace(t, home, transport, openState(t))
	ws.MaxInlineBytes = 16
	ws.Generated = []GeneratedFile{{Path: ".pdw/session-manifest/0.json", Content: []byte(strings.Repeat("x", 17))}}
	summary, err := ws.Sync()
	if err != nil || summary.GeneratedTooLarge != 1 || summary.GeneratedSent != 0 || len(transport.batches) != 0 {
		t.Fatalf("summary = %+v err = %v batches = %d", summary, err, len(transport.batches))
	}
}

func TestTheManifestCacheLivesInTheWorkspaceState(t *testing.T) {
	state := openState(t)
	cache := state.ManifestCache()
	if _, ok := cache.Lookup("/a.jsonl", 10, 5); ok {
		t.Fatal("hit on an empty cache")
	}
	if err := cache.Store("/a.jsonl", 10, 5, agentsessions.MuseManifestCounts{EventCount: 3, LastTimestamp: "t"}); err != nil {
		t.Fatal(err)
	}
	if got, ok := cache.Lookup("/a.jsonl", 10, 5); !ok || got.EventCount != 3 || got.LastTimestamp != "t" {
		t.Fatalf("lookup = %+v %v", got, ok)
	}
	if _, ok := cache.Lookup("/a.jsonl", 11, 5); ok {
		t.Fatal("hit after the file grew")
	}
	if err := cache.Retain(map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.Lookup("/a.jsonl", 10, 5); ok {
		t.Fatal("hit after the file was dropped")
	}
}

func TestANewManifestRuleDropsOnlyTheManifestCache(t *testing.T) {
	path := t.TempDir() + "/state.sqlite"
	open := func() *WorkspaceState {
		t.Helper()
		state, err := OpenWorkspaceState(path, "z@x", "/home/hatch")
		if err != nil {
			t.Fatal(err)
		}
		return state
	}
	state := open()
	if err := state.ManifestCache().Store("/a.jsonl", 10, 5, agentsessions.MuseManifestCounts{EventCount: 3}); err != nil {
		t.Fatal(err)
	}
	if err := state.Record("MEMORY.md", 9, 1, "sha", time.Now()); err != nil {
		t.Fatal(err)
	}
	state.Close()

	// Reopening under the same rule keeps both.
	state = open()
	if _, ok := state.ManifestCache().Lookup("/a.jsonl", 10, 5); !ok {
		t.Fatal("same rule dropped the cache")
	}
	state.Close()

	previous := manifestCacheRuleVersion
	manifestCacheRuleVersion = "test-next"
	t.Cleanup(func() { manifestCacheRuleVersion = previous })
	state = open()
	defer state.Close()
	if _, ok := state.ManifestCache().Lookup("/a.jsonl", 10, 5); ok {
		t.Fatal("a new rule kept counts made by the old one")
	}
	all, err := state.All()
	if err != nil || all["MEMORY.md"].ContentSHA256 != "sha" {
		t.Fatalf("workspace state lost: %v (%v)", all, err)
	}
}
