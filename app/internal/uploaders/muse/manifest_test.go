package muse

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
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
	files, err := SessionManifestFiles(home + "/agents")
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
