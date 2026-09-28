package agentsessions

import (
	"os"
	"path/filepath"
	"testing"
)

const (
	museMain = "9f2189a3-3d40-4253-b3c3-f6342cd83882"
	museSub  = "cca278e9-4051-462d-9586-55ebbffefb89"
	museLoop = "df5e19bd-b381-4544-a43b-5bdcedc7510c"
)

// writeMuseSession lays out one Muse agent the way the VM does:
// <agents>/agent-<id>/sessions/<id>.jsonl beside a sessions.json index.
func writeMuseSession(t *testing.T, agents, id, firstItem, model string) string {
	t.Helper()
	dir := filepath.Join(agents, "agent-"+id, "sessions")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	header := `{"type":"session_header","version":1,"session_id":"` + id + `","agent_id":"` + id + `","created_at":"2026-09-27T20:39:30.844916987+00:00"}`
	path := filepath.Join(dir, id+".jsonl")
	if err := os.WriteFile(path, []byte(header+"\n"+firstItem+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	index := `{"version":1,"sessions":[{"session_id":"` + id + `","agent_id":"` + id + `","context_window_usage":{"model_id":"` + model + `"}}]}`
	if err := os.WriteFile(filepath.Join(dir, "sessions.json"), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDiscoverMuseReadsEachSessionsKindOpenerAndModel(t *testing.T) {
	agents := t.TempDir()
	writeMuseSession(t, agents, museMain,
		`{"type":"item","seq":0,"source":"runtime.onboarding","item":{"type":"message","role":"developer","text":"The user has just completed app setup."}}`,
		"muse-spark")
	writeMuseSession(t, agents, museSub,
		`{"type":"item","seq":0,"source":"runtime","item":{"type":"message_parts","role":"user","parts":[{"type":"text","text":"[Subagent Context] You are running as a subagent (depth 1/2)\n- Requester agent id: `+museMain+`.\n"}]}}`,
		"muse-spark")
	// A transcript can open with a reasoning item; the opener is the first MESSAGE.
	writeMuseSession(t, agents, museLoop,
		`{"type":"item","seq":0,"source":"runtime","item":{"type":"thinking","thinking":"hm"}}`+"\n"+
			`{"type":"item","seq":1,"source":"runtime.self_improvement","item":{"type":"message","role":"user","text":"## Step instructions"}}`,
		"")
	// Not a transcript: the index itself, and a stray file outside sessions/.
	os.WriteFile(filepath.Join(agents, "notes.jsonl"), []byte("{}\n"), 0o644)

	files := Discover(Dirs{MuseAgents: agents})
	byID := map[string]SessionFile{}
	for _, f := range files {
		if f.Tool != MuseTool {
			t.Fatalf("unexpected tool %q", f.Tool)
		}
		byID[f.SessionID] = f
	}
	if len(byID) != 3 {
		t.Fatalf("discovered %d muse sessions, want 3: %+v", len(byID), files)
	}
	checks := map[string]map[string]string{
		museMain: {"kind": "chat", "opening_source": "runtime.onboarding", "requester_agent_id": "", "model": "muse-spark"},
		museSub:  {"kind": "subagent", "opening_source": "runtime", "requester_agent_id": museMain, "model": "muse-spark"},
		museLoop: {"kind": "background", "opening_source": "runtime.self_improvement", "requester_agent_id": "", "model": ""},
	}
	for id, want := range checks {
		got := byID[id].Session
		for key, value := range want {
			if got[key] != value {
				t.Fatalf("%s session[%s] = %v, want %q (all: %v)", id, key, got[key], value, got)
			}
		}
	}
}

func TestMuseEnvelopeCarriesTheSessionMetadataOnEveryLine(t *testing.T) {
	agents := t.TempDir()
	writeMuseSession(t, agents, museSub,
		`{"type":"item","seq":0,"source":"runtime","item":{"type":"message_parts","role":"user","parts":[{"type":"text","text":"[Subagent Context] Requester agent id: `+museMain+`."}]}}`,
		"muse-spark")
	uploads := &fakeUploads{}
	runner := newRunner(t, agents, uploads, nil)
	runner.Dirs = Dirs{MuseAgents: agents}
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.LinesSelected != 2 {
		t.Fatalf("summary = %+v", summary)
	}
	for _, record := range uploads.batches[0] {
		if record["record_type"] != "muse_event" {
			t.Fatalf("record_type = %v", record["record_type"])
		}
		session := record["record"].(map[string]any)["session"].(map[string]any)
		if session["kind"] != "subagent" || session["requester_agent_id"] != museMain {
			t.Fatalf("session = %v", session)
		}
	}
}

func TestMuseIsNeverDiscoveredFromTheDefaultEnvironment(t *testing.T) {
	// A Mac running `pdw ingest agent-sessions` must never pick Muse up; only
	// `pdw ingest muse` points the runner at a Muse home.
	if dirs := DirsFromEnv(func(string) string { return "" }); dirs.MuseAgents != "" {
		t.Fatalf("MuseAgents = %q", dirs.MuseAgents)
	}
}
