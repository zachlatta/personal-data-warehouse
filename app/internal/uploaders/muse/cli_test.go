package muse

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/testfixtures"
)

// museHome lays out a miniature Muse VM home: one chat transcript, a memory
// file, and a generated binary.
func museHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	id := "9f2189a3-3d40-4253-b3c3-f6342cd83882"
	write(t, home, "agents/agent-"+id+"/sessions/"+id+".jsonl", []byte(
		`{"type":"session_header","version":1,"session_id":"`+id+`","created_at":"2026-09-27T20:39:30Z"}`+"\n"+
			`{"type":"item","seq":0,"source":"runtime","item":{"type":"message","role":"user","text":"hi Muse"},"created_at":"2026-09-27T21:00:00Z"}`+"\n"))
	write(t, home, "agents/agent-"+id+"/sessions/sessions.json", []byte(`{"version":1,"sessions":[]}`))
	write(t, home, "MEMORY.md", []byte("# MEMORY\n"))
	write(t, home, "workspace/podcasts/ep.mp3", []byte("ID3\x00audio"))
	return home
}

func runMuse(t *testing.T, args []string, env map[string]string, cfg ingestclient.Config) (int, string, string) {
	t.Helper()
	common.OverrideBeforeUploadCheck = func() string { return "" }
	t.Cleanup(func() { common.OverrideBeforeUploadCheck = nil })
	var out, errOut bytes.Buffer
	code := Run(args, strings.NewReader(""), &out, &errOut, func(k string) string { return env[k] }, cfg)
	return code, out.String(), errOut.String()
}

func museEnv(home string) map[string]string {
	return map[string]string{"MUSE_ACCOUNT": "zach@example.com", "PDW_INGEST_PROJECT_DIR": home}
}

func TestRunHelp(t *testing.T) {
	code, out, _ := runMuse(t, []string{"--help"}, map[string]string{}, ingestclient.Config{})
	if code != 0 || !strings.Contains(out, "pdw ingest muse") || !strings.Contains(out, "--home") {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func TestRunRejectsBadArguments(t *testing.T) {
	for _, args := range [][]string{{"--mode", "weekly"}, {"--limit", "-1"}, {"--bogus"}, {"--no-transcripts", "--no-files"}} {
		code, _, errOut := runMuse(t, args, map[string]string{}, ingestclient.Config{})
		if code != 2 || !strings.Contains(errOut, "error") {
			t.Fatalf("%v: code=%d stderr=%q", args, code, errOut)
		}
	}
}

func TestRunRequiresAnAccount(t *testing.T) {
	home := museHome(t)
	env := museEnv(home)
	delete(env, "MUSE_ACCOUNT")
	code, _, errOut := runMuse(t, []string{"--home", home}, env, ingestclient.Config{BaseURL: "http://x", Token: "t"})
	if code != 1 || !strings.Contains(errOut, "MUSE_ACCOUNT") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestRunShipsTranscriptsAndWorkspaceThroughTheApp(t *testing.T) {
	home := museHome(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	code, out, errOut := runMuse(t, []string{"--home", home}, museEnv(home), cfg)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	paths := strings.Join(app.Paths(), ",")
	if paths != "/ingest/agent-sessions/batch,/ingest/muse/file,/ingest/agent-sessions/batch" {
		t.Fatalf("app calls = %s", paths)
	}
	transcript := testfixtures.DecodeGzipJSONL(app.Calls()[0].Body)
	if len(transcript) != 2 || transcript[0]["record_type"] != "muse_event" || transcript[0]["device"] != "muse" || transcript[0]["account"] != "zach@example.com" {
		t.Fatalf("transcript batch = %v", transcript)
	}
	files := testfixtures.DecodeGzipJSONL(app.Calls()[2].Body)
	var shipped []string
	for _, record := range files {
		shipped = append(shipped, record["record"].(map[string]any)["path"].(string))
	}
	if strings.Join(shipped, ",") != "MEMORY.md,workspace/podcasts/ep.mp3" {
		t.Fatalf("workspace paths = %v", shipped)
	}
	// State lives inside the Muse home, which is what survives a VM restart.
	if _, err := os.Stat(filepath.Join(home, ".local", "state", "pdw", "muse-workspace-state.sqlite")); err != nil {
		t.Fatalf("workspace state not in the Muse home: %v", err)
	}
	if !strings.Contains(out, "Muse upload complete") {
		t.Fatalf("summary missing: %q", out)
	}

	// A second run with nothing new uploads nothing.
	code, _, errOut = runMuse(t, []string{"--home", home}, museEnv(home), cfg)
	if code != 0 || len(app.Calls()) != 3 {
		t.Fatalf("idle run code=%d calls=%d stderr=%q", code, len(app.Calls()), errOut)
	}
}

func TestRunCanShipOnlyOneHalf(t *testing.T) {
	home := museHome(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	if code, _, errOut := runMuse(t, []string{"--home", home, "--no-files"}, museEnv(home), cfg); code != 0 {
		t.Fatalf("stderr=%q", errOut)
	}
	if paths := strings.Join(app.Paths(), ","); paths != "/ingest/agent-sessions/batch" {
		t.Fatalf("--no-files calls = %s", paths)
	}
}
