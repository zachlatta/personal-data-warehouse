package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type heartbeatRecorder struct {
	mu     sync.Mutex
	bodies []map[string]any
	paths  []string
	status int
}

func (r *heartbeatRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, _ := io.ReadAll(req.Body)
	var body map[string]any
	_ = json.Unmarshal(data, &body)
	r.bodies = append(r.bodies, body)
	r.paths = append(r.paths, req.URL.Path)
	if r.status != 0 {
		w.WriteHeader(r.status)
		return
	}
	io.WriteString(w, `{"ok":true}`)
}

func pinHeartbeatHostname(t *testing.T, name string, err error) {
	t.Helper()
	prev := heartbeatHostname
	heartbeatHostname = func() (string, error) { return name, err }
	t.Cleanup(func() { heartbeatHostname = prev })
}

func TestHeartbeatPostsOneRecordPerPipelineWithTheRunVerdict(t *testing.T) {
	rec := &heartbeatRecorder{}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	pinHeartbeatHostname(t, "porygon.local", nil)
	env := isolatedEnv(t, map[string]string{"PDW_API_URL": srv.URL, "PDW_SECRET_TOKEN": "secret-token"})
	var out, errBuf bytes.Buffer
	code := run([]string{"heartbeat", "--pipeline", "claude_code, codex,,pi", "--exit-code", "1", "--duration-seconds", "42", "--ran-at", "2026-08-27T03:00:00-04:00", "--error", "boom"}, strings.NewReader(""), &out, &errBuf, env)
	if code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, errBuf.String())
	}
	if len(rec.bodies) != 3 {
		t.Fatalf("posted %d records, want 3", len(rec.bodies))
	}
	for i, wantPipeline := range []string{"claude_code", "codex", "pi"} {
		if rec.paths[i] != "/ingest/heartbeat" {
			t.Fatalf("path = %s", rec.paths[i])
		}
		b := rec.bodies[i]
		if b["pipeline"] != wantPipeline || b["device"] != "porygon" || b["ran_at"] != "2026-08-27T03:00:00-04:00" || b["error"] != "boom" {
			t.Fatalf("body[%d] = %v", i, b)
		}
		if b["exit_code"] != float64(1) || b["duration_seconds"] != float64(42) {
			t.Fatalf("body[%d] numbers = %v", i, b)
		}
	}
	if strings.Contains(out.String()+errBuf.String(), "secret-token") {
		t.Fatal("token leaked to output")
	}
}

func TestHeartbeatDefaultsDeviceRanAtAndDuration(t *testing.T) {
	rec := &heartbeatRecorder{}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	pinHeartbeatHostname(t, "", errors.New("no hostname"))
	env := isolatedEnv(t, map[string]string{"PDW_API_URL": srv.URL, "PDW_SECRET_TOKEN": "s"})
	var out, errBuf bytes.Buffer
	if code := runHeartbeat([]string{"--pipeline", "apple_notes", "--exit-code", "0"}, &out, &errBuf, env, "", ""); code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, errBuf.String())
	}
	b := rec.bodies[0]
	if b["device"] != "unknown" || b["duration_seconds"] != float64(0) || b["error"] != "" {
		t.Fatalf("body = %v", b)
	}
	if ranAt, _ := b["ran_at"].(string); !strings.HasSuffix(ranAt, "Z") || len(ranAt) < len("2026-01-01T00:00:00Z") {
		t.Fatalf("ran_at should default to now in UTC: %v", b["ran_at"])
	}
}

func TestHeartbeatRoundsFractionalDurations(t *testing.T) {
	rec := &heartbeatRecorder{}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	pinHeartbeatHostname(t, "crobat", nil)
	env := isolatedEnv(t, map[string]string{"PDW_API_URL": srv.URL, "PDW_SECRET_TOKEN": "s"})
	var out, errBuf bytes.Buffer
	if code := runHeartbeat([]string{"--pipeline", "pi", "--exit-code", "0", "--duration-seconds", "1.6"}, &out, &errBuf, env, "", ""); code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, errBuf.String())
	}
	if rec.bodies[0]["duration_seconds"] != float64(2) || rec.bodies[0]["device"] != "crobat" {
		t.Fatalf("body = %v", rec.bodies[0])
	}
}

func TestHeartbeatArgumentErrorsExitTwoWithoutPosting(t *testing.T) {
	rec := &heartbeatRecorder{}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	pinHeartbeatHostname(t, "crobat", nil)
	env := isolatedEnv(t, map[string]string{"PDW_API_URL": srv.URL, "PDW_SECRET_TOKEN": "s"})
	cases := map[string][]string{
		"no pipeline":      {"--exit-code", "0"},
		"empty pipeline":   {"--pipeline", " , ", "--exit-code", "0"},
		"no exit code":     {"--pipeline", "pi"},
		"bad exit code":    {"--pipeline", "pi", "--exit-code", "x"},
		"unknown flag":     {"--pipeline", "pi", "--exit-code", "0", "--bogus"},
		"positional":       {"--pipeline", "pi", "--exit-code", "0", "extra"},
		"negative seconds": {"--pipeline", "pi", "--exit-code", "0", "--duration-seconds", "-1"},
	}
	for name, args := range cases {
		var out, errBuf bytes.Buffer
		if code := runHeartbeat(args, &out, &errBuf, env, "", ""); code != 2 {
			t.Fatalf("%s: exit=%d want 2 (stderr=%s)", name, code, errBuf.String())
		}
	}
	if len(rec.bodies) != 0 {
		t.Fatalf("posted %d records on bad arguments", len(rec.bodies))
	}
}

func TestHeartbeatHelp(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := run([]string{"heartbeat", "--help"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil))
	if code != 0 {
		t.Fatalf("exit=%d", code)
	}
	for _, flag := range []string{"--pipeline", "--exit-code", "--duration-seconds", "--device", "--ran-at", "--error"} {
		if !strings.Contains(out.String(), flag) {
			t.Fatalf("help missing %s:\n%s", flag, out.String())
		}
	}
}

func TestHeartbeatWithoutConfigExitsTwoAndNamesTheFix(t *testing.T) {
	pinHeartbeatHostname(t, "crobat", nil)
	var out, errBuf bytes.Buffer
	code := runHeartbeat([]string{"--pipeline", "pi", "--exit-code", "0"}, &out, &errBuf, isolatedEnv(t, nil), "", "")
	if code != 2 || !strings.Contains(errBuf.String(), "pdw login") {
		t.Fatalf("exit=%d stderr=%s", code, errBuf.String())
	}
}

func TestHeartbeatFallsBackToTheProjectDotenv(t *testing.T) {
	// The wrappers post outside any uploader; a machine with the credentials
	// only in the repo .env (openclaw) must still reach the app.
	rec := &heartbeatRecorder{}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	pinHeartbeatHostname(t, "openclaw", nil)
	env := isolatedEnv(t, nil)
	dir := env("PDW_INGEST_PROJECT_DIR")
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("PDW_API_URL="+srv.URL+"\nPDW_SECRET_TOKEN=dotenv-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errBuf bytes.Buffer
	if code := runHeartbeat([]string{"--pipeline", "openclaw", "--exit-code", "0"}, &out, &errBuf, env, "", ""); code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, errBuf.String())
	}
	if len(rec.bodies) != 1 || rec.bodies[0]["device"] != "openclaw" {
		t.Fatalf("bodies = %v", rec.bodies)
	}
}

func TestHeartbeatReportsFailedPipelinesAndExitsOne(t *testing.T) {
	rec := &heartbeatRecorder{status: http.StatusInternalServerError}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	pinHeartbeatHostname(t, "crobat", nil)
	env := isolatedEnv(t, map[string]string{"PDW_API_URL": srv.URL, "PDW_SECRET_TOKEN": "secret-token"})
	var out, errBuf bytes.Buffer
	code := runHeartbeat([]string{"--pipeline", "claude_code,codex", "--exit-code", "0"}, &out, &errBuf, env, "", "")
	if code != 1 {
		t.Fatalf("exit=%d want 1", code)
	}
	for _, p := range []string{"claude_code", "codex"} {
		if !strings.Contains(errBuf.String(), p) {
			t.Fatalf("stderr should name %s: %s", p, errBuf.String())
		}
	}
	if strings.Contains(errBuf.String(), "secret-token") {
		t.Fatal("token leaked to stderr")
	}
}
