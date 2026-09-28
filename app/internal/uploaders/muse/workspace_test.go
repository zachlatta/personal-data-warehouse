package muse

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type fakeTransport struct {
	batches       [][]map[string]any
	blobs         []string // extension|content-type|content
	failNextBatch bool
}

func (f *fakeTransport) batch(gz []byte, _ time.Time) (ingestclient.StoredObject, error) {
	if f.failNextBatch {
		f.failNextBatch = false
		return ingestclient.StoredObject{}, io.ErrUnexpectedEOF
	}
	reader, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	data, _ := io.ReadAll(reader)
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return ingestclient.StoredObject{}, err
		}
		records = append(records, record)
	}
	f.batches = append(f.batches, records)
	return ingestclient.StoredObject{StorageKey: "agent-sessions/inbox/batch"}, nil
}

func (f *fakeTransport) blob(content []byte, extension, contentType string) (ingestclient.StoredObject, error) {
	f.blobs = append(f.blobs, extension+"|"+contentType+"|"+string(content))
	return ingestclient.StoredObject{
		StorageBackend: "google_drive", StorageKey: "muse/files/xx/blob" + extension,
		StorageFileID: "drive-" + string(content[:1]), StorageURL: "https://drive/x",
	}, nil
}

// records flattens every uploaded muse_file record, keyed by path.
func (f *fakeTransport) records() map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, batch := range f.batches {
		for _, envelope := range batch {
			record := envelope["record"].(map[string]any)
			out[record["path"].(string)] = record
		}
	}
	return out
}

func write(t *testing.T, home, rel string, content []byte) {
	t.Helper()
	path := filepath.Join(home, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func newWorkspace(t *testing.T, home string, transport *fakeTransport, state *WorkspaceState) *Workspace {
	t.Helper()
	return &Workspace{
		Home: home, Account: "z@x", Device: "muse",
		UploadBatch: transport.batch, UploadBlob: transport.blob,
		Logger: &common.RecordingLogger{}, State: state,
		Now: func() time.Time { return time.Date(2026, 9, 28, 22, 0, 0, 0, time.UTC) },
	}
}

func openState(t *testing.T) *WorkspaceState {
	t.Helper()
	state, err := OpenWorkspaceState(filepath.Join(t.TempDir(), "state.sqlite"), "z@x", "/home/hatch")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	return state
}

func TestWorkspaceShipsTextInlineAndBinariesAsBlobs(t *testing.T) {
	home := t.TempDir()
	write(t, home, "MEMORY.md", []byte("# MEMORY\n- Zach lives in Vermont\n"))
	write(t, home, "memory/people/INDEX.md", []byte("people"))
	write(t, home, "workspace/podcasts/ep/ep.mp3", []byte("ID3\x00\x01binary"))
	transport := &fakeTransport{}
	summary, err := newWorkspace(t, home, transport, openState(t)).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesChanged != 3 || summary.FilesInline != 2 || summary.FilesBlob != 1 || summary.BatchesUploaded != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	envelope := transport.batches[0][0]
	if envelope["record_type"] != "muse_file" || envelope["source"] != "agent_sessions" || envelope["account"] != "z@x" || envelope["device"] != "muse" {
		t.Fatalf("envelope = %v", envelope)
	}
	records := transport.records()
	memory := records["MEMORY.md"]
	if memory["is_text"] != true || memory["content_text"] != "# MEMORY\n- Zach lives in Vermont\n" || memory["mime_type"] != "text/markdown" {
		t.Fatalf("MEMORY.md = %v", memory)
	}
	if len(memory["content_sha256"].(string)) != 64 || memory["size_bytes"].(float64) != 33 || memory["deleted"] != false {
		t.Fatalf("MEMORY.md provenance = %v", memory)
	}
	podcast := records["workspace/podcasts/ep/ep.mp3"]
	if podcast["is_text"] != false || podcast["content_text"] != "" || podcast["storage_file_id"] != "drive-I" || podcast["storage_key"] != "muse/files/xx/blob.mp3" {
		t.Fatalf("podcast = %v", podcast)
	}
	if len(transport.blobs) != 1 || transport.blobs[0] != ".mp3|audio/mpeg|ID3\x00\x01binary" {
		t.Fatalf("blobs = %q", transport.blobs)
	}
}

func TestWorkspaceSkipsSecretsRuntimePlumbingAndTheTranscripts(t *testing.T) {
	home := t.TempDir()
	keep := []string{
		"MEMORY.md", "USER.md", "memory/2026-09-28.md", "dreams/2026-09-28.md",
		"workspace/goals/sell-gpu/listing.md", "workspace/your_files/plan.md",
		"workspace/agents/abc/tool-output/exec-call_1.json", "workspace/.jarvis/idea/scratch.md",
		"hooks/scripts/pdw-ingest.sh", "subscriptions/activity_feed/subscription.md",
	}
	skip := []string{
		"agents/agent-1/sessions/1.jsonl", // transcripts ship as muse_event lines instead
		".ssh/id_ed25519", ".config/pdw/config.json", ".ttyd-creds", ".tmate.sock.txt",
		".cache/x", ".local/bin/pdw", ".pki/nssdb/cert9.db", ".vscode/cli/token.json",
		".postgres-ready", ".profile", "runtime.lock", "tmate-client-6936.log",
		"docs/muse.md", "assets/logo.png", "config/filesystem_watch_hashes.json",
		"hooks/runtime/hatch_hook_runtime.sh", "hooks/logs/pdw.jsonl", "hooks/state/pdw.json",
		"workspace/.hatch-browser/Default/Cookies", "workspace/tools/vscode-cli/code.tar.gz",
		"workspace/remote-access/reverse-tunnel.sh", "workspace/project/.git/HEAD",
		"workspace/project/node_modules/x/index.js",
	}
	for _, rel := range append(append([]string{}, keep...), skip...) {
		write(t, home, rel, []byte("x"))
	}
	write(t, home, "workspace/tools/pdw", append([]byte("\x7fELF"), 0, 1, 2))
	transport := &fakeTransport{}
	if _, err := newWorkspace(t, home, transport, openState(t)).Sync(); err != nil {
		t.Fatal(err)
	}
	var got []string
	for path := range transport.records() {
		got = append(got, path)
	}
	sort.Strings(got)
	sort.Strings(keep)
	if strings.Join(got, "\n") != strings.Join(keep, "\n") {
		t.Fatalf("shipped:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(keep, "\n"))
	}
}

func TestWorkspaceOnlyReshipsChangedContentAndTombstonesRemovedPaths(t *testing.T) {
	home := t.TempDir()
	write(t, home, "MEMORY.md", []byte("v1"))
	write(t, home, "memory/old.md", []byte("old"))
	state := openState(t)
	transport := &fakeTransport{}
	if _, err := newWorkspace(t, home, transport, state).Sync(); err != nil {
		t.Fatal(err)
	}

	// Unchanged run: nothing to send.
	summary, err := newWorkspace(t, home, transport, state).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesChanged != 0 || summary.BatchesUploaded != 0 || len(transport.batches) != 1 {
		t.Fatalf("idle summary = %+v batches=%d", summary, len(transport.batches))
	}

	// A touch that does not change the bytes is not a change either.
	later := time.Now().Add(time.Minute)
	os.Chtimes(filepath.Join(home, "MEMORY.md"), later, later)
	if summary, _ := newWorkspace(t, home, transport, state).Sync(); summary.FilesChanged != 0 {
		t.Fatalf("touched summary = %+v", summary)
	}

	write(t, home, "MEMORY.md", []byte("v2 longer"))
	os.Remove(filepath.Join(home, "memory/old.md"))
	summary, err = newWorkspace(t, home, transport, state).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesChanged != 1 || summary.FilesDeleted != 1 || len(transport.batches) != 2 {
		t.Fatalf("change summary = %+v", summary)
	}
	last := map[string]map[string]any{}
	for _, envelope := range transport.batches[1] {
		record := envelope["record"].(map[string]any)
		last[record["path"].(string)] = record
	}
	if last["MEMORY.md"]["content_text"] != "v2 longer" {
		t.Fatalf("MEMORY.md = %v", last["MEMORY.md"])
	}
	if last["memory/old.md"]["deleted"] != true || last["memory/old.md"]["content_text"] != "" {
		t.Fatalf("tombstone = %v", last["memory/old.md"])
	}

	// The tombstone is sent once.
	if summary, _ := newWorkspace(t, home, transport, state).Sync(); summary.FilesDeleted != 0 || summary.BatchesUploaded != 0 {
		t.Fatalf("after-tombstone summary = %+v", summary)
	}
}

func TestWorkspaceCommitsStateOnlyAfterTheBatchLands(t *testing.T) {
	home := t.TempDir()
	write(t, home, "MEMORY.md", []byte("v1"))
	state := openState(t)
	transport := &fakeTransport{failNextBatch: true}
	if _, err := newWorkspace(t, home, transport, state).Sync(); err == nil {
		t.Fatal("a failed batch upload must fail the run")
	}
	summary, err := newWorkspace(t, home, transport, state).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesChanged != 1 || len(transport.batches) != 1 {
		t.Fatalf("retry summary = %+v", summary)
	}
}

func TestWorkspaceDefersFilesAboveTheUploadCeilingAndRespectsTheLimit(t *testing.T) {
	home := t.TempDir()
	write(t, home, "a.md", []byte("a"))
	write(t, home, "b.md", []byte("b"))
	write(t, home, "workspace/big.bin", bytes.Repeat([]byte{0}, 64))
	state := openState(t)
	transport := &fakeTransport{}
	ws := newWorkspace(t, home, transport, state)
	ws.MaxBlobBytes = 32
	ws.Limit = 1
	summary, err := ws.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesChanged != 1 || !summary.LimitReached || summary.FilesDeleted != 0 {
		t.Fatalf("limited summary = %+v", summary)
	}
	ws = newWorkspace(t, home, transport, state)
	ws.MaxBlobBytes = 32
	summary, err = ws.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesChanged != 1 || summary.FilesTooLarge != 1 {
		t.Fatalf("second summary = %+v", summary)
	}
	if _, shipped := transport.records()["workspace/big.bin"]; shipped {
		t.Fatal("a file above the upload ceiling must not ship")
	}
}

func TestLargeTextIsABlobNotInline(t *testing.T) {
	home := t.TempDir()
	write(t, home, "workspace/survey/submissions.json", []byte(strings.Repeat("a", 40)))
	transport := &fakeTransport{}
	ws := newWorkspace(t, home, transport, openState(t))
	ws.MaxInlineBytes = 16
	if _, err := ws.Sync(); err != nil {
		t.Fatal(err)
	}
	record := transport.records()["workspace/survey/submissions.json"]
	if record["is_text"] != false || record["storage_file_id"] == "" {
		t.Fatalf("record = %v", record)
	}
}
