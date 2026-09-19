package voicememos

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/testfixtures"
)

var testNow = time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)

func fixedNow() time.Time { return testNow }

func noKick(string, common.Logger) AppKick { return AppKick{Reason: "test"} }

// --- scanner + metadata against the Python goldens -------------------------

// testdata/golden.jsonl was produced by running the Python scanner and
// build_metadata over testdata/Recordings (account "z@x", uploaded_at
// 2026-05-21T12:00:00Z). File birth/modification times are not preserved by
// git, so the golden's own stat fields are substituted before the payload is
// compared byte for byte.
func TestScanMatchesPythonGoldens(t *testing.T) {
	goldens := testfixtures.LoadGoldens(t, filepath.Join("testdata", "golden.jsonl"))["recordings"]
	root, _ := filepath.Abs(filepath.Join("testdata", "Recordings"))
	candidates, err := ScanCandidates(root, DefaultExtensions)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != len(goldens) {
		t.Fatalf("scanned %d candidates, golden has %d", len(candidates), len(goldens))
	}
	for i, candidate := range candidates {
		want := goldens[i]
		if candidate.Filename != want.Extra["filename"] || candidate.RecordingID != want.Extra["recording_id"] {
			t.Fatalf("candidate %d = %s, golden %s", i, candidate.Filename, want.Extra["filename"])
		}
		if candidate.SizeBytes != int64(want.Extra["size"].(float64)) {
			t.Errorf("%s size = %d", candidate.Filename, candidate.SizeBytes)
		}
		if !floatPtrEqual(candidate.Duration, want.Extra["duration"]) || !floatPtrEqual(candidate.LocalDuration, want.Extra["local_duration"]) {
			t.Errorf("%s durations = %v / %v, want %v / %v", candidate.Filename, deref(candidate.Duration), deref(candidate.LocalDuration), want.Extra["duration"], want.Extra["local_duration"])
		}
		if IsPartiallyMaterialized(candidate) != want.Extra["partial"].(bool) {
			t.Errorf("%s partial = %v", candidate.Filename, IsPartiallyMaterialized(candidate))
		}
		recording, err := RecordingFromCandidate(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if recording.ContentSHA256 != want.Fingerprint {
			t.Errorf("%s sha = %s, want %s", candidate.Filename, recording.ContentSHA256, want.Fingerprint)
		}
		// Substitute the stat-derived fields the golden recorded.
		recording.FileCreatedAt = common.ParseISO(want.Extra["file_created_at"].(string))
		recording.FileModifiedAt = common.ParseISO(want.Extra["file_modified_at"].(string))
		recording.RecordedAt = common.ParseISO(want.Extra["recorded_at"].(string))
		if _, ok := RecordedAtFromFilename(candidate.RecordingID); ok && !recording.RecordedAt.Equal(candidate.RecordedAt) {
			t.Errorf("%s recorded_at = %s, want %s", candidate.Filename, common.ISOFormat(candidate.RecordedAt), want.Extra["recorded_at"])
		}
		var goldenPayload map[string]any
		if err := json.Unmarshal([]byte(want.Payload), &goldenPayload); err != nil {
			t.Fatal(err)
		}
		goldenRoot := filepath.Dir(goldenPayload["recording"].(map[string]any)["original_path"].(string))
		expected := strings.ReplaceAll(want.Payload, goldenRoot, root)
		encoded, err := common.CanonicalJSON(BuildMetadata("z@x", recording, testNow))
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != expected {
			t.Errorf("%s payload differs:\n got %s\nwant %s", candidate.Filename, encoded, expected)
		}
	}
}

func floatPtrEqual(value *float64, want any) bool {
	if want == nil {
		return value == nil
	}
	return value != nil && *value == want.(float64)
}

func deref(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func TestContentTypeAndRecordedAt(t *testing.T) {
	if ContentTypeForExtension(".qta") != "audio/quicktime" || ContentTypeForExtension(".m4a") != "audio/mp4" || ContentTypeForExtension(".zzz") != "application/octet-stream" {
		t.Fatal("content types")
	}
	if ContentTypeForExtension(".mp3") != "audio/mpeg" {
		t.Fatalf("mp3 = %s", ContentTypeForExtension(".mp3"))
	}
	at, ok := RecordedAtFromFilename("20260430 110736-8BB8E57D")
	if !ok || !at.Equal(time.Date(2026, 4, 30, 11, 7, 36, 0, time.UTC)) {
		t.Fatalf("recorded at = %v %v", at, ok)
	}
	if _, ok := RecordedAtFromFilename("badname"); ok {
		t.Fatal("badname parsed")
	}
}

func TestPartialMaterialization(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	cases := []struct {
		duration, local *float64
		want            bool
	}{
		{nil, nil, false},
		{f(10), nil, false},
		{f(9349.66), f(6228.63), true},
		{f(30), f(0), true},
		{f(12.5), f(12.5), false},
		{f(20), f(16), false},
		{f(20), f(14), true},
		{f(0), f(0), false},
	}
	for _, c := range cases {
		got := IsPartiallyMaterialized(Candidate{Duration: c.duration, LocalDuration: c.local})
		if got != c.want {
			t.Errorf("partial(%v, %v) = %v", deref(c.duration), deref(c.local), got)
		}
	}
}

// --- state -------------------------------------------------------------------

func TestStateRoundTripsThePythonFile(t *testing.T) {
	original, err := os.ReadFile(filepath.Join("testdata", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		RecordingsPath string `json:"recordings_path"`
	}
	json.Unmarshal(original, &raw)
	state := LoadState(filepath.Join("testdata", "state.json"), "z@x", raw.RecordingsPath)
	entry, ok := state.Entries["20260325 145019-DAAC9394.qta"]
	if !ok || !entry.Complete() || entry.ContentSHA256 != "abc" || entry.SizeBytes != 16 || entry.MtimeNS != 1789348224770870204 {
		t.Fatalf("entry = %+v", entry)
	}
	out := filepath.Join(t.TempDir(), "state.json")
	if err := state.Save(out); err != nil {
		t.Fatal(err)
	}
	saved, _ := os.ReadFile(out)
	if !bytes.Equal(saved, original) {
		t.Fatalf("saved state differs from the Python file:\n%s\n---\n%s", saved, original)
	}
	if other := LoadState(filepath.Join("testdata", "state.json"), "other@x", raw.RecordingsPath); len(other.Entries) != 0 {
		t.Fatal("state for another account must load empty")
	}
	if other := LoadState(filepath.Join("testdata", "state.json"), "z@x", "/elsewhere"); len(other.Entries) != 0 {
		t.Fatal("state for another root must load empty")
	}
}

func TestStateMarksAndMatches(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "20260427 100004-40DC0200.m4a")
	os.WriteFile(path, []byte("audio"), 0o644)
	candidates, _ := ScanCandidates(root, []string{".m4a"})
	c := candidates[0]
	state := EmptyState("z@x", root)
	state.MarkFailure(c, "", "boom", testNow)
	entry, _ := state.EntryFor(c)
	if entry.Complete() || entry.LastError != "boom" || entry.LastFailureAt != "2026-05-21T12:00:00+00:00" || entry.LastSuccessAt != "" {
		t.Fatalf("failure entry = %+v", entry)
	}
	state.MarkSuccess(c, "sha", true, true, testNow)
	entry, _ = state.EntryFor(c)
	if !entry.Complete() || !entry.Matches(c) || entry.Path != c.Filename || entry.LastFailureAt != "" {
		t.Fatalf("success entry = %+v", entry)
	}
	state.MarkFailure(c, "", "again", testNow.Add(time.Minute))
	entry, _ = state.EntryFor(c)
	if entry.ContentSHA256 != "sha" || !entry.AudioUploaded || entry.LastSuccessAt != "2026-05-21T12:00:00+00:00" || entry.LastError != "again" {
		t.Fatalf("second failure must keep the earlier success: %+v", entry)
	}
	if got := state.SHAByFilename(); got[c.Filename] != "sha" {
		t.Fatalf("sha by filename = %v", got)
	}
	changed := c
	changed.SizeBytes++
	if entry.Matches(changed) {
		t.Fatal("a resized file must not match")
	}
}

// --- sync runner -------------------------------------------------------------

type fakeUploader struct {
	mu       sync.Mutex
	audio    []map[string]string
	metadata []map[string]any
	fail     func(recordedAt string) error
	inflight atomic.Int32
	peak     atomic.Int32
}

func (f *fakeUploader) UploadVoiceMemoAudio(content []byte, recordedAt, extension, contentType string) (ingestclient.StoredObject, error) {
	n := f.inflight.Add(1)
	defer f.inflight.Add(-1)
	for {
		peak := f.peak.Load()
		if n <= peak || f.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	time.Sleep(5 * time.Millisecond)
	if f.fail != nil {
		if err := f.fail(recordedAt); err != nil {
			return ingestclient.StoredObject{}, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.audio = append(f.audio, map[string]string{"recorded_at": recordedAt, "extension": extension, "content_type": contentType, "sha": common.BytesSHA256(content)})
	return ingestclient.StoredObject{StorageKey: "audio"}, nil
}

func (f *fakeUploader) UploadVoiceMemoMetadata(payload map[string]any, recordedAt, sha string) (ingestclient.StoredObject, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metadata = append(f.metadata, payload)
	return ingestclient.StoredObject{StorageKey: "metadata"}, nil
}

func writeRecordings(t *testing.T, root string, names ...string) {
	t.Helper()
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(root, name), []byte("audio-"+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func createCloudRecordingsDB(t *testing.T, root string, rows ...[]any) {
	t.Helper()
	statements := []string{"CREATE TABLE ZCLOUDRECORDING (Z_PK INTEGER PRIMARY KEY, ZUNIQUEID VARCHAR, ZPATH VARCHAR, ZENCRYPTEDTITLE VARCHAR, ZFLAGS INTEGER, ZDURATION FLOAT, ZLOCALDURATION FLOAT)"}
	for _, row := range rows {
		values := make([]string, len(row))
		for i, v := range row {
			switch value := v.(type) {
			case nil:
				values[i] = "NULL"
			case string:
				values[i] = common.SQLiteLiteral(value)
			default:
				values[i] = fmt.Sprint(value)
			}
		}
		statements = append(statements, "INSERT INTO ZCLOUDRECORDING (ZUNIQUEID, ZPATH, ZENCRYPTEDTITLE, ZFLAGS, ZDURATION, ZLOCALDURATION) VALUES ("+strings.Join(values, ", ")+")")
	}
	testfixtures.ExecScript(t, filepath.Join(root, "CloudRecordings.db"), statements...)
}

func newRunner(root string, client Uploader, state *State) *Runner {
	return &Runner{
		Account: "zach@example.com", RecordingsPath: root, Extensions: []string{".m4a", ".qta"}, Client: client,
		Logger: &common.RecordingLogger{}, Now: fixedNow, Mode: "incremental", State: state, EnsureApp: noKick,
	}
}

func TestIncrementalRunUploadsAudioThenMetadataAndRecordsState(t *testing.T) {
	root := t.TempDir()
	writeRecordings(t, root, "20260427 100004-40DC0200.m4a", "20260325 145019-DAAC9394.qta", "notes.txt")
	client := &fakeUploader{}
	state := EmptyState("zach@example.com", root)
	runner := newRunner(root, client, state)
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecordingsSeen != 2 || summary.RecordingsSelected != 2 || summary.RecordingsUploaded != 2 || summary.MetadataUploaded != 2 || summary.RecordingsSkipped != 0 {
		t.Fatalf("summary = %+v", summary)
	}
	if len(client.audio) != 2 || client.audio[0]["recorded_at"] != "2026-03-25T14:50:19+00:00" || client.audio[0]["extension"] != ".qta" || client.audio[0]["content_type"] != "audio/quicktime" {
		t.Fatalf("audio uploads = %v", client.audio)
	}
	payload := client.metadata[0]
	recording := payload["recording"].(map[string]any)
	if payload["source"] != "apple_voice_memos" || payload["uploaded_at"] != "2026-05-21T12:00:00+00:00" || recording["recording_id"] != "20260325 145019-DAAC9394" || recording["content_sha256"] != client.audio[0]["sha"] {
		t.Fatalf("metadata = %v", payload)
	}
	if _, present := recording["duration_seconds"]; present {
		t.Fatal("no CloudRecordings.db means no duration keys")
	}
	for _, entry := range state.Entries {
		if !entry.Complete() {
			t.Fatalf("state entry incomplete: %+v", entry)
		}
	}
	// A second run skips everything without touching the network guard.
	client2 := &fakeUploader{}
	checks := 0
	runner2 := newRunner(root, client2, state)
	runner2.BeforeUploadCheck = func() string { checks++; return "" }
	summary, err = runner2.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecordingsSkipped != 2 || summary.RecordingsSelected != 0 || len(client2.audio) != 0 || checks != 0 {
		t.Fatalf("second run = %+v (checks %d)", summary, checks)
	}
	if summary.BytesSkipped != summary.BytesSeen {
		t.Fatalf("bytes skipped %d != seen %d", summary.BytesSkipped, summary.BytesSeen)
	}
}

func TestIncrementalRunDefersPartialRecentAndOversizeFiles(t *testing.T) {
	root := t.TempDir()
	writeRecordings(t, root, "20260430 110736-8BB8E57D.qta", "20260427 100004-40DC0200.m4a", "20260101 000000-BIGBIG00.m4a")
	createCloudRecordingsDB(t, root, []any{"U1", "20260430 110736-8BB8E57D.qta", "New Recording 1", 4100, 9349.66, 6228.63})
	if err := os.WriteFile(filepath.Join(root, "20260101 000000-BIGBIG00.m4a"), bytes.Repeat([]byte("x"), 100), 0o644); err != nil {
		t.Fatal(err)
	}
	// The age rule runs first, in Python and here, so only the recent file
	// may carry a fresh mtime: the partial and oversize files must be old
	// enough to reach their own rules (and log their own reasons).
	old := time.Now().Add(-time.Hour)
	for _, name := range []string{"20260430 110736-8BB8E57D.qta", "20260101 000000-BIGBIG00.m4a"} {
		if err := os.Chtimes(filepath.Join(root, name), old, old); err != nil {
			t.Fatal(err)
		}
	}
	recent := time.Now()
	os.Chtimes(filepath.Join(root, "20260427 100004-40DC0200.m4a"), recent, recent)
	client := &fakeUploader{}
	logger := &common.RecordingLogger{}
	runner := newRunner(root, client, EmptyState("zach@example.com", root))
	runner.Logger = logger
	runner.Now = func() time.Time { return time.Now() }
	runner.MinFileAgeSeconds = 120
	runner.MaxUploadBytes = 50
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecordingsSeen != 3 || summary.RecordingsSelected != 0 || summary.RecordingsDeferred != 3 || len(client.audio) != 0 {
		t.Fatalf("summary = %+v", summary)
	}
	lines := strings.Join(logger.Lines(), "\n")
	if !strings.Contains(lines, "Deferring 20260430 110736-8BB8E57D.qta because Voice Memos reports 9349.66s total but only 6228.63s local audio") {
		t.Fatalf("partial warning missing: %s", lines)
	}
	if !strings.Contains(lines, "Deferring 20260101 000000-BIGBIG00.m4a (100 B): exceeds the 50 B upload ceiling for the current route") {
		t.Fatalf("oversize warning missing: %s", lines)
	}
	if !strings.Contains(lines, "Incremental selection: selected=0 skipped=0 deferred=3") {
		t.Fatalf("selection line missing: %s", lines)
	}
}

func TestIncrementalRunDefersWhenTheNetworkGuardBlocks(t *testing.T) {
	root := t.TempDir()
	writeRecordings(t, root, "20260427 100004-40DC0200.m4a")
	client := &fakeUploader{}
	runner := newRunner(root, client, EmptyState("zach@example.com", root))
	runner.BeforeUploadCheck = func() string { return "blocked Wi-Fi SSID: United Wi-Fi" }
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecordingsSeen != 1 || summary.RecordingsSelected != 1 || summary.RecordingsDeferred != 1 || summary.RecordingsUploaded != 0 || len(client.audio) != 0 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestIncrementalRunContinuesPastAFailingUploadThenReturnsIt(t *testing.T) {
	root := t.TempDir()
	writeRecordings(t, root, "20260101 100000-AAAA0001.m4a", "20260101 110000-BBBB0002.m4a", "20260101 120000-CCCC0003.m4a")
	client := &fakeUploader{fail: func(recordedAt string) error {
		if recordedAt == "2026-01-01T11:00:00+00:00" {
			return errors.New("499 simulated timeout")
		}
		return nil
	}}
	state := EmptyState("zach@example.com", root)
	runner := newRunner(root, client, state)
	summary, err := runner.Sync()
	if err == nil || !strings.Contains(err.Error(), "499 simulated timeout") {
		t.Fatalf("err = %v", err)
	}
	if summary.RecordingsUploaded != 2 || summary.RecordingsSelected != 3 {
		t.Fatalf("summary = %+v", summary)
	}
	var uploaded []string
	for _, a := range client.audio {
		uploaded = append(uploaded, a["recorded_at"])
	}
	sort.Strings(uploaded)
	if !reflect.DeepEqual(uploaded, []string{"2026-01-01T10:00:00+00:00", "2026-01-01T12:00:00+00:00"}) {
		t.Fatalf("uploaded = %v", uploaded)
	}
	failed := state.Entries["20260101 110000-BBBB0002.m4a"]
	if failed.Complete() || failed.LastError != "499 simulated timeout" || failed.ContentSHA256 == "" {
		t.Fatalf("failed entry = %+v", failed)
	}
	if !state.Entries["20260101 100000-AAAA0001.m4a"].Complete() {
		t.Fatal("successful upload must be recorded")
	}
}

func TestRunnerUsesParallelWorkersAndHonoursLimit(t *testing.T) {
	root := t.TempDir()
	var names []string
	for i := 0; i < 6; i++ {
		names = append(names, fmt.Sprintf("20260101 10000%d-AAAA000%d.m4a", i, i))
	}
	writeRecordings(t, root, names...)
	client := &fakeUploader{}
	runner := newRunner(root, client, EmptyState("zach@example.com", root))
	runner.Workers = 3
	runner.Limit = 4
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecordingsSeen != 4 || summary.RecordingsUploaded != 4 {
		t.Fatalf("summary = %+v", summary)
	}
	if client.peak.Load() < 2 {
		t.Fatalf("peak concurrency = %d, want parallel uploads", client.peak.Load())
	}
}

func TestFullModeUploadsEverythingAndStopsOnError(t *testing.T) {
	root := t.TempDir()
	writeRecordings(t, root, "20260101 100000-AAAA0001.m4a", "20260101 110000-BBBB0002.m4a")
	client := &fakeUploader{}
	state := EmptyState("zach@example.com", root)
	runner := newRunner(root, client, state)
	runner.Mode = "full"
	logger := &common.RecordingLogger{}
	runner.Logger = logger
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	// Full mode re-uploads even when the state says complete.
	client2 := &fakeUploader{}
	runner2 := newRunner(root, client2, state)
	runner2.Mode = "full"
	summary, err := runner2.Sync()
	if err != nil || summary.RecordingsUploaded != 2 || summary.RecordingsSkipped != 0 {
		t.Fatalf("summary = %+v err %v", summary, err)
	}
	if !strings.Contains(strings.Join(logger.Lines(), "\n"), "Voice Memos upload summary: seen=2 (") {
		t.Fatalf("full summary line missing: %v", logger.Lines())
	}
	failing := &fakeUploader{fail: func(string) error { return errors.New("boom") }}
	runner3 := newRunner(root, failing, state)
	runner3.Mode = "full"
	if _, err := runner3.Sync(); err == nil || err.Error() != "boom" {
		t.Fatalf("err = %v", err)
	}
	if runner4 := newRunner(root, client, state); func() bool { runner4.Mode = "sideways"; _, err := runner4.Sync(); return err == nil }() {
		t.Fatal("invalid mode must be rejected")
	}
}

func TestRunnerKicksTheAppBeforeScanning(t *testing.T) {
	root := t.TempDir()
	var order []string
	runner := newRunner(root, &fakeUploader{}, EmptyState("zach@example.com", root))
	runner.EnsureApp = func(path string, logger common.Logger) AppKick {
		order = append(order, "kick:"+path)
		if entries, _ := os.ReadDir(root); len(entries) == 0 {
			writeRecordings(t, root, "20260101 100000-AAAA0001.m4a")
		}
		return AppKick{}
	}
	summary, err := runner.Sync()
	if err != nil || summary.RecordingsSeen != 1 || len(order) != 1 || order[0] != "kick:"+root {
		t.Fatalf("summary = %+v err %v order %v", summary, err, order)
	}
}

// --- wire bytes through the real ingest client --------------------------------

type recorded struct {
	Path          string
	Query         map[string]string
	Body          []byte
	ContentType   string
	Authorization string
}

// fakeSQLRows is what the fake app's sql tool answers, as ndjson lines.
var fakeSQLRows []map[string]any

func newFakeApp(t *testing.T) (*httptest.Server, *[]recorded) {
	t.Helper()
	var mu sync.Mutex
	requests := &[]recorded{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := map[string]string{}
		for key := range r.URL.Query() {
			query[key] = r.URL.Query().Get(key)
		}
		mu.Lock()
		*requests = append(*requests, recorded{Path: r.URL.Path, Query: query, Body: body, ContentType: r.Header.Get("Content-Type"), Authorization: r.Header.Get("Authorization")})
		mu.Unlock()
		if r.URL.Path == "/healthz" {
			w.Write([]byte("ok"))
			return
		}
		if r.URL.Path == "/api/tools/sql" {
			var lines []string
			for _, row := range fakeSQLRows {
				encoded, _ := json.Marshal(row)
				lines = append(lines, string(encoded))
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"rows": strings.Join(lines, "\n"), "error": ""}})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"storage_backend": "google_drive", "storage_key": "key", "storage_file_id": "file-1"})
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func TestWirePayloadsMatchThePythonUploader(t *testing.T) {
	server, requests := newFakeApp(t)
	root := t.TempDir()
	writeRecordings(t, root, "20260427 100004-40DC0200.m4a")
	createCloudRecordingsDB(t, root, []any{"U1", "20260427 100004-40DC0200.m4a", "New Recording 3", 4100, 12.5, 12.5})
	client, err := ingestclient.New(server.URL, "secret", ingestclient.WithSleep(func(time.Duration) {}))
	if err != nil {
		t.Fatal(err)
	}
	runner := newRunner(root, client, EmptyState("zach@example.com", root))
	runner.MaxUploadBytes = client.EffectiveMaxUploadBytes()
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(*requests) != 2 {
		t.Fatalf("requests = %d", len(*requests))
	}
	audio, metadata := (*requests)[0], (*requests)[1]
	if audio.Path != "/ingest/voice-memos/audio" || audio.ContentType != "audio/mp4" || string(audio.Body) != "audio-20260427 100004-40DC0200.m4a" {
		t.Fatalf("audio request = %+v", audio)
	}
	if audio.Query["recorded_at"] != "2026-04-27T10:00:04+00:00" || audio.Query["extension"] != ".m4a" || audio.Query["content_sha256"] != common.BytesSHA256(audio.Body) || audio.Query["sig"] == "" {
		t.Fatalf("audio query = %v", audio.Query)
	}
	if metadata.Path != "/ingest/voice-memos/metadata" || metadata.Query["audio_content_sha256"] != common.BytesSHA256(audio.Body) || metadata.Query["recorded_at"] != "2026-04-27T10:00:04+00:00" {
		t.Fatalf("metadata request = %+v", metadata)
	}
	want := `{"account":"zach@example.com","recording":{"content_sha256":"` + common.BytesSHA256(audio.Body) + `","content_type":"audio/mp4","duration_seconds":12.5,"extension":".m4a","file_created_at":"`
	if !strings.HasPrefix(string(metadata.Body), want) {
		t.Fatalf("metadata body = %s", metadata.Body)
	}
	if !strings.HasSuffix(string(metadata.Body), `"local_duration_seconds":12.5,"original_path":"`+filepath.Join(root, "20260427 100004-40DC0200.m4a")+`","recorded_at":"2026-04-27T10:00:04+00:00","recording_id":"20260427 100004-40DC0200","size_bytes":34,"title":"20260427 100004-40DC0200"},"schema_version":1,"source":"apple_voice_memos","uploaded_at":"2026-05-21T12:00:00+00:00"}`) {
		t.Fatalf("metadata body = %s", metadata.Body)
	}
}

// --- app kick ------------------------------------------------------------------

const systemStore = "/Users/zrl/Library/Group Containers/group.com.apple.VoiceMemos.shared/Recordings"

type fakeProcesses struct {
	running map[string]bool
	launch  int
	calls   [][]string
}

func (f *fakeProcesses) run(args ...string) (int, string, error) {
	f.calls = append(f.calls, args)
	if args[0] == "pgrep" {
		if f.running[args[2]] {
			return 0, "", nil
		}
		return 1, "", nil
	}
	if f.launch != 0 {
		return f.launch, "boom", nil
	}
	return 0, "", nil
}

func TestAppKickLaunchesByBundleIDOnlyForAFrozenSystemStore(t *testing.T) {
	env := common.Getenv(func(string) string { return "" })
	logger := &common.RecordingLogger{}
	procs := &fakeProcesses{running: map[string]bool{}}
	kick := EnsureVoiceMemosAppRunning(systemStore, logger, env, "darwin", procs.run)
	if !kick.Attempted || !kick.Launched || kick.Reason != "launched" {
		t.Fatalf("kick = %+v", kick)
	}
	if !reflect.DeepEqual(procs.calls[len(procs.calls)-1], []string{"open", "-g", "-j", "-b", "com.apple.VoiceMemos"}) {
		t.Fatalf("calls = %v", procs.calls)
	}
	procs = &fakeProcesses{running: map[string]bool{"voicememod": true}}
	kick = EnsureVoiceMemosAppRunning(systemStore, logger, env, "darwin", procs.run)
	if !kick.Attempted || kick.Launched || kick.Reason != "voicememod already running" || len(procs.calls) != 1 {
		t.Fatalf("daemon kick = %+v calls %v", kick, procs.calls)
	}
	procs = &fakeProcesses{running: map[string]bool{}}
	if kick := EnsureVoiceMemosAppRunning(t.TempDir(), logger, env, "darwin", procs.run); kick.Attempted || len(procs.calls) != 0 {
		t.Fatalf("test store kick = %+v", kick)
	}
	off := common.Getenv(func(name string) string {
		if name == "VOICE_MEMOS_OPEN_APP" {
			return "0"
		}
		return ""
	})
	if kick := EnsureVoiceMemosAppRunning(systemStore, logger, off, "darwin", procs.run); kick.Attempted || kick.Reason != "disabled by VOICE_MEMOS_OPEN_APP" {
		t.Fatalf("kill switch kick = %+v", kick)
	}
	if kick := EnsureVoiceMemosAppRunning(systemStore, logger, env, "linux", procs.run); kick.Attempted || len(procs.calls) != 0 {
		t.Fatalf("linux kick = %+v", kick)
	}
	procs = &fakeProcesses{running: map[string]bool{}, launch: 1}
	kick = EnsureVoiceMemosAppRunning(systemStore, logger, env, "darwin", procs.run)
	if !kick.Attempted || kick.Launched || kick.Reason != "launch failed: boom" {
		t.Fatalf("failed launch kick = %+v", kick)
	}
}

// --- write-back ----------------------------------------------------------------

func TestIsAutoNamed(t *testing.T) {
	if !IsAutoNamed("Anything At All", AutoNamedFlag|4) || !IsAutoNamed("Some Location Name 3", 0x1604) {
		t.Fatal("flag bit must be trusted")
	}
	if !IsAutoNamed("New Recording 12", 4) || !IsAutoNamed("New Recording 1", 0) {
		t.Fatal("default title pattern must match")
	}
	if IsAutoNamed("Board prep walkthrough", 4) || IsAutoNamed("New Recording 12 with my notes", 0) || IsAutoNamed("Old Location Name 3", 0x604) {
		t.Fatal("user titles must never match")
	}
}

func TestSanitizeTitle(t *testing.T) {
	if got, ok := SanitizeTitle("  Weekly \n sync\tnotes  "); !ok || got != "Weekly sync notes" {
		t.Fatalf("got %q %v", got, ok)
	}
	if got, _ := SanitizeTitle("plan\x00ning"); got != "planning" {
		t.Fatalf("got %q", got)
	}
	if got, ok := SanitizeTitle(strings.Repeat("x", 500)); !ok || len(got) != 200 {
		t.Fatalf("got len %d", len(got))
	}
	if got, ok := SanitizeTitle(strings.Repeat("é", 250)); !ok || len([]rune(got)) != 200 {
		t.Fatalf("length must be in code points, got %d", len([]rune(got)))
	}
	for _, empty := range []string{"   \n\t  ", ""} {
		if _, ok := SanitizeTitle(empty); ok {
			t.Fatalf("%q must sanitize to nothing", empty)
		}
	}
}

func TestLoadLocalRecordingTitles(t *testing.T) {
	root := t.TempDir()
	if titles, err := LoadLocalRecordingTitles(root); err != nil || len(titles) != 0 {
		t.Fatalf("missing db = %v %v", titles, err)
	}
	createCloudRecordingsDB(t, root,
		[]any{"AAAA-1111", "20260101 090000-AAAA1111.qta", "New Recording 3", 4100, nil, nil},
		[]any{"BBBB-2222", "20260102 100000-BBBB2222.m4a", "My handwritten title", 4, nil, nil},
		[]any{nil, "20260103 110000-CCCC3333.qta", "New Recording 4", 4100, nil, nil},
		[]any{"DDDD-4444", nil, "New Recording 5", 4100, nil, nil},
	)
	titles, err := LoadLocalRecordingTitles(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []LocalRecordingTitle{
		{UniqueID: "AAAA-1111", RecordingID: "20260101 090000-AAAA1111", Title: "New Recording 3", Flags: 4100, Filename: "20260101 090000-AAAA1111.qta"},
		{UniqueID: "BBBB-2222", RecordingID: "20260102 100000-BBBB2222", Title: "My handwritten title", Flags: 4, Filename: "20260102 100000-BBBB2222.m4a"},
	}
	if !reflect.DeepEqual(titles, want) {
		t.Fatalf("titles = %+v", titles)
	}
}

func local(uniqueID, recordingID, title string, flags int64) LocalRecordingTitle {
	return LocalRecordingTitle{UniqueID: uniqueID, RecordingID: recordingID, Title: title, Flags: flags, Filename: recordingID + ".qta"}
}

func TestBuildRenamePlan(t *testing.T) {
	items := []LocalRecordingTitle{
		local("A", "20260101 090000-AAAA1111", "New Recording 3", 4100),
		local("B", "20260102 100000-BBBB2222", "My handwritten title", 4),
		local("C", "20260103 110000-CCCC3333", "Some Street 2", 0x1604),
		local("D", "20260104 120000-DDDD4444", "New Recording 9", 4100),
	}
	enriched := map[string]string{
		"20260101 090000-AAAA1111": "Quarterly planning discussion",
		"20260102 100000-BBBB2222": "Should never be applied",
		"20260103 110000-CCCC3333": "Vendor onboarding call",
	}
	plan := BuildRenamePlan(items, enriched, 0)
	want := []RenamePlanItem{
		{UniqueID: "C", RecordingID: "20260103 110000-CCCC3333", OldTitle: "Some Street 2", NewTitle: "Vendor onboarding call"},
		{UniqueID: "A", RecordingID: "20260101 090000-AAAA1111", OldTitle: "New Recording 3", NewTitle: "Quarterly planning discussion"},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("plan = %+v", plan)
	}
	noop := BuildRenamePlan([]LocalRecordingTitle{
		local("A", "20260101 090000-AAAA1111", "Quarterly planning discussion", 4100),
		local("B", "20260102 100000-BBBB2222", "New Recording 4", 4100),
	}, map[string]string{"20260101 090000-AAAA1111": "Quarterly planning discussion", "20260102 100000-BBBB2222": "   "}, 0)
	if len(noop) != 0 {
		t.Fatalf("noop plan = %+v", noop)
	}
	limited := BuildRenamePlan([]LocalRecordingTitle{
		local("A", "20260101 090000-AAAA1111", "New Recording 1", 4100),
		local("B", "20260301 090000-BBBB2222", "New Recording 2", 4100),
		local("C", "20260201 090000-CCCC3333", "New Recording 3", 4100),
	}, map[string]string{"20260101 090000-AAAA1111": "January review", "20260301 090000-BBBB2222": "March review", "20260201 090000-CCCC3333": "February review"}, 2)
	if len(limited) != 2 || limited[0].NewTitle != "March review" || limited[1].NewTitle != "February review" {
		t.Fatalf("limited plan = %+v", limited)
	}
}

func TestResolveEffectiveTitlesPrefersStemThenSHA(t *testing.T) {
	items := []LocalRecordingTitle{
		local("A", "20260101 090000-AAAA1111", "New Recording 3", 4100),
		local("B", "20260430 140736-BBBB2222", "Some Park", 4100),
		local("C", "20260501 090000-CCCC3333", "New Recording 9", 4100),
	}
	titles := EnrichedTitles{
		ByRecordingID:   map[string]string{"20260101 090000-AAAA1111": "Quarterly planning discussion", "20260430 110736-BBBB2222": "Park walk debrief"},
		ByContentSHA256: map[string]string{"aaa111": "Should lose to the stem match", "bbb222": "Park walk debrief"},
	}
	shaByFilename := map[string]string{"20260101 090000-AAAA1111.qta": "aaa111", "20260430 140736-BBBB2222.qta": "bbb222"}
	got := ResolveEffectiveTitles(items, titles, shaByFilename)
	want := map[string]string{"20260101 090000-AAAA1111": "Quarterly planning discussion", "20260430 140736-BBBB2222": "Park walk debrief"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("effective = %v", got)
	}
	if got := ResolveEffectiveTitles(items[:1], titles, nil); !reflect.DeepEqual(got, map[string]string{"20260101 090000-AAAA1111": "Quarterly planning discussion"}) {
		t.Fatalf("stems only = %v", got)
	}
}

type fakeQuery struct {
	rows       []map[string]any
	err        error
	statements []string
	questions  []string
}

func (f *fakeQuery) query(question, statement string) ([]map[string]any, error) {
	f.questions = append(f.questions, question)
	f.statements = append(f.statements, statement)
	return f.rows, f.err
}

func TestFetchEnrichedTitlesQueriesTheCatalogRelation(t *testing.T) {
	q := &fakeQuery{rows: []map[string]any{
		{"recording_id": "20260101 090000-AAAA1111", "content_sha256": "aaa111", "title": "Quarterly planning discussion"},
		{"recording_id": "20260102 100000-BBBB2222", "content_sha256": "bbb222", "title": "Vendor onboarding call"},
		{"recording_id": "20260103 100000-CCCC3333", "content_sha256": "bbb222", "title": "Second title for the same sha"},
		{"recording_id": "", "title": "no id"},
		{"recording_id": "x", "title": nil},
	}}
	titles, err := FetchEnrichedTitles(q.query, "quote'test@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if titles.ByRecordingID["20260101 090000-AAAA1111"] != "Quarterly planning discussion" || len(titles.ByRecordingID) != 3 {
		t.Fatalf("by id = %v", titles.ByRecordingID)
	}
	if titles.ByContentSHA256["bbb222"] != "Vendor onboarding call" || len(titles.ByContentSHA256) != 2 {
		t.Fatalf("by sha = %v", titles.ByContentSHA256)
	}
	sql := q.statements[0]
	for _, fragment := range []string{"FROM derived_voice_memos.enrichments ", "content_sha256", "'quote''test@example.com'", "status = 'completed'", "DISTINCT ON (recording_id)"} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("sql %q lacks %q", sql, fragment)
		}
	}
	if strings.Contains(sql, "@apple_voice_memos_enrichments") || strings.Contains(sql, `"derived_voice_memos"`) || q.questions[0] != "Voice memo enriched titles for app write-back" {
		t.Fatalf("sql %q / question %q", sql, q.questions[0])
	}
	if _, err := FetchEnrichedTitles((&fakeQuery{err: errors.New("relation does not exist")}).query, "z"); err == nil || !strings.Contains(err.Error(), "relation does not exist") {
		t.Fatalf("err = %v", err)
	}
}

type fakeWriter struct {
	calls   []map[string]any
	results map[string]string
}

func (f *fakeWriter) write(storePath string, items []RenamePlanItem, author string, dryRun bool) ([]WriteResult, error) {
	f.calls = append(f.calls, map[string]any{"store_path": storePath, "items": items, "author": author, "dry_run": dryRun})
	var results []WriteResult
	for _, item := range items {
		status, ok := f.results[item.UniqueID]
		if !ok {
			status = StatusRenamed
		}
		results = append(results, WriteResult{UniqueID: item.UniqueID, Status: status})
	}
	return results, nil
}

func writebackFixture(t *testing.T, writer *fakeWriter, dryRun bool, limit int) (*WritebackRunner, string, *common.RecordingLogger) {
	root := t.TempDir()
	createCloudRecordingsDB(t, root,
		[]any{"A", "20260101 090000-AAAA1111.qta", "New Recording 3", 4100, nil, nil},
		[]any{"B", "20260102 100000-BBBB2222.qta", "My handwritten title", 4, nil, nil},
	)
	q := &fakeQuery{rows: []map[string]any{
		{"recording_id": "20260101 090000-AAAA1111", "title": "Quarterly planning discussion"},
		{"recording_id": "20260102 100000-BBBB2222", "title": "Should never be applied"},
	}}
	logger := &common.RecordingLogger{}
	return &WritebackRunner{RecordingsPath: root, Account: "zach@example.com", Query: q.query, Logger: logger, Writer: writer.write, Limit: limit, DryRun: dryRun}, root, logger
}

func TestWritebackRunnerAppliesPlannedRenames(t *testing.T) {
	writer := &fakeWriter{}
	runner, root, logger := writebackFixture(t, writer, false, 0)
	summary, err := runner.Run()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Renamed != 1 || summary.Planned != 1 || summary.LocalRecordings != 2 || summary.AutoNamed != 1 || summary.EnrichedTitles != 2 || summary.Skipped != 0 || summary.DryRun {
		t.Fatalf("summary = %+v", summary)
	}
	if len(writer.calls) != 1 {
		t.Fatalf("calls = %v", writer.calls)
	}
	call := writer.calls[0]
	items := call["items"].([]RenamePlanItem)
	if call["store_path"] != filepath.Join(root, "CloudRecordings.db") || len(items) != 1 || items[0].UniqueID != "A" || call["author"] != "com.zachlatta.pdw.voice-memo-writeback" || call["dry_run"] != false {
		t.Fatalf("call = %v", call)
	}
	if !strings.Contains(strings.Join(logger.Lines(), "\n"), "will rename 20260101 090000-AAAA1111: 'New Recording 3' -> 'Quarterly planning discussion'") {
		t.Fatalf("log = %v", logger.Lines())
	}
}

func TestWritebackRunnerDryRunNeverWritesAndSkipsAreCounted(t *testing.T) {
	writer := &fakeWriter{}
	runner, _, logger := writebackFixture(t, writer, true, 0)
	summary, err := runner.Run()
	if err != nil {
		t.Fatal(err)
	}
	if !summary.DryRun || summary.Planned != 1 || summary.Renamed != 0 || len(writer.calls) != 0 {
		t.Fatalf("summary = %+v calls %v", summary, writer.calls)
	}
	if !strings.Contains(strings.Join(logger.Lines(), "\n"), "[dry-run] would rename") {
		t.Fatalf("log = %v", logger.Lines())
	}
	skipping := &fakeWriter{results: map[string]string{"A": StatusSkippedNotAutoNamed}}
	runner, _, logger = writebackFixture(t, skipping, false, 0)
	summary, err = runner.Run()
	if err != nil || summary.Renamed != 0 || summary.Skipped != 1 {
		t.Fatalf("summary = %+v err %v", summary, err)
	}
	if !strings.Contains(strings.Join(logger.Warnings, "\n"), "Voice Memos write-back skipped A: skipped_not_auto_named") {
		t.Fatalf("warnings = %v", logger.Warnings)
	}
}

func TestWritebackRunnerSkipsTheQueryWhenNothingIsAutoNamed(t *testing.T) {
	root := t.TempDir()
	createCloudRecordingsDB(t, root, []any{"B", "20260102 100000-BBBB2222.qta", "My handwritten title", 4, nil, nil})
	q := &fakeQuery{err: errors.New("must not be called")}
	runner := &WritebackRunner{RecordingsPath: root, Account: "z", Query: q.query, Logger: &common.RecordingLogger{}, Writer: (&fakeWriter{}).write}
	summary, err := runner.Run()
	if err != nil || summary.LocalRecordings != 1 || summary.AutoNamed != 0 || len(q.statements) != 0 {
		t.Fatalf("summary = %+v err %v", summary, err)
	}
}

func TestWritebackEnabledFromEnvDefaultsOn(t *testing.T) {
	for value, want := range map[string]bool{"": true, "1": true, "0": false, "false": false, "off": false} {
		env := common.Getenv(func(string) string { return value })
		if WritebackEnabledFromEnv(env) != want {
			t.Fatalf("%q -> %v", value, !want)
		}
	}
}

func TestPyRepr(t *testing.T) {
	cases := map[string]string{"New Recording 3": "'New Recording 3'", "it's": `"it's"`, "a\\b": `'a\\b'`, "tab\there": `'tab\there'`, "é": "'é'"}
	for in, want := range cases {
		if got := pyRepr(in); got != want {
			t.Errorf("pyRepr(%q) = %s, want %s", in, got, want)
		}
	}
}

// --- store writer --------------------------------------------------------------

func TestLoadCachedModelDataInflatesTheRawDeflateArchive(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "CloudRecordings.db")
	archive := []byte("bplist00 pretend keyed archive of an NSManagedObjectModel")
	var compressed bytes.Buffer
	writer, _ := flate.NewWriter(&compressed, flate.BestCompression)
	writer.Write(archive)
	writer.Close()
	testfixtures.ExecScript(t, store,
		"CREATE TABLE Z_MODELCACHE (Z_CONTENT BLOB)",
		"INSERT INTO Z_MODELCACHE (Z_CONTENT) VALUES (X'"+fmt.Sprintf("%X", compressed.Bytes())+"')",
	)
	data, err := LoadCachedModelData(store)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, archive) {
		t.Fatalf("data = %q", data)
	}
	empty := filepath.Join(root, "empty.db")
	testfixtures.ExecScript(t, empty, "CREATE TABLE Z_MODELCACHE (Z_CONTENT BLOB)")
	if _, err := LoadCachedModelData(empty); err == nil || !strings.Contains(err.Error(), "no cached managed object model") {
		t.Fatalf("err = %v", err)
	}
	none := filepath.Join(root, "none.db")
	testfixtures.ExecScript(t, none, "CREATE TABLE ZCLOUDRECORDING (Z_PK INTEGER)")
	if _, err := LoadCachedModelData(none); err == nil {
		t.Fatal("a store without Z_MODELCACHE must fail")
	}
}

func TestBuildWritebackHelperRebuildsOnlyWhenTheSourceChanges(t *testing.T) {
	prev := hostOS
	hostOS = "darwin"
	t.Cleanup(func() { hostOS = prev })
	root := filepath.Join(t.TempDir(), "helper")
	builds := 0
	compile := func(source, output string) error {
		builds++
		src, err := os.ReadFile(source)
		if err != nil || !bytes.Equal(src, writebackHelperSource) {
			t.Fatalf("compile got a different source (%v)", err)
		}
		return os.WriteFile(output, []byte("#!/bin/sh\n"), 0o755)
	}
	binary, err := BuildWritebackHelper(root, compile)
	if err != nil {
		t.Fatal(err)
	}
	if binary != HelperBinaryPath(root) || !HelperIsCurrent(root) || builds != 1 {
		t.Fatalf("binary %s current %v builds %d", binary, HelperIsCurrent(root), builds)
	}
	if _, err := BuildWritebackHelper(root, compile); err != nil || builds != 1 {
		t.Fatalf("cached helper rebuilt (builds %d, err %v)", builds, err)
	}
	os.WriteFile(filepath.Join(root, "source.sha256"), []byte("stale\n"), 0o644)
	if _, err := BuildWritebackHelper(root, compile); err != nil || builds != 2 {
		t.Fatalf("stale stamp not rebuilt (builds %d, err %v)", builds, err)
	}
	failing := func(string, string) error { return errors.New("swiftc exploded") }
	os.Remove(filepath.Join(root, "source.sha256"))
	if _, err := BuildWritebackHelper(root, failing); err == nil || !strings.Contains(err.Error(), "swiftc exploded") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunStoreWriterHelperRoundTripsThePlan(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "CloudRecordings.db")
	var compressed bytes.Buffer
	writer, _ := flate.NewWriter(&compressed, flate.DefaultCompression)
	writer.Write([]byte("model"))
	writer.Close()
	testfixtures.ExecScript(t, store, "CREATE TABLE Z_MODELCACHE (Z_CONTENT BLOB)", "INSERT INTO Z_MODELCACHE (Z_CONTENT) VALUES (X'"+fmt.Sprintf("%X", compressed.Bytes())+"')")
	// A stand-in helper that echoes the request back as results, so the
	// stdin/stdout contract is exercised without swiftc or Core Data.
	helper := filepath.Join(root, "helper.sh")
	os.WriteFile(helper, []byte(`#!/bin/sh
input=$(cat)
model=$(echo "$input" | sed -n 's/.*"model_path":"\([^"]*\)".*/\1/p')
[ "$(cat "$model")" = "model" ] || { echo "model missing" >&2; exit 1; }
echo "$input" | grep -q '"author":"com.zachlatta.pdw.voice-memo-writeback"' || { echo "author missing" >&2; exit 1; }
echo "$input" | grep -q '"dry_run":true' || { echo "dry_run missing" >&2; exit 1; }
echo '[{"unique_id":"A","status":"would_rename"},{"unique_id":"B","status":"skipped_missing"}]'
`), 0o755)
	results, err := RunStoreWriterHelper(helper, store, []RenamePlanItem{{UniqueID: "A", NewTitle: "x"}, {UniqueID: "B", NewTitle: "y"}}, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(results, []WriteResult{{"A", StatusWouldRename}, {"B", StatusSkippedMissing}}) {
		t.Fatalf("results = %+v", results)
	}
	failing := filepath.Join(root, "failing.sh")
	os.WriteFile(failing, []byte("#!/bin/sh\necho 'could not open Voice Memos store' >&2\nexit 1\n"), 0o755)
	if _, err := RunStoreWriterHelper(failing, store, nil, "", false); err == nil || !strings.Contains(err.Error(), "could not open Voice Memos store") {
		t.Fatalf("err = %v", err)
	}
}

// --- CLI -------------------------------------------------------------------------

func TestParseArgsDefaultsAndValidation(t *testing.T) {
	opts, err := ParseArgs(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Limit != 0 || opts.Workers != 0 || opts.Mode != "incremental" || opts.MinFileAgeSeconds != 120 || opts.WritebackLimit != 0 || opts.NoWriteback || opts.WritebackOnly || opts.WritebackDryRun || opts.NetworkDiagnostics {
		t.Fatalf("defaults = %+v", opts)
	}
	if opts.StateFile != DefaultStateFile() || opts.LockFile != strings.TrimSuffix(DefaultStateFile(), ".json")+".lock" {
		t.Fatalf("paths = %s %s", opts.StateFile, opts.LockFile)
	}
	opts, err = ParseArgs([]string{"--mode", "full", "--limit", "5", "--workers", "3", "--state-file", "/tmp/s.json", "--lock-file", "/tmp/s.lock", "--min-file-age-seconds", "0", "--writeback-only", "--writeback-dry-run", "--writeback-limit", "2", "--no-writeback", "--recordings-path", "/tmp/rec"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Mode != "full" || opts.Limit != 5 || opts.Workers != 3 || opts.StateFile != "/tmp/s.json" || opts.LockFile != "/tmp/s.lock" || opts.MinFileAgeSeconds != 0 || !opts.WritebackOnly || !opts.WritebackDryRun || opts.WritebackLimit != 2 || !opts.NoWriteback || opts.RecordingsPath != "/tmp/rec" {
		t.Fatalf("opts = %+v", opts)
	}
	if _, err := ParseArgs([]string{"--mode", "sideways"}, io.Discard); err == nil || !strings.Contains(err.Error(), "invalid choice") {
		t.Fatalf("err = %v", err)
	}
	if _, err := ParseArgs([]string{"--bogus"}, io.Discard); err == nil {
		t.Fatal("unknown flag accepted")
	}
	var help bytes.Buffer
	if _, err := ParseArgs([]string{"--help"}, &help); !errors.Is(err, flag.ErrHelp) || !strings.Contains(help.String(), "--writeback-dry-run") {
		t.Fatalf("help: %v %q", err, help.String())
	}
}

func TestSettingsFromEnv(t *testing.T) {
	env := common.Getenv(func(name string) string {
		return map[string]string{"GMAIL_ACCOUNTS": "first@example.com, second@example.com", "VOICE_MEMOS_EXTENSIONS": "M4A, .qta"}[name]
	})
	settings, err := SettingsFromEnv(env)
	if err != nil {
		t.Fatal(err)
	}
	if settings.Account != "first@example.com" || !reflect.DeepEqual(settings.Extensions, []string{".m4a", ".qta"}) || !strings.HasSuffix(settings.RecordingsPath, "group.com.apple.VoiceMemos.shared/Recordings") || strings.HasPrefix(settings.RecordingsPath, "~") {
		t.Fatalf("settings = %+v", settings)
	}
	if _, err := SettingsFromEnv(common.Getenv(func(string) string { return "" })); err == nil {
		t.Fatal("missing account must fail")
	}
}

func TestGetenvWithDotenvLetsTheEnvironmentWin(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, ".env"), []byte("VOICE_MEMOS_ACCOUNT=dotenv@example.com\nPDW_API_URL=\"https://dotenv.example\"\n"), 0o644)
	getenv := GetenvWithDotenv(func(name string) string {
		if name == "PDW_API_URL" {
			return "https://env.example"
		}
		return ""
	}, dir)
	if getenv("VOICE_MEMOS_ACCOUNT") != "dotenv@example.com" || getenv("PDW_API_URL") != "https://env.example" || getenv("MISSING") != "" {
		t.Fatalf("dotenv overlay wrong: %s %s", getenv("VOICE_MEMOS_ACCOUNT"), getenv("PDW_API_URL"))
	}
}

func runCLI(t *testing.T, args []string, env map[string]string, cfg ingestclient.Config) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(args, strings.NewReader(""), &stdout, &stderr, func(name string) string { return env[name] }, cfg)
	return code, stdout.String(), stderr.String()
}

func TestRunExitCodes(t *testing.T) {
	previous := beforeUploadCheck
	beforeUploadCheck = func(*common.NetworkPolicy, string, time.Duration) func() string { return func() string { return "" } }
	t.Cleanup(func() { beforeUploadCheck = previous })

	server, requests := newFakeApp(t)
	cfg := ingestclient.Config{BaseURL: server.URL, Token: "secret"}
	root := t.TempDir()
	writeRecordings(t, root, "20260427 100004-40DC0200.m4a")
	old := time.Now().Add(-time.Hour)
	os.Chtimes(filepath.Join(root, "20260427 100004-40DC0200.m4a"), old, old)
	work := t.TempDir()
	env := map[string]string{"VOICE_MEMOS_ACCOUNT": "zach@example.com", "VOICE_MEMOS_RECORDINGS_PATH": root, "PDW_INGEST_PROJECT_DIR": work}
	stateFile := filepath.Join(work, "state.json")
	lockFile := filepath.Join(work, "state.lock")
	base := []string{"--state-file", stateFile, "--lock-file", lockFile, "--no-writeback"}

	if code, out, _ := runCLI(t, []string{"--help"}, env, cfg); code != 0 || !strings.Contains(out, "usage: pdw ingest voice-memos") {
		t.Fatalf("help: %d %q", code, out)
	}
	if code, _, errOut := runCLI(t, []string{"--mode", "nope"}, env, cfg); code != 2 || !strings.Contains(errOut, "invalid choice") {
		t.Fatalf("usage error: %d %q", code, errOut)
	}
	if code, _, errOut := runCLI(t, base, map[string]string{}, cfg); code != 1 || !strings.Contains(errOut, "VOICE_MEMOS_ACCOUNT or GMAIL_ACCOUNTS") {
		t.Fatalf("missing account: %d %q", code, errOut)
	}
	if code, _, errOut := runCLI(t, base, env, ingestclient.Config{}); code != 1 || !strings.Contains(errOut, "PDW_API_URL") {
		t.Fatalf("missing app config: %d %q", code, errOut)
	}
	if _, err := os.Stat(stateFile); err != nil {
		t.Fatal("the state must be saved even when the run fails")
	}

	code, out, errOut := runCLI(t, base, env, cfg)
	if code != 0 || errOut != "" {
		t.Fatalf("happy path: %d %q %q", code, out, errOut)
	}
	if !strings.Contains(out, "Voice Memos upload complete: seen=1 selected=1 uploaded=1 skipped=0 deferred=0 metadata=1") || !strings.Contains(out, "[1/1] upload 20260427 100004-40DC0200.m4a") {
		t.Fatalf("stdout = %q", out)
	}
	if len(*requests) != 2 {
		t.Fatalf("requests = %d", len(*requests))
	}
	state := LoadState(stateFile, "zach@example.com", root)
	if !state.Entries["20260427 100004-40DC0200.m4a"].Complete() {
		t.Fatalf("state = %+v", state.Entries)
	}

	lock, acquired, err := common.TryRunLock(lockFile)
	if err != nil || !acquired {
		t.Fatal("could not take the lock for the test")
	}
	code, out, _ = runCLI(t, base, env, cfg)
	lock.Release()
	if code != 0 || !strings.Contains(out, "Voice Memos upload skipped: another uploader run is active") {
		t.Fatalf("held lock: %d %q", code, out)
	}

	// A failing app after the run started is exit 1 with the state saved.
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "nope", http.StatusBadRequest) }))
	defer failing.Close()
	writeRecordings(t, root, "20260101 100000-AAAA0001.m4a")
	os.Chtimes(filepath.Join(root, "20260101 100000-AAAA0001.m4a"), old, old)
	code, _, errOut = runCLI(t, base, env, ingestclient.Config{BaseURL: failing.URL, Token: "secret"})
	if code != 1 || !strings.Contains(errOut, "error:") {
		t.Fatalf("failing app: %d %q", code, errOut)
	}
	state = LoadState(stateFile, "zach@example.com", root)
	if entry := state.Entries["20260101 100000-AAAA0001.m4a"]; entry.Complete() || entry.LastError == "" {
		t.Fatalf("failure not recorded: %+v", entry)
	}

	// --writeback-only never uploads and needs the app credentials.
	if code, _, errOut := runCLI(t, []string{"--state-file", stateFile, "--lock-file", lockFile, "--writeback-only"}, env, ingestclient.Config{}); code != 1 || !strings.Contains(errOut, "write-back requires PDW_API_URL") {
		t.Fatalf("writeback without config: %d %q", code, errOut)
	}
	before := len(*requests)
	code, out, errOut = runCLI(t, []string{"--state-file", stateFile, "--lock-file", lockFile, "--writeback-only", "--writeback-dry-run"}, env, cfg)
	if code != 0 || len(*requests) != before || !strings.Contains(out, "Voice Memos write-back complete: local=0 auto_named=0 titles=0 planned=0 renamed=0 skipped=0 (dry run)") {
		t.Fatalf("writeback only: %d %q %q", code, out, errOut)
	}
}

func TestEnrichedTitlesSQLIsThePythonStatementVerbatim(t *testing.T) {
	// fetch_enriched_titles rendered the catalog relation unquoted; the app's
	// sql tool sees the same text from both uploaders.
	want := "SELECT DISTINCT ON (recording_id) recording_id, content_sha256, title " +
		"FROM derived_voice_memos.enrichments " +
		"WHERE status = 'completed' AND title IS NOT NULL " +
		"AND account = 'zach@example.com' " +
		"ORDER BY recording_id, created_at DESC"
	if got := EnrichedTitlesSQL("zach@example.com"); got != want {
		t.Fatalf("sql = %q\nwant %q", got, want)
	}
}

func TestWritebackRunnerMatchesDriftedFilenamesBySHA(t *testing.T) {
	// Voice Memos rebases filename timestamps across timezone changes, so the
	// warehouse can know a memo only under an older stem; the audio sha from
	// the upload state still identifies it.
	root := t.TempDir()
	createCloudRecordingsDB(t, root, []any{"A", "20260430 140736-AAAA1111.qta", "Some Park", 4100, nil, nil})
	q := &fakeQuery{rows: []map[string]any{{"recording_id": "20260430 110736-AAAA1111", "content_sha256": "aaa111", "title": "Park walk debrief"}}}
	writer := &fakeWriter{}
	runner := &WritebackRunner{RecordingsPath: root, Account: "zach@example.com", Query: q.query, Logger: &common.RecordingLogger{}, Writer: writer.write,
		SHAByFilename: map[string]string{"20260430 140736-AAAA1111.qta": "aaa111"}}
	summary, err := runner.Run()
	if err != nil || summary.Planned != 1 || summary.Renamed != 1 {
		t.Fatalf("summary = %+v err %v", summary, err)
	}
	item := writer.calls[0]["items"].([]RenamePlanItem)[0]
	if item.UniqueID != "A" || item.NewTitle != "Park walk debrief" {
		t.Fatalf("item = %+v", item)
	}
}

func TestDefaultWorkersPerMode(t *testing.T) {
	if DefaultWorkers("incremental", 0) != 1 || DefaultWorkers("full", 0) != 8 || DefaultWorkers("incremental", 3) != 3 || DefaultWorkers("full", 2) != 2 {
		t.Fatal("worker defaults")
	}
}

func TestRunWritebackFollowsTheUploadUnlessKilled(t *testing.T) {
	previous := beforeUploadCheck
	beforeUploadCheck = func(*common.NetworkPolicy, string, time.Duration) func() string { return func() string { return "" } }
	t.Cleanup(func() { beforeUploadCheck = previous })
	server, requests := newFakeApp(t)
	cfg := ingestclient.Config{BaseURL: server.URL, Token: "secret"}
	root := t.TempDir()
	writeRecordings(t, root, "20260427 100004-40DC0200.m4a")
	old := time.Now().Add(-time.Hour)
	os.Chtimes(filepath.Join(root, "20260427 100004-40DC0200.m4a"), old, old)
	createCloudRecordingsDB(t, root, []any{"U1", "20260427 100004-40DC0200.m4a", "New Recording 3", 4100, nil, nil})
	fakeSQLRows = []map[string]any{{"recording_id": "20260427 100004-40DC0200", "content_sha256": "x", "title": "Quarterly planning discussion"}}
	t.Cleanup(func() { fakeSQLRows = nil })
	work := t.TempDir()
	env := map[string]string{"VOICE_MEMOS_ACCOUNT": "zach@example.com", "VOICE_MEMOS_RECORDINGS_PATH": root, "PDW_INGEST_PROJECT_DIR": work, "PDW_CLIENT_NAME": "custom"}
	base := []string{"--state-file", filepath.Join(work, "state.json"), "--lock-file", filepath.Join(work, "state.lock"), "--writeback-dry-run"}

	// Default: upload, then the write-back, through the app's sql tool with
	// the configured client name and the Python's ndjson format.
	code, out, errOut := runCLI(t, base, env, cfg)
	if code != 0 || errOut != "" {
		t.Fatalf("run: %d %q %q", code, out, errOut)
	}
	if !strings.Contains(out, "Voice Memos upload complete: seen=1 selected=1 uploaded=1") || !strings.Contains(out, "Voice Memos write-back complete: local=1 auto_named=1 titles=1 planned=1 renamed=0 skipped=0 (dry run)") || !strings.Contains(out, "[dry-run] would rename 20260427 100004-40DC0200: 'New Recording 3' -> 'Quarterly planning discussion'") {
		t.Fatalf("stdout = %q", out)
	}
	var sqlCalls []recorded
	for _, r := range *requests {
		if r.Path == "/api/tools/sql" {
			sqlCalls = append(sqlCalls, r)
		}
	}
	if len(sqlCalls) != 1 || sqlCalls[0].Authorization != "Bearer custom:secret" {
		t.Fatalf("sql calls = %+v", sqlCalls)
	}
	var input map[string]string
	json.Unmarshal(sqlCalls[0].Body, &input)
	if input["format"] != "ndjson" || input["sql"] != EnrichedTitlesSQL("zach@example.com") || input["question"] != "Voice memo enriched titles for app write-back" {
		t.Fatalf("sql input = %v", input)
	}

	// The kill switch skips the write-back without touching the upload.
	killed := map[string]string{}
	for k, v := range env {
		killed[k] = v
	}
	killed["VOICE_MEMOS_WRITEBACK_ENABLED"] = "0"
	before := len(*requests)
	code, out, _ = runCLI(t, base, killed, cfg)
	if code != 0 || strings.Contains(out, "write-back complete") || !strings.Contains(out, "Voice Memos upload complete: seen=1 selected=0 uploaded=0 skipped=1") {
		t.Fatalf("killed run: %d %q", code, out)
	}
	for _, r := range (*requests)[before:] {
		if r.Path == "/api/tools/sql" {
			t.Fatal("the kill switch must not query the app")
		}
	}
	// --writeback-only overrides the kill switch, as it did in Python.
	code, out, _ = runCLI(t, append(base, "--writeback-only"), killed, cfg)
	if code != 0 || !strings.Contains(out, "write-back complete") || strings.Contains(out, "upload complete") {
		t.Fatalf("writeback-only under kill switch: %d %q", code, out)
	}
}

func TestRunFullModeIgnoresTheMinimumFileAge(t *testing.T) {
	previous := beforeUploadCheck
	beforeUploadCheck = func(*common.NetworkPolicy, string, time.Duration) func() string { return func() string { return "" } }
	t.Cleanup(func() { beforeUploadCheck = previous })
	server, _ := newFakeApp(t)
	cfg := ingestclient.Config{BaseURL: server.URL, Token: "secret"}
	root := t.TempDir()
	writeRecordings(t, root, "20260427 100004-40DC0200.m4a") // just written: younger than 120s
	work := t.TempDir()
	env := map[string]string{"VOICE_MEMOS_ACCOUNT": "zach@example.com", "VOICE_MEMOS_RECORDINGS_PATH": root, "PDW_INGEST_PROJECT_DIR": work}
	base := []string{"--state-file", filepath.Join(work, "state.json"), "--lock-file", filepath.Join(work, "state.lock"), "--no-writeback"}
	if code, out, _ := runCLI(t, base, env, cfg); code != 0 || !strings.Contains(out, "Voice Memos upload complete: seen=1 selected=0 uploaded=0 skipped=0 deferred=1") {
		t.Fatalf("incremental must defer a fresh file: %d %q", code, out)
	}
	if code, out, _ := runCLI(t, append(base, "--mode", "full"), env, cfg); code != 0 || !strings.Contains(out, "Voice Memos upload complete: seen=1 selected=1 uploaded=1 skipped=0 deferred=0") || !strings.Contains(out, "Uploading with 8 worker(s)") {
		t.Fatalf("full mode must ignore the age rule: %d %q", code, out)
	}
}

func TestRunNetworkDiagnosticsPrintsTheVerdictAndExits(t *testing.T) {
	previous := newNetworkPolicy
	newNetworkPolicy = func(env common.Getenv) *common.NetworkPolicy {
		policy := common.NetworkPolicyFromEnv(env, "VOICE_MEMOS_UPLOAD", "")
		policy.Runner = func(args []string) string {
			switch args[0] {
			case "route":
				return "   route to: default\ndestination: default\n   interface: en0\n"
			case "networksetup":
				if args[1] == "-listallhardwareports" {
					return "Hardware Port: Wi-Fi\nDevice: en0\n"
				}
				return "Current Wi-Fi Network: United Wi-Fi\n"
			}
			return ""
		}
		return policy
	}
	t.Cleanup(func() { newNetworkPolicy = previous })
	// It runs before settings are loaded, so no account is needed, and it
	// never touches the app.
	code, out, errOut := runCLI(t, []string{"--network-diagnostics"}, map[string]string{}, ingestclient.Config{})
	if code != 0 || errOut != "" {
		t.Fatalf("diagnostics: %d %q %q", code, out, errOut)
	}
	for _, line := range []string{"Default interface: en0", "Hardware port: Wi-Fi", "Wi-Fi SSID: United Wi-Fi", "SSID source: networksetup", "Decision: blocked (blocked Wi-Fi SSID: United Wi-Fi)"} {
		if !strings.Contains(out, line) {
			t.Fatalf("diagnostics output lacks %q:\n%s", line, out)
		}
	}
}
