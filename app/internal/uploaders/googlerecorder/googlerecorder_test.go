package googlerecorder

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// The tables and columns below are Recorder's own (com.google.android.apps.recorder
// 4.2.20260823, read off a Pixel 11 Pro on 2026-10-23), trimmed to what the
// uploader reads.
const recorderSchema = `
CREATE TABLE recording_meta (uuid TEXT NOT NULL, account TEXT DEFAULT 'RECORDER_DEFAULT_ACCOUNT', title TEXT NOT NULL,
  loc_lat REAL, loc_lng REAL, loc_name TEXT, loc_place_id TEXT, created_time INTEGER NOT NULL, duration INTEGER NOT NULL,
  file_path TEXT NOT NULL, recording_config TEXT NOT NULL, recording_state INTEGER NOT NULL, parent_uuid TEXT,
  backup_format INTEGER NOT NULL, producer_type INTEGER NOT NULL, PRIMARY KEY(uuid));
CREATE TABLE fai_record (uuid TEXT NOT NULL, uri TEXT, file_sha256 TEXT, file_size INTEGER NOT NULL DEFAULT 0,
  file_state INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(uuid));
`

const recordingConfig = `{"version":2,"audio-format":2,"mime":"audio\/mp4a-latm","sample-rate":48000,"channel-count":1}`

type fixture struct {
	t    *testing.T
	dir  string
	db   *sql.DB
	now  time.Time
	logs *common.RecordingLogger
}

// newFixture lays out a Recorder data directory. The writer connection stays
// open in WAL mode, as Recorder's does, so every row lives only in the -wal:
// a reader that ignored the WAL would see an empty table.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	for _, sub := range []string{"databases", "files/recordings"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "databases", "recording_database"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0", recorderSchema} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Close() })
	return &fixture{t: t, dir: dir, db: db, now: time.Date(2026, 10, 23, 22, 0, 0, 0, time.UTC), logs: &common.RecordingLogger{}}
}

type recordingRow struct {
	uuid, account, title, locName string
	lat, lng                      *float64
	created                       time.Time
	durationMS                    int64
	state                         int
	audio                         string
	modifiedAgo                   time.Duration
}

func (f *fixture) add(r recordingRow) string {
	f.t.Helper()
	if r.state == 0 {
		r.state = CompletedState
	}
	if r.account == "" {
		r.account = "zach@example.com"
	}
	stored := "/data/user/0/com.google.android.apps.recorder/files/recordings/" + r.uuid + ".m4a"
	if _, err := f.db.Exec(`INSERT INTO recording_meta (uuid, account, title, loc_lat, loc_lng, loc_name, created_time,
		duration, file_path, recording_config, recording_state, backup_format, producer_type)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 5, 1)`,
		r.uuid, r.account, r.title, r.lat, r.lng, nullable(r.locName), r.created.UnixMilli(), r.durationMS, stored,
		recordingConfig, r.state); err != nil {
		f.t.Fatal(err)
	}
	path := filepath.Join(f.dir, "files", "recordings", r.uuid+".m4a")
	if r.audio != "" {
		f.writeAudio(path, r.audio, r.modifiedAgo)
	}
	return path
}

func (f *fixture) writeAudio(path, content string, modifiedAgo time.Duration) {
	f.t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		f.t.Fatal(err)
	}
	if modifiedAgo == 0 {
		modifiedAgo = time.Minute
	}
	mtime := f.now.Add(-modifiedAgo)
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		f.t.Fatal(err)
	}
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func ptr(v float64) *float64 { return &v }

type fakeUploader struct {
	audio    []string // content sha of each audio upload
	metadata []map[string]any
	params   []map[string]string
	failOn   string // "audio" or "metadata"
}

func (u *fakeUploader) UploadGoogleRecorderAudio(path, recordedAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error) {
	if u.failOn == "audio" {
		return ingestclient.StoredObject{}, errors.New("app unreachable")
	}
	if extension != ".m4a" || contentType != "audio/mp4" {
		return ingestclient.StoredObject{}, errors.New("unexpected audio type " + extension + " " + contentType)
	}
	u.audio = append(u.audio, contentSHA256)
	return ingestclient.StoredObject{StorageFileID: "audio"}, nil
}

func (u *fakeUploader) UploadGoogleRecorderMetadata(payload map[string]any, recordedAt, audioContentSHA256, metadataDedupSHA256 string) (ingestclient.StoredObject, error) {
	if u.failOn == "metadata" {
		return ingestclient.StoredObject{}, errors.New("app unreachable")
	}
	u.metadata = append(u.metadata, payload)
	u.params = append(u.params, map[string]string{
		"recorded_at": recordedAt, "audio": audioContentSHA256, "dedup": metadataDedupSHA256,
	})
	return ingestclient.StoredObject{StorageFileID: "meta"}, nil
}

func (f *fixture) runner(client Uploader, state *State) *Runner {
	return &Runner{
		RecorderDir: f.dir,
		Device:      "pixel11pro",
		Client:      client,
		State:       state,
		Logger:      f.logs,
		Now:         func() time.Time { return f.now },
		ScratchDir:  f.t.TempDir(),
	}
}

func (f *fixture) state() *State {
	f.t.Helper()
	state, err := OpenState(filepath.Join(f.t.TempDir(), "state.sqlite"))
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { state.Close() })
	return state
}

func TestReadRecordingsReadsRowsThatLiveOnlyInTheWAL(t *testing.T) {
	f := newFixture(t)
	created := time.Date(2026, 10, 23, 21, 56, 24, 803_000_000, time.UTC)
	f.add(recordingRow{uuid: "0e3f45d8", title: "Oct 23 at 5:56 PM", created: created, durationMS: 4350,
		locName: "Springfield Park", lat: ptr(40.01), lng: ptr(-75.02), audio: "pcm"})
	if _, err := os.Stat(filepath.Join(f.dir, "databases", "recording_database-wal")); err != nil {
		t.Fatalf("fixture must keep its rows in the WAL: %v", err)
	}

	recordings, err := ReadRecordings(f.dir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(recordings) != 1 {
		t.Fatalf("recordings = %+v", recordings)
	}
	r := recordings[0]
	if r.UUID != "0e3f45d8" || r.Account != "zach@example.com" || r.Title != "Oct 23 at 5:56 PM" {
		t.Fatalf("recording = %+v", r)
	}
	// created_time is epoch milliseconds in UTC: no zone, no filename stamp.
	if !r.CreatedAt.Equal(created) || r.DurationMS != 4350 || r.State != CompletedState {
		t.Fatalf("time fields = %v %d %d", r.CreatedAt, r.DurationMS, r.State)
	}
	if r.LocationName != "Springfield Park" || r.Latitude == nil || *r.Latitude != 40.01 {
		t.Fatalf("location = %q %v", r.LocationName, r.Latitude)
	}
	// Recorder stores the absolute path inside its own sandbox; the file is
	// found under the configured data directory instead.
	if r.Path != filepath.Join(f.dir, "files", "recordings", "0e3f45d8.m4a") || r.SizeBytes != 3 {
		t.Fatalf("path = %q size %d", r.Path, r.SizeBytes)
	}
}

func TestReadRecordingsNeverWritesBesideTheLiveDatabase(t *testing.T) {
	// A root reader that creates -shm or -wal beside Recorder's database
	// leaves files Recorder's own uid cannot write. Only a copy is opened.
	f := newFixture(t)
	f.add(recordingRow{uuid: "u1", title: "t", created: f.now.Add(-time.Hour), durationMS: 1000, audio: "a"})
	if _, err := f.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	f.db.Close()
	for _, suffix := range []string{"-wal", "-shm"} {
		os.Remove(filepath.Join(f.dir, "databases", "recording_database"+suffix))
	}
	if _, err := ReadRecordings(f.dir, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(f.dir, "databases"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "recording_database" {
		names := []string{}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("databases dir = %v, want only recording_database", names)
	}
}

func TestASyncSendsAFinishedRecordingAndItsSidecar(t *testing.T) {
	f := newFixture(t)
	created := time.Date(2026, 10, 23, 21, 56, 24, 803_000_000, time.UTC)
	f.add(recordingRow{uuid: "0e3f45d8", title: "Oct 23 at 5:56 PM", created: created, durationMS: 4350,
		locName: "Springfield Park", lat: ptr(40.01), lng: ptr(-75.02), audio: "pcm"})
	client := &fakeUploader{}

	summary, err := f.runner(client, f.state()).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.AudioUploaded != 1 || summary.MetadataUploaded != 1 || summary.Seen != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	if client.audio[0] != common.BytesSHA256([]byte("pcm")) {
		t.Fatalf("audio sha = %q", client.audio[0])
	}
	payload := client.metadata[0]
	if payload["source"] != "google_recorder" || payload["account"] != "zach@example.com" || payload["device"] != "pixel11pro" {
		t.Fatalf("payload = %v", payload)
	}
	recording := payload["recording"].(map[string]any)
	if recording["recording_id"] != "0e3f45d8" || recording["title"] != "Oct 23 at 5:56 PM" ||
		recording["recorded_at"] != common.ISOFormat(created) || recording["duration_seconds"] != 4.35 ||
		recording["content_type"] != "audio/mp4" || recording["extension"] != ".m4a" ||
		recording["content_sha256"] != client.audio[0] || recording["size_bytes"] != int64(3) {
		t.Fatalf("recording = %v", recording)
	}
	location := recording["location"].(map[string]any)
	if location["name"] != "Springfield Park" || location["latitude"] != 40.01 || location["longitude"] != -75.02 {
		t.Fatalf("location = %v", location)
	}
	if client.params[0]["recorded_at"] != common.ISOFormat(created) || client.params[0]["audio"] != client.audio[0] ||
		client.params[0]["dedup"] != MetadataDedupSHA256(payload) {
		t.Fatalf("params = %v", client.params[0])
	}
}

func TestASecondSyncSendsNothingNew(t *testing.T) {
	f := newFixture(t)
	f.add(recordingRow{uuid: "u1", title: "t", created: f.now.Add(-time.Hour), durationMS: 1000, audio: "a"})
	state := f.state()
	if _, err := f.runner(&fakeUploader{}, state).Sync(); err != nil {
		t.Fatal(err)
	}
	again := &fakeUploader{}
	summary, err := f.runner(again, state).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(again.audio)+len(again.metadata) != 0 || summary.Skipped != 1 {
		t.Fatalf("second run sent %d audio, %d sidecars; summary %+v", len(again.audio), len(again.metadata), summary)
	}
}

func TestARenameSendsOnlyANewSidecar(t *testing.T) {
	f := newFixture(t)
	f.add(recordingRow{uuid: "u1", title: "Oct 23 at 5:56 PM", created: f.now.Add(-time.Hour), durationMS: 1000, audio: "a"})
	state := f.state()
	if _, err := f.runner(&fakeUploader{}, state).Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Exec("UPDATE recording_meta SET title = 'Grocery list' WHERE uuid = 'u1'"); err != nil {
		t.Fatal(err)
	}
	again := &fakeUploader{}
	if _, err := f.runner(again, state).Sync(); err != nil {
		t.Fatal(err)
	}
	if len(again.audio) != 0 || len(again.metadata) != 1 {
		t.Fatalf("rename sent %d audio, %d sidecars", len(again.audio), len(again.metadata))
	}
	if title := again.metadata[0]["recording"].(map[string]any)["title"]; title != "Grocery list" {
		t.Fatalf("title = %v", title)
	}
}

func TestAnEditedRecordingSendsItsNewAudio(t *testing.T) {
	// Recorder edits (a crop) in place: same uuid, new bytes.
	f := newFixture(t)
	path := f.add(recordingRow{uuid: "u1", title: "t", created: f.now.Add(-time.Hour), durationMS: 1000, audio: "long take"})
	state := f.state()
	if _, err := f.runner(&fakeUploader{}, state).Sync(); err != nil {
		t.Fatal(err)
	}
	f.writeAudio(path, "cropped", 30*time.Second)
	again := &fakeUploader{}
	if _, err := f.runner(again, state).Sync(); err != nil {
		t.Fatal(err)
	}
	if len(again.audio) != 1 || again.audio[0] != common.BytesSHA256([]byte("cropped")) || len(again.metadata) != 1 {
		t.Fatalf("edit sent audio %v and %d sidecars", again.audio, len(again.metadata))
	}
}

func TestOnlyFinishedSettledRecordingsAreSent(t *testing.T) {
	f := newFixture(t)
	f.add(recordingRow{uuid: "recording-now", title: "t", created: f.now.Add(-time.Minute), durationMS: 0, state: 1, audio: "partial"})
	f.add(recordingRow{uuid: "just-saved", title: "t", created: f.now.Add(-time.Minute), durationMS: 50_000, audio: "x", modifiedAgo: time.Second})
	f.add(recordingRow{uuid: "no-file", title: "t", created: f.now.Add(-time.Hour), durationMS: 1000})
	f.add(recordingRow{uuid: "done", title: "t", created: f.now.Add(-time.Hour), durationMS: 1000, audio: "y"})
	client := &fakeUploader{}

	summary, err := f.runner(client, f.state()).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.metadata) != 1 || client.metadata[0]["recording"].(map[string]any)["recording_id"] != "done" {
		t.Fatalf("sent %v", client.metadata)
	}
	if summary.InProgress != 1 || summary.Unsettled != 1 || summary.Missing != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestNewestRecordingsGoFirstAndTheLimitHoldsTheRest(t *testing.T) {
	f := newFixture(t)
	for i, uuid := range []string{"old", "middle", "new"} {
		f.add(recordingRow{uuid: uuid, title: uuid, created: f.now.Add(-time.Duration(3-i) * time.Hour), durationMS: 1000, audio: uuid})
	}
	client := &fakeUploader{}
	r := f.runner(client, f.state())
	r.Limit = 2
	summary, err := r.Sync()
	if err != nil {
		t.Fatal(err)
	}
	var sent []string
	for _, m := range client.metadata {
		sent = append(sent, m["recording"].(map[string]any)["recording_id"].(string))
	}
	if strings.Join(sent, ",") != "new,middle" || summary.Remaining != 1 {
		t.Fatalf("sent %v, summary %+v", sent, summary)
	}
}

func TestAFailedUploadStopsTheRunAndIsRetried(t *testing.T) {
	f := newFixture(t)
	f.add(recordingRow{uuid: "u1", title: "t", created: f.now.Add(-time.Hour), durationMS: 1000, audio: "a"})
	state := f.state()
	if _, err := f.runner(&fakeUploader{failOn: "metadata"}, state).Sync(); err == nil || !strings.Contains(err.Error(), "u1") {
		t.Fatalf("err = %v, want one naming the recording", err)
	}
	retry := &fakeUploader{}
	if _, err := f.runner(retry, state).Sync(); err != nil {
		t.Fatal(err)
	}
	if len(retry.metadata) != 1 {
		t.Fatalf("retry sent %d sidecars", len(retry.metadata))
	}
}

func TestASignedOutRecorderFallsBackToTheConfiguredAccount(t *testing.T) {
	f := newFixture(t)
	f.add(recordingRow{uuid: "u1", account: "RECORDER_DEFAULT_ACCOUNT", title: "t", created: f.now.Add(-time.Hour), durationMS: 1000, audio: "a"})
	client := &fakeUploader{}
	r := f.runner(client, f.state())
	r.Account = "zach@example.com"
	if _, err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	if client.metadata[0]["account"] != "zach@example.com" {
		t.Fatalf("account = %v", client.metadata[0]["account"])
	}
}

func TestTheDedupShaIgnoresOnlyTheUploadTime(t *testing.T) {
	payload := func(title, uploadedAt string) map[string]any {
		return map[string]any{"source": "google_recorder", "uploaded_at": uploadedAt,
			"recording": map[string]any{"recording_id": "u1", "title": title}}
	}
	a := MetadataDedupSHA256(payload("t", "2026-10-23T22:00:00+00:00"))
	if a != MetadataDedupSHA256(payload("t", "2026-10-24T09:00:00+00:00")) {
		t.Fatal("a re-send must dedup")
	}
	if a == MetadataDedupSHA256(payload("renamed", "2026-10-23T22:00:00+00:00")) || len(a) != 64 {
		t.Fatalf("dedup = %q", a)
	}
	// And the payload itself still carries its upload time.
	p := payload("t", "x")
	MetadataDedupSHA256(p)
	if p["uploaded_at"] != "x" {
		t.Fatal("hashing must not mutate the payload")
	}
}

func TestTheMetadataIsCanonicalJSON(t *testing.T) {
	f := newFixture(t)
	f.add(recordingRow{uuid: "u1", title: "t", created: f.now.Add(-time.Hour), durationMS: 1000, audio: "a"})
	client := &fakeUploader{}
	if _, err := f.runner(client, f.state()).Sync(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(client.metadata[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"recording_config":{"audio-format":2`) {
		t.Fatalf("Recorder's recording_config rides along as an object: %s", raw)
	}
}
