package photos

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/testfixtures"
)

// testdata/Fixture.photoslibrary is the Python test fixture library
// (tests/test_photos_scanner.py::_build_fixture_library): a Live Photo still
// with GPS + camera, a cloud-only plain photo, a video, a trashed asset and a
// bundle-scoped syndicated record. golden.jsonl is what the Python scanner +
// envelope produced for it with account "z@x", a 123-byte "aaaa..." file and
// uploaded_at 2026-05-21T12:00:00+00:00.
const (
	goldenAccount    = "z@x"
	goldenSHA        = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	goldenSize       = int64(123)
	goldenUploadedAt = "2026-05-21T12:00:00+00:00"
)

var fixtureNow = time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)

// copyFixtureLibrary copies the fixture library into a scratch directory so
// a test can edit Photos.sqlite.
func copyFixtureLibrary(t *testing.T, root string) string {
	t.Helper()
	library := filepath.Join(root, "Fixture.photoslibrary")
	if err := os.MkdirAll(filepath.Join(library, "database"), 0o755); err != nil {
		t.Fatal(err)
	}
	testfixtures.CopyFile(t, filepath.Join("testdata", "Fixture.photoslibrary", "database", "Photos.sqlite"), filepath.Join(library, "database", "Photos.sqlite"))
	return library
}

func scanFixture(t *testing.T, library string) []Candidate {
	t.Helper()
	snapshot, err := SnapshotStore(library, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := Scan(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return candidates
}

func execFixtureSQL(t *testing.T, library, statement string) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(library, "database", "Photos.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}

func TestScanMatchesPythonGoldens(t *testing.T) {
	goldens := testfixtures.LoadGoldens(t, filepath.Join("testdata", "golden.jsonl"))["library"]
	candidates := scanFixture(t, filepath.Join("testdata", "Fixture.photoslibrary"))
	if len(candidates) != len(goldens) {
		t.Fatalf("scanned %d candidates, golden has %d", len(candidates), len(goldens))
	}
	for i, candidate := range candidates {
		want := goldens[i]
		if candidate.StateID() != want.Extra["state_id"] {
			t.Fatalf("candidate %d is %s, golden %v", i, candidate.StateID(), want.Extra["state_id"])
		}
		if candidate.Fingerprint() != want.Fingerprint {
			t.Errorf("%s fingerprint %s != %s", candidate.StateID(), candidate.Fingerprint(), want.Fingerprint)
		}
		if candidate.Filename != want.Extra["filename"] || candidate.Extension != want.Extra["extension"] || candidate.MimeType != want.Extra["mime"] || candidate.AssetKind != want.Extra["asset_kind"] {
			t.Errorf("%s resolved %q/%q/%q/%q, golden %v", candidate.StateID(), candidate.Filename, candidate.Extension, candidate.MimeType, candidate.AssetKind, want.Extra)
		}
		if float64(candidate.ExpectedSizeBytes) != want.Extra["expected_size"] {
			t.Errorf("%s expected size %d != %v", candidate.StateID(), candidate.ExpectedSizeBytes, want.Extra["expected_size"])
		}
		envelope, err := BuildPhotoMetadata(Envelope{
			Source: PhotoSource, Account: goldenAccount, NativeID: candidate.NativeID, Role: candidate.Role,
			Filename: candidate.Filename, MimeType: candidate.MimeType, SizeBytes: goldenSize, ContentSHA256: goldenSHA,
			UploadedAt: goldenUploadedAt, Width: candidate.Width, Height: candidate.Height, CapturedAt: candidate.CapturedAt,
			CaptureTZOffset: candidate.CaptureTZOffset, CameraMake: candidate.CameraMake, CameraModel: candidate.CameraModel,
			RecordKey: "apple_record", Record: candidate.AppleRecord,
		})
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := common.CanonicalJSON(envelope)
		if string(encoded) != want.Payload {
			t.Errorf("%s payload differs:\n got %s\nwant %s", candidate.StateID(), encoded, want.Payload)
		}
		if got := ProvenanceDedupSHA256(PhotoSource, goldenAccount, candidate.NativeID, candidate.Role, goldenSHA); got != want.Extra["dedup"] {
			t.Errorf("%s dedup %s != %v", candidate.StateID(), got, want.Extra["dedup"])
		}
	}
}

func TestScannerExcludesTrashedAndBundledAndPairsLiveVideo(t *testing.T) {
	candidates := scanFixture(t, filepath.Join("testdata", "Fixture.photoslibrary"))
	byID := map[string]Candidate{}
	for _, c := range candidates {
		byID[c.StateID()] = c
	}
	for _, absent := range []string{"UUID-TRASHED|original", "UUID-BUNDLED|original", "UUID-MISSING|live_video", "UUID-VIDEO|live_video"} {
		if _, ok := byID[absent]; ok {
			t.Errorf("%s must not be offered", absent)
		}
	}
	cloudOnly := byID["UUID-MISSING|original"]
	if cloudOnly.Filename != "IMG_0002.HEIC" || cloudOnly.ExpectedSizeBytes != 2_000_000 || cloudOnly.CaptureTZOffset != "+00:00" {
		t.Fatalf("cloud-only = %+v", cloudOnly)
	}
	if _, ok := cloudOnly.AppleRecord["latitude"]; ok {
		t.Fatal("GPS sentinel must be omitted from the record")
	}
	still, live := byID["UUID-LIVE|original"], byID["UUID-LIVE|live_video"]
	if still.MimeType != "image/heic" || live.MimeType != "video/quicktime" || live.Filename != "IMG_0001.MOV" || live.NativeID != still.NativeID {
		t.Fatalf("live pair = %+v / %+v", still, live)
	}
	if still.CapturedAt != "2026-06-01T14:30:00" || still.CaptureTZOffset != "-07:00" || still.CameraModel != "iPhone 16 Pro" || still.AppleRecord["latitude"] != 45.5 {
		t.Fatalf("still = %+v", still)
	}
	if live.Width != 0 || live.ExpectedSizeBytes != 0 || still.Width != 4284 {
		t.Fatalf("live video must carry no dimensions: %+v", live)
	}
}

func TestScanRejectsAnUnsupportedSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "Photos.sqlite")
	testfixtures.ExecScript(t, path, "CREATE TABLE ZASSET (Z_PK INTEGER PRIMARY KEY)")
	_, err := Scan(path)
	var schemaErr *SchemaError
	if !errors.As(err, &schemaErr) || !strings.Contains(err.Error(), "ZADDITIONALASSETATTRIBUTES, ZEXTENDEDATTRIBUTES") {
		t.Fatalf("err = %v", err)
	}
}

func TestEnvelopeFailsFastOnContractViolations(t *testing.T) {
	base := Envelope{Source: PhotoSource, Account: "z@x.test", NativeID: "UUID-1", Role: "original", Filename: "IMG_0001.HEIC",
		MimeType: "image/heic", SizeBytes: 123, ContentSHA256: "sha-still", UploadedAt: "2026-06-01T14:31:00+00:00",
		RecordKey: "apple_record", Record: map[string]any{"uuid": "UUID-1", "kind": int64(0)}}
	cases := []struct {
		mutate  func(*Envelope)
		message string
	}{
		{func(e *Envelope) { e.Source = "" }, "source is required"},
		{func(e *Envelope) { e.Account = "" }, "account is required"},
		{func(e *Envelope) { e.NativeID = "" }, "native_id is required"},
		{func(e *Envelope) { e.Role = "thumbnail" }, "role must be one of"},
		{func(e *Envelope) { e.ContentSHA256 = "" }, "content_sha256 is required"},
		{func(e *Envelope) { e.RecordKey = "" }, "record_key is required"},
		{func(e *Envelope) { e.RecordKey = "record" }, "source-named"},
	}
	for _, tc := range cases {
		e := base
		tc.mutate(&e)
		if _, err := BuildPhotoMetadata(e); err == nil || !strings.Contains(err.Error(), tc.message) {
			t.Errorf("want %q, got %v", tc.message, err)
		}
	}
	env, err := BuildPhotoMetadata(base)
	if err != nil {
		t.Fatal(err)
	}
	if env["apple_record"].(map[string]any)["uuid"] != "UUID-1" || env["file"].(map[string]any)["role"] != "original" {
		t.Fatalf("envelope = %v", env)
	}
	live := base
	live.Role = "live_video"
	if _, err := BuildPhotoMetadata(live); err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256([]byte("apple_photos|z@x.test|UUID-1|original|filesha"))
	if ProvenanceDedupSHA256("apple_photos", "z@x.test", "UUID-1", "original", "filesha") != hex.EncodeToString(want[:]) {
		t.Fatal("provenance sha seed drifted from the Go handler's")
	}
}

// --- fakes ------------------------------------------------------------------

type fakeClient struct {
	mu            sync.Mutex
	files         []map[string]any
	metadata      []map[string]any
	failFilenames map[string]bool
}

func (f *fakeClient) UploadPhotoFile(path, capturedAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error) {
	info, err := os.Stat(path)
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files = append(f.files, map[string]any{"size": info.Size(), "content_sha256": contentSHA256, "captured_at": capturedAt, "extension": extension, "content_type": contentType})
	return ingestclient.StoredObject{StorageBackend: "google_drive", StorageKey: fmt.Sprintf("photos/inbox/x%d", len(f.files)), StorageFileID: fmt.Sprintf("fid-%d", len(f.files))}, nil
}

func (f *fakeClient) UploadPhotoMetadata(payload map[string]any, capturedAt, fileContentSHA256, metadataDedupSHA256 string) (ingestclient.StoredObject, error) {
	filename := payload["file"].(map[string]any)["filename"].(string)
	if f.failFilenames[filename] {
		return ingestclient.StoredObject{}, fmt.Errorf("boom on %s", filename)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metadata = append(f.metadata, map[string]any{"payload": payload, "captured_at": capturedAt, "file_content_sha256": fileContentSHA256, "metadata_dedup_sha256": metadataDedupSHA256})
	return ingestclient.StoredObject{StorageBackend: "google_drive", StorageKey: "photos/inbox/meta", StorageFileID: "fid-m"}, nil
}

func (f *fakeClient) uploadedIDs() map[string]bool {
	out := map[string]bool{}
	for _, m := range f.metadata {
		file := m["payload"].(map[string]any)["file"].(map[string]any)
		out[file["native_id"].(string)+"|"+file["role"].(string)] = true
	}
	return out
}

var fakeContent = map[string][]byte{
	"UUID-LIVE|original":    []byte("still-bytes"),
	"UUID-LIVE|live_video":  []byte("live-video-bytes"),
	"UUID-MISSING|original": []byte("cloud"),
	"UUID-VIDEO|original":   []byte("video-bytes"),
}

type fakeExporter struct {
	failIDs        map[string]bool
	calls          []string
	maxStagedFiles int
	accessErr      *PhotosAccessError
}

func (f *fakeExporter) Export(candidate Candidate, destinationDir string) (ExportedFile, error) {
	key := candidate.StateID()
	f.calls = append(f.calls, key)
	if f.accessErr != nil {
		return ExportedFile{}, f.accessErr
	}
	if f.failIDs[key] {
		return ExportedFile{}, &ExportError{Message: "iCloud failed for " + candidate.Filename}
	}
	content := fakeContent[key]
	path := filepath.Join(destinationDir, candidate.NativeID+"-"+candidate.Role+candidate.Extension)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return ExportedFile{}, err
	}
	entries, _ := os.ReadDir(destinationDir)
	staged := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			staged++
		}
	}
	if staged > f.maxStagedFiles {
		f.maxStagedFiles = staged
	}
	return ExportedFile{Path: path, Filename: candidate.Filename, Extension: candidate.Extension, MimeType: candidate.MimeType, SizeBytes: int64(len(content))}, nil
}

func ids(keys ...string) map[string]bool {
	out := map[string]bool{}
	for _, key := range keys {
		out[key] = true
	}
	return out
}

func openTestState(t *testing.T, dir, library string) *State {
	t.Helper()
	state, err := OpenState(filepath.Join(dir, "state.sqlite"), "z@x.test", library)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func newRunner(library string, client Uploader, state *State, exporter Exporter, at time.Time) *Runner {
	return &Runner{Account: "z@x.test", LibraryPath: library, Client: client, Logger: &common.RecordingLogger{},
		Now: func() time.Time { return at }, State: state, Exporter: exporter}
}

func TestSyncExportsAndUploadsEveryOriginalIncludingICloudOnly(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	state := openTestState(t, dir, library)
	defer state.Close()
	client := &fakeClient{}
	exporter := &fakeExporter{}
	summary, err := newRunner(library, client, state, exporter, fixtureNow).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.AssetsSeen != 3 || summary.FilesSeen != 4 || summary.FilesExported != 4 || summary.FilesUploaded != 4 || summary.MetadataUploaded != 4 {
		t.Fatalf("summary = %+v", summary)
	}
	if len(client.files) != 4 || len(client.metadata) != 4 || !client.uploadedIDs()["UUID-MISSING|original"] {
		t.Fatalf("files=%d metadata=%d ids=%v", len(client.files), len(client.metadata), client.uploadedIDs())
	}
	// A large backfill must not retain every hydrated original until the end
	// of the run and consume the size of the whole library in temp space.
	if exporter.maxStagedFiles != 1 {
		t.Fatalf("max staged files = %d", exporter.maxStagedFiles)
	}
	// Envelopes carry source, role and provenance: the .mov ships under the
	// STILL's native id, and the two roles have distinct dedup keys.
	var still, live map[string]any
	for _, m := range client.metadata {
		file := m["payload"].(map[string]any)["file"].(map[string]any)
		switch file["native_id"].(string) + "|" + file["role"].(string) {
		case "UUID-LIVE|original":
			still = m
		case "UUID-LIVE|live_video":
			live = m
		}
	}
	livePayload := live["payload"].(map[string]any)
	if livePayload["source"] != PhotoSource || livePayload["apple_record"].(map[string]any)["uuid"] != "UUID-LIVE" {
		t.Fatalf("live payload = %v", livePayload)
	}
	if still["payload"].(map[string]any)["file"].(map[string]any)["camera_model"] != "iPhone 16 Pro" {
		t.Fatalf("still payload = %v", still["payload"])
	}
	if still["metadata_dedup_sha256"] == live["metadata_dedup_sha256"] {
		t.Fatal("still and live video must have distinct dedup keys")
	}
	sum := sha256.Sum256([]byte("live-video-bytes"))
	if live["file_content_sha256"] != hex.EncodeToString(sum[:]) || livePayload["file"].(map[string]any)["size_bytes"] != int64(16) {
		t.Fatalf("live file sha/size = %v / %v", live["file_content_sha256"], livePayload["file"])
	}
	if livePayload["uploaded_at"] != "2026-06-02T12:00:00+00:00" {
		t.Fatalf("uploaded_at = %v", livePayload["uploaded_at"])
	}
}

func TestSyncSkipsCompleteFilesAndReuploadsOnFingerprintChange(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	state := openTestState(t, dir, library)
	defer state.Close()
	client := &fakeClient{}
	if _, err := newRunner(library, client, state, &fakeExporter{}, fixtureNow).Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.files) != 4 {
		t.Fatalf("files = %d", len(client.files))
	}
	client2 := &fakeClient{}
	summary, err := newRunner(library, client2, state, &fakeExporter{}, fixtureNow.Add(time.Hour)).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesUploaded != 0 || summary.FilesSkipped != 4 || len(client2.files) != 0 {
		t.Fatalf("second run = %+v", summary)
	}
	execFixtureSQL(t, library, "UPDATE ZASSET SET ZFAVORITE = 1 WHERE ZUUID = 'UUID-VIDEO'")
	client3 := &fakeClient{}
	summary, err = newRunner(library, client3, state, &fakeExporter{}, fixtureNow.Add(2*time.Hour)).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesUploaded != 1 || client3.files[0]["extension"] != ".mov" {
		t.Fatalf("third run = %+v files=%v", summary, client3.files)
	}
	// Full mode ignores the state and re-uploads everything.
	client4 := &fakeClient{}
	full := newRunner(library, client4, state, &fakeExporter{}, fixtureNow.Add(3*time.Hour))
	full.Mode = "full"
	summary, err = full.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesUploaded != 4 || summary.FilesSkipped != 0 {
		t.Fatalf("full run = %+v", summary)
	}
}

func TestSyncCollectsFailuresAndReturnsTheFirstAfterTheBatch(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	state := openTestState(t, dir, library)
	defer state.Close()
	client := &fakeClient{failFilenames: map[string]bool{"IMG_0001.HEIC": true}}
	_, err := newRunner(library, client, state, &fakeExporter{}, fixtureNow).Sync()
	if err == nil || err.Error() != "boom on IMG_0001.HEIC" {
		t.Fatalf("err = %v", err)
	}
	if len(client.metadata) != 3 {
		t.Fatalf("metadata = %d", len(client.metadata))
	}
	entry, ok, _ := state.EntryFor(SourceTypeAssetFile, "UUID-VIDEO|original")
	if !ok || !entry.Complete {
		t.Fatalf("video entry = %+v", entry)
	}
	failed, ok, _ := state.EntryFor(SourceTypeAssetFile, "UUID-LIVE|original")
	if !ok || failed.Complete || !strings.Contains(failed.LastError, "boom") || failed.FailureCount != 1 {
		t.Fatalf("failed entry = %+v", failed)
	}
}

func TestSyncLimitAppliesAfterStateSelection(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	state := openTestState(t, dir, library)
	defer state.Close()
	client := &fakeClient{}
	runner := newRunner(library, client, state, &fakeExporter{}, fixtureNow)
	runner.Limit = 1
	summary, err := runner.Sync()
	if err != nil || summary.FilesUploaded != 1 {
		t.Fatalf("summary = %+v err = %v", summary, err)
	}
	client2 := &fakeClient{}
	runner2 := newRunner(library, client2, state, &fakeExporter{}, fixtureNow.Add(time.Hour))
	runner2.Limit = 1
	summary, err = runner2.Sync()
	if err != nil || summary.FilesUploaded != 1 || summary.FilesSkipped != 1 {
		t.Fatalf("summary = %+v err = %v", summary, err)
	}
}

func TestBeforeUploadCheckBlocksTheBatch(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	state := openTestState(t, dir, library)
	defer state.Close()
	client := &fakeClient{}
	exporter := &fakeExporter{}
	runner := newRunner(library, client, state, exporter, fixtureNow)
	runner.BeforeUploadCheck = func() string { return "metered network" }
	summary, err := runner.Sync()
	if err != nil || summary.FilesUploaded != 0 || summary.FilesSelected != 4 || len(client.files) != 0 || len(exporter.calls) != 0 {
		t.Fatalf("summary = %+v err = %v calls = %v", summary, err, exporter.calls)
	}
}

func TestSyncStopsTheBatchOnAMissingPhotosGrant(t *testing.T) {
	// A lost grant fails every file the same way; backing each one off would
	// burn a helper launch per file and read as "failed=N" instead of the
	// one repair a person has to make.
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	state := openTestState(t, dir, library)
	defer state.Close()
	client := &fakeClient{}
	exporter := &fakeExporter{accessErr: &PhotosAccessError{Status: 0, Message: "Photos library access is not determined (status 0): run `pdw ingest apple-photos --authorize`"}}
	summary, err := newRunner(library, client, state, exporter, fixtureNow).Sync()
	var access *PhotosAccessError
	if !errors.As(err, &access) || len(exporter.calls) != 1 || len(client.files) != 0 {
		t.Fatalf("err = %v calls = %v files = %d", err, exporter.calls, len(client.files))
	}
	if summary.FilesFailed != 1 || summary.FilesUploaded != 0 {
		t.Fatalf("summary = %+v", summary)
	}
	if entry, ok, _ := state.EntryFor(SourceTypeAssetFile, exporter.calls[0]); ok && entry.FailureCount != 0 {
		t.Fatalf("a missing grant must not back off the file: %+v", entry)
	}
}

func TestSyncFailsLoudlyWhenAnICloudOriginalCannotBeDownloaded(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	state := openTestState(t, dir, library)
	defer state.Close()
	client := &fakeClient{}
	_, err := newRunner(library, client, state, &fakeExporter{failIDs: ids("UUID-MISSING|original")}, fixtureNow).Sync()
	var exportErr *ExportError
	if !errors.As(err, &exportErr) || err.Error() != "iCloud failed for IMG_0002.HEIC" {
		t.Fatalf("err = %v", err)
	}
	if len(client.metadata) != 3 {
		t.Fatalf("metadata = %d", len(client.metadata))
	}
	failed, ok, _ := state.EntryFor(SourceTypeAssetFile, "UUID-MISSING|original")
	if !ok || failed.Complete {
		t.Fatalf("entry = %+v", failed)
	}
}

const unexportable = "UUID-MISSING|original"

// scheduledRun is one scheduled run against the shared state file.
func scheduledRun(t *testing.T, dir, library string, at time.Time, exporter Exporter, limit int) (Summary, error, *fakeClient) {
	t.Helper()
	client := &fakeClient{}
	state := openTestState(t, dir, library)
	defer state.Close()
	runner := newRunner(library, client, state, exporter, at)
	runner.Limit = limit
	summary, err := runner.Sync()
	return summary, err, client
}

func TestAFilePhotoKitCanNeverExportStopsFailingEveryLaterRun(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	var summaries []Summary
	var errs []error
	for attempt := 0; attempt < MaxFatalAttempts; attempt++ {
		// Space runs past the longest backoff so each one really re-attempts.
		at := fixtureNow.Add(time.Duration(attempt) * (MaxRetryBackoff + time.Hour))
		summary, err, _ := scheduledRun(t, dir, library, at, &fakeExporter{failIDs: ids(unexportable)}, 0)
		summaries = append(summaries, summary)
		errs = append(errs, err)
	}
	// The first attempts are loud: a fresh failure is assumed retryable.
	for i := 0; i < MaxFatalAttempts-1; i++ {
		var exportErr *ExportError
		if !errors.As(errs[i], &exportErr) {
			t.Fatalf("attempt %d err = %v", i, errs[i])
		}
	}
	// Once the file has burned its attempts, and other files have proven the
	// export path still works, it stops turning every scheduled run red.
	final := len(errs) - 1
	if errs[final] != nil || summaries[final].FilesFailed != 1 || summaries[final].FilesUploaded != 0 {
		t.Fatalf("final run = %+v err = %v", summaries[final], errs[final])
	}
}

func TestAFailingFileBacksOffInsteadOfConsumingTheNextRunsLimit(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	// The newest candidate is the one PhotoKit rejects, so an unbacked-off
	// retry would eat the whole limit of every run behind it.
	failing := ids("UUID-LIVE|original")
	_, err, first := scheduledRun(t, dir, library, fixtureNow, &fakeExporter{failIDs: failing}, 1)
	var exportErr *ExportError
	if !errors.As(err, &exportErr) || len(first.files) != 0 {
		t.Fatalf("first run err = %v files = %v", err, first.files)
	}
	summary, err, client := scheduledRun(t, dir, library, fixtureNow.Add(5*time.Minute), &fakeExporter{failIDs: failing}, 1)
	if err != nil || summary.FilesDeferred != 1 || summary.FilesUploaded != 1 || client.files[0]["extension"] != ".mov" {
		t.Fatalf("second run = %+v err = %v files = %v", summary, err, client.files)
	}
}

func TestADeferredFileIsRetriedOnceItsBackoffExpires(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	scheduledRun(t, dir, library, fixtureNow, &fakeExporter{failIDs: ids(unexportable)}, 0)
	summary, _, client := scheduledRun(t, dir, library, fixtureNow.Add(RetryDelay(1)+time.Minute), &fakeExporter{}, 0)
	if summary.FilesDeferred != 0 || len(client.files) != 1 || client.files[0]["extension"] != ".heic" {
		t.Fatalf("summary = %+v files = %v", summary, client.files)
	}
	// Inside the backoff it is deferred instead.
	summary, _, client = scheduledRun(t, dir, library, fixtureNow.Add(time.Minute), &fakeExporter{}, 0)
	if summary.FilesDeferred != 0 || len(client.files) != 0 {
		t.Fatalf("after success nothing should be pending: %+v", summary)
	}
}

func TestFailuresKeepFailingTheRunWhileNothingAtAllSucceeds(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	everything := ids("UUID-LIVE|original", "UUID-LIVE|live_video", unexportable, "UUID-VIDEO|original")
	// A revoked Photos grant or a dead network fails every file. However
	// many attempts that accumulates, the run must stay red rather than
	// reporting a green "everything is just failing" steady state.
	for attempt := 0; attempt < MaxFatalAttempts+3; attempt++ {
		at := fixtureNow.Add(time.Duration(attempt) * (MaxRetryBackoff + time.Hour))
		_, err, _ := scheduledRun(t, dir, library, at, &fakeExporter{failIDs: everything}, 0)
		var exportErr *ExportError
		if !errors.As(err, &exportErr) {
			t.Fatalf("attempt %d went quiet: %v", attempt, err)
		}
	}
}

func TestEditingTheAssetClearsTheFailureBackoff(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	scheduledRun(t, dir, library, fixtureNow, &fakeExporter{failIDs: ids(unexportable)}, 0)
	execFixtureSQL(t, library, "UPDATE ZASSET SET ZFAVORITE = 1 WHERE ZUUID = 'UUID-MISSING'")
	summary, err, client := scheduledRun(t, dir, library, fixtureNow.Add(5*time.Minute), &fakeExporter{}, 0)
	if err != nil || summary.FilesDeferred != 0 || len(client.files) != 1 {
		t.Fatalf("summary = %+v err = %v", summary, err)
	}
}

func TestRetryDelayGrowsAndIsCapped(t *testing.T) {
	if RetryDelay(0) != 0 || RetryDelay(1) != 30*time.Minute || RetryDelay(2) != time.Hour || RetryDelay(3) != 2*time.Hour {
		t.Fatalf("delays = %v %v %v %v", RetryDelay(0), RetryDelay(1), RetryDelay(2), RetryDelay(3))
	}
	if RetryDelay(500) != MaxRetryBackoff || RetryDelay(10) != MaxRetryBackoff || RetryDelay(9) != 128*time.Hour {
		t.Fatalf("cap = %v %v %v", RetryDelay(500), RetryDelay(10), RetryDelay(9))
	}
}

func TestStateCountsConsecutiveFailuresAndResetsThemOnSuccess(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	state := openTestState(t, dir, library)
	defer state.Close()
	moment := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	first, err := state.MarkFailure(SourceTypeAssetFile, "UUID-LIVE|original", "1|2", "boom", moment)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := state.MarkFailure(SourceTypeAssetFile, "UUID-LIVE|original", "1|2", "boom", moment.Add(time.Hour))
	if first.FailureCount != 1 || second.FailureCount != 2 || second.FirstFailureAt != first.FirstFailureAt || first.FirstFailureAt != "2026-06-02T00:00:00+00:00" {
		t.Fatalf("first = %+v second = %+v", first, second)
	}
	// A changed fingerprint is a different file: the streak starts over.
	restarted, _ := state.MarkFailure(SourceTypeAssetFile, "UUID-LIVE|original", "9|9", "boom", moment.Add(2*time.Hour))
	if restarted.FailureCount != 1 {
		t.Fatalf("restarted = %+v", restarted)
	}
	if err := state.MarkSuccess(SourceTypeAssetFile, "UUID-LIVE|original", "9|9", moment.Add(3*time.Hour), "", ""); err != nil {
		t.Fatal(err)
	}
	entry, _, _ := state.EntryFor(SourceTypeAssetFile, "UUID-LIVE|original")
	if entry.FailureCount != 0 || entry.FirstFailureAt != "" || entry.LastError != "" || !entry.Complete {
		t.Fatalf("entry = %+v", entry)
	}
	latest, ok, _ := state.LatestSuccessAt()
	if !ok || !latest.Equal(moment.Add(3*time.Hour)) {
		t.Fatalf("latest = %v %v", latest, ok)
	}
	state.MarkFailure(SourceTypeAssetFile, "UUID-VIDEO|original", "1|2", "boom", moment.Add(4*time.Hour))
	cleared, err := state.ClearFailures()
	if err != nil || cleared != 1 {
		t.Fatalf("cleared = %d err = %v", cleared, err)
	}
	video, _, _ := state.EntryFor(SourceTypeAssetFile, "UUID-VIDEO|original")
	if video.FailureCount != 0 || video.Complete {
		t.Fatalf("video = %+v", video)
	}
}

func TestStateAddsRetryColumnsToAStateFileWrittenBeforeThem(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	path := filepath.Join(dir, "state.sqlite")
	testfixtures.ExecScript(t, path,
		`CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '')`,
		`CREATE TABLE upload_state (
            source_type TEXT NOT NULL, source_id TEXT NOT NULL,
            fingerprint TEXT NOT NULL DEFAULT '', complete INTEGER NOT NULL DEFAULT 0,
            content_sha256 TEXT NOT NULL DEFAULT '', storage_key TEXT NOT NULL DEFAULT '',
            last_success_at TEXT NOT NULL DEFAULT '', last_failure_at TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '', last_checked_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (source_type, source_id))`,
		`INSERT INTO metadata VALUES ('schema_version', '1'), ('account', 'z@x.test'), ('library_path', '`+library+`')`,
		`INSERT INTO upload_state (source_type, source_id, fingerprint, complete) VALUES ('asset_file', 'UUID-LIVE|original', 'fp', 1)`,
	)
	state, err := OpenState(path, "z@x.test", library)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	// Migrating in place, not wiping: already-uploaded files must not be
	// re-exported just because the schema grew two columns.
	complete, _ := state.IsComplete(SourceTypeAssetFile, "UUID-LIVE|original", "fp")
	entry, ok, _ := state.EntryFor(SourceTypeAssetFile, "UUID-LIVE|original")
	if !complete || !ok || entry.FailureCount != 0 {
		t.Fatalf("complete = %v entry = %+v", complete, entry)
	}
}

func TestStateWipesWhenLibraryChangesAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	state := openTestState(t, dir, library)
	state.MarkSuccess(SourceTypeAssetFile, "UUID-LIVE|original", "1|2", fixtureNow, "sha", "")
	state.Close()
	reopened := openTestState(t, dir, library)
	complete, _ := reopened.IsComplete(SourceTypeAssetFile, "UUID-LIVE|original", "1|2")
	stale, _ := reopened.IsComplete(SourceTypeAssetFile, "UUID-LIVE|original", "9|9")
	if !complete || stale {
		t.Fatalf("complete = %v stale = %v", complete, stale)
	}
	reopened.Close()
	other, err := OpenState(filepath.Join(dir, "state.sqlite"), "z@x.test", filepath.Join(dir, "Other.photoslibrary"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, ok, _ := other.EntryFor(SourceTypeAssetFile, "UUID-LIVE|original"); ok {
		t.Fatal("state must wipe when the library changes")
	}
}

// --- exporter ------------------------------------------------------------------

func exporterCandidate(role string) Candidate {
	c := Candidate{NativeID: "UUID-ICLOUD", Role: role, AssetKind: "image", Filename: "IMG_0357.HEIC", Extension: ".heic",
		MimeType: "image/heic", ExpectedSizeBytes: 20, Width: 4032, Height: 3024, CapturedAt: "2026-07-01T12:00:00",
		CaptureTZOffset: "-04:00", CameraMake: "Apple", CameraModel: "iPhone",
		AppleRecord: map[string]any{"uuid": "UUID-ICLOUD", "modification_date": "2026-07-01T16:00:00+00:00"}}
	if role == "live_video" {
		c.Filename, c.Extension, c.MimeType, c.ExpectedSizeBytes = "IMG_0357.MOV", ".mov", "video/quicktime", 0
	}
	return c
}

type helperRunner struct {
	content []byte
	errText string
	status  int
	delay   time.Duration
	calls   [][]string
}

func argAfter(command []string, flag string) string {
	for i, arg := range command {
		if arg == flag && i+1 < len(command) {
			return command[i+1]
		}
	}
	return ""
}

func (h *helperRunner) run(command []string, timeout time.Duration) (CommandResult, error) {
	h.calls = append(h.calls, command)
	stdoutPath := argAfter(command, "--stdout")
	stderrPath := argAfter(command, "--stderr")
	var helperArgs []string
	for i, arg := range command {
		if arg == "--args" {
			helperArgs = command[i+1:]
		}
	}
	complete := func() {
		if h.errText != "" {
			os.WriteFile(stderrPath, []byte(h.errText), 0o644)
			return
		}
		var payload map[string]any
		if helperArgs[0] == "export" {
			os.WriteFile(argAfter(helperArgs, "--destination"), h.content, 0o644)
			filename, uti := "IMG_0357.HEIC", "public.heic"
			if argAfter(helperArgs, "--role") == "live_video" {
				filename, uti = "IMG_0357.MOV", "com.apple.quicktime-movie"
			}
			payload = map[string]any{"filename": filename, "uti": uti, "size_bytes": len(h.content)}
		} else {
			payload = map[string]any{"status": h.status}
		}
		encoded, _ := json.Marshal(payload)
		os.WriteFile(stdoutPath, encoded, 0o644)
	}
	if h.delay > 0 {
		time.AfterFunc(h.delay, complete)
	} else {
		complete()
	}
	return CommandResult{}, nil
}

func newExporterForTest(runner *helperRunner, dir string) *PhotoKitExporter {
	return &PhotoKitExporter{HelperPath: filepath.Join(dir, HelperAppName), CommandRunner: runner.run, Timeout: time.Second,
		LookPath: func(string) (string, error) { return "/usr/bin/open", nil }}
}

func TestExportDownloadsFullOriginalThroughLaunchServices(t *testing.T) {
	dir := t.TempDir()
	runner := &helperRunner{content: []byte("full-icloud-original"), status: 3}
	exported, err := newExporterForTest(runner, dir).Export(exporterCandidate("original"), dir)
	if err != nil {
		t.Fatal(err)
	}
	command := runner.calls[0]
	if filepath.Base(command[0]) != "open" || command[1] != "-n" || command[2] != "-j" {
		t.Fatalf("command = %v", command)
	}
	for _, arg := range command {
		if arg == "-W" {
			t.Fatal("open -W races short-lived helper instances and must not be used")
		}
	}
	if argAfter(command, "--args") != "export" || argAfter(command, "--uuid") != "UUID-ICLOUD" || argAfter(command, "--role") != "original" || argAfter(command, "--kind") != "image" {
		t.Fatalf("command = %v", command)
	}
	argsIndex := -1
	for i, arg := range command {
		if arg == "--args" {
			argsIndex = i
		}
	}
	if argsIndex < 1 || !strings.HasSuffix(command[argsIndex-1], HelperAppName) {
		t.Fatalf("helper bundle not before --args: %v", command)
	}
	data, _ := os.ReadFile(exported.Path)
	if string(data) != "full-icloud-original" || exported.Filename != "IMG_0357.HEIC" || exported.SizeBytes != 20 || exported.MimeType != "image/heic" {
		t.Fatalf("exported = %+v", exported)
	}
	if filepath.Base(exported.Path) != "UUID-ICLOUD-original.heic" {
		t.Fatalf("destination = %s", exported.Path)
	}
}

func TestExportRequestsLivePhotosOriginalPairedVideo(t *testing.T) {
	dir := t.TempDir()
	runner := &helperRunner{content: []byte("original-live-video")}
	exported, err := newExporterForTest(runner, dir).Export(exporterCandidate("live_video"), dir)
	if err != nil {
		t.Fatal(err)
	}
	if argAfter(runner.calls[0], "--role") != "live_video" || exported.Extension != ".mov" || exported.MimeType != "video/quicktime" {
		t.Fatalf("exported = %+v", exported)
	}
}

func TestExportFailsLoudlyWhenPhotoKitCannotDownload(t *testing.T) {
	dir := t.TempDir()
	_, err := newExporterForTest(&helperRunner{errText: "iCloud is unavailable"}, dir).Export(exporterCandidate("original"), dir)
	var exportErr *ExportError
	if !errors.As(err, &exportErr) || err.Error() != "iCloud is unavailable" {
		t.Fatalf("err = %v", err)
	}
}

func TestExportRejectsATruncatedOrEmptyOriginal(t *testing.T) {
	dir := t.TempDir()
	_, err := newExporterForTest(&helperRunner{content: []byte("truncated")}, dir).Export(exporterCandidate("original"), dir)
	if err == nil || !strings.Contains(err.Error(), "full original is 20 bytes") {
		t.Fatalf("err = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "UUID-ICLOUD-original.heic")); !os.IsNotExist(statErr) {
		t.Fatal("a truncated export must be removed")
	}
	_, err = newExporterForTest(&helperRunner{content: []byte{}}, dir).Export(exporterCandidate("live_video"), dir)
	if err == nil || !strings.Contains(err.Error(), "exported an empty original") {
		t.Fatalf("err = %v", err)
	}
}

func TestExportTimesOutWaitingForTheHelper(t *testing.T) {
	dir := t.TempDir()
	exporter := newExporterForTest(&helperRunner{content: []byte("late"), delay: 5 * time.Second}, dir)
	exporter.Timeout = 100 * time.Millisecond
	_, err := exporter.Export(exporterCandidate("original"), dir)
	if err == nil || !strings.Contains(err.Error(), "Timed out after 0.1s waiting for Apple Photos/iCloud") {
		t.Fatalf("err = %v", err)
	}
}

// spawnSleeper starts a process that would outlive any test deadline and
// returns its pid; the caller decides whether the exporter kills it.
func spawnSleeper(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sleep", "300")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	return cmd
}

func TestExportKillsAHelperThatOutlivesItsDeadline(t *testing.T) {
	// `open -j` returns at once, so a helper stuck on iCloud (or on a consent
	// dialog nobody can click) used to outlive the export's deadline and pile
	// up as orphans: two were still alive from earlier runs on 2026-09-20.
	dir := t.TempDir()
	sleeper := spawnSleeper(t)
	runner := func(command []string, timeout time.Duration) (CommandResult, error) {
		if err := os.WriteFile(argAfter(command, "--pid-path"), []byte(fmt.Sprintf("%d\n", sleeper.Process.Pid)), 0o644); err != nil {
			t.Fatal(err)
		}
		return CommandResult{}, nil
	}
	exporter := &PhotoKitExporter{HelperPath: filepath.Join(dir, HelperAppName), CommandRunner: runner, Timeout: 150 * time.Millisecond,
		LookPath: func(string) (string, error) { return "/usr/bin/open", nil }}
	_, err := exporter.Export(exporterCandidate("original"), dir)
	if err == nil || !strings.Contains(err.Error(), "Timed out after 0.15s waiting for Apple Photos/iCloud") {
		t.Fatalf("err = %v", err)
	}
	// A killed child is a zombie until it is reaped, and kill(pid, 0) still
	// succeeds on a zombie, so wait for the exit rather than probing.
	exited := make(chan error, 1)
	go func() { exited <- sleeper.Wait() }()
	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatalf("helper pid %d survived the export deadline", sleeper.Process.Pid)
	}
}

func TestExportFailsFastWhenTheHelperDiesWithoutOutput(t *testing.T) {
	dir := t.TempDir()
	gone := exec.Command("true")
	if err := gone.Run(); err != nil {
		t.Fatal(err)
	}
	runner := func(command []string, timeout time.Duration) (CommandResult, error) {
		if err := os.WriteFile(argAfter(command, "--pid-path"), []byte(fmt.Sprintf("%d\n", gone.Process.Pid)), 0o644); err != nil {
			t.Fatal(err)
		}
		return CommandResult{}, nil
	}
	exporter := &PhotoKitExporter{HelperPath: filepath.Join(dir, HelperAppName), CommandRunner: runner, Timeout: 10 * time.Second,
		LookPath: func(string) (string, error) { return "/usr/bin/open", nil }}
	started := time.Now()
	_, err := exporter.Export(exporterCandidate("original"), dir)
	if err == nil || !strings.Contains(err.Error(), "exited without reporting a result") {
		t.Fatalf("err = %v", err)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatalf("waited %s for a helper that had already exited", time.Since(started))
	}
}

func TestExportReportsAMissingGrantAsAnAccessError(t *testing.T) {
	dir := t.TempDir()
	runner := &helperRunner{errText: "Photos library access is not determined (status 0): the uploader does not have Full Photos library access. Run `pdw ingest apple-photos --authorize` from a GUI session on this Mac.\n"}
	_, err := newExporterForTest(runner, dir).Export(exporterCandidate("original"), dir)
	var access *PhotosAccessError
	if !errors.As(err, &access) || access.Status != 0 || !strings.Contains(access.Message, "pdw ingest apple-photos --authorize") {
		t.Fatalf("err = %#v", err)
	}
	if !strings.Contains(strings.Join(runner.calls[0], " "), "--pid-path") {
		t.Fatalf("export did not hand the helper a pid path: %v", runner.calls[0])
	}
}

func TestRequestAuthorizationWaitsForTheHelperWithoutOpenWait(t *testing.T) {
	dir := t.TempDir()
	runner := &helperRunner{status: 3, delay: 50 * time.Millisecond}
	exporter := newExporterForTest(runner, dir)
	status, err := exporter.RequestAuthorization()
	if err != nil || status != 3 {
		t.Fatalf("status = %d err = %v", status, err)
	}
	command := runner.calls[0]
	if argAfter(command, "--args") != "authorize" || !strings.Contains(strings.Join(command, " "), filepath.Join(dir, HelperAppName)) {
		t.Fatalf("command = %v", command)
	}
	if status, err := exporter.AuthorizationStatus(); err != nil || status != 3 || argAfter(runner.calls[1], "--args") != "status" {
		t.Fatalf("status = %d err = %v", status, err)
	}
}

func TestNativeHelperSourcesAreEmbeddedAndPinTheContract(t *testing.T) {
	source := string(helperSwiftSource)
	plist := string(helperInfoPlist)
	for _, needle := range []string{
		"options.isNetworkAccessAllowed = true", "wantedType = .photo", "wantedType = .video", "wantedType = .pairedVideo",
		"PHAsset.fetchAssets(with: libraryFetchOptions())", "options.includeAllBurstAssets = true", "options.includeHiddenAssets = true",
		"let status = PHPhotoLibrary.authorizationStatus(for: .readWrite)", "guard status.rawValue == authorizedStatus",
		photosAccessErrorPrefix, "--pid-path",
	} {
		if !strings.Contains(source, needle) {
			t.Errorf("helper source lost %q", needle)
		}
	}
	for _, needle := range []string{"NSPhotoLibraryUsageDescription", HelperIdentifier, "<key>CFBundlePackageType</key>", "<string>APPL</string>", "<key>CFBundleExecutable</key>", "<key>LSUIElement</key>", "<true/>"} {
		if !strings.Contains(plist, needle) {
			t.Errorf("Info.plist lost %q", needle)
		}
	}
	if len(HelperSourceDigest()) != 64 {
		t.Fatal("digest")
	}
}

// --- CLI -------------------------------------------------------------------------

// fakeApp implements the photo ingest endpoints plus a Drive resumable
// session, verifying the upload signature like the real app does.
type fakeApp struct {
	mu       sync.Mutex
	server   *httptest.Server
	secret   string
	sessions map[string][]byte
	metadata []map[string]any
	files    int
}

func (f *fakeApp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.HasPrefix(r.URL.Path, "/ingest/") {
		sum := sha256.Sum256(body)
		svc := pdwauth.NewService([]byte(f.secret), time.Now)
		if err := svc.VerifyObjectUpload(r.URL.Path, hex.EncodeToString(sum[:]), r.URL.Query().Get("exp"), r.URL.Query().Get("sig")); err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/ingest/photos/file/resumable":
		var start map[string]any
		json.Unmarshal(body, &start)
		id := fmt.Sprintf("session-%d", len(f.sessions)+1)
		f.sessions[id] = nil
		json.NewEncoder(w).Encode(map[string]any{"storage_backend": "google_drive", "storage_key": "photos/inbox/" + id,
			"upload_url": f.server.URL + "/drive/" + id, "chunk_size_bytes": 4})
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/drive/"):
		id := strings.TrimPrefix(r.URL.Path, "/drive/")
		var start, end, total int
		fmt.Sscanf(r.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total)
		f.sessions[id] = append(f.sessions[id], body...)
		if len(f.sessions[id]) < total {
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(f.sessions[id])-1))
			w.WriteHeader(308)
			return
		}
		sum := sha256.Sum256(f.sessions[id])
		f.files++
		json.NewEncoder(w).Encode(map[string]any{"id": "drive-" + id, "sha256Checksum": hex.EncodeToString(sum[:]), "size": fmt.Sprint(total)})
	case r.Method == http.MethodPost && r.URL.Path == "/ingest/photos/metadata":
		var payload map[string]any
		json.Unmarshal(body, &payload)
		payload["_query"] = r.URL.Query().Get("metadata_dedup_sha256")
		f.metadata = append(f.metadata, payload)
		json.NewEncoder(w).Encode(map[string]any{"storage_backend": "google_drive", "storage_key": "photos/inbox/meta", "storage_file_id": "fid-m"})
	default:
		http.NotFound(w, r)
	}
}

func newFakeApp(t *testing.T) *fakeApp {
	t.Helper()
	app := &fakeApp{secret: "test-secret", sessions: map[string][]byte{}}
	app.server = httptest.NewServer(app)
	t.Cleanup(app.server.Close)
	return app
}

func installCLIHooks(t *testing.T, exporter Exporter) {
	t.Helper()
	oldExporter, oldCheck := newExporter, newBeforeUploadCheck
	newExporter = func(time.Duration) *PhotoKitExporter { return nil }
	newBeforeUploadCheck = func(common.Getenv, string) func() string { return func() string { return "" } }
	oldRunnerExporter := runnerExporterOverride
	runnerExporterOverride = exporter
	t.Cleanup(func() {
		newExporter, newBeforeUploadCheck, runnerExporterOverride = oldExporter, oldCheck, oldRunnerExporter
	})
}

func runCLI(t *testing.T, args []string, env map[string]string, cfg ingestclient.Config) (int, string, string) {
	t.Helper()
	var stdout, stderr strings.Builder
	code := Run(args, strings.NewReader(""), &stdout, &stderr, func(k string) string { return env[k] }, cfg)
	return code, stdout.String(), stderr.String()
}

func TestRunUploadsThroughTheAppAndSkipsOnTheSecondPass(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	app := newFakeApp(t)
	installCLIHooks(t, &fakeExporter{})
	env := map[string]string{"GMAIL_ACCOUNTS": "z@x.test,other@x.test", "APPLE_PHOTOS_LIBRARY_PATH": library, "PDW_INGEST_PROJECT_DIR": dir}
	cfg := ingestclient.Config{BaseURL: app.server.URL, Token: "test-secret"}
	stateFile := filepath.Join(dir, "state.sqlite")
	code, stdout, stderr := runCLI(t, []string{"--state-file", stateFile, "--lock-file", filepath.Join(dir, "run.lock"), "--limit", "3"}, env, cfg)
	if code != 0 {
		t.Fatalf("exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "Photo upload complete: assets=3 files=4 selected=3 exported=3 uploaded=3 skipped=0 deferred=0 failed=0") {
		t.Fatalf("stdout = %s", stdout)
	}
	if app.files != 3 || len(app.metadata) != 3 {
		t.Fatalf("files = %d metadata = %d", app.files, len(app.metadata))
	}
	first := app.metadata[0]
	file := first["file"].(map[string]any)
	if first["account"] != "z@x.test" || first["source"] != PhotoSource || file["native_id"] != "UUID-LIVE" || file["role"] != "original" {
		t.Fatalf("metadata = %v", first)
	}
	sum := sha256.Sum256([]byte("still-bytes"))
	if file["content_sha256"] != hex.EncodeToString(sum[:]) || first["_query"] != ProvenanceDedupSHA256(PhotoSource, "z@x.test", "UUID-LIVE", "original", hex.EncodeToString(sum[:])) {
		t.Fatalf("file block = %v", file)
	}
	// The state file is the Python one's path and shape.
	state, err := OpenState(stateFile, "z@x.test", library)
	if err != nil {
		t.Fatal(err)
	}
	complete, _ := state.IsComplete(SourceTypeAssetFile, "UUID-LIVE|original", scanFixture(t, library)[0].Fingerprint())
	state.Close()
	if !complete {
		t.Fatal("state must record the upload")
	}
	code, stdout, _ = runCLI(t, []string{"--state-file", stateFile, "--lock-file", filepath.Join(dir, "run.lock")}, env, cfg)
	if code != 0 || !strings.Contains(stdout, "selected=1 exported=1 uploaded=1 skipped=3") {
		t.Fatalf("second run exit %d stdout = %s", code, stdout)
	}
}

func TestRunExitCodesAndFlagParsing(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	installCLIHooks(t, &fakeExporter{failIDs: ids("UUID-MISSING|original")})
	app := newFakeApp(t)
	env := map[string]string{"PHOTOS_ACCOUNT": "z@x.test", "APPLE_PHOTOS_LIBRARY_PATH": library, "PDW_INGEST_PROJECT_DIR": dir}
	cfg := ingestclient.Config{BaseURL: app.server.URL, Token: "test-secret"}
	base := []string{"--state-file", filepath.Join(dir, "state.sqlite"), "--lock-file", filepath.Join(dir, "run.lock")}

	if code, stdout, _ := runCLI(t, []string{"--help"}, env, cfg); code != 0 || !strings.Contains(stdout, "--retry-failed") || !strings.Contains(stdout, "--authorize") {
		t.Fatalf("help exit %d: %s", code, stdout)
	}
	if code, _, _ := runCLI(t, []string{"--bogus"}, env, cfg); code != 2 {
		t.Fatalf("unknown flag exit %d", code)
	}
	if code, _, stderr := runCLI(t, []string{"--mode", "sideways"}, env, cfg); code != 2 || !strings.Contains(stderr, "invalid choice") {
		t.Fatalf("bad mode exit %d: %s", code, stderr)
	}
	if code, _, stderr := runCLI(t, base, map[string]string{"APPLE_PHOTOS_LIBRARY_PATH": library, "PDW_INGEST_PROJECT_DIR": dir}, cfg); code != 1 || !strings.Contains(stderr, "PHOTOS_ACCOUNT or GMAIL_ACCOUNTS") {
		t.Fatalf("missing account exit %d: %s", code, stderr)
	}
	if code, _, stderr := runCLI(t, base, env, ingestclient.Config{}); code != 1 || !strings.Contains(stderr, "PDW_API_URL") {
		t.Fatalf("missing config exit %d: %s", code, stderr)
	}
	// A still-retryable failure after the batch exits 1 with the successes
	// recorded, and the next run within the backoff defers it.
	code, stdout, stderr := runCLI(t, base, env, cfg)
	if code != 1 || !strings.Contains(stderr, "iCloud failed for IMG_0002.HEIC") || !strings.Contains(stdout, "Photo upload summary") {
		t.Fatalf("failed run exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	code, stdout, _ = runCLI(t, base, env, cfg)
	if code != 0 || !strings.Contains(stdout, "skipped=3 deferred=1 failed=0") {
		t.Fatalf("deferred run exit %d: %s", code, stdout)
	}
	// --retry-failed clears the backoff so it is attempted (and fails) again.
	code, stdout, _ = runCLI(t, append([]string{"--retry-failed"}, base...), env, cfg)
	if code != 1 || !strings.Contains(stdout, "Cleared the retry backoff on 1 previously failed file(s)") {
		t.Fatalf("retry-failed exit %d: %s", code, stdout)
	}
	// A held lock is a deliberate skip, exit 0.
	lock, acquired, err := common.TryRunLock(filepath.Join(dir, "run.lock"))
	if err != nil || !acquired {
		t.Fatal(err)
	}
	code, stdout, _ = runCLI(t, base, env, cfg)
	lock.Release()
	if code != 0 || !strings.Contains(stdout, "Photo upload skipped: another uploader run is active") {
		t.Fatalf("locked exit %d: %s", code, stdout)
	}
}

func TestRunAuthorizeUsesTheHelperAndReportsTheGrant(t *testing.T) {
	dir := t.TempDir()
	granted := &helperRunner{status: 3}
	old := newExporter
	newExporter = func(timeout time.Duration) *PhotoKitExporter {
		e := newExporterForTest(granted, dir)
		e.Timeout = timeout
		return e
	}
	t.Cleanup(func() { newExporter = old })
	code, stdout, _ := runCLI(t, []string{"--authorize"}, map[string]string{}, ingestclient.Config{})
	if code != 0 || !strings.Contains(stdout, "Photos library access granted") || argAfter(granted.calls[0], "--args") != "authorize" {
		t.Fatalf("exit %d stdout = %s", code, stdout)
	}
	denied := &helperRunner{status: 2}
	newExporter = func(timeout time.Duration) *PhotoKitExporter { return newExporterForTest(denied, dir) }
	code, _, stderr := runCLI(t, []string{"--authorize"}, map[string]string{}, ingestclient.Config{})
	if code != 1 || !strings.Contains(stderr, "Full Photos library access was not granted") {
		t.Fatalf("exit %d stderr = %s", code, stderr)
	}
}

func TestRunReadsTheRepoDotenvUnderTheEnvironment(t *testing.T) {
	dir := t.TempDir()
	library := copyFixtureLibrary(t, dir)
	app := newFakeApp(t)
	installCLIHooks(t, &fakeExporter{})
	os.WriteFile(filepath.Join(dir, ".env"), []byte("PHOTOS_ACCOUNT=dotenv@x.test\nAPPLE_PHOTOS_LIBRARY_PATH="+library+"\nPDW_API_URL="+app.server.URL+"\nPDW_SECRET_TOKEN=test-secret\n"), 0o644)
	env := map[string]string{"PDW_INGEST_PROJECT_DIR": dir}
	code, stdout, stderr := runCLI(t, []string{"--state-file", filepath.Join(dir, "state.sqlite"), "--lock-file", filepath.Join(dir, "run.lock"), "--limit", "1"}, env, ingestclient.Config{})
	if code != 0 || len(app.metadata) != 1 || app.metadata[0]["account"] != "dotenv@x.test" {
		t.Fatalf("exit %d stdout=%s stderr=%s metadata=%v", code, stdout, stderr, app.metadata)
	}
}

// --- gaps closed on the verification pass ------------------------------------

func TestNativeHelperNamesThePdwCommandNotThePythonModule(t *testing.T) {
	source := string(helperSwiftSource)
	for _, stale := range []string{"uv run", "python -m", "personal_data_warehouse_photos"} {
		if strings.Contains(source, stale) {
			t.Errorf("helper source still tells the user to run %q", stale)
		}
	}
	if !strings.Contains(source, "pdw ingest apple-photos --authorize") {
		t.Error("helper source must name `pdw ingest apple-photos --authorize` as the repair")
	}
}

func TestProbeOpenableFailsFastWhenOpenBlocks(t *testing.T) {
	// macOS TCC can BLOCK open(2) on Photos-library files indefinitely for a
	// launchd process without Full Disk Access; a hung run holds the uploader
	// lock and looks healthy. A writerless FIFO parks O_RDONLY in the kernel
	// exactly like the TCC stall, and the probe must turn it into a loud
	// permission error rather than waiting forever.
	fifo := filepath.Join(t.TempDir(), "blocking-open")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	err := probeOpenable(fifo, 200*time.Millisecond)
	var perm *common.PermissionError
	if !errors.As(err, &perm) || !strings.Contains(err.Error(), "blocked for") || !strings.Contains(err.Error(), "Full Disk Access") {
		t.Fatalf("err = %v", err)
	}
}

func TestProbeOpenableConvertsOpenErrorsToPermissionErrors(t *testing.T) {
	dir := t.TempDir()
	readable := filepath.Join(dir, "ok.sqlite")
	if err := os.WriteFile(readable, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := probeOpenable(readable, 2*time.Second); err != nil {
		t.Fatalf("a readable file must pass: %v", err)
	}
	err := probeOpenable(filepath.Join(dir, "missing.sqlite"), 2*time.Second)
	var perm *common.PermissionError
	if !errors.As(err, &perm) || !strings.Contains(err.Error(), "Full Disk Access") {
		t.Fatalf("err = %v", err)
	}
	// SnapshotStore is where the probe runs, so the same guidance reaches a
	// run whose library is unreadable.
	if _, err := SnapshotStore(filepath.Join(dir, "Nope.photoslibrary"), dir); err == nil || !strings.Contains(err.Error(), "Full Disk Access") {
		t.Fatalf("snapshot err = %v", err)
	}
}

func TestRunHelpGoesToStdoutLikeArgparse(t *testing.T) {
	code, stdout, stderr := runCLI(t, []string{"--help"}, map[string]string{}, ingestclient.Config{})
	if code != 0 || stderr != "" {
		t.Fatalf("exit %d stderr = %q", code, stderr)
	}
	for _, flag := range []string{"--limit", "--mode", "--state-file", "--lock-file", "--library-path", "--authorize", "--retry-failed", "--network-diagnostics"} {
		if !strings.Contains(stdout, flag) {
			t.Errorf("usage lost %s: %s", flag, stdout)
		}
	}
	if code, _, stderr := runCLI(t, []string{"stray"}, map[string]string{}, ingestclient.Config{}); code != 2 || !strings.Contains(stderr, "unrecognized arguments: stray") {
		t.Fatalf("positional exit %d stderr = %s", code, stderr)
	}
}
