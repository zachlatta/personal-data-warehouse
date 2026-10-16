package voicememos

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/testfixtures"
)

// zdate is a Core Data timestamp: seconds since 2001-01-01T00:00:00Z.
func zdate(t time.Time) float64 {
	return float64(t.Unix()-coreDataEpochUnix) + float64(t.Nanosecond())/1e9
}

func createCloudRecordingsDBWithDates(t *testing.T, root string, rows ...[]any) {
	t.Helper()
	statements := []string{"CREATE TABLE ZCLOUDRECORDING (Z_PK INTEGER PRIMARY KEY, ZPATH VARCHAR, ZDATE TIMESTAMP, ZDURATION FLOAT, ZLOCALDURATION FLOAT)"}
	for _, row := range rows {
		statements = append(statements, "INSERT INTO ZCLOUDRECORDING (ZPATH, ZDATE, ZDURATION, ZLOCALDURATION) VALUES ("+
			common.SQLiteLiteral(row[0].(string))+", "+sqlNumber(row[1])+", "+sqlNumber(row[2])+", "+sqlNumber(row[3])+")")
	}
	testfixtures.ExecScript(t, filepath.Join(root, "CloudRecordings.db"), statements...)
}

func sqlNumber(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(v)
}

func withFilenameLocation(t *testing.T, name string) {
	t.Helper()
	location, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	previous := filenameLocation
	filenameLocation = location
	t.Cleanup(func() { filenameLocation = previous })
}

// The filename stamp is the phone's wall clock where the memo was made, so
// reading it as UTC stored every memo 4-5 hours early (2026-10-16: 231 of 262
// calendar matches started ~4 h after the stored time). The store's ZDATE is
// an absolute instant and is what recorded_at must be.
func TestRecordedAtComesFromTheStoresAbsoluteDate(t *testing.T) {
	withFilenameLocation(t, "UTC") // a wrong zone, to prove ZDATE wins over the filename
	root := t.TempDir()
	writeRecordings(t, root, "20261015 205541-03A81A2D.m4a")
	actual := time.Date(2026, 10, 16, 0, 55, 41, 558_818_000, time.UTC)
	createCloudRecordingsDBWithDates(t, root, []any{"20261015 205541-03A81A2D.m4a", zdate(actual), 16.25, 16.25})

	candidates, err := ScanCandidates(root, DefaultExtensions)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d", len(candidates))
	}
	if got := candidates[0].RecordedAt; got.Sub(actual).Abs() > time.Millisecond {
		t.Fatalf("recorded_at = %s, want ZDATE %s", got, actual)
	}
	if candidates[0].Duration == nil || *candidates[0].Duration != 16.25 {
		t.Fatalf("durations must still load beside the date: %+v", candidates[0])
	}
}

func TestRecordedAtFallsBackToTheFilenameReadInTheMacsZone(t *testing.T) {
	withFilenameLocation(t, "America/New_York")
	root := t.TempDir()
	writeRecordings(t, root, "20261015 205541-03A81A2D.m4a", "20260115 090000-AAAA1111.m4a")

	candidates, err := ScanCandidates(root, DefaultExtensions)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]time.Time{
		"20261015 205541-03A81A2D": time.Date(2026, 10, 16, 0, 55, 41, 0, time.UTC), // EDT, UTC-4
		"20260115 090000-AAAA1111": time.Date(2026, 1, 15, 14, 0, 0, 0, time.UTC),   // EST, UTC-5
	}
	for _, c := range candidates {
		if !c.RecordedAt.Equal(want[c.RecordingID]) {
			t.Errorf("%s recorded_at = %s, want %s", c.RecordingID, c.RecordedAt.UTC(), want[c.RecordingID])
		}
	}
}

func TestAStoreWithoutZDATEStillLoadsDurations(t *testing.T) {
	withFilenameLocation(t, "UTC")
	root := t.TempDir()
	writeRecordings(t, root, "20260427 100004-40DC0200.m4a")
	createCloudRecordingsDB(t, root, []any{"U1", "20260427 100004-40DC0200.m4a", "New Recording 3", 4100, 12.5, 12.5})

	candidates, err := ScanCandidates(root, DefaultExtensions)
	if err != nil {
		t.Fatal(err)
	}
	if candidates[0].Duration == nil || *candidates[0].Duration != 12.5 {
		t.Fatalf("durations = %+v", candidates[0])
	}
	if !candidates[0].RecordedAt.Equal(time.Date(2026, 4, 27, 10, 0, 4, 0, time.UTC)) {
		t.Fatalf("recorded_at = %s", candidates[0].RecordedAt)
	}
}

// An already-uploaded memo whose recorded_at was computed the old way gets a
// corrected metadata sidecar -- and only that: the audio is not re-sent and
// the file is not re-hashed, so correcting ~750 memos costs ~750 small POSTs.
func TestACompleteMemoWithAStaleRecordedAtResendsOnlyItsMetadata(t *testing.T) {
	withFilenameLocation(t, "UTC")
	root := t.TempDir()
	name := "20261015 205541-03A81A2D.m4a"
	writeRecordings(t, root, name)
	actual := time.Date(2026, 10, 16, 0, 55, 41, 0, time.UTC)
	createCloudRecordingsDBWithDates(t, root, []any{name, zdate(actual), 16.0, 16.0})
	old := time.Now().Add(-time.Hour)
	os.Chtimes(filepath.Join(root, name), old, old)

	candidates, _ := ScanCandidates(root, DefaultExtensions)
	state := EmptyState("zach@example.com", root)
	state.MarkSuccess(candidates[0], "stored-sha", true, true, testNow)
	entry, _ := state.EntryFor(candidates[0])
	entry.RecordedAt = "2026-10-15T20:55:41+00:00" // what the filename-as-UTC parse stored
	state.Entries[name] = entry

	client := &fakeUploader{}
	runner := newRunner(root, client, state)
	runner.Now = time.Now
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.audio) != 0 {
		t.Fatalf("audio must not be re-sent: %+v", client.audio)
	}
	if len(client.metadata) != 1 {
		t.Fatalf("metadata posts = %d, want 1", len(client.metadata))
	}
	recording := client.metadata[0]["recording"].(map[string]any)
	if recording["recorded_at"] != "2026-10-16T00:55:41+00:00" || recording["content_sha256"] != "stored-sha" {
		t.Fatalf("corrected sidecar = %+v", recording)
	}
	if summary.MetadataUploaded != 1 || summary.RecordingsUploaded != 0 {
		t.Fatalf("summary = %+v", summary)
	}
	entry, _ = state.EntryFor(candidates[0])
	if entry.RecordedAt != "2026-10-16T00:55:41+00:00" || !entry.Complete() || entry.ContentSHA256 != "stored-sha" {
		t.Fatalf("state after correction = %+v", entry)
	}

	client2 := &fakeUploader{}
	runner2 := newRunner(root, client2, state)
	runner2.Now = time.Now
	if _, err := runner2.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client2.metadata) != 0 || len(client2.audio) != 0 {
		t.Fatalf("a corrected memo is complete: audio=%d metadata=%d", len(client2.audio), len(client2.metadata))
	}
}

func TestANewUploadRecordsItsRecordedAtInTheState(t *testing.T) {
	withFilenameLocation(t, "UTC")
	root := t.TempDir()
	name := "20261015 205541-03A81A2D.m4a"
	writeRecordings(t, root, name)
	old := time.Now().Add(-time.Hour)
	os.Chtimes(filepath.Join(root, name), old, old)
	state := EmptyState("zach@example.com", root)
	runner := newRunner(root, &fakeUploader{}, state)
	runner.Now = time.Now
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	candidates, _ := ScanCandidates(root, DefaultExtensions)
	entry, _ := state.EntryFor(candidates[0])
	if entry.RecordedAt != "2026-10-15T20:55:41+00:00" {
		t.Fatalf("state recorded_at = %q", entry.RecordedAt)
	}
	out := filepath.Join(t.TempDir(), "state.json")
	if err := state.Save(out); err != nil {
		t.Fatal(err)
	}
	reloaded := LoadState(out, "zach@example.com", root)
	if got, _ := reloaded.EntryFor(candidates[0]); got.RecordedAt != entry.RecordedAt {
		t.Fatalf("recorded_at did not survive a save: %+v", got)
	}
}
