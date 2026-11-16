package androidcallrecordings

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// This fixture is consumed by Python's source-row contract test. Compare it
// with the real reader and BuildMetadata so it cannot drift into a Recorder sidecar.
func TestBCRMetadataFixture(t *testing.T) {
	dir := t.TempDir()
	at := time.UnixMilli(1794787200000).UTC()
	pair(t, dir, "example-call", ".oga", bcrJSON, at.Add(13*time.Second))
	records, err := ReadRecordings(dir)
	if err != nil || len(records) != 1 {
		t.Fatalf("read fixture: %v", err)
	}
	got := BuildMetadata("owner@example.com", "example-phone", records[0], strings.Repeat("a", 64), at.Add(time.Minute))
	encoded, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("PDW_WRITE_BCR_FIXTURE"); path != "" {
		if err := os.WriteFile(path, append(encoded, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile("../../../../tests/fixtures/android_call_recordings_metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	var want, actual map[string]any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatal("Python's call sidecar fixture differs from BuildMetadata")
	}
}

const privateStem = "example_+15550100100_Example Contact"
const bcrJSON = `{ "timestamp_unix_ms":1794787200000,"direction":"out","sim_slot":1,"calls":[{"phone_number_e164":"+15550100100","contact_name":"Example Contact"}],"output":{"recording":{"duration_secs_total":12.5,"duration_secs_encoded":4.25,"duration_secs_wall":12.5},"format":{"mime_type_container":"audio/ogg"}}}`

type fakeUpload struct {
	audio, metadata int
	fail            string
	types           []string
	payloads        []map[string]any
}

func (f *fakeUpload) UploadAndroidCallRecordingsAudio(a, b, c, d, e string) (ingestclient.StoredObject, error) {
	f.audio++
	f.types = append(f.types, d)
	if f.fail == "audio" {
		return ingestclient.StoredObject{}, errors.New(privateStem)
	}
	return ingestclient.StoredObject{}, nil
}
func (f *fakeUpload) UploadAndroidCallRecordingsMetadata(a map[string]any, b, c, d string) (ingestclient.StoredObject, error) {
	f.metadata++
	f.payloads = append(f.payloads, a)
	if f.fail == "metadata" {
		return ingestclient.StoredObject{}, errors.New(privateStem)
	}
	return ingestclient.StoredObject{}, nil
}
func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}
func ageFile(t *testing.T, path string, at time.Time) {
	t.Helper()
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
}
func pair(t *testing.T, dir, stem, ext, raw string, age time.Time) {
	t.Helper()
	for path, body := range map[string]string{filepath.Join(dir, stem+ext): "audio-" + stem, filepath.Join(dir, stem+".json"): raw} {
		writeFile(t, path, body)
		ageFile(t, path, age)
	}
}
func runner(t *testing.T, dir string, client Uploader) *Runner {
	t.Helper()
	state, err := OpenState(filepath.Join(t.TempDir(), "ledger.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	return &Runner{RecorderDir: dir, State: state, Client: client, Account: "owner@example.com", Device: "example-phone"}
}
func assertPrivate(t *testing.T, text string) {
	t.Helper()
	for _, secret := range []string{privateStem, "+15550100100", "Example Contact"} {
		if strings.Contains(text, secret) {
			t.Fatalf("private value leaked: %q", text)
		}
	}
}

func TestPairsSettleAndDeletionIsNotATombstone(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	old := now.Add(-time.Minute)
	pair(t, dir, privateStem, ".oga", bcrJSON, old)
	client := &fakeUpload{}
	r := runner(t, dir, client)
	r.Now = func() time.Time { return now }
	ageFile(t, filepath.Join(dir, privateStem+".json"), now)
	summary, err := r.Sync()
	if err != nil || summary.Unsettled != 1 || client.audio != 0 {
		t.Fatalf("settle: %+v %v", summary, err)
	}
	ageFile(t, filepath.Join(dir, privateStem+".json"), old)
	records, err := ReadRecordings(dir)
	if err != nil || len(records) != 1 {
		t.Fatalf("read: %v", err)
	}
	payload := BuildMetadata(r.Account, r.Device, records[0], "abc", now)
	if payload["bcr_raw_json"] != bcrJSON || records[0].DurationMS != 4250 {
		t.Fatal("BCR evidence lost")
	}
	if records[0].Fields["ended_at"] != records[0].CreatedAt.Add(12500*time.Millisecond).Format(time.RFC3339Nano) {
		t.Fatal("end must use wall time")
	}
	for _, key := range []string{"recording_state", "original_path", "location"} {
		if _, ok := payload["recording"].(map[string]any)[key]; ok {
			t.Fatalf("Recorder leftover %s", key)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := r.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if client.audio != 1 || client.metadata != 1 {
		t.Fatal("ledger did not suppress resend")
	}
	for _, ext := range []string{".oga", ".json"} {
		if err := os.Remove(filepath.Join(dir, privateStem+ext)); err != nil {
			t.Fatal(err)
		}
	}
	summary, err = r.Sync()
	if err != nil || summary.Seen != 0 || client.metadata != 1 {
		t.Fatalf("reaping emitted mutation: %+v %v", summary, err)
	}
}
func TestConferenceNullFieldsAndAudioTypes(t *testing.T) {
	dir := t.TempDir()
	raw := `{"timestamp_unix_ms":1794787200000,"direction":"conference","sim_slot":null,"call_log_name":null,"calls":[{"phone_number":null,"caller_name":null,"contact_name":null},{"phone_number_e164":"+15550100100","contact_name":"Example Contact"},{"phone_number":"+15550100101","caller_name":"Example Caller"}],"output":{"recording":null,"format":{}}}`
	for _, ext := range []string{".oga", ".m4a"} {
		pair(t, dir, "conference"+ext, ext, raw, time.Now().Add(-time.Minute))
	}
	client := &fakeUpload{}
	if _, err := runner(t, dir, client).Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.types) != 2 || !strings.Contains(strings.Join(client.types, ","), "audio/ogg") || !strings.Contains(strings.Join(client.types, ","), "audio/mp4") {
		t.Fatalf("types: %v", client.types)
	}
	record := client.payloads[0]["recording"].(map[string]any)
	if record["sim_slot"] != nil || record["call_log_name"] != nil || len(record["phone_numbers"].([]any)) != 2 {
		t.Fatalf("conference: %v", record)
	}
}
func TestInvalidPairDoesNotBlockValidCallsAndIsPrivate(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().Add(-time.Minute)
	pair(t, dir, privateStem, ".oga", "invalid JSON "+privateStem, old)
	pair(t, dir, "valid", ".oga", bcrJSON, old)
	client := &fakeUpload{}
	summary, err := runner(t, dir, client).Sync()
	if err == nil || summary.Invalid != 1 || client.audio != 1 || client.metadata != 1 {
		t.Fatalf("isolation: %+v %v", summary, err)
	}
	assertPrivate(t, err.Error())
}
func TestMissingSidecarWaitsAndBrokenStatIsCounted(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "unpaired.oga"), "audio")
	if err := os.Symlink("absent", filepath.Join(dir, privateStem+".oga")); err != nil {
		t.Fatal(err)
	}
	client := &fakeUpload{}
	summary, err := runner(t, dir, client).Sync()
	if err == nil || summary.Invalid != 1 || client.audio != 0 {
		t.Fatalf("stat failure: %+v %v", summary, err)
	}
	assertPrivate(t, err.Error())
}
func TestLimitRemainingAndDeferral(t *testing.T) {
	for _, deferUpload := range []bool{false, true} {
		t.Run(fmt.Sprint(deferUpload), func(t *testing.T) {
			dir := t.TempDir()
			for i := 0; i < 3; i++ {
				pair(t, dir, fmt.Sprint(i), ".oga", bcrJSON, time.Now().Add(-time.Minute))
			}
			client := &fakeUpload{}
			r := runner(t, dir, client)
			r.Limit = 1
			var logs bytes.Buffer
			r.Logger = common.NewWriterLogger(&logs)
			if deferUpload {
				r.BeforeUploadCheck = func() string { return privateStem }
			}
			summary, err := r.Sync()
			if err != nil {
				t.Fatal(err)
			}
			if deferUpload {
				if !summary.Deferred || summary.Remaining != 3 || client.audio != 0 {
					t.Fatalf("defer: %+v", summary)
				}
			} else if summary.Remaining != 2 || client.audio != 1 {
				t.Fatalf("limit: %+v", summary)
			}
			assertPrivate(t, logs.String())
		})
	}
}
func TestFailureStopsSerialUploadsAndNeverLeaksIdentity(t *testing.T) {
	for _, failure := range []string{"audio", "metadata"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			pair(t, dir, privateStem, ".oga", bcrJSON, time.Now().Add(-time.Minute))
			pair(t, dir, "other", ".oga", bcrJSON, time.Now().Add(-time.Minute))
			client := &fakeUpload{fail: failure}
			r := runner(t, dir, client)
			var logs bytes.Buffer
			r.Logger = common.NewWriterLogger(&logs)
			_, err := r.Sync()
			if err == nil || client.audio != 1 || client.metadata > 1 {
				t.Fatalf("serial failure: %+v %v", client, err)
			}
			assertPrivate(t, err.Error())
			assertPrivate(t, logs.String())
		})
	}
}
func TestDryRunNeverLeaksIdentity(t *testing.T) {
	dir := t.TempDir()
	pair(t, dir, privateStem, ".oga", bcrJSON, time.Now().Add(-time.Minute))
	project := t.TempDir()
	var out, stderr bytes.Buffer
	getenv := func(key string) string {
		if key == "PDW_INGEST_PROJECT_DIR" {
			return project
		}
		return ""
	}
	code := Run([]string{"--dry-run", "--recorder-dir", dir, "--state-file", filepath.Join(project, "state.sqlite"), "--lock-file", filepath.Join(project, "lock")}, nil, &out, &stderr, getenv, ingestclient.Config{})
	if code != 0 {
		t.Fatalf("dry run: %s", stderr.String())
	}
	assertPrivate(t, out.String()+stderr.String())
}

func TestCLILimitLeavesRemainingCallsForNextRun(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		pair(t, dir, fmt.Sprintf("example-call-%d", i), ".oga", bcrJSON, time.Now().Add(-time.Minute))
	}
	project := t.TempDir()
	var out, stderr bytes.Buffer
	getenv := func(key string) string {
		if key == "PDW_INGEST_PROJECT_DIR" {
			return project
		}
		return ""
	}
	code := Run([]string{"--dry-run", "--limit", "1", "--recorder-dir", dir,
		"--state-file", filepath.Join(project, "state.sqlite"), "--lock-file", filepath.Join(project, "lock")},
		nil, &out, &stderr, getenv, ingestclient.Config{})
	if code != 0 || !strings.Contains(out.String(), "audio=1 sidecars=1") || !strings.Contains(out.String(), "remaining=2") {
		t.Fatalf("CLI limit: code=%d stdout=%s stderr=%s", code, out.String(), stderr.String())
	}
	assertPrivate(t, out.String()+stderr.String())
}
