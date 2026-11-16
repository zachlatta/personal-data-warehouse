package androidcallrecordings

import (
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeUpload struct{ audio, metadata int }

func (f *fakeUpload) UploadAndroidCallRecordingsAudio(a, b, c, d, e string) (ingestclient.StoredObject, error) {
	f.audio++
	return ingestclient.StoredObject{}, nil
}
func (f *fakeUpload) UploadAndroidCallRecordingsMetadata(a map[string]any, b, c, d string) (ingestclient.StoredObject, error) {
	f.metadata++
	return ingestclient.StoredObject{}, nil
}

func TestPairsSettleAndDeletionIsNotATombstone(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "example.oga")
	meta := filepath.Join(dir, "example.json")
	raw := `{ "timestamp_unix_ms":1794787200000,"direction":"out","sim_slot":1,"calls":[{"phone_number_e164":"+15550100100","contact_name":"Example Contact"}],"output":{"recording":{"duration_secs_total":12.5,"duration_secs_encoded":4.25,"duration_secs_wall":12.5},"format":{"mime_type":"audio/ogg"}}}`
	os.WriteFile(path, []byte("audio"), 0600)
	records, err := ReadRecordings(dir, "")
	if err != nil || len(records) != 0 {
		t.Fatalf("unpaired audio: %v %v", records, err)
	}
	os.WriteFile(meta, []byte(raw), 0600)
	now := time.Now().UTC()
	old := now.Add(-time.Minute)
	os.Chtimes(path, old, old)
	state, err := OpenState(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	client := &fakeUpload{}
	runner := Runner{RecorderDir: dir, State: state, Client: client, Now: func() time.Time { return now }}
	summary, err := runner.Sync()
	if err != nil || summary.Unsettled != 1 || client.audio != 0 {
		t.Fatalf("must settle both files: %+v %v", summary, err)
	}
	os.Chtimes(meta, old, old)
	records, err = ReadRecordings(dir, "")
	if err != nil || len(records) != 1 {
		t.Fatal(err)
	}
	payload := BuildMetadata("owner@example.com", "example-phone", records[0], "abc", now)
	if payload["bcr_raw_json"] != raw || records[0].DurationMS != 4250 || records[0].Title != "Call with Example Contact" {
		t.Fatalf("metadata lost: %v", payload)
	}
	if records[0].Fields["ended_at"] != records[0].CreatedAt.Add(12500*time.Millisecond).Format(time.RFC3339Nano) {
		t.Fatal("call end must use wall time, not saved audio length")
	}
	summary, err = runner.Sync()
	if err != nil || client.audio != 1 || client.metadata != 1 {
		t.Fatalf("upload: %+v %v", summary, err)
	}
	runner.Sync()
	if client.audio != 1 || client.metadata != 1 {
		t.Fatal("ledger did not suppress resend")
	}
	os.Remove(path)
	os.Remove(meta)
	summary, err = runner.Sync()
	if err != nil || client.metadata != 1 {
		t.Fatalf("reaped file emitted mutation: %+v %v", summary, err)
	}
}
