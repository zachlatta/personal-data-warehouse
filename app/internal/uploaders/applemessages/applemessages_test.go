package applemessages

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/testfixtures"
)

// attributed_bodies.jsonl holds NSArchiver blobs produced by Foundation on
// macOS plus the text the Python typedstream library decoded from them.
func TestDecodeAttributedBodyMatchesPythonTypedstream(t *testing.T) {
	file, err := os.Open(filepath.Join("testdata", "attributed_bodies.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<20), 16<<20)
	count := 0
	for scanner.Scan() {
		var fixture struct {
			Name     string `json:"name"`
			Hex      string `json:"hex"`
			Expected struct {
				Text   string `json:"text"`
				Source string `json:"source"`
				Status string `json:"status"`
				SHA    string `json:"attributed_body_sha256"`
				Error  string `json:"error"`
			} `json:"expected"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &fixture); err != nil {
			t.Fatal(err)
		}
		data, _ := hex.DecodeString(fixture.Hex)
		got := DecodeMessageBody("", data)
		if got.Text != fixture.Expected.Text || got.Source != fixture.Expected.Source || got.Status != fixture.Expected.Status || got.AttributedBodySHA256 != fixture.Expected.SHA {
			t.Errorf("%s: got %+v want %+v", fixture.Name, got, fixture.Expected)
		}
		count++
	}
	if count < 8 {
		t.Fatalf("only %d fixtures", count)
	}
	bad := DecodeMessageBody("fallback", []byte("not a typedstream"))
	if bad.Status != "fallback_text" || bad.Text != "fallback" || bad.Error == "" {
		t.Fatalf("bad blob = %+v", bad)
	}
	if empty := DecodeMessageBody("", nil); empty.Status != "empty" {
		t.Fatalf("empty = %+v", empty)
	}
}

func scanFixture(t *testing.T) *Snapshot {
	t.Helper()
	snapshot, err := Scan(filepath.Join("testdata", "chat.db"), "testdata")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestScanMatchesPythonGoldens(t *testing.T) {
	goldens := testfixtures.LoadGoldens(t, filepath.Join("testdata", "golden.jsonl"))["chat"]
	snapshot := scanFixture(t)
	var got []testfixtures.Golden
	for _, item := range SnapshotPayloads(snapshot) {
		encoded, _ := common.CanonicalJSON(item.payload)
		got = append(got, testfixtures.Golden{Kind: item.sourceType, Fingerprint: common.JSONSHA256(item.payload), Payload: string(encoded), Extra: map[string]any{"source_id": item.sourceID}})
	}
	for _, message := range snapshot.Messages {
		encoded, _ := common.CanonicalJSON(MessagePayload(message))
		got = append(got, testfixtures.Golden{Kind: "message", Fingerprint: common.JSONSHA256(MessageFingerprintPayload(message)), Payload: string(encoded), Extra: map[string]any{"source_id": message.MessageID}})
	}
	if len(got) != len(goldens) {
		t.Fatalf("got %d records, golden has %d", len(got), len(goldens))
	}
	for i := range goldens {
		if got[i].Kind != goldens[i].Kind || got[i].Extra["source_id"] != goldens[i].Extra["source_id"] {
			t.Fatalf("record %d is %s/%v, golden %s/%v", i, got[i].Kind, got[i].Extra["source_id"], goldens[i].Kind, goldens[i].Extra["source_id"])
		}
		if got[i].Payload != goldens[i].Payload {
			t.Errorf("%s %v payload differs:\n got %s\nwant %s", got[i].Kind, got[i].Extra["source_id"], got[i].Payload, goldens[i].Payload)
		}
		if got[i].Fingerprint != goldens[i].Fingerprint {
			t.Errorf("%s %v fingerprint differs", got[i].Kind, got[i].Extra["source_id"])
		}
	}
}

type fakeClient struct {
	batches     [][]map[string]any
	attachments []ingestclient.AppleMessagesAttachment
}

func (f *fakeClient) UploadAppleMessagesBatch(gz []byte, exportedAt string) (ingestclient.StoredObject, error) {
	f.batches = append(f.batches, testfixtures.DecodeGzipJSONL(gz))
	return ingestclient.StoredObject{StorageKey: "apple-messages/inbox/batch"}, nil
}

func (f *fakeClient) UploadAppleMessagesAttachment(content []byte, a ingestclient.AppleMessagesAttachment) (ingestclient.StoredObject, error) {
	f.attachments = append(f.attachments, a)
	return ingestclient.StoredObject{StorageBackend: "google_drive", StorageKey: "apple-messages/inbox/attachments/" + a.AttachmentGUID, StorageFileID: "f"}, nil
}

func TestRunnerUploadsManifestAndAttachmentThenSkipsUnchanged(t *testing.T) {
	state, err := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "zach@example.com", "/store")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	client := &fakeClient{}
	runner := &Runner{Account: "zach@example.com", Client: client, Logger: &common.RecordingLogger{}, State: state,
		Now:                   func() time.Time { return time.Date(2026, 5, 21, 13, 0, 0, 0, time.UTC) },
		AttachmentCountPerRun: 1, AttachmentBytesPerRun: 1024 * 1024, Workers: 2, snapshotForTest: scanFixture(t)}
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.BatchesUploaded != 1 || summary.AttachmentsUploaded != 1 || summary.MessagesSeen != 6 {
		t.Fatalf("summary = %+v", summary)
	}
	if client.attachments[0].AttachmentGUID != "att-2" || client.attachments[0].ContentType != "video/quicktime" || client.attachments[0].MessageGUID != "m-attr" {
		t.Fatalf("attachment upload = %+v", client.attachments[0])
	}
	var messages, withFile int
	for _, record := range client.batches[0] {
		if record["record_type"] == "message" {
			messages++
			body := record["record"].(map[string]any)
			if body["message_id"] == "m-attr" && body["body_text"] != "Emoji 🙂 and café — “quotes”" {
				t.Fatalf("body_text = %v", body["body_text"])
			}
		}
		if record["record_type"] == "attachment" {
			if _, ok := record["record"].(map[string]any)["file"]; ok {
				withFile++
			}
		}
	}
	if messages != 6 || withFile != 1 {
		t.Fatalf("messages=%d withFile=%d", messages, withFile)
	}
	second, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if second.RecordsSelected != 0 || second.BatchesUploaded != 0 || second.AttachmentsUploaded != 0 {
		t.Fatalf("second summary = %+v", second)
	}
	entry, _ := state.EntryFor("attachment_blob", "att-2")
	if entry == nil || !entry.Complete || entry.StorageKey != "apple-messages/inbox/attachments/att-2" {
		t.Fatalf("blob state = %+v", entry)
	}
}

func TestLimitKeepsMarksForKeptRecordsOnly(t *testing.T) {
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z", "/store")
	defer state.Close()
	client := &fakeClient{}
	runner := &Runner{Account: "z", Client: client, Logger: &common.RecordingLogger{}, State: state, Limit: 3, AttachmentCountPerRun: 0, snapshotForTest: scanFixture(t)}
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.BatchesUploaded != 1 || len(client.batches[0]) < 3 {
		t.Fatalf("summary = %+v", summary)
	}
	// Only the three kept manifest records (plus the attachment upload) are
	// marked complete; the rest re-select next run.
	complete, _ := state.IsComplete("handle", "1", common.JSONSHA256(HandlePayload(runner.snapshotForTest.Handles[0])))
	if !complete {
		t.Fatal("first handle must be marked complete")
	}
	if ok, _ := state.IsComplete("message", "m-attr", common.JSONSHA256(MessageFingerprintPayload(runner.snapshotForTest.Messages[1]))); ok {
		t.Fatal("a deferred message must not be marked complete")
	}
}

func TestNetworkGuardDefersEverything(t *testing.T) {
	client := &fakeClient{}
	runner := &Runner{Account: "z", Client: client, Logger: &common.RecordingLogger{}, BeforeUploadCheck: func() string { return "blocked" }, snapshotForTest: scanFixture(t)}
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.BatchesUploaded != 0 || len(client.batches) != 0 || summary.AttachmentsDeferred == 0 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestResolveAttachmentPath(t *testing.T) {
	home, _ := os.UserHomeDir()
	if got := ResolveAttachmentPath("~/Library/Messages/x.jpg", "/root"); got != filepath.Join(home, "Library/Messages/x.jpg") {
		t.Fatalf("home = %q", got)
	}
	if got := ResolveAttachmentPath("Attachments/x.jpg", "/root"); got != "/root/Attachments/x.jpg" {
		t.Fatalf("relative = %q", got)
	}
	if got := ResolveAttachmentPath("/abs/x.jpg", "/root"); got != "/abs/x.jpg" {
		t.Fatalf("absolute = %q", got)
	}
	if !strings.HasPrefix(NormalizedContentType("", "com.apple.m4a-audio", ""), "audio/") {
		t.Fatal("uti fallback")
	}
}
