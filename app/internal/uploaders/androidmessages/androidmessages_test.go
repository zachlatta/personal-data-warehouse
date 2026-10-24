package androidmessages

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type fakeUploader struct {
	batches [][]map[string]any
	fail    error
}

func (f *fakeUploader) UploadAndroidMessagesBatch(body []byte, exportedAt string) (ingestclient.StoredObject, error) {
	if f.fail != nil {
		return ingestclient.StoredObject{}, f.fail
	}
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return ingestclient.StoredObject{}, err
		}
		records = append(records, record)
	}
	f.batches = append(f.batches, records)
	return ingestclient.StoredObject{StorageKey: fmt.Sprintf("batch-%d", len(f.batches))}, nil
}

func (f *fakeUploader) sent() []map[string]any {
	var out []map[string]any
	for _, batch := range f.batches {
		for _, envelope := range batch {
			out = append(out, envelope["record"].(map[string]any))
		}
	}
	return out
}

var (
	switchedAt = time.Date(2026, 10, 23, 20, 0, 0, 0, time.UTC)
	now        = time.Date(2026, 10, 24, 20, 0, 0, 0, time.UTC)
)

// bugleDB lays out the part of Google Messages' schema the reader uses, with
// the -wal/-shm files Messages keeps beside an open database.
func bugleDB(t *testing.T) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bugle_db")
	db, err := common.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, statement := range []string{
		`CREATE TABLE messages (_id INTEGER PRIMARY KEY, conversation_id INT, sender_id INT, self_id INT,
			sent_timestamp INT, received_timestamp INT, message_protocol INT, message_status INT, seen INT, read INT,
			sms_message_uri TEXT, mms_subject TEXT, rcs_message_id STRING, is_hidden INT, sms_error_code INT)`,
		`CREATE TABLE parts (_id INTEGER PRIMARY KEY, message_id INT, text TEXT, content_type TEXT, width INT,
			height INT, duration INT, file_name TEXT, file_size_bytes INT)`,
		`CREATE TABLE conversations (_id INTEGER PRIMARY KEY, name TEXT, participant_count INT, conv_type INT, rcs_group_id TEXT)`,
		`CREATE TABLE participants (_id INTEGER PRIMARY KEY, normalized_destination TEXT, full_name TEXT, subscription_name TEXT)`,
		`CREATE TABLE conversation_participants (_id INTEGER PRIMARY KEY, conversation_id INT, participant_id INT)`,
		`INSERT INTO participants VALUES (2, '+15550000001', '', 'Example Carrier'), (5, '+15550000002', 'Alex Example', ''),
			(6, '+15550000003', 'Sam Sample', '')`,
		`INSERT INTO conversations VALUES (10, 'Alex Example', 1, 0, ''), (11, 'Weekend plans', 2, 2, 'group-abc')`,
		`INSERT INTO conversation_participants (conversation_id, participant_id) VALUES (10, 5), (11, 5), (11, 6)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.WriteFile(path+suffix, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return path, db
}

func addMessage(t *testing.T, db *sql.DB, id int64, conversation, sender int64, at time.Time, status int, uri, text string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO messages VALUES (?, ?, ?, 2, ?, ?, 3, ?, 1, 0, ?, '', NULL, 0, 0)`,
		id, conversation, sender, at.UnixMilli(), at.UnixMilli(), status, uri); err != nil {
		t.Fatal(err)
	}
	if text != "" {
		if _, err := db.Exec(`INSERT INTO parts (message_id, text, content_type, width, height, duration, file_name, file_size_bytes)
			VALUES (?, ?, 'text/plain', 0, 0, 0, '', 0)`, id, text); err != nil {
			t.Fatal(err)
		}
	}
}

func runner(path string, client Uploader) *Runner {
	return &Runner{
		DatabasePath: path, StatePath: filepath.Join(filepath.Dir(path), "state.json"), Since: switchedAt,
		Account: "zach@example.test", Device: "pichu", Client: client,
		Logger: &common.RecordingLogger{}, Now: func() time.Time { return now },
	}
}

func TestSyncSendsOnlyMessagesSinceTheSwitch(t *testing.T) {
	path, db := bugleDB(t)
	// The iPhone history imported into Messages: before the switch.
	addMessage(t, db, 1, 10, 5, switchedAt.Add(-48*time.Hour), 100, "content://mms/1", "old iPhone text")
	addMessage(t, db, 2, 10, 5, switchedAt.Add(-time.Minute), 100, "content://mms/2", "just before")
	addMessage(t, db, 3, 10, 5, switchedAt.Add(time.Hour), 100, "content://sms/1", "lunch?")
	addMessage(t, db, 4, 11, 2, switchedAt.Add(2*time.Hour), 2, "content://mms/40", "sure")
	client := &fakeUploader{}

	summary, err := runner(path, client).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Read != 2 || summary.Uploaded != 2 || summary.Batches != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	sent := client.sent()
	if sent[0]["message_key"] != "content://sms/1" || sent[1]["message_key"] != "content://mms/40" {
		t.Fatalf("sent = %v", sent)
	}
	first := sent[0]
	if first["sender_address"] != "+15550000002" || first["sender_name"] != "Alex Example" ||
		first["chat_key"] != "+15550000002" || first["sim_name"] != "Example Carrier" {
		t.Fatalf("first = %v", first)
	}
	if parts := first["parts"].([]any); len(parts) != 1 || parts[0].(map[string]any)["text"] != "lunch?" {
		t.Fatalf("parts = %v", first["parts"])
	}
	if sent[1]["chat_key"] != "rcs-group:group-abc" || len(sent[1]["participants"].([]any)) != 2 {
		t.Fatalf("group = %v", sent[1])
	}
	envelope := client.batches[0][0]
	if envelope["source"] != "android_messages" || envelope["record_type"] != "message" ||
		envelope["device"] != "pichu" || envelope["exported_at"] != "2026-10-24T20:00:00+00:00" {
		t.Fatalf("envelope = %v", envelope)
	}
}

func TestSyncResendsOnlyWhatChanged(t *testing.T) {
	path, db := bugleDB(t)
	addMessage(t, db, 3, 10, 5, now.Add(-time.Hour), 100, "content://sms/1", "lunch?")
	addMessage(t, db, 4, 10, 2, now.Add(-time.Hour), 1, "content://sms/2", "sure")
	if _, err := runner(path, &fakeUploader{}).Sync(); err != nil {
		t.Fatal(err)
	}

	client := &fakeUploader{}
	summary, err := runner(path, client).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Uploaded != 0 || summary.Unchanged != 2 {
		t.Fatalf("an unchanged database must send nothing: %+v", summary)
	}

	// Delivered: the status changes after it was sent.
	if _, err := db.Exec(`UPDATE messages SET message_status = 2 WHERE _id = 4`); err != nil {
		t.Fatal(err)
	}
	addMessage(t, db, 5, 10, 5, now.Add(-time.Minute), 100, "content://sms/3", "see you")
	summary, err = runner(path, client).Sync()
	if err != nil {
		t.Fatal(err)
	}
	sent := client.sent()
	if summary.Uploaded != 2 || sent[0]["message_key"] != "content://sms/2" || sent[1]["message_key"] != "content://sms/3" {
		t.Fatalf("summary = %+v, sent = %v", summary, sent)
	}
}

func TestAFailedUploadIsSentAgain(t *testing.T) {
	path, db := bugleDB(t)
	addMessage(t, db, 3, 10, 5, now.Add(-time.Hour), 100, "content://sms/1", "lunch?")
	if _, err := runner(path, &fakeUploader{fail: errors.New("502")}).Sync(); err == nil {
		t.Fatal("want the upload error")
	}
	client := &fakeUploader{}
	if summary, err := runner(path, client).Sync(); err != nil || summary.Uploaded != 1 {
		t.Fatalf("summary = %+v, err = %v", summary, err)
	}
}

func TestAClosedDatabaseIsNotOpened(t *testing.T) {
	path, db := bugleDB(t)
	addMessage(t, db, 3, 10, 5, now.Add(-time.Hour), 100, "content://sms/1", "lunch?")
	if err := os.Remove(path + "-shm"); err != nil {
		t.Fatal(err)
	}
	client := &fakeUploader{}
	summary, err := runner(path, client).Sync()
	if err != nil || !summary.Deferred || len(client.batches) != 0 {
		t.Fatalf("summary = %+v, err = %v", summary, err)
	}
	if _, err := os.Stat(path + "-shm"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the reader must not create Messages' -shm file: %v", err)
	}
}

func TestMessageKeyPrefersWhatSurvivesAMessagesReset(t *testing.T) {
	for _, c := range []struct {
		message map[string]any
		want    string
	}{
		{map[string]any{"rcs_message_id": "abc", "telephony_uri": "content://mms/9", "bugle_id": int64(7)}, "rcs:abc"},
		{map[string]any{"rcs_message_id": "", "telephony_uri": "content://mms/9", "bugle_id": int64(7)}, "content://mms/9"},
		{map[string]any{"rcs_message_id": "", "telephony_uri": "", "bugle_id": int64(7)}, "bugle:7"},
	} {
		if got := messageKey(c.message); got != c.want {
			t.Fatalf("messageKey(%v) = %q, want %q", c.message, got, c.want)
		}
	}
}

func TestRunRequiresAStartTime(t *testing.T) {
	var out, errOut bytes.Buffer
	env := func(key string) string {
		if key == "HOME" {
			return t.TempDir()
		}
		return ""
	}
	code := Run([]string{"--account", "zach@example.test", "--dry-run"}, nil, &out, &errOut, env, ingestclient.Config{})
	if code == 0 || !strings.Contains(errOut.String(), "ANDROID_MESSAGES_SINCE") {
		t.Fatalf("code = %d, stderr = %q", code, errOut.String())
	}
}

func TestRunDryRunReportsWhatItWouldSend(t *testing.T) {
	path, db := bugleDB(t)
	addMessage(t, db, 3, 10, 5, now.Add(-time.Hour), 100, "content://sms/1", "lunch?")
	var out, errOut bytes.Buffer
	dir := t.TempDir()
	code := Run([]string{"--database", path, "--since", "2026-10-23T16:00:00-04:00", "--account", "zach@example.test",
		"--device", "pichu", "--dry-run", "--state-file", filepath.Join(dir, "state.json"),
		"--lock-file", filepath.Join(dir, "run.lock")}, nil, &out, &errOut, func(string) string { return "" }, ingestclient.Config{})
	if code != 0 {
		t.Fatalf("code = %d, stderr = %q", code, errOut.String())
	}
	if !strings.Contains(out.String(), "read=1 unchanged=0 messages=1 batches=1") {
		t.Fatalf("stdout = %q", out.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "state.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a dry run must record nothing: %v", err)
	}
}
