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
			sms_message_uri TEXT, mms_subject TEXT, rcs_message_id STRING, is_hidden INT, sms_error_code INT,
			original_rcs_message_id TEXT, original_message_id INT, parent_message_id INTEGER, sender_send_destination TEXT,
			message_report_status INT, outgoing_delivery_report_status INT, outgoing_read_report_status INT,
			encryption_protocol INT, message_original_protocol INT, fallback_reason INT)`,
		`CREATE TABLE parts (_id INTEGER PRIMARY KEY, message_id INT, text TEXT, content_type TEXT, width INT,
			height INT, duration INT, file_name TEXT, file_size_bytes INT, raw_text TEXT, uri TEXT, local_cache_path TEXT,
			timestamp INT, latitude REAL, longitude REAL, sticker_set_id INT, sticker_id INT, expressive_sticker_name TEXT,
			content_description TEXT, attachment_id TEXT, preview_content_type TEXT)`,
		`CREATE TABLE message_reactions (_id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER NOT NULL UNIQUE,
			reactions_data BLOB, reacted_message_id INTEGER, reaction INTEGER, applied_reaction BLOB, animation_effect BLOB)`,
		`CREATE TABLE message_edits (message_id INTEGER NOT NULL, latest_message_id INTEGER, original_rcs_message_id TEXT,
			edited_at_timestamp_ms INTEGER, received_at_timestamp_ms INTEGER)`,
		`CREATE TABLE message_replies (message_id INTEGER NOT NULL, replied_to_message_id INTEGER,
			replied_to_message_id_null_reason INT, replied_to_rcs_message_id TEXT, message_thread_id TEXT)`,
		`CREATE TABLE message_captions (message_id INTEGER NOT NULL, caption TEXT NOT NULL)`,
		`CREATE TABLE message_star (_id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER)`,
		`CREATE TABLE message_self_mention (message_id INTEGER PRIMARY KEY NOT NULL, conversation_id INTEGER)`,
		`CREATE TABLE link_preview (_id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER NOT NULL UNIQUE,
			trigger_url TEXT NOT NULL, expiration_time_millis INTEGER DEFAULT(0), link_title TEXT, link_description TEXT,
			link_image_url TEXT, link_domain TEXT, link_canonical_url TEXT, link_preview_prevented INTEGER DEFAULT(0),
			link_preview_failed INTEGER DEFAULT(0), deferred INTEGER DEFAULT(0), favicon_url TEXT, link_image_attributes BLOB)`,
		`CREATE TABLE read_reports (_id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INT NOT NULL, participant_id INT NOT NULL,
			receive_time INT DEFAULT(0), read_time INT DEFAULT(0), ftd_time INTEGER DEFAULT(-1))`,
		`CREATE TABLE messages_annotations (_id INTEGER PRIMARY KEY AUTOINCREMENT, conversation_id TEXT, message_id INTEGER,
			annotation_type INT DEFAULT(0), annotation_details BLOB)`,
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
	if _, err := db.Exec(`INSERT INTO messages VALUES (?, ?, ?, 2, ?, ?, 3, ?, 1, 0, ?, '', NULL, 0, 0,
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`,
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

type fakeFiles struct {
	uploads []map[string]string
}

func (f *fakeFiles) UploadAndroidMessagesAttachment(path, messageAt, extension, contentType, sha string) (ingestclient.StoredObject, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	f.uploads = append(f.uploads, map[string]string{
		"content": string(raw), "message_at": messageAt, "extension": extension, "content_type": contentType, "sha": sha,
	})
	return ingestclient.StoredObject{
		StorageBackend: "google_drive", StorageKey: "android-messages/library/attachments/" + sha + extension,
		StorageFileID: fmt.Sprintf("file-%d", len(f.uploads)),
	}, nil
}

func addPart(t *testing.T, db *sql.DB, messageID int64, contentType, fileName, uri, cachePath string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO parts (message_id, text, content_type, width, height, duration, file_name,
		file_size_bytes, uri, local_cache_path) VALUES (?, '', ?, 0, 0, 0, ?, 0, ?, ?)`,
		messageID, contentType, fileName, uri, cachePath); err != nil {
		t.Fatal(err)
	}
}

func TestSyncSendsReactionsEditsRepliesAndPreviews(t *testing.T) {
	path, db := bugleDB(t)
	// A text from before the switch, reacted to after it: only its key is read.
	addMessage(t, db, 1, 10, 5, switchedAt.Add(-time.Hour), 1, "content://sms/1", "old text")
	addMessage(t, db, 2, 10, 5, now.Add(-2*time.Hour), 100, "content://sms/2", "got your card")
	addMessage(t, db, 3, 10, 5, now.Add(-time.Hour), 100, "content://sms/3", " ❤️ to “old text”")
	addMessage(t, db, 4, 10, 5, now.Add(-time.Hour), 100, "content://sms/4", "fixed typo")
	addMessage(t, db, 5, 10, 2, now.Add(-time.Minute), 1, "content://sms/5", "see https://example.com")
	for _, statement := range []string{
		// The reaction message (3) names its target; the target's tally lives on 2.
		`INSERT INTO message_reactions (message_id, reacted_message_id, reaction, applied_reaction)
			VALUES (3, 1, 12, X'0A06E29DA4EFB88F100C')`,
		`INSERT INTO message_reactions (message_id, reaction, reactions_data)
			VALUES (2, 0, X'0A160A0A0A06E29DA4EFB88F100C12080A040A023138100110ECB6E7E39634')`,
		`INSERT INTO message_edits VALUES (2, 4, 'orig-rcs', 1792900000000, 1792900001000)`,
		`INSERT INTO message_replies VALUES (5, 2, 0, '', 'thread-1')`,
		`INSERT INTO message_captions VALUES (2, 'a caption')`,
		`INSERT INTO message_star (message_id) VALUES (2)`,
		`INSERT INTO link_preview (message_id, trigger_url, link_title, link_domain) VALUES (5, 'https://example.com', 'Example', 'example.com')`,
		`INSERT INTO read_reports (message_id, participant_id, receive_time, read_time) VALUES (5, 5, 1792900002000, 1792900003000)`,
		`INSERT INTO messages_annotations (message_id, annotation_type, annotation_details) VALUES (5, 8, X'0801')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	client := &fakeUploader{}
	if _, err := runner(path, client).Sync(); err != nil {
		t.Fatal(err)
	}
	byKey := map[string]map[string]any{}
	for _, message := range client.sent() {
		byKey[message["message_key"].(string)] = message
	}
	if _, ok := byKey["content://sms/1"]; ok {
		t.Fatal("the message from before the switch must not be sent")
	}
	reaction := byKey["content://sms/3"]["reaction"].(map[string]any)
	if reaction["emoji"] != "❤️" || reaction["reacted_message_key"] != "content://sms/1" || reaction["reaction_type"] != float64(12) {
		t.Fatalf("reaction = %v", reaction)
	}
	target := byKey["content://sms/2"]
	if target["reactions_data"] == nil || target["caption"] != "a caption" || target["is_starred"] != float64(1) {
		t.Fatalf("target = %v", target)
	}
	if edit := target["edit"].(map[string]any); edit["latest_message_key"] != "content://sms/4" || edit["edited_at_ms"] != float64(1792900000000) {
		t.Fatalf("edit = %v", edit)
	}
	if editOf := byKey["content://sms/4"]["edit_of"].(map[string]any); editOf["original_message_key"] != "content://sms/2" {
		t.Fatalf("edit_of = %v", editOf)
	}
	latest := byKey["content://sms/5"]
	if reply := latest["reply"].(map[string]any); reply["replied_to_message_key"] != "content://sms/2" || reply["thread_id"] != "thread-1" {
		t.Fatalf("reply = %v", reply)
	}
	if preview := latest["link_preview"].(map[string]any); preview["title"] != "Example" || preview["url"] != "https://example.com" {
		t.Fatalf("link_preview = %v", preview)
	}
	reports := latest["read_reports"].([]any)
	if len(reports) != 1 || reports[0].(map[string]any)["address"] != "+15550000002" || reports[0].(map[string]any)["read_at_ms"] != float64(1792900003000) {
		t.Fatalf("read_reports = %v", reports)
	}
	if annotations := latest["annotations"].([]any); len(annotations) != 1 {
		t.Fatalf("annotations = %v", annotations)
	}
}

func TestSyncUploadsEachAttachmentFileOnce(t *testing.T) {
	path, db := bugleDB(t)
	dir := t.TempDir()
	photo := filepath.Join(dir, "IMG 1.jpg")
	cached := filepath.Join(dir, "part_9.bin")
	if err := os.WriteFile(photo, []byte("jpeg bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cached, []byte("cached mms bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	addMessage(t, db, 2, 10, 5, now.Add(-time.Hour), 100, "", "look")
	addPart(t, db, 2, "image/jpeg", "IMG 1.jpg", "file://"+strings.ReplaceAll(photo, " ", "%20"), "")
	addMessage(t, db, 3, 10, 5, now.Add(-time.Hour), 100, "content://mms/3", "")
	addPart(t, db, 3, "image/png", "", "content://mms/part/9", cached)
	addMessage(t, db, 4, 10, 5, now.Add(-time.Hour), 100, "content://mms/4", "")
	addPart(t, db, 4, "video/mp4", "", "content://mms/part/10", "")
	addMessage(t, db, 5, 10, 5, now.Add(-time.Hour), 100, "content://mms/5", "")
	addPart(t, db, 5, "image/gif", "", "file:///nowhere/never-downloaded.gif", "")
	previous := readContentURI
	readContentURI = func(uri, dest string) error {
		if uri != "content://mms/part/10" {
			return fmt.Errorf("unexpected uri %s", uri)
		}
		return os.WriteFile(dest, []byte("video bytes"), 0o600)
	}
	t.Cleanup(func() { readContentURI = previous })

	client, files := &fakeUploader{}, &fakeFiles{}
	r := runner(path, client)
	r.Files = files
	summary, err := r.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.AttachmentsUploaded != 3 || len(files.uploads) != 3 {
		t.Fatalf("summary = %+v, uploads = %v", summary, files.uploads)
	}
	got := map[string]string{}
	for _, upload := range files.uploads {
		got[upload["content"]] = upload["extension"]
	}
	if got["jpeg bytes"] != ".jpg" || got["cached mms bytes"] != ".png" || got["video bytes"] != ".mp4" {
		t.Fatalf("uploads = %v", files.uploads)
	}
	status := map[string]map[string]any{}
	for _, message := range client.sent() {
		for _, part := range message["parts"].([]any) {
			part := part.(map[string]any)
			if part["content_type"] != "text/plain" {
				status[message["message_key"].(string)] = part
			}
		}
	}
	if status["content://mms/3"]["file_status"] != "stored" || status["content://mms/3"]["storage_file_id"] == "" ||
		status["content://mms/3"]["content_sha256"] == "" || status["content://mms/5"]["file_status"] != "missing" {
		t.Fatalf("parts = %v", status)
	}
	if _, ok := status["content://mms/3"]["uri"]; ok {
		t.Fatal("the phone's file location is not sent")
	}

	// Next run: nothing is read or sent again.
	client2, files2 := &fakeUploader{}, &fakeFiles{}
	r2 := runner(path, client2)
	r2.Files = files2
	summary, err = r2.Sync()
	if err != nil || len(files2.uploads) != 0 || summary.Uploaded != 0 {
		t.Fatalf("summary = %+v, uploads = %v, err = %v", summary, files2.uploads, err)
	}
}

func TestAttachmentBudgetLeavesTheRestForTheNextRun(t *testing.T) {
	path, db := bugleDB(t)
	dir := t.TempDir()
	for i, id := range []int64{2, 3} {
		file := filepath.Join(dir, fmt.Sprintf("f%d.jpg", i))
		if err := os.WriteFile(file, []byte(strings.Repeat("x", 10)), 0o600); err != nil {
			t.Fatal(err)
		}
		addMessage(t, db, id, 10, 5, now.Add(-time.Hour), 100, fmt.Sprintf("content://mms/%d", id), "")
		addPart(t, db, id, "image/jpeg", "", "file://"+file, "")
	}
	files := &fakeFiles{}
	r := runner(path, &fakeUploader{})
	r.Files = files
	r.AttachmentBytesPerRun = 15
	if summary, err := r.Sync(); err != nil || summary.AttachmentsUploaded != 1 {
		t.Fatalf("summary = %+v, err = %v", summary, err)
	}
	client := &fakeUploader{}
	r = runner(path, client)
	r.Files = files
	r.AttachmentBytesPerRun = 15
	if summary, err := r.Sync(); err != nil || summary.AttachmentsUploaded != 1 || summary.Uploaded != 1 {
		t.Fatalf("summary = %+v, err = %v", summary, err)
	}
	if len(files.uploads) != 2 {
		t.Fatalf("uploads = %v", files.uploads)
	}
}

func TestDecodeProtobufReadsAReaction(t *testing.T) {
	decoded := decodeProtobufHex("0A06E29DA4EFB88F100C")
	if protobufString(decoded, 1) != "❤️" {
		t.Fatalf("decoded = %v", decoded)
	}
	if decodeProtobufHex("") != nil {
		t.Fatal("no blob is nil")
	}
	// Not a protobuf: the bytes are still kept.
	if m := decodeProtobufHex("FFFF").(map[string]any); m["hex"] != "FFFF" || m["fields"] != nil {
		t.Fatalf("decoded = %v", m)
	}
}

func TestFileExtension(t *testing.T) {
	for _, c := range []struct{ name, uri, contentType, want string }{
		{"Screenshot 2026-10-23 at 19.25.40.png", "", "image/png", ".png"},
		{"", "file:///x/78fed748.jpg", "image/jpeg", ".jpg"},
		{"", "content://mms/part/44711", "image/jpeg", ".jpg"},
		{"", "content://mms/part/1", "text/x-vCard", ".vcf"},
		{"", "content://mms/part/1", "application/x-unknown", ".bin"},
	} {
		if got := fileExtension(c.name, c.uri, c.contentType); got != c.want {
			t.Fatalf("fileExtension(%q, %q, %q) = %q, want %q", c.name, c.uri, c.contentType, got, c.want)
		}
	}
}
