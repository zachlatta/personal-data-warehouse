package applenotes

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/testfixtures"
)

// testdata/{synthetic,coredata}: stores from the Python test fixtures plus
// extra notes (unicode, HTML-looking protobuf text, raw gzip, binary blobs,
// fallback/link/table/inline attachments); golden.jsonl is what the Python
// scanner + note_payload produced for them.
func TestScanMatchesPythonGoldens(t *testing.T) {
	goldens := testfixtures.LoadGoldens(t, filepath.Join("testdata", "golden.jsonl"))
	stores := map[string]string{
		"synthetic": filepath.Join("testdata", "synthetic", "NoteStore.sqlite"),
		"coredata":  filepath.Join("testdata", "coredata", "CoreDataNoteStore.sqlite"),
	}
	for name, store := range stores {
		notes, err := Scan(store, filepath.Dir(store))
		if err != nil {
			t.Fatalf("scan %s: %v", name, err)
		}
		want := goldens[name]
		if len(notes) != len(want) {
			t.Fatalf("%s: scanned %d notes, golden has %d", name, len(notes), len(want))
		}
		for i, note := range notes {
			payload := NotePayload(note)
			encoded, _ := common.CanonicalJSON(payload)
			if string(encoded) != want[i].Payload {
				t.Errorf("%s note %d (%s) payload differs:\n got %s\nwant %s", name, i, note.NoteID, encoded, want[i].Payload)
			}
			if got := common.JSONSHA256(payload); got != want[i].Fingerprint {
				t.Errorf("%s note %d fingerprint differs", name, i)
			}
			if got := HTMLDocumentForNote(note); got != want[i].Extra["html_document"] {
				t.Errorf("%s note %d html document differs:\n got %s\nwant %s", name, i, got, want[i].Extra["html_document"])
			}
		}
	}
}

func TestDecodeNoteBlobPrefersProtobufText(t *testing.T) {
	notes, err := Scan(filepath.Join("testdata", "coredata", "CoreDataNoteStore.sqlite"), filepath.Join("testdata", "coredata"))
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]Note{}
	for _, note := range notes {
		byID[note.NoteID] = note
	}
	if byID["NOTE-1"].BodyText != "Clean protobuf body" || byID["NOTE-1"].BodyMarkdown != "Clean protobuf body" {
		t.Fatalf("NOTE-1 = %+v", byID["NOTE-1"])
	}
	if byID["NOTE-3"].BodyText != "plain gzip text, no protobuf" {
		t.Fatalf("NOTE-3 body = %q", byID["NOTE-3"].BodyText)
	}
	// An undecodable blob falls back to the title column, as the Python did.
	if byID["NOTE-4"].BodyText != "Binary blob" || byID["NOTE-4"].BodyHTML != "<html><body><pre>Binary blob</pre></body></html>" {
		t.Fatalf("binary blob fallback = %+v", byID["NOTE-4"])
	}
	attachments := map[string]Attachment{}
	for _, a := range byID["NOTE-1"].Attachments {
		attachments[a.AttachmentID] = a
	}
	if a := attachments["DRAWING-1"]; a.IsMissing || a.Filename != "Whiteboard sketch.png" || a.ContentType != "image/png" || a.ContentSHA256 == "" {
		t.Fatalf("drawing = %+v", a)
	}
	if a := attachments["LINK-1"]; a.IsMissing || !strings.Contains(a.Error, "URL") {
		t.Fatalf("link = %+v", a)
	}
	if a := attachments["DRAWING-2"]; !a.IsMissing || a.Error != "attachment file is not locally available" {
		t.Fatalf("absent drawing = %+v", a)
	}
}

type fakeClient struct {
	bodies      []string
	attachments []ingestclient.AppleNotesAttachment
	revisions   []map[string]any
}

func (f *fakeClient) UploadAppleNotesBody(html []byte, noteID, revisionID, modifiedAt string) (ingestclient.StoredObject, error) {
	f.bodies = append(f.bodies, string(html))
	return ingestclient.StoredObject{StorageKey: "body"}, nil
}

func (f *fakeClient) UploadAppleNotesAttachment(content []byte, a ingestclient.AppleNotesAttachment) (ingestclient.StoredObject, error) {
	f.attachments = append(f.attachments, a)
	return ingestclient.StoredObject{StorageKey: "attachment"}, nil
}

func (f *fakeClient) UploadAppleNotesRevision(payload map[string]any, noteID, revisionID, modifiedAt, sha string) (ingestclient.StoredObject, error) {
	f.revisions = append(f.revisions, payload)
	return ingestclient.StoredObject{StorageKey: "revision"}, nil
}

func syntheticNotes(t *testing.T) []Note {
	t.Helper()
	notes, err := Scan(filepath.Join("testdata", "synthetic", "NoteStore.sqlite"), filepath.Join("testdata", "synthetic"))
	if err != nil {
		t.Fatal(err)
	}
	return notes
}

func TestRunnerUploadsThenSkipsThenTombstones(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	state := EmptyState("zach@example.com", "/store")
	client := &fakeClient{}
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	runner := &Runner{Account: "zach@example.com", Client: client, Logger: &common.RecordingLogger{}, State: state, Now: func() time.Time { return now }, Workers: 2,
		SaveState: func() { state.Save(statePath) }}
	summary, err := runner.syncNotes(syntheticNotes(t))
	if err != nil {
		t.Fatal(err)
	}
	if summary.NotesSeen != 3 || summary.RevisionsUploaded != 3 || summary.AttachmentsUploaded != 1 || summary.AttachmentsMissing != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	if len(client.revisions) != 3 || client.revisions[0]["source"] != "apple_notes" {
		t.Fatalf("revisions = %v", client.revisions)
	}
	for _, payload := range client.revisions {
		note := payload["note"].(map[string]any)
		if note["revision_id"] != note["content_sha256"] {
			t.Fatal("revision id must be the content sha")
		}
	}
	reloaded := LoadState(statePath, "zach@example.com", "/store")
	if len(reloaded.Entries) != 3 || !reloaded.Entries["note-1"].Complete() {
		t.Fatalf("state = %+v", reloaded.Entries)
	}
	runner.State = reloaded
	summary, _ = runner.syncNotes(syntheticNotes(t))
	if summary.NotesSkipped != 3 || summary.NotesSelected != 0 {
		t.Fatalf("second = %+v", summary)
	}
	// note-3 was already deleted in the store, so only two tombstones appear.
	summary, _ = runner.syncNotes(nil)
	if summary.NotesDeleted != 2 || summary.RevisionsUploaded != 2 {
		t.Fatalf("tombstones = %+v", summary)
	}
	last := client.revisions[len(client.revisions)-1]["note"].(map[string]any)
	if last["is_deleted"] != true || last["raw"].(map[string]any)["tombstone_from_revision_id"] == "" {
		t.Fatalf("tombstone = %v", last)
	}
	if LoadState(statePath, "other", "/store").Entries != nil && len(LoadState(statePath, "other", "/store").Entries) != 0 {
		t.Fatal("account change must reset the state")
	}
}

func TestLimitDefersAndGuardSkips(t *testing.T) {
	client := &fakeClient{}
	runner := &Runner{Account: "z", Client: client, Logger: &common.RecordingLogger{}, Limit: 1}
	summary, err := runner.syncNotes(syntheticNotes(t))
	if err != nil {
		t.Fatal(err)
	}
	if summary.NotesSelected != 1 || summary.NotesDeferred != 2 {
		t.Fatalf("limit = %+v", summary)
	}
	blocked := &Runner{Account: "z", Client: client, Logger: &common.RecordingLogger{}, BeforeUploadCheck: func() string { return "no" }}
	summary, _ = blocked.syncNotes(syntheticNotes(t))
	if summary.NotesDeferred != 3 || summary.RevisionsUploaded != 0 {
		t.Fatalf("blocked = %+v", summary)
	}
}

func TestEnsureNotesAppRunning(t *testing.T) {
	logger := &common.RecordingLogger{}
	env := common.Getenv(func(string) string { return "" })
	store := "/Users/x/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite"
	var calls [][]string
	run := func(args ...string) (int, string, error) {
		calls = append(calls, args)
		if args[0] == "pgrep" {
			return 1, "", nil
		}
		return 0, "", nil
	}
	kick := EnsureNotesAppRunning(store, logger, env, "darwin", run)
	if !kick.Launched || len(calls) != 2 || calls[1][len(calls[1])-1] != "Notes" {
		t.Fatalf("kick = %+v calls=%v", kick, calls)
	}
	if kick := EnsureNotesAppRunning(store, logger, env, "linux", run); kick.Attempted {
		t.Fatalf("non-macOS must skip: %+v", kick)
	}
	if kick := EnsureNotesAppRunning("/tmp/x.sqlite", logger, env, "darwin", run); kick.Attempted {
		t.Fatalf("test store must skip: %+v", kick)
	}
	off := common.Getenv(func(k string) string { return "0" })
	if kick := EnsureNotesAppRunning(store, logger, off, "darwin", run); kick.Attempted {
		t.Fatalf("kill switch: %+v", kick)
	}
	_ = os.Getenv
}
