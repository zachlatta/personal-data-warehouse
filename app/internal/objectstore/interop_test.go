package objectstore

import (
	"context"
	"encoding/json"
	"testing"
)

// TestStoredObjectJSONMatchesPython asserts the Go StoredObject serializes with
// the exact field names the Python StoredObject / warehouse storage_* columns
// use, so references written by either runtime interoperate.
func TestStoredObjectJSONMatchesPython(t *testing.T) {
	ref := StoredObject{
		StorageBackend: "google_drive",
		StorageKey:     "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta",
		StorageFileID:  "drive-file-id",
		StorageURL:     "https://drive/file",
	}
	data, err := json.Marshal(ref)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var asMap map[string]any
	if err := json.Unmarshal(data, &asMap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"storage_backend", "storage_key", "storage_file_id", "storage_url"} {
		if _, ok := asMap[key]; !ok {
			t.Errorf("missing JSON key %q in %s", key, data)
		}
	}
	if len(asMap) != 4 {
		t.Errorf("unexpected extra JSON keys: %s", data)
	}
}

// TestStoredObjectRoundTripsPythonReference unmarshals a reference shaped exactly
// like one the Python layer writes, and confirms the Go backend can fetch by it.
func TestStoredObjectRoundTripsPythonReference(t *testing.T) {
	pythonJSON := `{"storage_backend":"google_drive","storage_key":"apple-notes/library/2026/05/note-1/rev-1.json","storage_file_id":"metadata-file","storage_url":"https://drive/metadata"}`
	var ref StoredObject
	if err := json.Unmarshal([]byte(pythonJSON), &ref); err != nil {
		t.Fatalf("unmarshal python ref: %v", err)
	}
	if ref.StorageFileID != "metadata-file" || ref.StorageKey != "apple-notes/library/2026/05/note-1/rev-1.json" {
		t.Fatalf("unexpected ref: %+v", ref)
	}
	fake := &driveFake{mediaByID: map[string][]byte{"metadata-file": []byte(`{"ok":true}`)}}
	store := voiceMemosStore(fake)
	data, err := store.GetObject(context.Background(), ref)
	if err != nil {
		t.Fatalf("GetObject by python ref: %v", err)
	}
	if string(data) != `{"ok":true}` {
		t.Fatalf("unexpected content: %q", data)
	}
}

// TestKeyConventionsMatchPython locks the shared object-key conventions (stage
// derivation and filename extraction) that both runtimes rely on.
func TestKeyConventionsMatchPython(t *testing.T) {
	cases := []struct {
		key       string
		wantStage string
		wantName  string
	}{
		{"apple-voice-memos/inbox/2026/03/2026-03-25-abc123.qta", "inbox", "2026-03-25-abc123.qta"},
		{"apple-notes/library/2026/05/note-1/rev-1.json", "library", "rev-1.json"},
		{"apple-messages/inbox/batches/2026/05/batch.jsonl.gz", "inbox", "batch.jsonl.gz"},
		{"top-level.bin", "", "top-level.bin"},
	}
	for _, tc := range cases {
		if got := objectStage(tc.key); got != tc.wantStage {
			t.Errorf("objectStage(%q) = %q, want %q", tc.key, got, tc.wantStage)
		}
		if got := driveNameFromObjectKey(tc.key); got != tc.wantName {
			t.Errorf("driveNameFromObjectKey(%q) = %q, want %q", tc.key, got, tc.wantName)
		}
	}
}
