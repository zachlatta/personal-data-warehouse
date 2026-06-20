package objectstore

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// recordedCall captures one request the Drive backend made.
type recordedCall struct {
	Method string
	Path   string
	Query  map[string][]string
	Body   string
}

// driveFake is an httpDoer that simulates the Drive REST API in memory.
type driveFake struct {
	objects        []driveFile // returned for object (pdw_kind) list queries
	objectsPages   [][]driveFile
	nextTokens     []string
	fileByID       map[string]driveFile
	mediaByID      map[string][]byte
	exportByID     map[string][]byte
	folderCreateID func(name string) string
	calls          []recordedCall
	pageIdx        int
}

func (f *driveFake) Do(req *http.Request) (*http.Response, error) {
	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, _ = io.ReadAll(req.Body)
	}
	f.calls = append(f.calls, recordedCall{
		Method: req.Method,
		Path:   req.URL.Path,
		Query:  req.URL.Query(),
		Body:   string(bodyBytes),
	})
	q := req.URL.Query()

	switch {
	case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/files") && q.Get("alt") != "media":
		return f.handleList(q)
	case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/export"):
		id := segmentBeforeLast(req.URL.Path)
		data, ok := f.exportByID[id]
		if !ok {
			return jsonResp(404, map[string]string{"error": "not found"})
		}
		return rawResp(200, data)
	case req.Method == http.MethodPost && strings.Contains(req.URL.Path, "/upload/"):
		return jsonResp(200, driveFile{ID: "uploaded-file", WebViewLink: "https://drive/uploaded"})
	case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/files"):
		// folder create
		var meta map[string]any
		_ = json.Unmarshal(bodyBytes, &meta)
		name, _ := meta["name"].(string)
		id := "folder-" + name
		if f.folderCreateID != nil {
			id = f.folderCreateID(name)
		}
		return jsonResp(200, driveFile{ID: id, WebViewLink: "https://drive/" + id})
	case req.Method == http.MethodGet && q.Get("alt") == "media":
		id := lastSegment(req.URL.Path)
		data, ok := f.mediaByID[id]
		if !ok {
			return jsonResp(404, map[string]string{"error": "not found"})
		}
		return rawResp(200, data)
	case req.Method == http.MethodGet:
		id := lastSegment(req.URL.Path)
		file, ok := f.fileByID[id]
		if !ok {
			return jsonResp(404, map[string]string{"error": "not found"})
		}
		return jsonResp(200, file)
	case req.Method == http.MethodPatch:
		id := lastSegment(req.URL.Path)
		return jsonResp(200, driveFile{ID: id, WebViewLink: "https://drive/promoted"})
	case req.Method == http.MethodDelete:
		return rawResp(204, nil)
	}
	return jsonResp(400, map[string]string{"error": "unexpected request"})
}

func (f *driveFake) handleList(q map[string][]string) (*http.Response, error) {
	query := ""
	if vals, ok := q["q"]; ok && len(vals) > 0 {
		query = vals[0]
	}
	// Folder lookups never return existing folders -> force creation.
	if strings.Contains(query, "in parents") {
		return jsonResp(200, driveFileList{Files: nil})
	}
	if len(f.objectsPages) > 0 {
		page := f.objectsPages[f.pageIdx]
		token := ""
		if f.pageIdx < len(f.nextTokens) {
			token = f.nextTokens[f.pageIdx]
		}
		f.pageIdx++
		return jsonResp(200, driveFileList{Files: page, NextPageToken: token})
	}
	return jsonResp(200, driveFileList{Files: f.objects})
}

func lastSegment(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return path
	}
	return path[idx+1:]
}

func segmentBeforeLast(path string) string {
	trimmed := strings.TrimRight(path, "/")
	idx := strings.LastIndex(trimmed, "/")
	if idx < 0 {
		return trimmed
	}
	parent := trimmed[:idx]
	return lastSegment(parent)
}

func jsonResp(status int, v any) (*http.Response, error) {
	data, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewReader(data)),
		Header:     make(http.Header),
	}, nil
}

func rawResp(status int, data []byte) (*http.Response, error) {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewReader(data)),
		Header:     make(http.Header),
	}, nil
}

func voiceMemosStore(fake *driveFake) *GoogleDriveStore {
	return NewGoogleDriveStore(GoogleDriveOptions{
		FolderID:      "root-folder",
		HTTPClient:    fake,
		Source:        "apple_voice_memos",
		LegacySources: []string{"voice_memos"},
		AudioKind:     "voice_memo_audio",
		MetadataKind:  "voice_memo_metadata",
	})
}

func lastListQuery(fake *driveFake) string {
	for i := len(fake.calls) - 1; i >= 0; i-- {
		c := fake.calls[i]
		if c.Method == http.MethodGet && strings.HasSuffix(c.Path, "/files") {
			if vals, ok := c.Query["q"]; ok && len(vals) > 0 && strings.Contains(vals[0], "pdw_kind") {
				return vals[0]
			}
		}
	}
	return ""
}

func TestListObjectsQueryAndParsing(t *testing.T) {
	fake := &driveFake{
		objects: []driveFile{
			{ID: "f1", Name: "rec.json", WebViewLink: "https://drive/f1", AppProperties: map[string]string{"pdw_kind": "voice_memo_metadata", "content_sha256": "abc"}},
		},
	}
	store := voiceMemosStore(fake)

	listings, err := store.ListObjects(context.Background(), ListQuery{Kind: "voice_memo_metadata", Stage: "inbox"})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(listings) != 1 {
		t.Fatalf("want 1 listing, got %d", len(listings))
	}
	if listings[0].Ref.StorageFileID != "f1" || listings[0].Ref.StorageBackend != "google_drive" {
		t.Fatalf("unexpected ref: %+v", listings[0].Ref)
	}
	if listings[0].AppProperties["content_sha256"] != "abc" || listings[0].Filename != "rec.json" {
		t.Fatalf("unexpected listing: %+v", listings[0])
	}
	query := lastListQuery(fake)
	for _, want := range []string{
		"pdw_root_folder_id", "root-folder",
		"value='apple_voice_memos'", "value='voice_memos'",
		"pdw_kind' and value='voice_memo_metadata'",
		"pdw_stage' and value='inbox'",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("query missing %q: %s", want, query)
		}
	}
	if strings.Contains(query, "in parents") {
		t.Errorf("object query should not use 'in parents': %s", query)
	}
}

func TestListObjectsPaginates(t *testing.T) {
	fake := &driveFake{
		objectsPages: [][]driveFile{
			{{ID: "a"}},
			{{ID: "b"}},
		},
		nextTokens: []string{"p2", ""},
	}
	store := voiceMemosStore(fake)
	listings, err := store.ListObjects(context.Background(), ListQuery{Kind: "voice_memo_metadata", Stage: "inbox"})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(listings) != 2 || listings[0].Ref.StorageFileID != "a" || listings[1].Ref.StorageFileID != "b" {
		t.Fatalf("unexpected pagination result: %+v", listings)
	}
}

func TestFindObjectFiltersProperties(t *testing.T) {
	fake := &driveFake{objects: []driveFile{{ID: "audio-1", AppProperties: map[string]string{"content_sha256": "sha"}}}}
	store := voiceMemosStore(fake)
	listing, err := store.FindObject(context.Background(), ListQuery{Kind: "voice_memo_audio", Stage: "inbox", Properties: map[string]string{"content_sha256": "sha"}})
	if err != nil {
		t.Fatalf("FindObject: %v", err)
	}
	if listing == nil || listing.Ref.StorageFileID != "audio-1" {
		t.Fatalf("unexpected listing: %+v", listing)
	}
	if !strings.Contains(lastListQuery(fake), "content_sha256' and value='sha'") {
		t.Errorf("query missing property filter: %s", lastListQuery(fake))
	}
}

func TestGetObjectReturnsBytes(t *testing.T) {
	fake := &driveFake{mediaByID: map[string][]byte{"fid": []byte("audio-bytes")}}
	store := voiceMemosStore(fake)
	data, err := store.GetObject(context.Background(), StoredObject{StorageFileID: "fid"})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if string(data) != "audio-bytes" {
		t.Fatalf("unexpected bytes: %q", data)
	}
}

func TestGetObjectExportsGoogleNativeDocument(t *testing.T) {
	fake := &driveFake{
		fileByID: map[string]driveFile{
			"doc": {ID: "doc", Name: "Doc", MimeType: "application/vnd.google-apps.document"},
		},
		exportByID: map[string][]byte{"doc": []byte("exported text")},
	}
	store := voiceMemosStore(fake)
	data, err := store.GetObject(context.Background(), StoredObject{StorageFileID: "doc"})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if string(data) != "exported text" {
		t.Fatalf("unexpected export bytes: %q", data)
	}
	last := fake.calls[len(fake.calls)-1]
	gotMime := ""
	if vals := last.Query["mimeType"]; len(vals) > 0 {
		gotMime = vals[0]
	}
	if !strings.HasSuffix(last.Path, "/files/doc/export") || gotMime != "text/plain" {
		t.Fatalf("unexpected export request: %+v", last)
	}
}

func TestObjectExists(t *testing.T) {
	fake := &driveFake{fileByID: map[string]driveFile{
		"live":    {ID: "live", Trashed: false},
		"trashed": {ID: "trashed", Trashed: true},
	}}
	store := voiceMemosStore(fake)
	ctx := context.Background()

	if ok, err := store.ObjectExists(ctx, StoredObject{StorageFileID: "live"}); err != nil || !ok {
		t.Fatalf("live exists: ok=%v err=%v", ok, err)
	}
	if ok, err := store.ObjectExists(ctx, StoredObject{StorageFileID: "trashed"}); err != nil || ok {
		t.Fatalf("trashed should not exist: ok=%v err=%v", ok, err)
	}
	if ok, err := store.ObjectExists(ctx, StoredObject{StorageFileID: "gone"}); err != nil || ok {
		t.Fatalf("missing should be false: ok=%v err=%v", ok, err)
	}
}

func TestGetMetadataMapsFields(t *testing.T) {
	fake := &driveFake{fileByID: map[string]driveFile{
		"fid": {
			ID: "fid", Name: "memo.m4a", MimeType: "audio/mp4", Size: "2048",
			CreatedTime: "2026-01-02T03:04:05Z", ModifiedTime: "2026-01-02T04:05:06Z",
			WebViewLink: "https://drive/fid", AppProperties: map[string]string{"content_sha256": "abc123"},
		},
	}}
	store := voiceMemosStore(fake)
	meta, err := store.GetMetadata(context.Background(), StoredObject{StorageFileID: "fid", StorageKey: "k.m4a"})
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	want := ObjectMetadata{
		Backend: "google_drive", StorageKey: "k.m4a", StorageFileID: "fid",
		ContentType: "audio/mp4", SizeBytes: 2048, ContentSHA256: "abc123", Filename: "memo.m4a",
		CreatedTime: "2026-01-02T03:04:05Z", ModifiedTime: "2026-01-02T04:05:06Z", StorageURL: "https://drive/fid",
	}
	if meta != want {
		t.Fatalf("metadata mismatch:\n got %+v\nwant %+v", meta, want)
	}
}

func TestGetMetadataMissingReturnsNotFound(t *testing.T) {
	store := voiceMemosStore(&driveFake{})
	if _, err := store.GetMetadata(context.Background(), StoredObject{StorageFileID: "nope"}); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestMoveObjectRelocatesToLibrary(t *testing.T) {
	fake := &driveFake{fileByID: map[string]driveFile{
		"drive-file-id": {ID: "drive-file-id", Parents: []string{"old-parent"}},
	}}
	store := voiceMemosStore(fake)

	result, err := store.MoveObject(context.Background(), StoredObject{StorageFileID: "drive-file-id"},
		"apple-voice-memos/library/2026/03/2026-03-25-abc123.qta", nil)
	if err != nil {
		t.Fatalf("MoveObject: %v", err)
	}
	if result.StorageKey != "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta" || result.StorageFileID != "drive-file-id" {
		t.Fatalf("unexpected result: %+v", result)
	}

	var createdFolders []string
	var patch *recordedCall
	for i := range fake.calls {
		c := fake.calls[i]
		if c.Method == http.MethodPost && strings.HasSuffix(c.Path, "/files") {
			var meta map[string]any
			_ = json.Unmarshal([]byte(c.Body), &meta)
			createdFolders = append(createdFolders, meta["name"].(string))
		}
		if c.Method == http.MethodPatch {
			patch = &fake.calls[i]
		}
	}
	wantFolders := []string{"apple-voice-memos", "library", "2026", "03"}
	if strings.Join(createdFolders, ",") != strings.Join(wantFolders, ",") {
		t.Fatalf("created folders = %v, want %v", createdFolders, wantFolders)
	}
	if patch == nil {
		t.Fatal("expected a PATCH update")
	}
	if got := patch.Query["addParents"]; len(got) != 1 || got[0] != "folder-03" {
		t.Errorf("addParents = %v, want [folder-03]", patch.Query["addParents"])
	}
	if got := patch.Query["removeParents"]; len(got) != 1 || got[0] != "old-parent" {
		t.Errorf("removeParents = %v, want [old-parent]", patch.Query["removeParents"])
	}
	var patchBody map[string]any
	_ = json.Unmarshal([]byte(patch.Body), &patchBody)
	if patchBody["name"] != "2026-03-25-abc123.qta" {
		t.Errorf("patch name = %v", patchBody["name"])
	}
	props, _ := patchBody["appProperties"].(map[string]any)
	if props["pdw_stage"] != "library" {
		t.Errorf("pdw_stage = %v, want library", props["pdw_stage"])
	}
}

func TestPutFileDedupesAndUploads(t *testing.T) {
	// First: existing object found -> no upload.
	existing := &driveFake{objects: []driveFile{{ID: "existing", WebViewLink: "https://drive/existing"}}}
	store := voiceMemosStore(existing)
	ref, err := store.PutFile(context.Background(), PutFileInput{
		ObjectKey: "apple-voice-memos/inbox/2026/03/x.qta", ContentSHA256: "sha", ContentType: "audio/mp4", Content: []byte("data"),
	})
	if err != nil {
		t.Fatalf("PutFile dedup: %v", err)
	}
	if ref.StorageFileID != "existing" {
		t.Fatalf("expected dedup to return existing, got %+v", ref)
	}
	for _, c := range existing.calls {
		if strings.Contains(c.Path, "/upload/") {
			t.Fatal("dedup hit should not upload")
		}
	}

	// Second: no existing object -> uploads.
	fresh := &driveFake{}
	store2 := voiceMemosStore(fresh)
	ref2, err := store2.PutFile(context.Background(), PutFileInput{
		ObjectKey: "apple-voice-memos/inbox/2026/03/x.qta", ContentSHA256: "sha", ContentType: "audio/mp4", Content: []byte("data"),
	})
	if err != nil {
		t.Fatalf("PutFile upload: %v", err)
	}
	if ref2.StorageFileID != "uploaded-file" || ref2.StorageKey != "apple-voice-memos/inbox/2026/03/x.qta" {
		t.Fatalf("unexpected upload result: %+v", ref2)
	}
	uploaded := false
	for _, c := range fresh.calls {
		if strings.Contains(c.Path, "/upload/") {
			uploaded = true
		}
	}
	if !uploaded {
		t.Fatal("expected an upload request")
	}
}

func TestPresenceReportsAudioAndMetadata(t *testing.T) {
	fake := &driveFake{objects: []driveFile{
		{ID: "a", AppProperties: map[string]string{"pdw_kind": "voice_memo_audio"}},
		{ID: "m", AppProperties: map[string]string{"pdw_kind": "voice_memo_metadata"}},
	}}
	store := voiceMemosStore(fake)
	presence, err := store.Presence(context.Background(), "sha")
	if err != nil {
		t.Fatalf("Presence: %v", err)
	}
	if !presence.AudioExists || !presence.MetadataExists {
		t.Fatalf("unexpected presence: %+v", presence)
	}
}
