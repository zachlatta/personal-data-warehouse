package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
)

// capturingStore records the last PutFile/PutJSON input and returns a canned
// StoredObject. It lets the handler tests assert exactly what the handler hands
// the object store (object key, kind, app properties, body) without exercising
// Drive itself, which is covered in the objectstore package.
type capturingStore struct {
	lastFile *objectstore.PutFileInput
	lastJSON *objectstore.PutJSONInput
}

func (c *capturingStore) Backend() string { return "google_drive" }
func (c *capturingStore) HasBlob(context.Context, string) (bool, error) {
	return false, objectstore.ErrNotImplemented
}
func (c *capturingStore) HasMetadata(context.Context, string) (bool, error) {
	return false, objectstore.ErrNotImplemented
}
func (c *capturingStore) HasObject(context.Context, string, string, string) (bool, error) {
	return false, objectstore.ErrNotImplemented
}
func (c *capturingStore) Presence(context.Context, string) (objectstore.ObjectPresence, error) {
	return objectstore.ObjectPresence{}, objectstore.ErrNotImplemented
}
func (c *capturingStore) PutFile(_ context.Context, in objectstore.PutFileInput) (objectstore.StoredObject, error) {
	c.lastFile = &in
	return objectstore.StoredObject{StorageBackend: "google_drive", StorageKey: in.ObjectKey, StorageFileID: "fid-file"}, nil
}
func (c *capturingStore) PutJSON(_ context.Context, in objectstore.PutJSONInput) (objectstore.StoredObject, error) {
	c.lastJSON = &in
	return objectstore.StoredObject{StorageBackend: "google_drive", StorageKey: in.ObjectKey, StorageFileID: "fid-json"}, nil
}
func (c *capturingStore) GetObject(context.Context, objectstore.StoredObject) ([]byte, error) {
	return nil, objectstore.ErrNotImplemented
}
func (c *capturingStore) ObjectExists(context.Context, objectstore.StoredObject) (bool, error) {
	return false, objectstore.ErrNotImplemented
}
func (c *capturingStore) DeleteObject(context.Context, objectstore.StoredObject) error {
	return objectstore.ErrNotImplemented
}
func (c *capturingStore) GetMetadata(context.Context, objectstore.StoredObject) (objectstore.ObjectMetadata, error) {
	return objectstore.ObjectMetadata{}, objectstore.ErrNotImplemented
}
func (c *capturingStore) GetShareURL(context.Context, objectstore.StoredObject) (string, error) {
	return "", objectstore.ErrNotImplemented
}
func (c *capturingStore) ListObjects(context.Context, objectstore.ListQuery) ([]objectstore.ObjectListing, error) {
	return nil, objectstore.ErrNotImplemented
}
func (c *capturingStore) FindObject(context.Context, objectstore.ListQuery) (*objectstore.ObjectListing, error) {
	return nil, objectstore.ErrNotImplemented
}
func (c *capturingStore) MoveObject(context.Context, objectstore.StoredObject, string, map[string]string) (objectstore.StoredObject, error) {
	return objectstore.StoredObject{}, objectstore.ErrNotImplemented
}

// ingestTestService builds an ingestService wired to capturingStores for every
// source, bypassing config/Drive construction.
func ingestTestService() (*ingestService, map[string]*capturingStore) {
	stores := map[string]*capturingStore{}
	ifaceStores := map[string]objectstore.ObjectStore{}
	for slug := range ingestSourceDefs {
		cs := &capturingStore{}
		stores[slug] = cs
		ifaceStores[slug] = cs
	}
	artifacts := map[string]ingestArtifact{}
	for _, a := range ingestArtifacts() {
		artifacts[a.endpoint] = a
	}
	svc := &ingestService{
		artifacts: artifacts,
		stores:    ifaceStores,
		signer:    objectsTestSigner(),
		maxBytes:  1024,
		now:       func() time.Time { return objectsTestNow },
		logger:    slog.Default(),
	}
	return svc, stores
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// signedIngestTarget builds a signed upload URL for the given endpoint, body,
// and extra domain query params.
func signedIngestTarget(endpoint string, body []byte, extra url.Values) string {
	sha := sha256Hex(body)
	exp := objectsTestNow.Add(time.Hour)
	q := url.Values{}
	for k, v := range extra {
		q[k] = v
	}
	q.Set("content_sha256", sha)
	q.Set("exp", strconv.FormatInt(exp.Unix(), 10))
	q.Set("sig", objectsTestSigner().SignObjectUpload(endpoint, sha, exp))
	return endpoint + "?" + q.Encode()
}

func postIngest(t *testing.T, svc *ingestService, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, target, bytesReader(body))
	svc.handler().ServeHTTP(rec, req)
	return rec
}

func bytesReader(b []byte) io.Reader {
	return &sliceReader{b: b}
}

type sliceReader struct {
	b   []byte
	pos int
}

func (s *sliceReader) Read(p []byte) (int, error) {
	if s.pos >= len(s.b) {
		return 0, io.EOF
	}
	n := copy(p, s.b[s.pos:])
	s.pos += n
	return n, nil
}

func TestIngestAgentSessionsBatchStoresExpectedObject(t *testing.T) {
	svc, stores := ingestTestService()
	body := []byte("gzipped-batch-bytes")
	exportedAt := "2026-06-19T12:34:56+00:00"
	target := signedIngestTarget("/ingest/agent-sessions/batch", body, url.Values{"exported_at": {exportedAt}})

	rec := postIngest(t, svc, target, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	put := stores["agent_sessions"].lastFile
	if put == nil {
		t.Fatal("expected PutFile to be called")
	}
	wantKey := "agent-sessions/inbox/batches/2026/06/20260619T123456Z-" + sha256Hex(body) + ".jsonl.gz"
	if put.ObjectKey != wantKey {
		t.Fatalf("object key = %q, want %q", put.ObjectKey, wantKey)
	}
	if put.Kind != "agent_sessions_export_batch" {
		t.Fatalf("kind = %q", put.Kind)
	}
	if put.ContentType != "application/gzip" {
		t.Fatalf("content type = %q", put.ContentType)
	}
	if string(put.Content) != string(body) {
		t.Fatalf("content = %q", put.Content)
	}
	if put.ContentSHA256 != sha256Hex(body) {
		t.Fatalf("content sha = %q", put.ContentSHA256)
	}
	if put.AppProperties["batch_sha256"] != sha256Hex(body) || put.AppProperties["exported_at"] != exportedAt {
		t.Fatalf("app properties = %v", put.AppProperties)
	}
	// Response echoes the stored reference.
	var resp ingestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.StorageKey != wantKey || resp.StorageFileID != "fid-file" {
		t.Fatalf("response = %+v", resp)
	}
}

func TestIngestRejectsBadSignature(t *testing.T) {
	svc, _ := ingestTestService()
	body := []byte("x")
	target := signedIngestTarget("/ingest/agent-sessions/batch", body, url.Values{"exported_at": {"2026-06-19T00:00:00+00:00"}})
	target += "tampered"
	rec := postIngest(t, svc, target, body)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestIngestRejectsBodyShaMismatch(t *testing.T) {
	svc, _ := ingestTestService()
	body := []byte("original")
	target := signedIngestTarget("/ingest/agent-sessions/batch", body, url.Values{"exported_at": {"2026-06-19T00:00:00+00:00"}})
	rec := postIngest(t, svc, target, []byte("tampered-body")) // different bytes than signed sha
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
}

func TestIngestRejectsNonPost(t *testing.T) {
	svc, _ := ingestTestService()
	rec := httptest.NewRecorder()
	svc.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ingest/agent-sessions/batch", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestIngestUnknownEndpoint(t *testing.T) {
	svc, _ := ingestTestService()
	body := []byte("x")
	target := signedIngestTarget("/ingest/nope/batch", body, nil)
	rec := postIngest(t, svc, target, body)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestIngestRejectsOversizeBody(t *testing.T) {
	svc, _ := ingestTestService()
	svc.maxBytes = 8
	body := []byte("this body is definitely longer than eight bytes")
	target := signedIngestTarget("/ingest/agent-sessions/batch", body, url.Values{"exported_at": {"2026-06-19T00:00:00+00:00"}})
	rec := postIngest(t, svc, target, body)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestIngestAppleMessagesAttachmentKey(t *testing.T) {
	svc, stores := ingestTestService()
	body := []byte("attachment-bytes")
	extra := url.Values{
		"attachment_guid": {"ABC/DEF guid"},
		"message_guid":    {"msg-1"},
		"content_type":    {"image/jpeg"},
		"created_at":      {"2025-03-04T10:00:00+00:00"},
		"filename":        {"photo.JPG"},
	}
	target := signedIngestTarget("/ingest/apple-messages/attachment", body, extra)
	rec := postIngest(t, svc, target, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	put := stores["apple_messages"].lastFile
	wantKey := "apple-messages/inbox/attachments/2025/03/2025-03-04-ABC-DEF-guid-" + sha256Hex(body) + ".JPG"
	if put.ObjectKey != wantKey {
		t.Fatalf("object key = %q, want %q", put.ObjectKey, wantKey)
	}
	if put.Kind != "apple_message_attachment" || put.ContentType != "image/jpeg" {
		t.Fatalf("kind/content type = %q/%q", put.Kind, put.ContentType)
	}
	if put.AppProperties["attachment_guid"] != "ABC/DEF guid" || put.AppProperties["message_guid"] != "msg-1" {
		t.Fatalf("app properties = %v", put.AppProperties)
	}
}

// fakeDriveDoer simulates just enough of the Drive REST API to drive a real
// GoogleDriveStore: folder lookups miss (forcing creation), object dedup
// queries are empty, and uploads are captured for assertion.
type fakeDriveDoer struct {
	uploadBody  string
	uploadCount int
}

func (f *fakeDriveDoer) Do(req *http.Request) (*http.Response, error) {
	body := ""
	if req.Body != nil {
		b, _ := io.ReadAll(req.Body)
		body = string(b)
	}
	q := req.URL.Query()
	switch {
	case req.Method == http.MethodGet && q.Get("alt") != "media": // list
		return driveJSON(`{"files":[]}`), nil
	case req.Method == http.MethodPost && contains(req.URL.Path, "/upload/"):
		f.uploadBody = body
		f.uploadCount++
		return driveJSON(`{"id":"uploaded-fid","webViewLink":"https://drive/uploaded"}`), nil
	case req.Method == http.MethodPost: // folder create
		return driveJSON(`{"id":"folder-id"}`), nil
	}
	return driveJSON(`{}`), nil
}

func driveJSON(body string) *http.Response {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(stringsReader(body)), Header: make(http.Header)}
}

func stringsReader(s string) io.Reader { return bytesReader([]byte(s)) }

func contains(s, sub string) bool { return len(s) >= len(sub) && indexOf(s, sub) >= 0 }

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestIngestAgentSessionsBatchEndToEndDriveTags drives the handler into a real
// GoogleDriveStore and asserts the resulting Drive upload carries every pdw_*
// tag the Dagster agent_sessions reader queries by. If these drift, the reader
// silently stops seeing app-written batches.
func TestIngestAgentSessionsBatchEndToEndDriveTags(t *testing.T) {
	fake := &fakeDriveDoer{}
	store, err := objectstore.BuildObjectStore(objectstore.GoogleDriveSpec(
		"root-folder", "agent_sessions", "agent_sessions_blob", "agent_sessions_export_batch", nil,
		objectstore.GoogleDriveConnection{HTTPClient: fake},
	))
	if err != nil {
		t.Fatalf("build store: %v", err)
	}
	svc, _ := ingestTestService()
	svc.stores["agent_sessions"] = store

	body := []byte("gzipped-batch-bytes")
	exportedAt := "2026-06-19T12:34:56+00:00"
	target := signedIngestTarget("/ingest/agent-sessions/batch", body, url.Values{"exported_at": {exportedAt}})
	rec := postIngest(t, svc, target, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if fake.uploadCount != 1 {
		t.Fatalf("expected 1 upload, got %d", fake.uploadCount)
	}
	for _, want := range []string{
		`"pdw_source":"agent_sessions"`,
		`"pdw_kind":"agent_sessions_export_batch"`,
		`"pdw_stage":"inbox"`,
		`"pdw_root_folder_id":"root-folder"`,
		`"content_sha256":"` + sha256Hex(body) + `"`,
		`"batch_sha256":"` + sha256Hex(body) + `"`,
		`"exported_at":"2026-06-19T12:34:56+00:00"`,
		`"name":"20260619T123456Z-` + sha256Hex(body) + `.jsonl.gz"`,
		"gzipped-batch-bytes",
	} {
		if !contains(fake.uploadBody, want) {
			t.Errorf("upload body missing %q", want)
		}
	}
}

func TestIngestVoiceMemosAudioAndMetadataKeys(t *testing.T) {
	svc, stores := ingestTestService()
	audio := []byte("audio-bytes")
	audioSHA := sha256Hex(audio)
	// Audio: recorded_at is wall-clock (no UTC shift), extension verbatim.
	target := signedIngestTarget("/ingest/voice-memos/audio", audio, url.Values{
		"recorded_at":  {"2025-07-15T09:00:00"},
		"extension":    {".m4a"},
		"content_type": {"audio/m4a"},
	})
	if rec := postIngest(t, svc, target, audio); rec.Code != http.StatusOK {
		t.Fatalf("audio status = %d, body %q", rec.Code, rec.Body.String())
	}
	put := stores["apple_voice_memos"].lastFile
	wantAudioKey := "apple-voice-memos/inbox/2025/07/2025-07-15-" + audioSHA + ".m4a"
	if put.ObjectKey != wantAudioKey {
		t.Fatalf("audio key = %q, want %q", put.ObjectKey, wantAudioKey)
	}
	if put.Kind != "voice_memo_audio" || put.SkipExistingCheck || put.ContentType != "audio/m4a" {
		t.Fatalf("audio put = %+v", put)
	}

	// Metadata sidecar shares the audio's basename via audio_content_sha256.
	meta := []byte(`{"schema_version":1}`)
	target = signedIngestTarget("/ingest/voice-memos/metadata", meta, url.Values{
		"recorded_at":          {"2025-07-15T09:00:00"},
		"audio_content_sha256": {audioSHA},
	})
	if rec := postIngest(t, svc, target, meta); rec.Code != http.StatusOK {
		t.Fatalf("metadata status = %d, body %q", rec.Code, rec.Body.String())
	}
	pj := stores["apple_voice_memos"].lastJSON
	wantMetaKey := "apple-voice-memos/inbox/2025/07/2025-07-15-" + audioSHA + ".json"
	if pj.ObjectKey != wantMetaKey {
		t.Fatalf("metadata key = %q, want %q", pj.ObjectKey, wantMetaKey)
	}
	if pj.Kind != "voice_memo_metadata" || pj.SourceContentSHA256 != audioSHA || pj.SkipExistingCheck {
		t.Fatalf("metadata put = %+v", pj)
	}
	if string(pj.Payload) != string(meta) {
		t.Fatalf("metadata payload = %q", pj.Payload)
	}
}

func TestIngestAppleNotesBodyAttachmentRevisionKeys(t *testing.T) {
	svc, stores := ingestTestService()
	common := url.Values{"note_id": {"note/123"}, "revision_id": {"rev-9"}, "modified_at": {"2026-01-02T03:04:05+00:00"}}
	base := "apple-notes/inbox/2026/01/note-123/rev-9"

	html := []byte("<html>body</html>")
	target := signedIngestTarget("/ingest/apple-notes/body", html, common)
	if rec := postIngest(t, svc, target, html); rec.Code != http.StatusOK {
		t.Fatalf("body status = %d, %q", rec.Code, rec.Body.String())
	}
	body := stores["apple_notes"].lastFile
	if body.ObjectKey != base+".html" || body.Kind != "apple_note_body_html" || body.ContentType != "text/html" || body.SkipExistingCheck {
		t.Fatalf("body put = %+v", body)
	}
	if body.AppProperties["note_id"] != "note/123" || body.AppProperties["revision_id"] != "rev-9" {
		t.Fatalf("body props = %v", body.AppProperties)
	}

	att := []byte("attachment-bytes")
	attExtra := url.Values{"attachment_id": {"att/7"}, "filename": {"doc.pdf"}, "content_type": {"application/pdf"}}
	for k, v := range common {
		attExtra[k] = v
	}
	target = signedIngestTarget("/ingest/apple-notes/attachment", att, attExtra)
	if rec := postIngest(t, svc, target, att); rec.Code != http.StatusOK {
		t.Fatalf("attachment status = %d, %q", rec.Code, rec.Body.String())
	}
	a := stores["apple_notes"].lastFile
	wantAttKey := base + "/attachments/att-7-" + sha256Hex(att) + ".pdf"
	if a.ObjectKey != wantAttKey || a.Kind != "apple_note_attachment" || a.SkipExistingCheck {
		t.Fatalf("attachment put = %+v (want key %q)", a, wantAttKey)
	}
	if a.AppProperties["attachment_id"] != "att/7" {
		t.Fatalf("attachment props = %v", a.AppProperties)
	}

	rev := []byte(`{"schema_version":1,"source":"apple_notes"}`)
	revExtra := url.Values{"note_content_sha256": {"fp-1"}}
	for k, v := range common {
		revExtra[k] = v
	}
	target = signedIngestTarget("/ingest/apple-notes/revision", rev, revExtra)
	if rec := postIngest(t, svc, target, rev); rec.Code != http.StatusOK {
		t.Fatalf("revision status = %d, %q", rec.Code, rec.Body.String())
	}
	r := stores["apple_notes"].lastJSON
	if r.ObjectKey != base+".json" || r.Kind != "apple_note_revision_metadata" || !r.SkipExistingCheck {
		t.Fatalf("revision put = %+v", r)
	}
	if r.AppProperties["note_content_sha256"] != "fp-1" {
		t.Fatalf("revision props = %v", r.AppProperties)
	}
}
