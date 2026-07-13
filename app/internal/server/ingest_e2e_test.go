package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
)

// fakeDrive is an httptest backend that simulates the slice of the Drive REST
// API the write path uses: list queries (always empty -> no dedup hit, folders
// always created) and multipart uploads, which it records for assertion.
type fakeDrive struct {
	mu      sync.Mutex
	uploads []capturedUpload
	nextID  int
}

type capturedUpload struct {
	Name      string
	Parents   []string
	MimeType  string
	AppProps  map[string]string
	MediaType string
	Media     []byte
}

func (d *fakeDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/files"):
		writeFakeJSON(w, `{"files":[]}`)
	case r.Method == http.MethodPost && r.URL.Query().Get("uploadType") == "multipart":
		d.recordUpload(r)
		d.mu.Lock()
		d.nextID++
		id := d.nextID
		d.mu.Unlock()
		writeFakeJSON(w, fmt.Sprintf(`{"id":"file-%d","webViewLink":"https://drive/file-%d"}`, id, id))
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/files"):
		writeFakeJSON(w, `{"id":"folder-x","webViewLink":"https://drive/folder-x"}`)
	default:
		http.Error(w, "unexpected", http.StatusBadRequest)
	}
}

func (d *fakeDrive) recordUpload(r *http.Request) {
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	up := capturedUpload{}
	idx := 0
	for {
		part, err := mr.NextPart()
		if err != nil {
			break
		}
		data, _ := io.ReadAll(part)
		if idx == 0 {
			var meta struct {
				Name          string            `json:"name"`
				Parents       []string          `json:"parents"`
				MimeType      string            `json:"mimeType"`
				AppProperties map[string]string `json:"appProperties"`
			}
			_ = json.Unmarshal(data, &meta)
			up.Name, up.Parents, up.MimeType, up.AppProps = meta.Name, meta.Parents, meta.MimeType, meta.AppProperties
		} else {
			up.MediaType = part.Header.Get("Content-Type")
			up.Media = data
		}
		idx++
	}
	d.mu.Lock()
	d.uploads = append(d.uploads, up)
	d.mu.Unlock()
}

func (d *fakeDrive) lastUpload() capturedUpload {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.uploads) == 0 {
		return capturedUpload{}
	}
	return d.uploads[len(d.uploads)-1]
}

func writeFakeJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, body)
}

// e2eService builds a real ingestService whose per-source GoogleDriveStores talk
// to the given fake Drive, exactly as production would (auth disabled, base URLs
// overridden).
func e2eService(t *testing.T, driveURL string) *ingestService {
	t.Helper()
	folders := map[string]string{}
	for slug := range ingestSourceDefs {
		folders[slug] = "root-folder"
	}
	cfg := config.Config{
		ObjectStoreBackend:                  "google_drive",
		ObjectStoreGoogleDriveDisableAuth:   true,
		ObjectStoreGoogleDriveAPIBaseURL:    driveURL,
		ObjectStoreGoogleDriveUploadBaseURL: driveURL,
		IngestFolderIDs:                     folders,
		IngestMaxObjectBytes:                64 * 1024 * 1024,
	}
	svc, enabled, err := newIngestService(cfg, objectsTestSigner(), func() time.Time { return objectsTestNow }, slog.Default())
	if err != nil || !enabled {
		t.Fatalf("newIngestService enabled=%v err=%v", enabled, err)
	}
	return svc
}

// e2eCase is one endpoint exercised end-to-end, with the Drive object it should
// produce.
type e2eCase struct {
	name      string
	endpoint  string
	body      []byte
	extra     url.Values
	wantName  string
	wantMime  string
	wantProps map[string]string
}

func e2eCases() []e2eCase {
	sha := func(b []byte) string { return sha256Hex(b) }
	batchBody := []byte("gzipped-batch")
	att := []byte("attachment-bytes")
	audio := []byte("audio-bytes")
	meta := []byte(`{"schema_version":1}`)
	html := []byte("<html>x</html>")
	noteAtt := []byte("note-attachment")
	revision := []byte(`{"schema_version":1,"source":"apple_notes"}`)
	audioSHA := sha(audio)
	photo := []byte("heic-bytes")
	photoSHA := sha(photo)
	photoMeta := []byte(`{"schema_version":1,"source":"apple_photos"}`)
	photoDedupSHA := sha([]byte("apple_photos|z@x.test|UUID-1|original|" + photoSHA))
	return []e2eCase{
		{
			name: "agent-sessions/batch", endpoint: "/ingest/agent-sessions/batch", body: batchBody,
			extra:    url.Values{"exported_at": {"2026-06-19T12:34:56+00:00"}},
			wantName: "20260619T123456Z-" + sha(batchBody) + ".jsonl.gz", wantMime: "application/gzip",
			wantProps: map[string]string{"pdw_kind": "agent_sessions_export_batch", "pdw_stage": "inbox", "pdw_source": "agent_sessions", "batch_sha256": sha(batchBody)},
		},
		{
			name: "apple-messages/batch", endpoint: "/ingest/apple-messages/batch", body: batchBody,
			extra:    url.Values{"exported_at": {"2026-06-19T12:34:56+00:00"}},
			wantName: "20260619T123456Z-" + sha(batchBody) + ".jsonl.gz", wantMime: "application/gzip",
			wantProps: map[string]string{"pdw_kind": "apple_message_export_batch", "pdw_source": "apple_messages"},
		},
		{
			name: "apple-messages/attachment", endpoint: "/ingest/apple-messages/attachment", body: att,
			extra:    url.Values{"attachment_guid": {"A1"}, "message_guid": {"M1"}, "content_type": {"image/jpeg"}, "created_at": {"2025-03-04T00:00:00+00:00"}, "filename": {"p.jpg"}},
			wantName: "2025-03-04-A1-" + sha(att) + ".jpg", wantMime: "image/jpeg",
			wantProps: map[string]string{"pdw_kind": "apple_message_attachment", "attachment_guid": "A1", "message_guid": "M1"},
		},
		{
			name: "voice-memos/audio", endpoint: "/ingest/voice-memos/audio", body: audio,
			extra:    url.Values{"recorded_at": {"2025-07-15T09:00:00"}, "extension": {".m4a"}, "content_type": {"audio/m4a"}},
			wantName: "2025-07-15-" + audioSHA + ".m4a", wantMime: "audio/m4a",
			wantProps: map[string]string{"pdw_kind": "voice_memo_audio", "pdw_source": "apple_voice_memos"},
		},
		{
			name: "voice-memos/metadata", endpoint: "/ingest/voice-memos/metadata", body: meta,
			extra:    url.Values{"recorded_at": {"2025-07-15T09:00:00"}, "audio_content_sha256": {audioSHA}},
			wantName: "2025-07-15-" + audioSHA + ".json", wantMime: "application/json",
			wantProps: map[string]string{"pdw_kind": "voice_memo_metadata", "audio_content_sha256": audioSHA},
		},
		{
			name: "photos/file", endpoint: "/ingest/photos/file", body: photo,
			extra:    url.Values{"captured_at": {"2026-06-01T14:30:00"}, "extension": {".heic"}, "content_type": {"image/heic"}},
			wantName: "2026-06-01-" + photoSHA + ".heic", wantMime: "image/heic",
			wantProps: map[string]string{"pdw_kind": "photo_file", "pdw_stage": "inbox", "pdw_source": "photos"},
		},
		{
			name: "photos/metadata", endpoint: "/ingest/photos/metadata", body: photoMeta,
			extra: url.Values{
				"captured_at":           {"2026-06-01T14:30:00"},
				"file_content_sha256":   {photoSHA},
				"metadata_dedup_sha256": {photoDedupSHA},
			},
			wantName: "2026-06-01-" + photoDedupSHA + ".json", wantMime: "application/json",
			wantProps: map[string]string{"pdw_kind": "photo_metadata", "file_content_sha256": photoSHA},
		},
		{
			name: "apple-notes/body", endpoint: "/ingest/apple-notes/body", body: html,
			extra:    url.Values{"note_id": {"N1"}, "revision_id": {"R1"}, "modified_at": {"2026-01-02T03:04:05+00:00"}},
			wantName: "R1.html", wantMime: "text/html",
			wantProps: map[string]string{"pdw_kind": "apple_note_body_html", "note_id": "N1", "revision_id": "R1"},
		},
		{
			name: "apple-notes/attachment", endpoint: "/ingest/apple-notes/attachment", body: noteAtt,
			extra:    url.Values{"note_id": {"N1"}, "revision_id": {"R1"}, "modified_at": {"2026-01-02T03:04:05+00:00"}, "attachment_id": {"AT1"}, "filename": {"d.pdf"}, "content_type": {"application/pdf"}},
			wantName: "AT1-" + sha(noteAtt) + ".pdf", wantMime: "application/pdf",
			wantProps: map[string]string{"pdw_kind": "apple_note_attachment", "attachment_id": "AT1"},
		},
		{
			name: "apple-notes/revision", endpoint: "/ingest/apple-notes/revision", body: revision,
			extra:    url.Values{"note_id": {"N1"}, "revision_id": {"R1"}, "modified_at": {"2026-01-02T03:04:05+00:00"}, "note_content_sha256": {"FP1"}},
			wantName: "R1.json", wantMime: "application/json",
			wantProps: map[string]string{"pdw_kind": "apple_note_revision_metadata", "note_content_sha256": "FP1"},
		},
	}
}

// TestIngestEndToEndEveryEndpoint drives every ingestion endpoint through the
// real GoogleDriveStore against a fake Drive and asserts the resulting upload's
// filename, mime type, body, and pdw_*/domain tags. This is the full write path:
// signed HTTP -> handler -> store -> Drive multipart.
func TestIngestEndToEndEveryEndpoint(t *testing.T) {
	drive := &fakeDrive{}
	driveSrv := httptest.NewServer(drive)
	defer driveSrv.Close()
	svc := e2eService(t, driveSrv.URL)

	cases := e2eCases()
	if len(cases) != len(ingestArtifacts()) {
		t.Fatalf("e2e covers %d endpoints but %d are registered", len(cases), len(ingestArtifacts()))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := len(drive.uploads)
			target := signedIngestTarget(tc.endpoint, tc.body, tc.extra)
			rec := postIngest(t, svc, target, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
			}
			if len(drive.uploads) != before+1 {
				t.Fatalf("expected exactly one Drive upload, got %d new", len(drive.uploads)-before)
			}
			up := drive.lastUpload()
			if up.Name != tc.wantName {
				t.Errorf("drive name = %q, want %q", up.Name, tc.wantName)
			}
			if string(up.Media) != string(tc.body) {
				t.Errorf("drive media = %q, want %q", up.Media, tc.body)
			}
			if tc.wantMime != "" && up.MimeType != tc.wantMime {
				t.Errorf("drive mimeType = %q, want %q", up.MimeType, tc.wantMime)
			}
			if up.AppProps["content_sha256"] != sha256Hex(tc.body) {
				t.Errorf("content_sha256 = %q, want %q", up.AppProps["content_sha256"], sha256Hex(tc.body))
			}
			if up.AppProps["pdw_root_folder_id"] != "root-folder" {
				t.Errorf("pdw_root_folder_id = %q", up.AppProps["pdw_root_folder_id"])
			}
			for k, v := range tc.wantProps {
				if up.AppProps[k] != v {
					t.Errorf("appProperty %q = %q, want %q", k, up.AppProps[k], v)
				}
			}
		})
	}
}

// TestIngestEndToEndPythonClient is the cross-runtime manual test: the real
// Python IngestClient posts through every endpoint to the live handler, which
// writes to the fake Drive. Opt-in (needs uv + the repo) via PDW_E2E=1.
func TestIngestEndToEndPythonClient(t *testing.T) {
	if os.Getenv("PDW_E2E") != "1" {
		t.Skip("set PDW_E2E=1 to run the Python<->Go cross-boundary e2e (needs uv)")
	}
	drive := &fakeDrive{}
	driveSrv := httptest.NewServer(drive)
	defer driveSrv.Close()
	svc := e2eService(t, driveSrv.URL)
	ingestSrv := httptest.NewServer(svc.handler())
	defer ingestSrv.Close()

	cmd := exec.Command("uv", "run", "python", "scripts/ingest_e2e_driver.py", ingestSrv.URL, objectsTestSecret)
	cmd.Dir = repoRootForE2E(t)
	out, err := cmd.CombinedOutput()
	t.Logf("python driver output:\n%s", out)
	if err != nil {
		t.Fatalf("python driver failed: %v", err)
	}
	if got := len(drive.uploads); got != len(ingestArtifacts()) {
		t.Fatalf("expected %d uploads from python client, got %d", len(ingestArtifacts()), got)
	}
}

func repoRootForE2E(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd() // .../app/internal/server
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(wd, "/app/internal/server")
}
