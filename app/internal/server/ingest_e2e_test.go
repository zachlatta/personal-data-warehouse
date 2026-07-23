package server

import (
	"crypto/sha256"
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
	"strconv"
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
	mu        sync.Mutex
	uploads   []capturedUpload
	resumable map[string]*capturedUpload
	nextID    int
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
	case r.Method == http.MethodPost && r.URL.Query().Get("uploadType") == "resumable":
		d.startResumable(w, r)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/resumable/"):
		d.putResumable(w, r)
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

func (d *fakeDrive) startResumable(w http.ResponseWriter, r *http.Request) {
	var metadata struct {
		Name          string            `json:"name"`
		Parents       []string          `json:"parents"`
		MimeType      string            `json:"mimeType"`
		AppProperties map[string]string `json:"appProperties"`
	}
	if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil {
		http.Error(w, "bad metadata", http.StatusBadRequest)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextID++
	sessionID := fmt.Sprintf("session-%d", d.nextID)
	if d.resumable == nil {
		d.resumable = map[string]*capturedUpload{}
	}
	d.resumable[sessionID] = &capturedUpload{
		Name:      metadata.Name,
		Parents:   metadata.Parents,
		MimeType:  metadata.MimeType,
		AppProps:  metadata.AppProperties,
		MediaType: r.Header.Get("X-Upload-Content-Type"),
	}
	w.Header().Set("Location", "http://"+r.Host+"/resumable/"+sessionID)
	writeFakeJSON(w, `{}`)
}

func (d *fakeDrive) putResumable(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimPrefix(r.URL.Path, "/resumable/")
	d.mu.Lock()
	upload := d.resumable[sessionID]
	d.mu.Unlock()
	if upload == nil {
		http.NotFound(w, r)
		return
	}
	contentRange := r.Header.Get("Content-Range")
	if strings.HasPrefix(contentRange, "bytes */") {
		if len(upload.Media) > 0 {
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(upload.Media)-1))
		}
		w.WriteHeader(308)
		return
	}
	body, _ := io.ReadAll(r.Body)
	d.mu.Lock()
	upload.Media = append(upload.Media, body...)
	parts := strings.Split(contentRange, "/")
	if len(parts) != 2 {
		d.mu.Unlock()
		http.Error(w, "bad content range", http.StatusBadRequest)
		return
	}
	total, err := strconv.Atoi(parts[1])
	if err != nil {
		d.mu.Unlock()
		http.Error(w, "bad content range", http.StatusBadRequest)
		return
	}
	if len(upload.Media) < total {
		w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(upload.Media)-1))
		d.mu.Unlock()
		w.WriteHeader(308)
		return
	}
	d.uploads = append(d.uploads, *upload)
	delete(d.resumable, sessionID)
	fileID := fmt.Sprintf("file-%d", d.nextID)
	content := append([]byte(nil), upload.Media...)
	d.mu.Unlock()
	checksum := sha256.Sum256(content)
	writeFakeJSON(
		w,
		fmt.Sprintf(
			`{"id":%q,"webViewLink":%q,"sha256Checksum":%q,"size":%q}`,
			fileID,
			"https://drive/"+fileID,
			fmt.Sprintf("%x", checksum),
			fmt.Sprintf("%d", len(content)),
		),
	)
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
	photoMeta := []byte(`{"schema_version":1,"source":"apple_photos"}`)
	photoSHA := sha([]byte("heic-bytes"))
	photoDedupSHA := sha([]byte("apple_photos|z@x.test|UUID-1|original|" + photoSHA))
	statement := []byte("%PDF-statement-bytes")
	statementSHA := sha(statement)
	financeMeta := []byte(`{"schema_version":1,"source":"manual"}`)
	financeDedupSHA := sha([]byte("manual|z@x.test|" + statementSHA + "|" + statementSHA))
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
			name: "manual-finance/file", endpoint: "/ingest/manual-finance/file", body: statement,
			extra: url.Values{
				"modified_at":    {"2026-06-30T10:00:00"},
				"account_folder": {"Acme-Checking-0001"},
				"extension":      {".pdf"},
				"content_type":   {"application/pdf"},
			},
			wantName: "2026-06-30-" + statementSHA + ".pdf", wantMime: "application/pdf",
			wantProps: map[string]string{"pdw_kind": "manual_finance_document", "pdw_stage": "inbox", "pdw_source": "manual_finance"},
		},
		{
			name: "manual-finance/metadata", endpoint: "/ingest/manual-finance/metadata", body: financeMeta,
			extra: url.Values{
				"modified_at":           {"2026-06-30T10:00:00"},
				"account_folder":        {"Acme-Checking-0001"},
				"file_content_sha256":   {statementSHA},
				"metadata_dedup_sha256": {financeDedupSHA},
			},
			wantName: "2026-06-30-" + financeDedupSHA + ".json", wantMime: "application/json",
			wantProps: map[string]string{"pdw_kind": "manual_finance_metadata", "file_content_sha256": statementSHA},
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

func TestIngestEndToEndPhotoResumableUpload(t *testing.T) {
	drive := &fakeDrive{}
	driveServer := httptest.NewServer(drive)
	defer driveServer.Close()
	service := e2eService(t, driveServer.URL)
	photo := []byte("heic-bytes")
	photoSHA := sha256Hex(photo)
	startBody, _ := json.Marshal(photoResumableRequest{
		CapturedAt:    "2026-06-01T14:30:00",
		Extension:     ".heic",
		ContentType:   "image/heic",
		ContentSHA256: photoSHA,
		SizeBytes:     int64(len(photo)),
	})
	startTarget := signedIngestTarget(photoResumableEndpoint, startBody, nil)
	startRecorder := postIngest(t, service, startTarget, startBody)
	if startRecorder.Code != http.StatusOK {
		t.Fatalf("start status = %d, body %q", startRecorder.Code, startRecorder.Body.String())
	}
	var started photoResumableResponse
	if err := json.Unmarshal(startRecorder.Body.Bytes(), &started); err != nil {
		t.Fatalf("decode start: %v", err)
	}
	request, _ := http.NewRequest(http.MethodPut, started.UploadURL, strings.NewReader(string(photo)))
	request.Header.Set("Content-Length", fmt.Sprintf("%d", len(photo)))
	request.Header.Set("Content-Type", "image/heic")
	request.Header.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(photo)-1, len(photo)))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("upload chunk: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("upload status = %d", response.StatusCode)
	}
	if len(drive.uploads) != 1 {
		t.Fatalf("Drive uploads = %d, want 1", len(drive.uploads))
	}
	uploaded := drive.lastUpload()
	if uploaded.Name != "2026-06-01-"+photoSHA+".heic" ||
		string(uploaded.Media) != string(photo) ||
		uploaded.AppProps["pdw_kind"] != "photo_file" ||
		uploaded.AppProps["pdw_source"] != "photos" {
		t.Fatalf("uploaded = %+v", uploaded)
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
	if got := len(drive.uploads); got != len(ingestArtifacts())+1 {
		t.Fatalf("expected %d uploads from python client, got %d", len(ingestArtifacts())+1, got)
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
