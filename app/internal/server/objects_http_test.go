package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
)

func objectsTestHandler(store objectstore.ObjectStore, maxBytes int64) http.Handler {
	return objectDownloadHandler(store, nil, objectsTestSigner(), maxBytes, slog.Default())
}

func objectsTestHandlerWithDrive(store objectstore.ObjectStore, driveStores map[string]objectstore.ObjectStore, maxBytes int64) http.Handler {
	return objectDownloadHandler(store, driveStores, objectsTestSigner(), maxBytes, slog.Default())
}

func signedObjectPath(fileID string, exp time.Time) string {
	return objectsPathPrefix + fileID +
		"?exp=" + strconv.FormatInt(exp.Unix(), 10) +
		"&sig=" + objectsTestSigner().SignObjectDownload(fileID, "", exp)
}

func serveObjectRequest(t *testing.T, store objectstore.ObjectStore, maxBytes int64, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	objectsTestHandler(store, maxBytes).ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

func TestObjectDownloadServesDriveSourceByAccount(t *testing.T) {
	primary := &fakeObjectStore{content: []byte("primary")}
	driveStore := &fakeObjectStore{
		meta:    objectstore.ObjectMetadata{StorageFileID: "DRIVEID", ContentType: "image/png", SizeBytes: 5},
		content: []byte("image"),
	}
	driveStores := map[string]objectstore.ObjectStore{"zach@hackclub.com": driveStore}
	exp := objectsTestNow.Add(time.Hour)
	target := objectsPathPrefix + "DRIVEID?exp=" + strconv.FormatInt(exp.Unix(), 10) +
		"&account=" + url.QueryEscape("zach@hackclub.com") +
		"&sig=" + objectsTestSigner().SignObjectDownload("DRIVEID", "zach@hackclub.com", exp)

	rec := httptest.NewRecorder()
	objectsTestHandlerWithDrive(primary, driveStores, 1024).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if body, _ := io.ReadAll(rec.Body); string(body) != "image" {
		t.Fatalf("served wrong store: %q", body)
	}
	if primary.getCalled {
		t.Fatal("primary store must not serve a drive-source file")
	}

	// Same link, swapped account -> signature mismatch (403).
	bad := objectsPathPrefix + "DRIVEID?exp=" + strconv.FormatInt(exp.Unix(), 10) +
		"&account=" + url.QueryEscape("zach@zachlatta.com") +
		"&sig=" + objectsTestSigner().SignObjectDownload("DRIVEID", "zach@hackclub.com", exp)
	rec2 := httptest.NewRecorder()
	objectsTestHandlerWithDrive(primary, driveStores, 1024).ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, bad, nil))
	if rec2.Code != http.StatusForbidden {
		t.Fatalf("account-swapped link status = %d", rec2.Code)
	}

	// Valid signature but unknown account -> 404.
	unknown := objectsPathPrefix + "DRIVEID?exp=" + strconv.FormatInt(exp.Unix(), 10) +
		"&account=" + url.QueryEscape("ghost@nowhere.com") +
		"&sig=" + objectsTestSigner().SignObjectDownload("DRIVEID", "ghost@nowhere.com", exp)
	rec3 := httptest.NewRecorder()
	objectsTestHandlerWithDrive(primary, driveStores, 1024).ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, unknown, nil))
	if rec3.Code != http.StatusNotFound {
		t.Fatalf("unknown-account link status = %d", rec3.Code)
	}
}

func TestObjectDownloadDriveSourceDoesNotLeakStoreToNextRequest(t *testing.T) {
	primary := &fakeObjectStore{
		meta:    objectstore.ObjectMetadata{StorageFileID: "fid", ContentType: "text/plain", SizeBytes: 7},
		content: []byte("primary"),
	}
	driveStore := &fakeObjectStore{
		meta:    objectstore.ObjectMetadata{StorageFileID: "DRIVEID", ContentType: "text/plain", SizeBytes: 5},
		content: []byte("drive"),
	}
	handler := objectsTestHandlerWithDrive(
		primary,
		map[string]objectstore.ObjectStore{"zach@hackclub.com": driveStore},
		1024,
	)
	exp := objectsTestNow.Add(time.Hour)
	driveTarget := objectsPathPrefix + "DRIVEID?exp=" + strconv.FormatInt(exp.Unix(), 10) +
		"&account=" + url.QueryEscape("zach@hackclub.com") +
		"&sig=" + objectsTestSigner().SignObjectDownload("DRIVEID", "zach@hackclub.com", exp)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, driveTarget, nil))
	if rec1.Code != http.StatusOK || rec1.Body.String() != "drive" {
		t.Fatalf("drive-source response = %d %q", rec1.Code, rec1.Body.String())
	}

	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, signedObjectPath("fid", exp), nil))
	if rec2.Code != http.StatusOK || rec2.Body.String() != "primary" {
		t.Fatalf("primary response after drive-source request = %d %q", rec2.Code, rec2.Body.String())
	}
}

func TestObjectDownloadServesContent(t *testing.T) {
	store := &fakeObjectStore{
		meta: objectstore.ObjectMetadata{
			StorageFileID: "fid", ContentType: "application/pdf", SizeBytes: 4,
			Filename: "receipt.pdf",
		},
		content: []byte("data"),
	}
	rec := serveObjectRequest(t, store, 1024, http.MethodGet, signedObjectPath("fid", objectsTestNow.Add(time.Hour)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	body, _ := io.ReadAll(rec.Body)
	if string(body) != "data" {
		t.Fatalf("body = %q", body)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/pdf" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("Content-Disposition"); got != `inline; filename=receipt.pdf` {
		t.Fatalf("Content-Disposition = %q", got)
	}
	if got := rec.Header().Get("Content-Length"); got != "4" {
		t.Fatalf("Content-Length = %q", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "private, no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}
}

func TestObjectDownloadHead(t *testing.T) {
	store := &fakeObjectStore{
		meta:    objectstore.ObjectMetadata{StorageFileID: "fid", ContentType: "image/png", SizeBytes: 4},
		content: []byte("data"),
	}
	rec := serveObjectRequest(t, store, 1024, http.MethodHead, signedObjectPath("fid", objectsTestNow.Add(time.Hour)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD wrote body: %q", rec.Body.String())
	}
	if store.getCalled {
		t.Fatal("HEAD must not download object content")
	}
}

func TestObjectDownloadRejectsExpiredLink(t *testing.T) {
	store := &fakeObjectStore{content: []byte("data")}
	rec := serveObjectRequest(t, store, 1024, http.MethodGet, signedObjectPath("fid", objectsTestNow.Add(-time.Second)))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestObjectDownloadRejectsTamperedSig(t *testing.T) {
	store := &fakeObjectStore{content: []byte("data")}
	exp := objectsTestNow.Add(time.Hour)
	target := objectsPathPrefix + "fid?exp=" + strconv.FormatInt(exp.Unix(), 10) + "&sig=bogus"
	rec := serveObjectRequest(t, store, 1024, http.MethodGet, target)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestObjectDownloadRejectsSigForOtherFile(t *testing.T) {
	store := &fakeObjectStore{content: []byte("data")}
	exp := objectsTestNow.Add(time.Hour)
	target := objectsPathPrefix + "other?exp=" + strconv.FormatInt(exp.Unix(), 10) +
		"&sig=" + objectsTestSigner().SignObjectDownload("fid", "", exp)
	rec := serveObjectRequest(t, store, 1024, http.MethodGet, target)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestObjectDownloadMissingObject(t *testing.T) {
	rec := serveObjectRequest(t, &fakeObjectStore{notFound: true}, 1024, http.MethodGet, signedObjectPath("gone", objectsTestNow.Add(time.Hour)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestObjectDownloadOversizeObject(t *testing.T) {
	store := &fakeObjectStore{meta: objectstore.ObjectMetadata{StorageFileID: "fid", SizeBytes: 2048}}
	rec := serveObjectRequest(t, store, 1024, http.MethodGet, signedObjectPath("fid", objectsTestNow.Add(time.Hour)))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rec.Code)
	}
}

// TestObjectDownloadServesSlackFileViaRouter exercises the full Slack path:
// a signed /objects/{slack file id} link routed through WithSlackFiles to a
// SlackFileStore backed by a fake Slack API.
func TestObjectDownloadServesSlackFileViaRouter(t *testing.T) {
	const fileID = "F0123ABCDEF"
	urlPrivate := "https://files.slack.com/files-pri/T01-" + fileID + "/notes.pdf"
	slackStore := objectstore.NewSlackFileStore(objectstore.SlackFileStoreOptions{
		Tokens: []string{"tok"},
		HTTPClient: &slackAPIFake{
			fileID:     fileID,
			urlPrivate: urlPrivate,
			content:    []byte("slack-bytes"),
		},
	})
	store := objectstore.WithSlackFiles(&fakeObjectStore{notFound: true}, slackStore)
	rec := serveObjectRequest(t, store, 1024, http.MethodGet, signedObjectPath(fileID, objectsTestNow.Add(time.Hour)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "slack-bytes" {
		t.Fatalf("body = %q", rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/pdf" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("Content-Disposition"); got != `inline; filename=notes.pdf` {
		t.Fatalf("Content-Disposition = %q", got)
	}
}

// slackAPIFake simulates files.info and the url_private download for a single file.
type slackAPIFake struct {
	fileID     string
	urlPrivate string
	content    []byte
}

func (f *slackAPIFake) Do(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Authorization") != "Bearer tok" {
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader("bad auth"))}, nil
	}
	if strings.HasSuffix(req.URL.Path, "/files.info") {
		if req.URL.Query().Get("file") != f.fileID {
			return slackFakeJSON(`{"ok":false,"error":"file_not_found"}`), nil
		}
		payload, _ := json.Marshal(map[string]any{"ok": true, "file": map[string]any{
			"id": f.fileID, "name": "notes.pdf", "mimetype": "application/pdf",
			"size": len(f.content), "url_private": f.urlPrivate,
		}})
		return slackFakeJSON(string(payload)), nil
	}
	if req.URL.String() != f.urlPrivate {
		return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader("not found"))}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/pdf"}},
		Body:       io.NopCloser(bytes.NewReader(f.content)),
	}, nil
}

func slackFakeJSON(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestObjectDownloadRejectsOtherMethods(t *testing.T) {
	rec := serveObjectRequest(t, &fakeObjectStore{}, 1024, http.MethodPost, signedObjectPath("fid", objectsTestNow.Add(time.Hour)))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rec.Code)
	}
}
