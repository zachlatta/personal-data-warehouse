package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
)

func objectsTestHandler(store objectstore.ObjectStore, maxBytes int64) http.Handler {
	return objectDownloadHandler(store, objectsTestSigner(), maxBytes, slog.Default())
}

func signedObjectPath(fileID string, exp time.Time) string {
	return objectsPathPrefix + fileID +
		"?exp=" + strconv.FormatInt(exp.Unix(), 10) +
		"&sig=" + objectsTestSigner().SignObjectDownload(fileID, exp)
}

func serveObjectRequest(t *testing.T, store objectstore.ObjectStore, maxBytes int64, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	objectsTestHandler(store, maxBytes).ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
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
		"&sig=" + objectsTestSigner().SignObjectDownload("fid", exp)
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

func TestObjectDownloadRejectsOtherMethods(t *testing.T) {
	rec := serveObjectRequest(t, &fakeObjectStore{}, 1024, http.MethodPost, signedObjectPath("fid", objectsTestNow.Add(time.Hour)))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rec.Code)
	}
}
