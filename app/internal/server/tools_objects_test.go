package server

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
)

// fakeObjectStore implements objectstore.ObjectStore for the get_object tool
// test. Only the read methods the tool uses are meaningful; the rest return
// ErrNotImplemented.
type fakeObjectStore struct {
	meta      objectstore.ObjectMetadata
	content   []byte
	notFound  bool
	getCalled bool
}

func (f *fakeObjectStore) Backend() string { return "google_drive" }
func (f *fakeObjectStore) HasBlob(context.Context, string) (bool, error) {
	return false, objectstore.ErrNotImplemented
}
func (f *fakeObjectStore) HasMetadata(context.Context, string) (bool, error) {
	return false, objectstore.ErrNotImplemented
}
func (f *fakeObjectStore) HasObject(context.Context, string, string, string) (bool, error) {
	return false, objectstore.ErrNotImplemented
}
func (f *fakeObjectStore) Presence(context.Context, string) (objectstore.ObjectPresence, error) {
	return objectstore.ObjectPresence{}, objectstore.ErrNotImplemented
}
func (f *fakeObjectStore) PutFile(context.Context, objectstore.PutFileInput) (objectstore.StoredObject, error) {
	return objectstore.StoredObject{}, objectstore.ErrNotImplemented
}
func (f *fakeObjectStore) PutJSON(context.Context, objectstore.PutJSONInput) (objectstore.StoredObject, error) {
	return objectstore.StoredObject{}, objectstore.ErrNotImplemented
}
func (f *fakeObjectStore) GetObject(context.Context, objectstore.StoredObject) ([]byte, error) {
	f.getCalled = true
	if f.notFound {
		return nil, objectstore.ErrNotFound
	}
	return f.content, nil
}
func (f *fakeObjectStore) ObjectExists(context.Context, objectstore.StoredObject) (bool, error) {
	return !f.notFound, nil
}
func (f *fakeObjectStore) DeleteObject(context.Context, objectstore.StoredObject) error {
	return objectstore.ErrNotImplemented
}
func (f *fakeObjectStore) GetMetadata(context.Context, objectstore.StoredObject) (objectstore.ObjectMetadata, error) {
	if f.notFound {
		return objectstore.ObjectMetadata{}, objectstore.ErrNotFound
	}
	return f.meta, nil
}
func (f *fakeObjectStore) GetShareURL(context.Context, objectstore.StoredObject) (string, error) {
	return f.meta.StorageURL, nil
}
func (f *fakeObjectStore) ListObjects(context.Context, objectstore.ListQuery) ([]objectstore.ObjectListing, error) {
	return nil, objectstore.ErrNotImplemented
}
func (f *fakeObjectStore) FindObject(context.Context, objectstore.ListQuery) (*objectstore.ObjectListing, error) {
	return nil, objectstore.ErrNotImplemented
}
func (f *fakeObjectStore) MoveObject(context.Context, objectstore.StoredObject, string, map[string]string) (objectstore.StoredObject, error) {
	return objectstore.StoredObject{}, objectstore.ErrNotImplemented
}

const objectsTestBaseURL = "http://example.test"

var objectsTestNow = time.Unix(1_700_000_000, 0)

func objectsTestSigner() *pdwauth.Service {
	return pdwauth.NewService([]byte("0123456789abcdef0123456789abcdef"), func() time.Time { return objectsTestNow })
}

func invokeGetObject(t *testing.T, store objectstore.ObjectStore, in getObjectInput) getObjectOutput {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	tl := getObjectTool(store, objectsTestSigner(), objectsTestBaseURL, time.Hour, func() time.Time { return objectsTestNow })
	out, isErr, err := tl.Invoke(context.Background(), raw)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	result, ok := out.(getObjectOutput)
	if !ok {
		t.Fatalf("unexpected output type %T", out)
	}
	if result.Error != "" && !isErr {
		t.Fatalf("error output not flagged as error: %+v", result)
	}
	return result
}

func TestGetObjectToolReturnsSignedDownloadURL(t *testing.T) {
	store := &fakeObjectStore{
		meta: objectstore.ObjectMetadata{
			Backend: "google_drive", StorageFileID: "fid", ContentType: "audio/mp4",
			SizeBytes: 4, ContentSHA256: "sha", Filename: "memo.m4a", StorageURL: "https://drive/fid",
		},
		content: []byte("data"),
	}
	out := invokeGetObject(t, store, getObjectInput{StorageFileID: "fid", StorageKey: "k"})
	if !out.Exists || out.ContentType != "audio/mp4" || out.Filename != "memo.m4a" {
		t.Fatalf("unexpected metadata: %+v", out)
	}
	if store.getCalled {
		t.Fatal("tool must not download object content")
	}
	wantExp := objectsTestNow.Add(time.Hour)
	if out.ExpiresAt != wantExp.UTC().Format(time.RFC3339) {
		t.Fatalf("unexpected expires_at: %q", out.ExpiresAt)
	}
	if !strings.HasPrefix(out.DownloadURL, objectsTestBaseURL+"/objects/fid?") {
		t.Fatalf("unexpected download_url: %q", out.DownloadURL)
	}
	parsed, err := url.Parse(out.DownloadURL)
	if err != nil {
		t.Fatalf("parse download_url: %v", err)
	}
	if got := parsed.Query().Get("exp"); got != strconv.FormatInt(wantExp.Unix(), 10) {
		t.Fatalf("unexpected exp: %q", got)
	}
	if err := objectsTestSigner().VerifyObjectDownload("fid", parsed.Query().Get("exp"), parsed.Query().Get("sig")); err != nil {
		t.Fatalf("signature does not verify: %v", err)
	}
}

func TestGetObjectToolMissingObject(t *testing.T) {
	out := invokeGetObject(t, &fakeObjectStore{notFound: true}, getObjectInput{StorageFileID: "gone"})
	if out.Exists {
		t.Fatalf("expected exists=false: %+v", out)
	}
	if out.DownloadURL != "" {
		t.Fatalf("expected no download_url for missing object: %+v", out)
	}
}

func TestGetObjectToolRequiresFileID(t *testing.T) {
	out := invokeGetObject(t, &fakeObjectStore{}, getObjectInput{})
	if out.Error == "" {
		t.Fatalf("expected error for missing storage_file_id")
	}
}
