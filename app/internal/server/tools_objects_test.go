package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
)

// fakeObjectStore implements objectstore.ObjectStore for the get_object tool
// test. Only the read methods the tool uses are meaningful; the rest return
// ErrNotImplemented.
type fakeObjectStore struct {
	meta     objectstore.ObjectMetadata
	content  []byte
	notFound bool
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

func invokeGetObject(t *testing.T, store objectstore.ObjectStore, maxBytes int64, in getObjectInput) getObjectOutput {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	out, isErr, err := getObjectTool(store, maxBytes).Invoke(context.Background(), raw)
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

func TestGetObjectToolReturnsContent(t *testing.T) {
	store := &fakeObjectStore{
		meta: objectstore.ObjectMetadata{
			Backend: "google_drive", StorageFileID: "fid", ContentType: "audio/mp4",
			SizeBytes: 4, ContentSHA256: "sha", Filename: "memo.m4a", StorageURL: "https://drive/fid",
		},
		content: []byte("data"),
	}
	out := invokeGetObject(t, store, 5*1024*1024, getObjectInput{StorageFileID: "fid", StorageKey: "k"})
	if !out.Exists || out.ContentType != "audio/mp4" || out.Filename != "memo.m4a" {
		t.Fatalf("unexpected metadata: %+v", out)
	}
	decoded, err := base64.StdEncoding.DecodeString(out.ContentBase64)
	if err != nil || string(decoded) != "data" {
		t.Fatalf("content mismatch: %q err=%v", decoded, err)
	}
}

func TestGetObjectToolOmitsLargeContent(t *testing.T) {
	store := &fakeObjectStore{
		meta:    objectstore.ObjectMetadata{StorageFileID: "fid", SizeBytes: 10_000_000},
		content: []byte("data"),
	}
	out := invokeGetObject(t, store, 1024, getObjectInput{StorageFileID: "fid"})
	if !out.Exists || !out.ContentOmitted || out.ContentBase64 != "" {
		t.Fatalf("expected content omitted: %+v", out)
	}
}

func TestGetObjectToolMissingObject(t *testing.T) {
	out := invokeGetObject(t, &fakeObjectStore{notFound: true}, 1024, getObjectInput{StorageFileID: "gone"})
	if out.Exists {
		t.Fatalf("expected exists=false: %+v", out)
	}
}

func TestGetObjectToolRequiresFileID(t *testing.T) {
	out := invokeGetObject(t, &fakeObjectStore{}, 1024, getObjectInput{})
	if out.Error == "" {
		t.Fatalf("expected error for missing storage_file_id")
	}
}
