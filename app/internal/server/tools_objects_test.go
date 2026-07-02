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
	meta       objectstore.ObjectMetadata
	content    []byte
	notFound   bool
	getCalled  bool
	metaCalled bool
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
	f.metaCalled = true
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

const objectsTestSecret = "0123456789abcdef0123456789abcdef"

func objectsTestSigner() *pdwauth.Service {
	return pdwauth.NewService([]byte(objectsTestSecret), func() time.Time { return objectsTestNow })
}

func invokeGetObject(t *testing.T, store objectstore.ObjectStore, in getObjectInput) getObjectOutput {
	return invokeGetObjectWithDrive(t, store, nil, in)
}

func invokeGetObjectWithDrive(t *testing.T, store objectstore.ObjectStore, driveStores map[string]objectstore.ObjectStore, in getObjectInput) getObjectOutput {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	tl := getObjectTool(store, driveStores, objectsTestSigner(), objectsTestBaseURL, time.Hour, func() time.Time { return objectsTestNow })
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
	if err := objectsTestSigner().VerifyObjectDownload("fid", "", parsed.Query().Get("exp"), parsed.Query().Get("sig")); err != nil {
		t.Fatalf("signature does not verify: %v", err)
	}
}

func TestGetObjectToolReportsGoogleNativeDocAsDocx(t *testing.T) {
	store := &fakeObjectStore{
		meta: objectstore.ObjectMetadata{
			Backend: "google_drive", StorageFileID: "doc", ContentType: "application/vnd.google-apps.document",
			Filename: "MOU <Draft>", StorageURL: "https://drive/doc",
		},
	}
	out := invokeGetObject(t, store, getObjectInput{StorageFileID: "doc"})
	if got, want := out.ContentType, "application/vnd.openxmlformats-officedocument.wordprocessingml.document"; got != want {
		t.Fatalf("ContentType = %q, want %q", got, want)
	}
	if got, want := out.Filename, "MOU <Draft>.docx"; got != want {
		t.Fatalf("Filename = %q, want %q", got, want)
	}
}

func TestGetObjectToolRoutesDriveSourceByAccount(t *testing.T) {
	primary := &fakeObjectStore{meta: objectstore.ObjectMetadata{Backend: "google_drive"}}
	driveStore := &fakeObjectStore{
		meta: objectstore.ObjectMetadata{
			Backend: "google_drive", StorageFileID: "DRIVEID", ContentType: "application/pdf",
			SizeBytes: 9, Filename: "doc.pdf", StorageURL: "https://drive/DRIVEID",
		},
		content: []byte("pdfbytes"),
	}
	driveStores := map[string]objectstore.ObjectStore{"zach@hackclub.com": driveStore}

	out := invokeGetObjectWithDrive(t, primary, driveStores, getObjectInput{
		StorageFileID: "DRIVEID", StorageBackend: "google_drive_source", Account: "zach@hackclub.com",
	})
	if !out.Exists || out.Filename != "doc.pdf" {
		t.Fatalf("unexpected metadata: %+v", out)
	}
	if primary.metaCalled {
		t.Fatal("primary store must not be consulted for a drive-source file")
	}
	parsed, err := url.Parse(out.DownloadURL)
	if err != nil {
		t.Fatalf("parse download_url: %v", err)
	}
	if got := parsed.Query().Get("account"); got != "zach@hackclub.com" {
		t.Fatalf("download_url missing account: %q", out.DownloadURL)
	}
	// The link is bound to the account and verifies only with it.
	q := parsed.Query()
	if err := objectsTestSigner().VerifyObjectDownload("DRIVEID", "zach@hackclub.com", q.Get("exp"), q.Get("sig")); err != nil {
		t.Fatalf("account-bound signature does not verify: %v", err)
	}
	if err := objectsTestSigner().VerifyObjectDownload("DRIVEID", "", q.Get("exp"), q.Get("sig")); err == nil {
		t.Fatal("expected account-bound link to fail without account")
	}
}

func TestGetObjectToolDriveSourceDoesNotLeakStoreToNextInvocation(t *testing.T) {
	primary := &fakeObjectStore{
		meta: objectstore.ObjectMetadata{
			Backend: "google_drive", StorageFileID: "fid", ContentType: "text/plain",
			SizeBytes: 7, Filename: "primary.txt",
		},
	}
	driveStore := &fakeObjectStore{
		meta: objectstore.ObjectMetadata{
			Backend: "google_drive", StorageFileID: "DRIVEID", ContentType: "text/plain",
			SizeBytes: 5, Filename: "drive.txt",
		},
	}
	tl := getObjectTool(
		primary,
		map[string]objectstore.ObjectStore{"zach@hackclub.com": driveStore},
		objectsTestSigner(),
		objectsTestBaseURL,
		time.Hour,
		func() time.Time { return objectsTestNow },
	)
	invoke := func(in getObjectInput) getObjectOutput {
		t.Helper()
		raw, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal input: %v", err)
		}
		out, _, err := tl.Invoke(context.Background(), raw)
		if err != nil {
			t.Fatalf("Invoke: %v", err)
		}
		result, ok := out.(getObjectOutput)
		if !ok {
			t.Fatalf("unexpected output type %T", out)
		}
		return result
	}

	driveOut := invoke(getObjectInput{
		StorageFileID: "DRIVEID", StorageBackend: "google_drive_source", Account: "zach@hackclub.com",
	})
	if !driveOut.Exists || driveOut.Filename != "drive.txt" {
		t.Fatalf("unexpected drive output: %+v", driveOut)
	}
	if primary.metaCalled {
		t.Fatal("primary store must not be consulted for drive-source invocation")
	}

	primaryOut := invoke(getObjectInput{StorageFileID: "fid"})
	if !primaryOut.Exists || primaryOut.Filename != "primary.txt" {
		t.Fatalf("unexpected primary output after drive-source invocation: %+v", primaryOut)
	}
	if !primary.metaCalled {
		t.Fatal("primary store was not consulted for normal invocation")
	}
}

func TestGetObjectToolUnknownDriveAccount(t *testing.T) {
	primary := &fakeObjectStore{meta: objectstore.ObjectMetadata{Backend: "google_drive"}}
	out := invokeGetObjectWithDrive(t, primary, map[string]objectstore.ObjectStore{}, getObjectInput{
		StorageFileID: "DRIVEID", StorageBackend: "google_drive_source", Account: "nobody@nowhere.com",
	})
	if out.Error == "" {
		t.Fatal("expected error for unknown drive account")
	}
}

func TestGetObjectToolReportsBackendFromMetadata(t *testing.T) {
	store := &fakeObjectStore{
		meta: objectstore.ObjectMetadata{
			Backend: "slack", StorageFileID: "F0123ABCDEF", ContentType: "image/png",
			SizeBytes: 10, Filename: "screenshot.png",
		},
	}
	out := invokeGetObject(t, store, getObjectInput{StorageFileID: "F0123ABCDEF"})
	if !out.Exists || out.Backend != "slack" {
		t.Fatalf("unexpected output: %+v", out)
	}
	if !strings.HasPrefix(out.DownloadURL, objectsTestBaseURL+"/objects/F0123ABCDEF?") {
		t.Fatalf("unexpected download_url: %q", out.DownloadURL)
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
