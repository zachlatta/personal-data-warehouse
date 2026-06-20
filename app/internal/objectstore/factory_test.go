package objectstore

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestBuildGoogleDriveStore(t *testing.T) {
	store, err := BuildObjectStore(GoogleDriveSpec(
		"folder-1", "apple_notes", "apple_note_body_html", "apple_note_revision_metadata",
		[]string{"legacy"},
		GoogleDriveConnection{HTTPClient: &driveFake{}},
	))
	if err != nil {
		t.Fatalf("BuildObjectStore: %v", err)
	}
	drive, ok := store.(*GoogleDriveStore)
	if !ok {
		t.Fatalf("expected *GoogleDriveStore, got %T", store)
	}
	if drive.Backend() != GoogleDriveBackend {
		t.Errorf("backend = %q", drive.Backend())
	}
	if drive.folderID != "folder-1" || drive.source != "apple_notes" {
		t.Errorf("unexpected drive config: folder=%q source=%q", drive.folderID, drive.source)
	}
	if drive.audioKind != "apple_note_body_html" || drive.metadataKind != "apple_note_revision_metadata" {
		t.Errorf("unexpected kinds: %q %q", drive.audioKind, drive.metadataKind)
	}
}

func TestBuildGoogleDriveAllowsEmptyNamespaceForDirectFileReads(t *testing.T) {
	store, err := BuildObjectStore(Spec{Backend: GoogleDriveBackend, GoogleDrive: &GoogleDriveConnection{HTTPClient: &driveFake{}}})
	if err != nil {
		t.Fatalf("BuildObjectStore: %v", err)
	}
	drive, ok := store.(*GoogleDriveStore)
	if !ok {
		t.Fatalf("expected *GoogleDriveStore, got %T", store)
	}
	if drive.folderID != "" {
		t.Fatalf("folderID = %q, want empty", drive.folderID)
	}
}

func TestGoogleDriveEmptyNamespaceRejectsManagedOperations(t *testing.T) {
	store, err := BuildObjectStore(Spec{Backend: GoogleDriveBackend, GoogleDrive: &GoogleDriveConnection{HTTPClient: &driveFake{}}})
	if err != nil {
		t.Fatalf("BuildObjectStore: %v", err)
	}
	_, err = store.PutJSON(context.Background(), PutJSONInput{ObjectKey: "x.json", Payload: []byte("{}")})
	if err == nil || !strings.Contains(err.Error(), "requires a folder id") {
		t.Fatalf("PutJSON error = %v", err)
	}
	_, err = store.ListObjects(context.Background(), ListQuery{Kind: "metadata"})
	if err == nil || !strings.Contains(err.Error(), "requires a folder id") {
		t.Fatalf("ListObjects error = %v", err)
	}
}

func TestBuildGoogleDriveRequiresConnection(t *testing.T) {
	_, err := BuildObjectStore(Spec{Backend: GoogleDriveBackend, Namespace: "folder"})
	if err == nil {
		t.Fatal("expected error for missing connection")
	}
}

func TestBuildGoogleDriveEmptyNamespaceStillRequiresConnection(t *testing.T) {
	_, err := BuildObjectStore(Spec{Backend: GoogleDriveBackend})
	if err == nil {
		t.Fatal("expected error for missing connection")
	}
}

func TestBuildUnknownBackend(t *testing.T) {
	_, err := BuildObjectStore(Spec{Backend: "bogus", Namespace: "x"})
	if err == nil {
		t.Fatal("expected error for unknown backend")
	}
}

func TestBuildS3ReturnsSeam(t *testing.T) {
	store, err := BuildObjectStore(Spec{
		Backend:   S3Backend,
		Namespace: "bucket/prefix",
		S3:        &S3Connection{Bucket: "my-bucket", Region: "us-east-1"},
	})
	if err != nil {
		t.Fatalf("BuildObjectStore s3: %v", err)
	}
	s3, ok := store.(*S3Store)
	if !ok {
		t.Fatalf("expected *S3Store, got %T", store)
	}
	if s3.Connection().Bucket != "my-bucket" {
		t.Errorf("bucket = %q", s3.Connection().Bucket)
	}
	if _, err := s3.HasBlob(context.Background(), "abc"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("want ErrNotImplemented, got %v", err)
	}
}

func TestBuildS3RequiresBucket(t *testing.T) {
	_, err := BuildObjectStore(Spec{Backend: S3Backend, Namespace: "b", S3: &S3Connection{}})
	if !errors.Is(err, ErrS3BucketRequired) {
		t.Fatalf("want ErrS3BucketRequired, got %v", err)
	}
}

func TestSupportedBackends(t *testing.T) {
	got := map[string]bool{}
	for _, b := range SupportedBackends {
		got[b] = true
	}
	if !got[GoogleDriveBackend] || !got[S3Backend] {
		t.Fatalf("unexpected supported backends: %v", SupportedBackends)
	}
}

func TestHTTPClientFromAuthorizedUserJSON(t *testing.T) {
	client, err := HTTPClientFromAuthorizedUserJSON(context.Background(),
		`{"refresh_token":"rt","client_id":"cid","client_secret":"secret","token_uri":"https://oauth2.googleapis.com/token","token":"at"}`)
	if err != nil {
		t.Fatalf("HTTPClientFromAuthorizedUserJSON: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}

	if _, err := HTTPClientFromAuthorizedUserJSON(context.Background(), `{"client_id":"cid","client_secret":"s"}`); err == nil {
		t.Fatal("expected error when refresh_token missing")
	}
	if _, err := HTTPClientFromAuthorizedUserJSON(context.Background(), `not json`); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
