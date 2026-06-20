package objectstore

import (
	"errors"
	"fmt"
)

// Backend names. Mirrors the Python SUPPORTED_BACKENDS.
const (
	GoogleDriveBackend = "google_drive"
	S3Backend          = "s3"
)

// SupportedBackends lists the backends the factory knows about.
var SupportedBackends = []string{GoogleDriveBackend, S3Backend}

// ErrS3BucketRequired is returned when an S3 spec omits the bucket.
var ErrS3BucketRequired = errors.New("objectstore: S3Connection.Bucket is required")

// GoogleDriveConnection carries the Drive connection for a Spec. HTTPClient must
// be authenticated for the Drive scope (see HTTPClientFromAuthorizedUserJSON);
// it is provided by the caller so this package never builds Drive credentials
// implicitly. APIBaseURL/UploadBaseURL default to Google and exist for tests.
type GoogleDriveConnection struct {
	HTTPClient    httpDoer
	APIBaseURL    string
	UploadBaseURL string
}

// Spec is the backend-neutral construction description handed to
// BuildObjectStore. It mirrors the Python ObjectStoreSpec.
type Spec struct {
	Backend       string
	Namespace     string // Drive folder id for managed stores, or S3 bucket/prefix root
	Source        string
	BlobKind      string
	MetadataKind  string
	LegacySources []string
	GoogleDrive   *GoogleDriveConnection
	S3            *S3Connection
}

// GoogleDriveSpec builds a Drive Spec. The httpClient must be authenticated for
// the Drive scope.
func GoogleDriveSpec(folderID, source, blobKind, metadataKind string, legacySources []string, conn GoogleDriveConnection) Spec {
	return Spec{
		Backend:       GoogleDriveBackend,
		Namespace:     folderID,
		Source:        source,
		BlobKind:      blobKind,
		MetadataKind:  metadataKind,
		LegacySources: legacySources,
		GoogleDrive:   &conn,
	}
}

// BuildObjectStore constructs the ObjectStore described by spec. Adding a
// backend means adding a case here; no call site changes.
func BuildObjectStore(spec Spec) (ObjectStore, error) {
	switch spec.Backend {
	case GoogleDriveBackend:
		if spec.GoogleDrive == nil {
			return nil, fmt.Errorf("objectstore: google_drive backend requires spec.GoogleDrive connection")
		}
		return NewGoogleDriveStore(GoogleDriveOptions{
			FolderID:      spec.Namespace,
			HTTPClient:    spec.GoogleDrive.HTTPClient,
			Source:        spec.Source,
			LegacySources: spec.LegacySources,
			AudioKind:     spec.BlobKind,
			MetadataKind:  spec.MetadataKind,
			APIBaseURL:    spec.GoogleDrive.APIBaseURL,
			UploadBaseURL: spec.GoogleDrive.UploadBaseURL,
		}), nil
	case S3Backend:
		if spec.S3 == nil {
			return nil, fmt.Errorf("objectstore: s3 backend requires spec.S3 connection")
		}
		return NewS3Store(*spec.S3)
	default:
		return nil, fmt.Errorf("objectstore: unsupported backend %q (supported: %v)", spec.Backend, SupportedBackends)
	}
}
