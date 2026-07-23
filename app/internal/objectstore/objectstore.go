// Package objectstore is the Go app's object storage layer: a backend-neutral
// interface for binary/blob objects with a Google Drive backend (default) and a
// seam for an S3-compatible backend. It mirrors the Python
// personal_data_warehouse.objectstore package so both runtimes read and write
// the same store directly, using the same backend-neutral StoredObject
// reference, object-key conventions, and pdw_* tags. No code outside this
// package is aware of Google Drive.
package objectstore

import (
	"context"
	"errors"
	"sort"
	"strings"
)

// ErrNotImplemented is returned by backends (or operations) that are not yet
// implemented, e.g. the S3-compatible seam.
var ErrNotImplemented = errors.New("objectstore: operation not implemented")

// ErrNotFound is returned when an object referenced by a StoredObject is missing.
var ErrNotFound = errors.New("objectstore: object not found")

// StoredObject is the durable, backend-neutral reference persisted in the
// warehouse. Its JSON shape matches the Python StoredObject and the warehouse
// storage_* columns so references written by either runtime interoperate.
type StoredObject struct {
	StorageBackend string `json:"storage_backend"`
	StorageKey     string `json:"storage_key"`
	StorageFileID  string `json:"storage_file_id"`
	StorageURL     string `json:"storage_url"`
}

// ObjectPresence reports a content-addressed dedup probe. The field names mirror
// the Python ObjectPresence: "audio" is the primary blob, "metadata" the JSON
// sidecar, for any object kind.
type ObjectPresence struct {
	AudioExists    bool
	MetadataExists bool
}

// ObjectMetadata is backend-neutral metadata for a stored object.
type ObjectMetadata struct {
	Backend       string
	StorageKey    string
	StorageFileID string
	ContentType   string
	SizeBytes     int64
	ContentSHA256 string
	Filename      string
	CreatedTime   string
	ModifiedTime  string
	StorageURL    string
}

// ObjectListing is one object returned by a list/find query. AppProperties are
// the pdw_* tags we attach to every object (our own metadata, not a
// Drive-specific concept).
type ObjectListing struct {
	Ref           StoredObject
	AppProperties map[string]string
	Filename      string
}

// ListQuery selects objects by kind and optional stage/property filters. An
// empty Stage means "any stage".
type ListQuery struct {
	Kind       string
	Stage      string
	Properties map[string]string
}

// PutFileInput uploads bytes as a blob object.
type PutFileInput struct {
	ObjectKey         string
	Content           []byte
	ContentSHA256     string
	ContentType       string
	SkipExistingCheck bool
	AppProperties     map[string]string
	Kind              string
}

// ResumableFileUploadInput asks a capable backend to create a scoped upload
// session without proxying the file bytes through the app.
type ResumableFileUploadInput struct {
	ObjectKey     string
	ContentSHA256 string
	ContentType   string
	SizeBytes     int64
	AppProperties map[string]string
	Kind          string
}

// ResumableFileUpload is either a content-addressed dedup hit or a fresh,
// backend-scoped upload URL. UploadURL is a write capability and must never be
// logged or persisted.
type ResumableFileUpload struct {
	UploadURL string
	Existing  *StoredObject
}

// ResumableFileStore is an optional capability implemented by backends that
// can let an authenticated client stream large files directly into a narrowly
// scoped upload session.
type ResumableFileStore interface {
	BeginResumableFileUpload(ctx context.Context, in ResumableFileUploadInput) (ResumableFileUpload, error)
}

// PutJSONInput uploads a JSON sidecar object.
type PutJSONInput struct {
	ObjectKey           string
	Payload             []byte
	ContentSHA256       string
	SourceContentSHA256 string
	SkipExistingCheck   bool
	AppProperties       map[string]string
	Kind                string
}

// ObjectStore is the backend-neutral interface. It is the Go counterpart of the
// Python ObjectStore protocol; operations and semantics match so both runtimes
// interoperate on the same store.
type ObjectStore interface {
	Backend() string

	// content-addressed dedup probes
	HasBlob(ctx context.Context, contentSHA256 string) (bool, error)
	HasMetadata(ctx context.Context, contentSHA256 string) (bool, error)
	HasObject(ctx context.Context, kind, key, value string) (bool, error)
	Presence(ctx context.Context, contentSHA256 string) (ObjectPresence, error)

	// writes
	PutFile(ctx context.Context, in PutFileInput) (StoredObject, error)
	PutJSON(ctx context.Context, in PutJSONInput) (StoredObject, error)

	// reads / management
	GetObject(ctx context.Context, ref StoredObject) ([]byte, error)
	ObjectExists(ctx context.Context, ref StoredObject) (bool, error)
	DeleteObject(ctx context.Context, ref StoredObject) error
	GetMetadata(ctx context.Context, ref StoredObject) (ObjectMetadata, error)
	GetShareURL(ctx context.Context, ref StoredObject) (string, error)

	// discovery / relocation
	ListObjects(ctx context.Context, q ListQuery) ([]ObjectListing, error)
	FindObject(ctx context.Context, q ListQuery) (*ObjectListing, error)
	MoveObject(ctx context.Context, ref StoredObject, newObjectKey string, appProperties map[string]string) (StoredObject, error)
}

// escapeQueryValue mirrors the Python escape_query_value: backslash then quote.
func escapeQueryValue(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	return strings.ReplaceAll(value, "'", "\\'")
}

// driveNameFromObjectKey returns the final path segment of an object key.
func driveNameFromObjectKey(objectKey string) string {
	if idx := strings.LastIndex(objectKey, "/"); idx >= 0 {
		return objectKey[idx+1:]
	}
	return objectKey
}

// objectStage mirrors the Python object_stage: the second path segment if it is
// "inbox" or "library", else "".
func objectStage(objectKey string) string {
	parts := splitNonEmpty(objectKey, "/")
	if len(parts) > 1 && (parts[1] == "inbox" || parts[1] == "library") {
		return parts[1]
	}
	return ""
}

// sortedKeys returns the map keys in deterministic order so generated Drive
// queries are stable (important for caching and tests).
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func splitNonEmpty(value, sep string) []string {
	out := make([]string, 0)
	for _, part := range strings.Split(value, sep) {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
