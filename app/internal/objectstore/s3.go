package objectstore

import "context"

// S3Connection holds connection knobs for a future S3-compatible backend. These
// map onto an S3 SDK client so the backend can target AWS S3 or any
// S3-compatible service (R2, MinIO, Backblaze B2, ...). Mirrors the Python
// S3Connection.
type S3Connection struct {
	Bucket          string
	Region          string
	EndpointURL     string
	AccessKey       string
	SecretKey       string
	AddressingStyle string // "virtual" or "path"
	KeyPrefix       string
}

// S3Store is the not-yet-implemented S3-compatible backend. It holds validated
// connection config and satisfies ObjectStore, but every operation returns
// ErrNotImplemented until implemented. This keeps the factory/registry honest
// and selectable in config without silently misbehaving.
//
// Implementation notes (mirroring the Python s3.py contract): content-addressed
// dedup has no server-side metadata search on S3, so implement HasBlob/Presence
// via deterministic content-addressed keys + HeadObject; PutFile/PutJSON set
// ContentType and store the pdw_* tags as object metadata; GetShareURL returns a
// presigned URL and must not make objects public.
type S3Store struct {
	conn S3Connection
}

// NewS3Store validates the connection and returns the seam implementation.
func NewS3Store(conn S3Connection) (*S3Store, error) {
	if conn.Bucket == "" {
		return nil, ErrS3BucketRequired
	}
	return &S3Store{conn: conn}, nil
}

// Connection exposes the validated config (used by tests and future impl).
func (s *S3Store) Connection() S3Connection { return s.conn }

func (s *S3Store) Backend() string { return "s3" }

func (s *S3Store) HasBlob(context.Context, string) (bool, error)     { return false, ErrNotImplemented }
func (s *S3Store) HasMetadata(context.Context, string) (bool, error) { return false, ErrNotImplemented }
func (s *S3Store) HasObject(context.Context, string, string, string) (bool, error) {
	return false, ErrNotImplemented
}
func (s *S3Store) Presence(context.Context, string) (ObjectPresence, error) {
	return ObjectPresence{}, ErrNotImplemented
}
func (s *S3Store) PutFile(context.Context, PutFileInput) (StoredObject, error) {
	return StoredObject{}, ErrNotImplemented
}
func (s *S3Store) PutJSON(context.Context, PutJSONInput) (StoredObject, error) {
	return StoredObject{}, ErrNotImplemented
}
func (s *S3Store) GetObject(context.Context, StoredObject) ([]byte, error) {
	return nil, ErrNotImplemented
}
func (s *S3Store) ObjectExists(context.Context, StoredObject) (bool, error) {
	return false, ErrNotImplemented
}
func (s *S3Store) DeleteObject(context.Context, StoredObject) error { return ErrNotImplemented }
func (s *S3Store) GetMetadata(context.Context, StoredObject) (ObjectMetadata, error) {
	return ObjectMetadata{}, ErrNotImplemented
}
func (s *S3Store) GetShareURL(context.Context, StoredObject) (string, error) {
	return "", ErrNotImplemented
}
func (s *S3Store) ListObjects(context.Context, ListQuery) ([]ObjectListing, error) {
	return nil, ErrNotImplemented
}
func (s *S3Store) FindObject(context.Context, ListQuery) (*ObjectListing, error) {
	return nil, ErrNotImplemented
}
func (s *S3Store) MoveObject(context.Context, StoredObject, string, map[string]string) (StoredObject, error) {
	return StoredObject{}, ErrNotImplemented
}
