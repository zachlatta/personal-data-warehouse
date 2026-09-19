package photos

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// SchemaVersion is the photo metadata envelope's schema version.
const SchemaVersion = 1

// PhotoRoles are the only roles the identity layer understands. `original`
// is the shot as captured; `edited` is a source-rendered adjustment of the
// same photo; `live_video` is a Live Photo's motion component (uploaded under
// the SAME native id as its still, which is what attaches it to the still's
// asset).
var PhotoRoles = []string{"original", "edited", "live_video"}

// ProvenanceDedupSHA256 is the stable dedup key for one (source, account,
// native_id, role, file) claim.
//
// Deliberately NOT the file sha: two sources holding identical bytes are two
// provenance claims and both envelopes must survive in the inbox, while a
// re-run of one source (same claim, envelope differing only in uploaded_at)
// must dedup away. Mirrors the Go handler's expectations in
// app/internal/server/ingest.go.
func ProvenanceDedupSHA256(source, account, nativeID, role, fileContentSHA256 string) string {
	seed := strings.Join([]string{source, account, nativeID, role, fileContentSHA256}, "|")
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}

// Envelope carries the inputs to BuildPhotoMetadata.
type Envelope struct {
	Source          string
	Account         string
	NativeID        string
	Role            string
	Filename        string
	MimeType        string
	SizeBytes       int64
	ContentSHA256   string
	UploadedAt      string
	Width           int64
	Height          int64
	CapturedAt      string
	CaptureTZOffset string
	CameraMake      string
	CameraModel     string
	FileModifiedAt  string
	RecordKey       string
	Record          map[string]any // nil means no record block
}

// BuildPhotoMetadata builds the photo metadata envelope, failing fast on
// contract violations exactly as the Python build_photo_metadata did.
func BuildPhotoMetadata(e Envelope) (map[string]any, error) {
	if e.Source == "" {
		return nil, errors.New("source is required")
	}
	if e.Account == "" {
		return nil, errors.New("account is required")
	}
	if e.NativeID == "" {
		return nil, errors.New("native_id is required")
	}
	if !isPhotoRole(e.Role) {
		return nil, fmt.Errorf("role must be one of ('original', 'edited', 'live_video'), got '%s'", e.Role)
	}
	if e.ContentSHA256 == "" {
		return nil, errors.New("content_sha256 is required")
	}
	if e.Record != nil && e.RecordKey == "" {
		return nil, errors.New("record_key is required when a record is provided")
	}
	if e.RecordKey != "" && !strings.HasSuffix(e.RecordKey, "_record") && !strings.HasSuffix(e.RecordKey, "_sidecar") && !strings.HasSuffix(e.RecordKey, "_exif") {
		return nil, fmt.Errorf("record_key must be source-named (e.g. 'apple_record', 'takeout_sidecar'), got '%s'", e.RecordKey)
	}
	envelope := map[string]any{
		"schema_version": int64(SchemaVersion),
		"source":         e.Source,
		"account":        e.Account,
		"uploaded_at":    e.UploadedAt,
		"file": map[string]any{
			"native_id":         e.NativeID,
			"role":              e.Role,
			"filename":          e.Filename,
			"mime_type":         e.MimeType,
			"size_bytes":        e.SizeBytes,
			"content_sha256":    e.ContentSHA256,
			"width":             e.Width,
			"height":            e.Height,
			"captured_at":       e.CapturedAt,
			"capture_tz_offset": e.CaptureTZOffset,
			"camera_make":       e.CameraMake,
			"camera_model":      e.CameraModel,
			"file_modified_at":  e.FileModifiedAt,
		},
	}
	if e.Record != nil {
		record := make(map[string]any, len(e.Record))
		for key, value := range e.Record {
			record[key] = value
		}
		envelope[e.RecordKey] = record
	}
	return envelope, nil
}

func isPhotoRole(role string) bool {
	for _, known := range PhotoRoles {
		if role == known {
			return true
		}
	}
	return false
}
