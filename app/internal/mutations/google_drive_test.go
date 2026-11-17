package mutations

import (
	"context"
	"encoding/base64"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	"io"
	"strings"
	"testing"
)

func TestDriveUploadSnapshotsMetadata(t *testing.T) {
	p, err := drivePayload(MutationInput{Type: "google_drive.upload_file", Raw: map[string]any{"name": "report.txt", "parent_id": "folder-1", "mime_type": "text/plain", "data_base64": base64.StdEncoding.EncodeToString([]byte("hello"))}})
	if err != nil {
		t.Fatal(err)
	}
	if p["size_bytes"] != 5 || len(p["sha256"].(string)) != 64 {
		t.Fatalf("%#v", p)
	}
}

type fakeDriveStaging struct {
	enabled bool
	bytes   []byte
	key     string
}

func (f *fakeDriveStaging) GetBucketLifecycle(context.Context, string) (*lifecycle.Configuration, error) {
	status := "Disabled"
	if f.enabled {
		status = "Enabled"
	}
	return &lifecycle.Configuration{Rules: []lifecycle.Rule{{Status: status, RuleFilter: lifecycle.Filter{Prefix: "pending/"}, Expiration: lifecycle.Expiration{Days: 7}}}}, nil
}
func (f *fakeDriveStaging) PutObject(_ context.Context, bucket, key string, r io.Reader, n int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	f.key = key
	f.bytes, _ = io.ReadAll(r)
	return minio.UploadInfo{}, nil
}
func TestDriveStagingRequiresExpiryAndRemovesInlineBytes(t *testing.T) {
	p, err := drivePayload(MutationInput{Type: "google_drive.upload_file", Raw: map[string]any{"name": "report.txt", "parent_id": "folder", "mime_type": "text/plain", "data_base64": "aGVsbG8="}})
	if err != nil {
		t.Fatal(err)
	}
	m := storedMutation{Payload: p}
	client := &fakeDriveStaging{}
	if err := stageDriveUpload(context.Background(), &m, client, "bucket"); err == nil || client.key != "" {
		t.Fatal("uploaded without expiration")
	}
	client.enabled = true
	if err := stageDriveUpload(context.Background(), &m, client, "bucket"); err != nil {
		t.Fatal(err)
	}
	if string(client.bytes) != "hello" || p["data_base64"] != nil || p["staging_key"] != client.key || !strings.HasPrefix(client.key, "pending/sha256/"+p["sha256"].(string)+"/") {
		t.Fatalf("%#v", p)
	}
}

func TestDriveHelpExamplesValidate(t *testing.T) {
	for _, entry := range driveMutationHelp() {
		m, err := mutationInputFromMap(entry.Example, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := drivePayload(m); err != nil {
			t.Fatalf("%s: %v", entry.Type, err)
		}
	}
}

func TestDriveRejectsUnsafeOrAmbiguousFields(t *testing.T) {
	for _, raw := range []map[string]any{
		{"file_id": "f", "permission_type": "anyone", "role": "owner", "send_notification_email": false},
		{"file_id": "f", "permission_type": "user", "role": "reader", "email_address": "person@example.com"},
		{"file_id": "f", "permission_type": "anyone", "role": "reader", "send_notification_email": "false"},
		{"file_id": "f", "permission_type": "anyone", "role": "reader", "send_notification_email": false, "typo": true},
	} {
		if _, err := drivePayload(MutationInput{Type: "google_drive.add_permission", Raw: raw}); err == nil {
			t.Fatalf("accepted %#v", raw)
		}
	}
}
