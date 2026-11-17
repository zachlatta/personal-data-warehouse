package mutations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/mail"
	"net/netip"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

const driveOperations = "google_drive.upload_file,google_drive.create_folder,google_drive.add_permission,google_drive.update_permission,google_drive.remove_permission"

// Never renew a request that contains a successful write or another operation:
// blindly repeating a partially applied batch could create duplicate folders.
func renewableDriveUpload(request Request) bool {
	if len(request.Mutations) == 0 {
		return false
	}
	expired := request.Status == "pending_review" && time.Since(request.CreatedAt) > 7*24*time.Hour
	if request.Status == "pending_review" && !expired {
		return false
	}
	for _, m := range request.Mutations {
		if m.Operation != "google_drive.upload_file" {
			return false
		}
		switch m.Status {
		case "failed_terminal", "rejected", "withdrawn":
		case "pending_review":
			if !expired {
				return false
			}
		default:
			return false
		}
	}
	return true
}

var driveID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)
var driveMIME = regexp.MustCompile(`^[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+$`)

func drivePayload(m MutationInput) (map[string]any, error) {
	fields := ""
	switch m.Type {
	case "google_drive.upload_file":
		fields = "name,parent_id,mime_type,data_base64"
	case "google_drive.create_folder":
		fields = "name,parent_id,initial_permissions"
	case "google_drive.add_permission":
		fields = "file_id,permission_type,email_address,domain,role,send_notification_email"
	case "google_drive.update_permission":
		fields = "file_id,permission_id,permission_type,email_address,domain,expected_role,role"
	case "google_drive.remove_permission":
		fields = "file_id,permission_id,permission_type,email_address,domain,expected_role"
	default:
		return nil, errors.New("unsupported Drive operation")
	}
	allowed := map[string]bool{}
	for _, k := range strings.Split("type,operation,account,title,reason,"+fields, ",") {
		allowed[k] = true
	}
	p := map[string]any{}
	for k, v := range m.Raw {
		if !allowed[k] {
			return nil, fmt.Errorf("unknown Drive field %q", k)
		}
		if !strings.Contains(",type,operation,account,title,reason,", ","+k+",") {
			p[k] = v
		}
	}
	required := func(k string) (string, error) {
		v, ok := p[k].(string)
		if !ok || strings.TrimSpace(v) == "" {
			return "", fmt.Errorf("%s must be a nonempty string", k)
		}
		return v, nil
	}
	for _, k := range []string{"file_id", "permission_id", "parent_id"} {
		if strings.Contains(","+fields+",", ","+k+",") {
			v, e := required(k)
			if e != nil {
				return nil, e
			}
			if !driveID.MatchString(v) {
				return nil, fmt.Errorf("invalid %s", k)
			}
		}
	}
	if strings.Contains(fields, "name") {
		name, e := required("name")
		if e != nil {
			return nil, e
		}
		if len(name) > 255 || strings.ContainsAny(name, "/\\") || strings.ContainsFunc(name, func(r rune) bool { return r < 32 || r == 127 }) {
			return nil, errors.New("invalid name (max 255 bytes, no paths or controls)")
		}
	}
	if m.Type == "google_drive.upload_file" {
		mime, e := required("mime_type")
		if e != nil {
			return nil, e
		}
		if !driveMIME.MatchString(mime) || strings.HasPrefix(mime, "application/vnd.google-apps.") {
			return nil, errors.New("mime_type must be a binary MIME type, not a Google native format")
		}
		encoded, ok := p["data_base64"].(string)
		if !ok {
			return nil, errors.New("data_base64 must be a string")
		}
		if len(encoded) > base64.StdEncoding.EncodedLen(20<<20) {
			return nil, errors.New("upload exceeds 20 MiB")
		}
		data, e := base64.StdEncoding.Strict().DecodeString(encoded)
		if e != nil || len(data) > 20<<20 {
			return nil, errors.New("invalid data_base64")
		}
		sum := sha256.Sum256(data)
		p["sha256"] = hex.EncodeToString(sum[:])
		p["size_bytes"] = len(data)
	}
	if strings.Contains(fields, "permission_type") {
		kind, _ := p["permission_type"].(string)
		switch kind {
		case "user", "group":
			email, e := required("email_address")
			if e != nil {
				return nil, e
			}
			addr, e := mail.ParseAddress(email)
			if e != nil || addr.Address != email {
				return nil, errors.New("invalid email_address")
			}
			if p["domain"] != nil {
				return nil, errors.New("domain only applies to domain permissions")
			}
		case "domain":
			domain, e := required("domain")
			if e != nil {
				return nil, e
			}
			if strings.ContainsAny(domain, "/@ \r\n") || !strings.Contains(domain, ".") {
				return nil, errors.New("invalid domain")
			}
			if p["email_address"] != nil {
				return nil, errors.New("email_address only applies to user/group permissions")
			}
		case "anyone":
			if p["email_address"] != nil || p["domain"] != nil {
				return nil, errors.New("anyone permission has no recipient")
			}
		default:
			return nil, errors.New("permission_type must be user, group, domain or anyone")
		}
		for _, k := range []string{"role", "expected_role"} {
			if !strings.Contains(","+fields+",", ","+k+",") {
				continue
			}
			v, _ := p[k].(string)
			if v != "reader" && v != "commenter" && v != "writer" {
				return nil, fmt.Errorf("%s must be reader, commenter or writer; ownership changes are unsupported", k)
			}
		}
		if m.Type == "google_drive.add_permission" {
			notify, ok := p["send_notification_email"].(bool)
			if !ok {
				return nil, errors.New("send_notification_email must explicitly be true or false")
			}
			if notify && (kind == "anyone" || kind == "domain") {
				return nil, errors.New("notification email only applies to user/group")
			}
		}
	}
	if value, present := p["initial_permissions"]; present {
		permissions, ok := value.([]any)
		if !ok || len(permissions) > 100 {
			return nil, errors.New("initial_permissions must be an array with at most 100 permissions")
		}
		validated := make([]any, 0, len(permissions))
		for _, value := range permissions {
			permission, ok := value.(map[string]any)
			if !ok {
				return nil, errors.New("each initial permission must be an object")
			}
			raw := map[string]any{"file_id": "new-folder"}
			for k, v := range permission {
				if k != "permission_type" && k != "email_address" && k != "domain" && k != "role" && k != "send_notification_email" {
					return nil, fmt.Errorf("unknown initial permission field %q", k)
				}
				raw[k] = v
			}
			item, err := drivePayload(MutationInput{Type: "google_drive.add_permission", Raw: raw})
			if err != nil {
				return nil, fmt.Errorf("initial_permissions: %w", err)
			}
			delete(item, "file_id")
			validated = append(validated, item)
		}
		p["initial_permissions"] = validated
	}
	return p, nil
}

// Plain HTTP is permitted only over the fleet's Tailscale address space.
// Credentials, paths and query strings in endpoint URLs are never accepted.
func validMutationStagingEndpoint(u *url.URL) bool {
	if u == nil || u.Host == "" || u.User != nil || u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	if u.Scheme != "http" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if addr, err := netip.ParseAddr(host); err == nil {
		return netip.MustParsePrefix("100.64.0.0/10").Contains(addr)
	}
	return strings.HasSuffix(host, ".ts.net") && len(strings.Split(host, ".")) >= 4
}

// stageDriveUploads runs after the duplicate-request check. Only the private
// object reference enters Postgres, the timeline, or a review response.
func stageDriveUploads(ctx context.Context, mutations []storedMutation) error {
	for i := range mutations {
		m := &mutations[i]
		if m.Operation != "google_drive.upload_file" {
			continue
		}
		endpoint, err := url.Parse(os.Getenv("PDW_MUTATION_S3_ENDPOINT_URL"))
		bucket := os.Getenv("PDW_MUTATION_S3_BUCKET")
		if err != nil || !validMutationStagingEndpoint(endpoint) || bucket == "" || os.Getenv("PDW_MUTATION_S3_ACCESS_KEY") == "" || os.Getenv("PDW_MUTATION_S3_SECRET_KEY") == "" || os.Getenv("PDW_MUTATION_S3_REGION") == "" {
			return errors.New("uploads require PDW_MUTATION_S3_ENDPOINT_URL (HTTPS or tailnet HTTP), BUCKET, ACCESS_KEY, SECRET_KEY and REGION")
		}
		client, err := minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("PDW_MUTATION_S3_ACCESS_KEY"), os.Getenv("PDW_MUTATION_S3_SECRET_KEY"), ""), Secure: endpoint.Scheme == "https", Region: os.Getenv("PDW_MUTATION_S3_REGION"), BucketLookup: minio.BucketLookupPath})
		if err != nil {
			return errors.New("could not configure mutation staging")
		}
		if err := stageDriveUpload(ctx, m, client, bucket); err != nil {
			return err
		}
	}
	return nil
}

type driveStagingClient interface {
	GetBucketLifecycle(context.Context, string) (*lifecycle.Configuration, error)
	PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error)
}

func stageDriveUpload(ctx context.Context, m *storedMutation, client driveStagingClient, bucket string) error {
	policy, err := client.GetBucketLifecycle(ctx, bucket)
	if err != nil {
		return errors.New("mutation staging requires a readable lifecycle policy expiring pending/ after 7 days")
	}
	expires := false
	for _, rule := range policy.Rules {
		if rule.Status == "Enabled" && rule.RuleFilter.Prefix == "pending/" && rule.Expiration.Days == 7 && rule.RuleFilter.Tag.Key == "" && rule.RuleFilter.And.Prefix == "" && len(rule.RuleFilter.And.Tags) == 0 && rule.RuleFilter.ObjectSizeGreaterThan == 0 && rule.RuleFilter.ObjectSizeLessThan == 0 {
			expires = true
		}
	}
	if !expires {
		return errors.New("mutation staging requires an enabled 7-day expiration rule for pending/")
	}
	id, err := randomID("upload")
	if err != nil {
		return err
	}
	key := "pending/sha256/" + m.Payload["sha256"].(string) + "/" + id
	data, _ := base64.StdEncoding.Strict().DecodeString(m.Payload["data_base64"].(string))
	if _, err = client.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream"}); err != nil {
		return errors.New("mutation upload staging failed")
	}
	delete(m.Payload, "data_base64")
	m.Payload["staging_key"] = key
	return nil
}

// Resolve file and folder names from the account's synced source; never trust
// agent-supplied display names for a destination or sharing target.
func (s *PostgresStore) enrichDrivePreviews(ctx context.Context, mutations []storedMutation) error {
	for i := range mutations {
		m := &mutations[i]
		if m.Provider != "google_drive" {
			continue
		}
		preview := map[string]any{}
		for k, v := range m.Payload {
			if k != "data_base64" {
				preview[k] = v
			}
		}
		for _, key := range []string{"parent_id", "file_id"} {
			id, ok := m.Payload[key].(string)
			if !ok {
				continue
			}
			var name, mime string
			if err := queryRowContext(ctx, s.db, `SELECT name, mime_type FROM @google_drive_files WHERE account=$1 AND file_id=$2 AND trashed=0`, m.Account, id).Scan(&name, &mime); err != nil {
				return invalidProposalInput(fmt.Errorf("Drive %s is not in the account's synced files", key))
			}
			if key == "parent_id" && mime != "application/vnd.google-apps.folder" {
				return invalidProposalInput(errors.New("parent_id is not a folder"))
			}
			preview[key+"_name"] = name
		}
		preview["effect"] = "Runs after approval. Folder sharing also affects its children. Public or domain sharing can expose content beyond named recipients."
		m.Preview = map[string]any{"google_drive": preview}
	}
	return nil
}

func driveMutationHelp() []MutationHelpType {
	var entries []MutationHelpType
	for _, op := range strings.Split(driveOperations, ",") {
		example := map[string]any{"type": op, "account": "you@example.com"}
		fields := []MutationHelpArg{{Name: "type", JSONType: "string", Required: true, Description: "literal " + op}, {Name: "account", JSONType: "string", Required: true, Description: "synced Drive account"}}
		add := func(k, kind, description string, required bool, value any) {
			fields = append(fields, MutationHelpArg{Name: k, JSONType: kind, Description: description, Required: required})
			if value != nil {
				example[k] = value
			}
		}
		switch op {
		case "google_drive.upload_file", "google_drive.create_folder":
			add("name", "string", "File or folder name, max 255 UTF-8 bytes", true, "Report")
			add("parent_id", "string", "Destination folder id from base_google_drive.files (must already exist)", true, "folder-1")
			if op == "google_drive.create_folder" {
				add("initial_permissions", "array<object>", "Optional sharing applied to the new folder in this same approval (max 100). Each object takes permission_type, email_address or domain when applicable, role, and explicit send_notification_email; same rules as add_permission.", false, []any{map[string]any{"permission_type": "user", "email_address": "person@example.com", "role": "reader", "send_notification_email": false}})
			}
			if op == "google_drive.upload_file" {
				add("mime_type", "string", "Binary MIME type; no native Google conversion", true, "text/plain")
				add("data_base64", "string", "Standard base64, at most 20 MiB decoded; use stdin JSON for large payloads", true, "aGVsbG8=")
			}
		default:
			add("file_id", "string", "File or folder id from base_google_drive.files", true, "file-1")
			add("permission_type", "string", "user, group, domain or anyone", true, "user")
			add("email_address", "string", "Required for user/group only", false, "person@example.com")
			add("domain", "string", "Required for domain only; omit for other types", false, nil)
			if op != "google_drive.add_permission" {
				add("permission_id", "string", "Existing Drive permission id", true, "permission-1")
				add("expected_role", "string", "Current reader, commenter or writer role; worker refuses a changed permission", true, "reader")
			}
			if op != "google_drive.remove_permission" {
				add("role", "string", "reader, commenter or writer; ownership changes unsupported", true, "writer")
			}
			if op == "google_drive.add_permission" {
				add("send_notification_email", "boolean", "Explicit notification choice; false for domain/anyone", true, false)
			}
		}
		entries = append(entries, MutationHelpType{Type: op, Summary: strings.ReplaceAll(strings.TrimPrefix(op, "google_drive."), "_", " ") + " in Google Drive after human review", RequiresEnv: "GOOGLE_DRIVE_SOURCE_ACCOUNTS", Fields: fields, Example: example, ExtraNotes: "Sharing folders affects children. No ownership transfer or inherited-permission changes. Uploads are staged privately in S3 and expire after 7 days, including rejected/withdrawn proposals; re-propose an expired upload. No automatic retry after an uncertain write: inspect Drive first. New folders cannot be referenced by another mutation until synced. Status is in ops.upstream_mutation_operations; normal Drive sync brings resulting files to the timeline."})
	}
	return entries
}
