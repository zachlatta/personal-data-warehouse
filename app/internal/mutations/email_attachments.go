package mutations

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
)

// Inline bytes are snapshotted with the proposal: approval never fetches a mutable
// URL or a worker-local path. Keep below Gmail's MIME message size limit.
const maxEmailAttachmentBytes = 20 << 20
const maxEmailAttachments = 100

var attachmentContentType = regexp.MustCompile(`^[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+$`)

func validateEmailAttachments(value any) error {
	if value == nil {
		return nil
	}
	var items []any
	switch v := value.(type) {
	case []any:
		items = v
	case []map[string]any:
		for _, item := range v {
			items = append(items, item)
		}
	default:
		return fmt.Errorf("attachments must be an array")
	}
	if len(items) > maxEmailAttachments {
		return fmt.Errorf("attachments must contain at most %d files", maxEmailAttachments)
	}
	total := 0
	for i, item := range items {
		a, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("attachment %d must be an object", i+1)
		}
		for key := range a {
			if key != "filename" && key != "content_type" && key != "data_base64" {
				return fmt.Errorf("attachment %d has unknown field %q", i+1, key)
			}
		}
		name, ok := a["filename"].(string)
		if !ok || strings.TrimSpace(name) == "" || len(name) > 255 || name == "." || name == ".." || strings.ContainsAny(name, "/\\") || strings.ContainsFunc(name, func(r rune) bool { return r < 32 || r == 127 }) {
			return fmt.Errorf("attachment %d needs a filename without paths or control characters (max 255 UTF-8 bytes)", i+1)
		}
		ct, ok := a["content_type"].(string)
		if !ok || !attachmentContentType.MatchString(ct) || strings.HasPrefix(strings.ToLower(ct), "multipart/") {
			return fmt.Errorf("attachment %d needs a non-multipart content_type such as application/pdf (no parameters)", i+1)
		}
		encoded, ok := a["data_base64"].(string)
		if !ok || len(encoded) > base64.StdEncoding.EncodedLen(maxEmailAttachmentBytes-total) || strings.ContainsAny(encoded, "\r\n") {
			return fmt.Errorf("attachment %d needs standard base64 data; attachments may total at most 20 MiB", i+1)
		}
		data, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil {
			return fmt.Errorf("attachment %d has invalid standard base64 data", i+1)
		}
		total += len(data)
		if total > maxEmailAttachmentBytes {
			return fmt.Errorf("attachments may total at most 20 MiB")
		}
	}
	return nil
}
