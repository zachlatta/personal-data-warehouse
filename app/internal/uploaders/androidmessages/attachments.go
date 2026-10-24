package androidmessages

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// A text's files (photos, videos, voice messages, contact cards, stickers)
// are not in bugle_db: each non-text part names where Messages keeps the
// bytes. An RCS file is a file:// path in Messages' own storage; an MMS
// part is content://mms/part/N in Android's telephony provider, often with
// a copy in Messages' cache (local_cache_path). The uploader reads the
// bytes from there and sends each file once, through a resumable upload;
// the part in the message record then carries the stored object's pointer.

// DefaultAttachmentBytesPerRun bounds one run's attachment upload so a run
// stays well inside its deadline on cellular; the rest go on the next run.
const DefaultAttachmentBytesPerRun = 300 * 1024 * 1024

// fileLocation is where Messages keeps one part's bytes.
type fileLocation struct {
	uri       string
	cachePath string
}

// storedFile is the ledger entry of a part whose file was sent.
type storedFile struct {
	ContentSHA256  string `json:"content_sha256"`
	SizeBytes      int64  `json:"size_bytes"`
	StorageBackend string `json:"storage_backend"`
	StorageKey     string `json:"storage_key"`
	StorageFileID  string `json:"storage_file_id"`
	StorageURL     string `json:"storage_url,omitempty"`
}

// AttachmentUploader is what the runner needs to send a file.
type AttachmentUploader interface {
	UploadAndroidMessagesAttachment(path, messageAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error)
}

// readContentURI copies a content:// URI's bytes to a file. Android's
// `content read` is the provider's own reader, so it needs no knowledge of
// where the telephony provider keeps its parts. Replaced in tests.
var readContentURI = func(uri, dest string) error {
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd := exec.Command("content", "read", "--uri", uri)
	cmd.Stdout = out
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	closeErr := out.Close()
	if runErr != nil {
		return fmt.Errorf("content read %s: %w: %s", uri, runErr, strings.TrimSpace(stderr.String()))
	}
	return closeErr
}

// partKey names a part across runs and across a Messages data reset: its
// message's key and its place among the message's parts.
func partKey(messageKey string, index int) string {
	return messageKey + "#" + strconv.Itoa(index)
}

// isFilePart is a part whose content is a file rather than the message's
// text (text/plain) or a rich card's JSON (which arrives as its text).
func isFilePart(part map[string]any) bool {
	contentType := strings.ToLower(stringOf(part["content_type"]))
	return contentType != "" && !strings.HasPrefix(contentType, "text/plain") &&
		!strings.HasPrefix(contentType, "application/vnd.gsma.botmessage") &&
		!strings.HasPrefix(contentType, "application/vnd.gsma.botsuggestion")
}

// attachFiles sends every file part not yet sent (within the run's byte
// budget) and records on each file part its file's status and, once sent,
// its stored pointer. Files already sent are trusted from the ledger and
// not read again.
func (r *Runner) attachFiles(messages []map[string]any, locations map[int64]fileLocation, st *state, kept map[string]storedFile) (uploaded int, uploadedBytes int64, err error) {
	budget := r.AttachmentBytesPerRun
	if budget == 0 {
		budget = DefaultAttachmentBytesPerRun
	}
	var guardChecked bool
	var guardReason string
	for _, message := range messages {
		parts, _ := message["parts"].([]map[string]any)
		key := stringOf(message["message_key"])
		for i, part := range parts {
			if !isFilePart(part) {
				continue
			}
			pk := partKey(key, i)
			part["part_key"] = pk
			if stored, ok := st.Files[pk]; ok {
				kept[pk] = stored
				markStored(part, stored)
				continue
			}
			if r.DryRun || r.Files == nil {
				part["file_status"] = "pending"
				continue
			}
			location := locations[intOf(part["part_id"])]
			path, cleanup, status, err := r.materialize(location)
			if err != nil {
				return uploaded, uploadedBytes, err
			}
			if status != "" {
				part["file_status"] = status
				continue
			}
			info, err := os.Stat(path)
			if err != nil {
				cleanup()
				return uploaded, uploadedBytes, err
			}
			if uploadedBytes > 0 && uploadedBytes+info.Size() > budget {
				part["file_status"] = "pending"
				cleanup()
				continue
			}
			if !guardChecked && r.BeforeUploadCheck != nil {
				guardChecked = true
				guardReason = r.BeforeUploadCheck()
			}
			if guardReason != "" {
				part["file_status"] = "pending"
				cleanup()
				continue
			}
			sha, err := common.FileSHA256(path)
			if err != nil {
				cleanup()
				return uploaded, uploadedBytes, err
			}
			messageAt := time.UnixMilli(intOf(message["received_timestamp_ms"])).UTC()
			object, err := r.Files.UploadAndroidMessagesAttachment(path, messageAt.Format(time.RFC3339),
				fileExtension(stringOf(part["file_name"]), location.uri, stringOf(part["content_type"])),
				stringOf(part["content_type"]), sha)
			cleanup()
			if err != nil {
				return uploaded, uploadedBytes, fmt.Errorf("upload attachment %s: %w", pk, err)
			}
			stored := storedFile{
				ContentSHA256: sha, SizeBytes: info.Size(), StorageBackend: object.StorageBackend,
				StorageKey: object.StorageKey, StorageFileID: object.StorageFileID, StorageURL: object.StorageURL,
			}
			st.Files[pk] = stored
			kept[pk] = stored
			markStored(part, stored)
			uploaded++
			uploadedBytes += info.Size()
			r.Logger.Infof("Uploaded Android message attachment %s (%s)", object.StorageKey, common.FormatBytes(info.Size()))
			if err := writeState(r.StatePath, *st); err != nil {
				return uploaded, uploadedBytes, err
			}
		}
	}
	return uploaded, uploadedBytes, nil
}

func markStored(part map[string]any, stored storedFile) {
	part["file_status"] = "stored"
	part["content_sha256"] = stored.ContentSHA256
	part["stored_size_bytes"] = stored.SizeBytes
	part["storage_backend"] = stored.StorageBackend
	part["storage_key"] = stored.StorageKey
	part["storage_file_id"] = stored.StorageFileID
	part["storage_url"] = stored.StorageURL
}

// materialize returns a readable path for a part's bytes, or a status
// saying why there is none: "no_file" (Messages names no file), "missing"
// (not downloaded yet, or deleted). cleanup removes a temporary copy.
func (r *Runner) materialize(location fileLocation) (path string, cleanup func(), status string, err error) {
	cleanup = func() {}
	if location.cachePath != "" && nonEmptyFile(location.cachePath) {
		return location.cachePath, cleanup, "", nil
	}
	uri := location.uri
	switch {
	case uri == "":
		return "", cleanup, "no_file", nil
	case strings.HasPrefix(uri, "file://"):
		parsed, err := url.Parse(uri)
		if err != nil || parsed.Path == "" {
			return "", cleanup, "missing", nil
		}
		if !nonEmptyFile(parsed.Path) {
			return "", cleanup, "missing", nil
		}
		return parsed.Path, cleanup, "", nil
	case strings.HasPrefix(uri, "content://"):
		dir := filepath.Join(filepath.Dir(r.StatePath), "attachment-tmp")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", cleanup, "", err
		}
		dest := filepath.Join(dir, "part.bin")
		if err := readContentURI(uri, dest); err != nil {
			os.Remove(dest)
			r.Logger.Warningf("Android message attachment %s is unreadable: %v", uri, err)
			return "", cleanup, "missing", nil
		}
		if !nonEmptyFile(dest) {
			os.Remove(dest)
			return "", cleanup, "missing", nil
		}
		return dest, func() { os.Remove(dest) }, "", nil
	default:
		return "", cleanup, "unsupported_uri", nil
	}
}

func nonEmptyFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular() && info.Size() > 0
}

var extensionPattern = regexp.MustCompile(`^\.[A-Za-z0-9]{1,16}$`)

// contentTypeExtensions covers what texts carry when the part names no
// file (MMS parts rarely do).
var contentTypeExtensions = map[string]string{
	"image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png", "image/gif": ".gif",
	"image/webp": ".webp", "image/heic": ".heic", "image/heif": ".heif", "image/bmp": ".bmp",
	"video/mp4": ".mp4", "video/3gpp": ".3gp", "video/quicktime": ".mov", "video/webm": ".webm",
	"audio/amr": ".amr", "audio/mp4": ".m4a", "audio/aac": ".aac", "audio/mpeg": ".mp3",
	"audio/ogg": ".ogg", "audio/wav": ".wav", "audio/x-wav": ".wav",
	"text/x-vcard": ".vcf", "text/vcard": ".vcf", "text/x-vcalendar": ".vcs",
	"application/pdf": ".pdf", "application/zip": ".zip",
}

// fileExtension picks the stored object's extension: the file's own, else
// the content type's, else .bin.
func fileExtension(fileName, uri, contentType string) string {
	for _, name := range []string{fileName, uri} {
		if unescaped, err := url.PathUnescape(name); err == nil {
			name = unescaped
		}
		if ext := strings.ToLower(filepath.Ext(name)); extensionPattern.MatchString(ext) {
			return ext
		}
	}
	if ext, ok := contentTypeExtensions[strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))]; ok {
		return ext
	}
	return ".bin"
}
