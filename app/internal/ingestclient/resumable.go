package ingestclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const (
	photoResumableEndpoint     = "/ingest/photos/file/resumable"
	voiceMemoResumableEndpoint = "/ingest/voice-memos/audio/resumable"
	resumableStatusCode        = 308
	resumableRetryAttempts     = 5
	resumableSessionRestart    = 3
)

var errResumableSessionExpired = errors.New("resumable session expired")

// UploadPhotoFile uploads a complete photo resource through a resumable Drive
// session (see uploadResumableFile).
func (c *Client) UploadPhotoFile(path, capturedAt, extension, contentType, contentSHA256 string) (StoredObject, error) {
	return c.uploadResumableFile(photoResumableEndpoint, "photo", "captured_at", capturedAt, path, extension, contentType, contentSHA256)
}

// UploadVoiceMemoAudio uploads a recording through a resumable Drive session.
// It used to POST the whole recording as one request body, which Traefik's
// 60s entrypoint read timeout cut for any memo that took longer than a
// minute to send: a 2 h 42 m, 625 MiB recording failed every run for a day.
func (c *Client) UploadVoiceMemoAudio(path, recordedAt, extension, contentType, contentSHA256 string) (StoredObject, error) {
	return c.uploadResumableFile(voiceMemoResumableEndpoint, "voice memo", "recorded_at", recordedAt, path, extension, contentType, contentSHA256)
}

// uploadResumableFile uploads a file through a resumable Drive session. Only
// the small, signed initiation request traverses the app/proxy; the file
// itself streams in bounded chunks to the scoped Drive upload URL the app
// creates with its credential, so neither Cloudflare's body cap nor a proxy
// read timeout can truncate a large file. Drive's final sha256 and size are
// checked before metadata is allowed to reference the object.
func (c *Client) uploadResumableFile(endpoint, label, timeField, timeValue, path, extension, contentType, contentSHA256 string) (StoredObject, error) {
	info, err := os.Stat(path)
	if err != nil {
		return StoredObject{}, err
	}
	sizeBytes := info.Size()
	if sizeBytes <= 0 {
		return StoredObject{}, fmt.Errorf("%s file must not be empty", label)
	}
	sha := strings.ToLower(contentSHA256)
	if len(sha) != 64 || strings.Trim(sha, "0123456789abcdef") != "" {
		return StoredObject{}, errors.New("content_sha256 must be a 64-character hexadecimal digest")
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	startBody, err := common.CanonicalJSON(map[string]any{
		timeField:        timeValue,
		"content_sha256": sha,
		"content_type":   contentType,
		"extension":      extension,
		"size_bytes":     sizeBytes,
	})
	if err != nil {
		return StoredObject{}, err
	}

	for restart := 0; restart < resumableSessionRestart; restart++ {
		started, err := c.SignedPost(endpoint, startBody, "application/json", nil)
		if err != nil {
			// Summarised, never wrapped: a transport error carries the signed URL.
			return StoredObject{}, fmt.Errorf("could not start resumable %s upload: %s", label, errorSummary(err))
		}
		if complete, _ := started["complete"].(bool); complete {
			stored := storedObjectFromPayload(started)
			if stored.StorageKey == "" || stored.StorageFileID == "" {
				return StoredObject{}, fmt.Errorf("app returned an invalid existing %s object", label)
			}
			return stored, nil
		}
		uploadURL := stringField(started, "upload_url")
		storageKey := stringField(started, "storage_key")
		chunkSize := int64(common.ToInt(started["chunk_size_bytes"]))
		if uploadURL == "" || storageKey == "" || chunkSize <= 0 {
			return StoredObject{}, fmt.Errorf("app returned an invalid resumable %s upload session", label)
		}
		parsed, err := url.Parse(uploadURL)
		if err != nil {
			return StoredObject{}, fmt.Errorf("app returned an invalid resumable %s upload session", label)
		}
		loopback := parsed.Scheme == "http" && (parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "::1" || parsed.Hostname() == "localhost")
		if parsed.Scheme != "https" && !loopback {
			return StoredObject{}, fmt.Errorf("app returned a non-HTTPS resumable %s upload session", label)
		}

		completed, err := c.uploadResumableChunks(path, label, uploadURL, contentType, sizeBytes, chunkSize)
		if errors.Is(err, errResumableSessionExpired) {
			if restart == resumableSessionRestart-1 {
				return StoredObject{}, fmt.Errorf("resumable %s upload session expired repeatedly", label)
			}
			continue
		}
		if err != nil {
			return StoredObject{}, err
		}
		if strings.ToLower(stringField(completed, "sha256Checksum")) != sha {
			return StoredObject{}, fmt.Errorf("Drive checksum does not match the complete %s file", label)
		}
		actualSize := int64(-1)
		switch v := completed["size"].(type) {
		case string:
			actualSize, _ = strconv.ParseInt(v, 10, 64)
		case float64:
			actualSize = int64(v)
		}
		if actualSize != sizeBytes {
			return StoredObject{}, fmt.Errorf("Drive size does not match the complete %s file", label)
		}
		fileID := stringField(completed, "id")
		if fileID == "" {
			return StoredObject{}, fmt.Errorf("Drive did not return a file id for the complete %s file", label)
		}
		backend := stringField(started, "storage_backend")
		if backend == "" {
			backend = "google_drive"
		}
		return StoredObject{
			StorageBackend: backend,
			StorageKey:     storageKey,
			StorageFileID:  fileID,
			StorageURL:     stringField(completed, "webViewLink"),
		}, nil
	}
	return StoredObject{}, fmt.Errorf("resumable %s upload session expired repeatedly", label)
}

func (c *Client) uploadResumableChunks(path, label, uploadURL, contentType string, sizeBytes, chunkSize int64) (map[string]any, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	offset := int64(0)
	for offset < sizeBytes {
		remaining := sizeBytes - offset
		size := chunkSize
		if remaining < size {
			size = remaining
		}
		chunk := make([]byte, size)
		n, err := file.ReadAt(chunk, offset)
		if err != nil && !(errors.Is(err, io.EOF) && int64(n) == size) {
			return nil, fmt.Errorf("%s file ended before its declared size", label)
		}
		if n == 0 {
			return nil, fmt.Errorf("%s file ended before its declared size", label)
		}
		chunk = chunk[:n]
		end := offset + int64(n) - 1
		resp, body, err := c.put(uploadURL, chunk, map[string]string{
			"Content-Length": strconv.Itoa(n),
			"Content-Range":  fmt.Sprintf("bytes %d-%d/%d", offset, end, sizeBytes),
			"Content-Type":   contentType,
		}, c.uploadTimeout(n))
		if err != nil {
			next, completed, qerr := c.queryResumableStatus(uploadURL, label, sizeBytes)
			if qerr != nil {
				return nil, qerr
			}
			if completed != nil {
				return completed, nil
			}
			offset = next
			continue
		}
		switch {
		case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
			return decodeJSONObject(body)
		case resp.StatusCode == resumableStatusCode:
			next, err := resumableNextOffset(resp.Header.Get("Range"), sizeBytes)
			if err != nil {
				return nil, err
			}
			offset = next
		case resp.StatusCode == http.StatusNotFound:
			return nil, errResumableSessionExpired
		case retryableStatusCodes[resp.StatusCode]:
			next, completed, qerr := c.queryResumableStatus(uploadURL, label, sizeBytes)
			if qerr != nil {
				return nil, qerr
			}
			if completed != nil {
				return completed, nil
			}
			offset = next
		default:
			return nil, fmt.Errorf("resumable %s upload failed with HTTP status %d", label, resp.StatusCode)
		}
	}
	return nil, fmt.Errorf("Drive did not confirm the complete %s upload", label)
}

func (c *Client) queryResumableStatus(uploadURL, label string, sizeBytes int64) (int64, map[string]any, error) {
	for attempt := 0; attempt < resumableRetryAttempts; attempt++ {
		resp, body, err := c.put(uploadURL, nil, map[string]string{
			"Content-Length": "0",
			"Content-Range":  fmt.Sprintf("bytes */%d", sizeBytes),
		}, c.timeout)
		if err != nil {
			if attempt == resumableRetryAttempts-1 {
				return 0, nil, fmt.Errorf("could not determine resumable %s upload status", label)
			}
			c.sleep(time.Duration(UploadRetryBaseSeconds * float64(int(1)<<attempt) * float64(time.Second)))
			continue
		}
		switch {
		case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
			completed, err := decodeJSONObject(body)
			return sizeBytes, completed, err
		case resp.StatusCode == resumableStatusCode:
			next, err := resumableNextOffset(resp.Header.Get("Range"), sizeBytes)
			return next, nil, err
		case resp.StatusCode == http.StatusNotFound:
			return 0, nil, errResumableSessionExpired
		case retryableStatusCodes[resp.StatusCode] && attempt < resumableRetryAttempts-1:
			c.sleep(time.Duration(UploadRetryBaseSeconds * float64(int(1)<<attempt) * float64(time.Second)))
			continue
		default:
			return 0, nil, fmt.Errorf("resumable %s upload failed with HTTP status %d", label, resp.StatusCode)
		}
	}
	return 0, nil, fmt.Errorf("could not determine resumable %s upload status", label)
}

// put issues a PUT and returns the response plus a bounded body; the error is
// non-nil only for transport failures (the caller inspects status codes). The
// upload URL is a bearer capability, so it is never included in an error.
func (c *Client) put(target string, body []byte, headers map[string]string, timeout time.Duration) (*http.Response, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, bytes.NewReader(body))
	if err != nil {
		return nil, nil, errors.New("resumable upload transport failed")
	}
	for key, value := range headers {
		if key == "Content-Length" {
			continue
		}
		req.Header.Set(key, value)
	}
	req.ContentLength = int64(len(body))
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, errors.New("resumable upload transport failed")
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp, data, nil
}

func decodeJSONObject(data []byte) (map[string]any, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, errors.New("Drive returned a non-JSON completion payload")
	}
	return payload, nil
}

func resumableNextOffset(rangeHeader string, sizeBytes int64) (int64, error) {
	if rangeHeader == "" {
		return 0, nil
	}
	const prefix = "bytes=0-"
	if !strings.HasPrefix(rangeHeader, prefix) {
		return 0, errors.New("Drive returned an invalid resumable upload range")
	}
	last, err := strconv.ParseInt(strings.TrimPrefix(rangeHeader, prefix), 10, 64)
	if err != nil {
		return 0, errors.New("Drive returned an invalid resumable upload range")
	}
	offset := last + 1
	if offset < 0 || offset > sizeBytes {
		return 0, errors.New("Drive returned a resumable upload range outside the file")
	}
	return offset, nil
}
