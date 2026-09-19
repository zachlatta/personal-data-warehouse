// Package ingestclient posts domain payloads to the app's semantic ingestion
// endpoints (POST /ingest/...). Client uploaders hand it a gzipped batch, an
// attachment, an audio file, a JSON envelope; the app owns every storage
// detail (folder ids, object keys, kinds, pdw_* tags, the Drive credential).
// A device holds only the app base URL and the shared signing key.
//
// Requests are authenticated with the same HMAC scheme the app uses for
// signed object download links (app/internal/auth/objectupload.go): the
// signature covers the endpoint, the body's sha256 and an expiry, so a link
// can neither be replayed against another endpoint nor reused for a
// different body.
package ingestclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const (
	// DefaultLinkTTL bounds how long a signed upload stays valid.
	DefaultLinkTTL = 15 * time.Minute
	// DefaultTimeout is the base request timeout, scaled up for large bodies.
	DefaultTimeout = 120 * time.Second
	// minUploadBytesPerSecond is the floor throughput used to scale the upload
	// timeout with body size (~1 MiB/s, deliberately pessimistic).
	minUploadBytesPerSecond = 1024 * 1024

	// UploadRetryAttempts: every ingest endpoint keys on the body's content
	// sha and the app dedups, so repeating an upload is free and safe; without
	// a retry a single transport blip failed an entire scheduled run.
	UploadRetryAttempts    = 3
	UploadRetryBaseSeconds = 2.0

	// DefaultMaxObjectBytes mirrors the app's PDW_INGEST_MAX_OBJECT_BYTES.
	DefaultMaxObjectBytes int64 = 512 * 1024 * 1024
	// CloudflareMaxBodyBytes is the hard cap in front of the public hostname.
	CloudflareMaxBodyBytes int64 = 100 * 1024 * 1024
)

// retryableStatusCodes are answers that a retry can change.
var retryableStatusCodes = map[int]bool{429: true, 499: true, 500: true, 502: true, 503: true, 504: true}

// StoredObject is the app's reference for an uploaded object (mirrors the
// warehouse storage_* columns).
type StoredObject struct {
	StorageBackend string `json:"storage_backend"`
	StorageKey     string `json:"storage_key"`
	StorageFileID  string `json:"storage_file_id"`
	StorageURL     string `json:"storage_url"`
}

// Map renders the stored reference as the JSON object the batch payloads embed.
func (s StoredObject) Map() map[string]any {
	return map[string]any{
		"storage_backend": s.StorageBackend,
		"storage_key":     s.StorageKey,
		"storage_file_id": s.StorageFileID,
		"storage_url":     s.StorageURL,
	}
}

// HTTPError is a non-2xx answer from the app.
type HTTPError struct {
	StatusCode int
	Endpoint   string
	Body       string
}

func (e *HTTPError) Error() string {
	body := strings.TrimSpace(e.Body)
	if body == "" {
		return fmt.Sprintf("%s: HTTP %d", e.Endpoint, e.StatusCode)
	}
	return fmt.Sprintf("%s: HTTP %d: %s", e.Endpoint, e.StatusCode, body)
}

// Client posts domain payloads to the app's semantic ingestion endpoints.
type Client struct {
	baseURL       string
	uploadBaseURL string
	hostHeader    string // set when uploads go to a different origin
	signer        *pdwauth.Service
	httpClient    *http.Client
	timeout       time.Duration
	linkTTL       time.Duration
	maxObject     int64
	now           func() time.Time
	sleep         func(time.Duration)
	logger        common.Logger
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient replaces the transport (tests).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.httpClient = h } }

// WithNow replaces the clock (tests).
func WithNow(now func() time.Time) Option { return func(c *Client) { c.now = now } }

// WithSleep replaces the retry backoff sleeper (tests).
func WithSleep(sleep func(time.Duration)) Option { return func(c *Client) { c.sleep = sleep } }

// WithUploadBaseURL routes uploads to a different origin (e.g. the app reached
// directly over Tailscale, bypassing Cloudflare's body-size cap) while still
// authenticating as the public host. The HMAC signature covers only the
// endpoint path + body sha + expiry, never the host, so the app verifies an
// upload identically no matter which origin served it.
func WithUploadBaseURL(direct string) Option {
	return func(c *Client) {
		direct = strings.TrimRight(strings.TrimSpace(direct), "/")
		if direct == "" {
			return
		}
		c.uploadBaseURL = direct
		if parsed, err := url.Parse(c.baseURL); err == nil {
			c.hostHeader = parsed.Host
		}
	}
}

// WithMaxObjectBytes sets the app's own body cap.
func WithMaxObjectBytes(n int64) Option {
	return func(c *Client) {
		if n > 0 {
			c.maxObject = n
		}
	}
}

// WithTimeout sets the base request timeout.
func WithTimeout(d time.Duration) Option { return func(c *Client) { c.timeout = d } }

// WithLogger sets where retry warnings go.
func WithLogger(l common.Logger) Option { return func(c *Client) { c.logger = l } }

// New builds a client for the app at baseURL, signing with the app secret.
func New(baseURL string, signingKey string, opts ...Option) (*Client, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil, errors.New("base_url is required")
	}
	if signingKey == "" {
		return nil, errors.New("signing_key is required")
	}
	c := &Client{
		baseURL:       baseURL,
		uploadBaseURL: baseURL,
		httpClient:    &http.Client{},
		timeout:       DefaultTimeout,
		linkTTL:       DefaultLinkTTL,
		maxObject:     DefaultMaxObjectBytes,
		now:           time.Now,
		sleep:         time.Sleep,
	}
	c.signer = pdwauth.NewService([]byte(signingKey), func() time.Time { return c.now() })
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// BaseURL is the public app URL.
func (c *Client) BaseURL() string { return c.baseURL }

// UploadBaseURL is the origin uploads are sent to.
func (c *Client) UploadBaseURL() string { return c.uploadBaseURL }

// HostHeader is the public Host sent when uploads go to a direct origin.
func (c *Client) HostHeader() string { return c.hostHeader }

// EffectiveMaxUploadBytes is the largest body this client can deliver on its
// route: the app's cap when going direct, else the smaller of that and
// Cloudflare's 100 MiB edge limit.
func (c *Client) EffectiveMaxUploadBytes() int64 {
	if c.hostHeader != "" {
		return c.maxObject
	}
	if c.maxObject < CloudflareMaxBodyBytes {
		return c.maxObject
	}
	return CloudflareMaxBodyBytes
}

// uploadTimeout scales the request timeout with body size: the app streams
// the whole body to object storage before answering, so a few-hundred-MiB
// upload routinely ran past a fixed 120 s.
func (c *Client) uploadTimeout(bodyBytes int) time.Duration {
	scaled := time.Duration(float64(bodyBytes) / minUploadBytesPerSecond * float64(time.Second))
	if scaled > c.timeout {
		return scaled
	}
	return c.timeout
}

// SignedPost posts body to endpoint with the object-upload signature, retrying
// transient failures, and returns the decoded JSON response.
func (c *Client) SignedPost(endpoint string, body []byte, contentType string, params map[string]string) (map[string]any, error) {
	var lastErr error
	for attempt := 0; attempt < UploadRetryAttempts; attempt++ {
		payload, err := c.signedPostOnce(endpoint, body, contentType, params)
		if err == nil {
			return payload, nil
		}
		lastErr = err
		if attempt == UploadRetryAttempts-1 || !isRetryable(err) {
			return nil, err
		}
		delay := time.Duration(UploadRetryBaseSeconds * float64(int(1)<<attempt) * float64(time.Second))
		if c.logger != nil {
			c.logger.Warningf("retrying %s upload in %.0fs after transient failure: %s", endpoint, delay.Seconds(), errorSummary(err))
		}
		c.sleep(delay)
	}
	return nil, lastErr
}

func (c *Client) signedPostOnce(endpoint string, body []byte, contentType string, params map[string]string) (map[string]any, error) {
	// Re-signed per attempt: the signature carries an expiry, so a retry after
	// a long stalled upload must not replay a stale one.
	sum := sha256.Sum256(body)
	contentSHA := hex.EncodeToString(sum[:])
	exp := c.now().Add(c.linkTTL)
	signature := c.signer.SignObjectUpload(endpoint, contentSHA, exp)
	query := url.Values{}
	for key, value := range params {
		query.Set(key, value)
	}
	query.Set("content_sha256", contentSHA)
	query.Set("exp", strconv.FormatInt(exp.Unix(), 10))
	query.Set("sig", signature)
	target := c.uploadBaseURL + endpoint + "?" + query.Encode()

	ctx, cancel := context.WithTimeout(context.Background(), c.uploadTimeout(len(body)))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", userAgent)
	if c.hostHeader != "" {
		// Route to the direct origin but keep the public Host so Traefik still
		// maps the request to the app's router.
		req.Host = c.hostHeader
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &transportError{err: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, &transportError{err: err}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &HTTPError{StatusCode: resp.StatusCode, Endpoint: endpoint, Body: string(data)}
	}
	payload := map[string]any{}
	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("%s: response was not JSON: %w", endpoint, err)
		}
	}
	return payload, nil
}

// userAgent names the client; Cloudflare's bot rule rejects the default
// Python-urllib signature and every server-side caller sets its own.
const userAgent = "pdw-cli-ingest/1.0"

type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

func isRetryable(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return retryableStatusCodes[httpErr.StatusCode]
	}
	var tErr *transportError
	// A dropped connection or a read timeout never got a verdict from the
	// app, and the endpoints are content-sha idempotent, so repeating is safe.
	return errors.As(err, &tErr)
}

func errorSummary(err error) string {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return fmt.Sprintf("HTTP %d", httpErr.StatusCode)
	}
	return "transport error"
}

// post is SignedPost narrowed to the stored-object answer.
func (c *Client) post(endpoint string, body []byte, contentType string, params map[string]string) (StoredObject, error) {
	payload, err := c.SignedPost(endpoint, body, contentType, params)
	if err != nil {
		return StoredObject{}, err
	}
	return storedObjectFromPayload(payload), nil
}

func storedObjectFromPayload(payload map[string]any) StoredObject {
	return StoredObject{
		StorageBackend: stringField(payload, "storage_backend"),
		StorageKey:     stringField(payload, "storage_key"),
		StorageFileID:  stringField(payload, "storage_file_id"),
		StorageURL:     stringField(payload, "storage_url"),
	}
}

func stringField(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	switch v := payload[key].(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

// postJSON encodes payload canonically (sorted keys, no spaces) so the sidecar
// bytes, and their sha, are deterministic.
func (c *Client) postJSON(endpoint string, payload map[string]any, params map[string]string) (StoredObject, error) {
	body, err := common.CanonicalJSON(payload)
	if err != nil {
		return StoredObject{}, err
	}
	return c.post(endpoint, body, "application/json", params)
}

// --- agent sessions ---------------------------------------------------------

// UploadAgentSessionsBatch posts a gzipped JSONL batch of agent session lines.
func (c *Client) UploadAgentSessionsBatch(gzipBytes []byte, exportedAt string) (StoredObject, error) {
	return c.post("/ingest/agent-sessions/batch", gzipBytes, "application/gzip", map[string]string{"exported_at": exportedAt})
}

// --- apple messages ---------------------------------------------------------

// UploadAppleMessagesBatch posts a gzipped JSONL batch of message records.
func (c *Client) UploadAppleMessagesBatch(gzipBytes []byte, exportedAt string) (StoredObject, error) {
	return c.post("/ingest/apple-messages/batch", gzipBytes, "application/gzip", map[string]string{"exported_at": exportedAt})
}

// AppleMessagesAttachment names one attachment upload.
type AppleMessagesAttachment struct {
	AttachmentGUID string
	MessageGUID    string
	ContentType    string
	CreatedAt      string
	Filename       string
}

// UploadAppleMessagesAttachment posts one attachment's bytes.
func (c *Client) UploadAppleMessagesAttachment(content []byte, a AppleMessagesAttachment) (StoredObject, error) {
	contentType := a.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return c.post("/ingest/apple-messages/attachment", content, contentType, map[string]string{
		"attachment_guid": a.AttachmentGUID,
		"message_guid":    a.MessageGUID,
		"content_type":    a.ContentType,
		"created_at":      a.CreatedAt,
		"filename":        a.Filename,
	})
}

// --- apple contacts ---------------------------------------------------------

// UploadAppleContactsBatch posts a gzipped JSONL batch of contact records.
func (c *Client) UploadAppleContactsBatch(gzipBytes []byte, exportedAt string) (StoredObject, error) {
	return c.post("/ingest/apple-contacts/batch", gzipBytes, "application/gzip", map[string]string{"exported_at": exportedAt})
}

// --- voice memos ------------------------------------------------------------

// UploadVoiceMemoAudio posts a recording's bytes.
func (c *Client) UploadVoiceMemoAudio(content []byte, recordedAt, extension, contentType string) (StoredObject, error) {
	ct := contentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	return c.post("/ingest/voice-memos/audio", content, ct, map[string]string{
		"recorded_at": recordedAt, "extension": extension, "content_type": contentType,
	})
}

// UploadVoiceMemoMetadata posts a recording's JSON sidecar.
func (c *Client) UploadVoiceMemoMetadata(payload map[string]any, recordedAt, audioContentSHA256 string) (StoredObject, error) {
	return c.postJSON("/ingest/voice-memos/metadata", payload, map[string]string{
		"recorded_at": recordedAt, "audio_content_sha256": audioContentSHA256,
	})
}

// --- photos -----------------------------------------------------------------

// UploadPhotoMetadata posts a photo's metadata envelope.
func (c *Client) UploadPhotoMetadata(payload map[string]any, capturedAt, fileContentSHA256, metadataDedupSHA256 string) (StoredObject, error) {
	return c.postJSON("/ingest/photos/metadata", payload, map[string]string{
		"captured_at":           capturedAt,
		"file_content_sha256":   fileContentSHA256,
		"metadata_dedup_sha256": metadataDedupSHA256,
	})
}

// --- manual finance ---------------------------------------------------------

// UploadManualFinanceDocument posts a document's bytes.
func (c *Client) UploadManualFinanceDocument(content []byte, modifiedAt, accountFolder, extension, contentType string) (StoredObject, error) {
	ct := contentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	return c.post("/ingest/manual-finance/file", content, ct, map[string]string{
		"modified_at":    modifiedAt,
		"account_folder": accountFolder,
		"extension":      extension,
		"content_type":   contentType,
	})
}

// UploadManualFinanceMetadata posts a document's envelope.
func (c *Client) UploadManualFinanceMetadata(payload map[string]any, modifiedAt, accountFolder, fileContentSHA256, metadataDedupSHA256 string) (StoredObject, error) {
	return c.postJSON("/ingest/manual-finance/metadata", payload, map[string]string{
		"modified_at":           modifiedAt,
		"account_folder":        accountFolder,
		"file_content_sha256":   fileContentSHA256,
		"metadata_dedup_sha256": metadataDedupSHA256,
	})
}

// --- apple notes ------------------------------------------------------------

// UploadAppleNotesBody posts a note revision's HTML.
func (c *Client) UploadAppleNotesBody(html []byte, noteID, revisionID, modifiedAt string) (StoredObject, error) {
	return c.post("/ingest/apple-notes/body", html, "text/html", map[string]string{
		"note_id": noteID, "revision_id": revisionID, "modified_at": modifiedAt,
	})
}

// AppleNotesAttachment names one attachment upload.
type AppleNotesAttachment struct {
	NoteID       string
	RevisionID   string
	ModifiedAt   string
	AttachmentID string
	Filename     string
	ContentType  string
}

// UploadAppleNotesAttachment posts one attachment's bytes.
func (c *Client) UploadAppleNotesAttachment(content []byte, a AppleNotesAttachment) (StoredObject, error) {
	ct := a.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	return c.post("/ingest/apple-notes/attachment", content, ct, map[string]string{
		"note_id":       a.NoteID,
		"revision_id":   a.RevisionID,
		"modified_at":   a.ModifiedAt,
		"attachment_id": a.AttachmentID,
		"filename":      a.Filename,
		"content_type":  a.ContentType,
	})
}

// UploadAppleNotesRevision posts a revision's metadata sidecar.
func (c *Client) UploadAppleNotesRevision(payload map[string]any, noteID, revisionID, modifiedAt, noteContentSHA256 string) (StoredObject, error) {
	return c.postJSON("/ingest/apple-notes/revision", payload, map[string]string{
		"note_id":             noteID,
		"revision_id":         revisionID,
		"modified_at":         modifiedAt,
		"note_content_sha256": noteContentSHA256,
	})
}

// --- credentials and heartbeats --------------------------------------------

// PublishChatGPTSession publishes a captured chatgpt.com web session. The
// acknowledgement carries a token sha, never the token.
func (c *Client) PublishChatGPTSession(account, sessionToken, sessionKey, sourceBrowser string) (map[string]any, error) {
	body, err := common.CanonicalJSON(map[string]any{
		"account": account, "session_key": sessionKey, "session_token": sessionToken, "source_browser": sourceBrowser,
	})
	if err != nil {
		return nil, err
	}
	return c.SignedPost("/ingest/chatgpt/session", body, "application/json", nil)
}

// WhoopSession is a captured app.whoop.com browser session.
type WhoopSession struct {
	Account          string
	SessionKey       string
	AccessToken      string
	RefreshToken     string
	AccessExpiresAt  string
	RefreshExpiresAt string
	SourceBrowser    string
}

// PublishWhoopPrivateSession publishes a captured WHOOP browser session.
func (c *Client) PublishWhoopPrivateSession(s WhoopSession) (map[string]any, error) {
	body, err := common.CanonicalJSON(map[string]any{
		"account":            s.Account,
		"session_key":        s.SessionKey,
		"access_token":       s.AccessToken,
		"refresh_token":      s.RefreshToken,
		"access_expires_at":  s.AccessExpiresAt,
		"refresh_expires_at": s.RefreshExpiresAt,
		"source_browser":     s.SourceBrowser,
	})
	if err != nil {
		return nil, err
	}
	return c.SignedPost("/ingest/whoop-private/session", body, "application/json", nil)
}

// HackerNewsSession is a captured news.ycombinator.com login: the `user`
// cookie, sent as the full Cookie header value the poller replays.
type HackerNewsSession struct {
	Account       string
	SessionKey    string
	SessionToken  string
	SourceBrowser string
}

// PublishHackerNewsSession publishes a captured Hacker News login cookie. The
// acknowledgement carries a token sha, never the cookie.
func (c *Client) PublishHackerNewsSession(s HackerNewsSession) (map[string]any, error) {
	body, err := common.CanonicalJSON(map[string]any{
		"account":        s.Account,
		"session_key":    s.SessionKey,
		"session_token":  s.SessionToken,
		"source_browser": s.SourceBrowser,
	})
	if err != nil {
		return nil, err
	}
	return c.SignedPost("/ingest/hacker-news/session", body, "application/json", nil)
}

// SlackSession is a captured Slack client session: both halves go together on
// purpose, because an xoxc token without the d cookie authenticates as nobody.
type SlackSession struct {
	Account         string
	SessionKey      string
	SessionToken    string
	SessionCookie   string
	TeamID          string
	EnterpriseID    string
	UserID          string
	TeamURL         string
	CookieExpiresAt string
	SourceApp       string
}

// PublishSlackSession publishes a captured Slack client session.
func (c *Client) PublishSlackSession(s SlackSession) (map[string]any, error) {
	body, err := common.CanonicalJSON(map[string]any{
		"account":           s.Account,
		"session_key":       s.SessionKey,
		"session_token":     s.SessionToken,
		"session_cookie":    s.SessionCookie,
		"team_id":           s.TeamID,
		"enterprise_id":     s.EnterpriseID,
		"user_id":           s.UserID,
		"team_url":          s.TeamURL,
		"source_app":        s.SourceApp,
		"cookie_expires_at": s.CookieExpiresAt,
	})
	if err != nil {
		return nil, err
	}
	return c.SignedPost("/ingest/slack/session", body, "application/json", nil)
}

// Heartbeat is one uploader run's verdict.
type Heartbeat struct {
	Pipeline        string
	Device          string
	RanAt           string
	ExitCode        int
	DurationSeconds int
	Error           string
}

// PostHeartbeat records one uploader run in ops.uploader_heartbeats.
func (c *Client) PostHeartbeat(h Heartbeat) (map[string]any, error) {
	errText := h.Error
	if len(errText) > 500 {
		errText = errText[:500]
	}
	body, err := common.CanonicalJSON(map[string]any{
		"pipeline":         h.Pipeline,
		"device":           h.Device,
		"ran_at":           h.RanAt,
		"exit_code":        int64(h.ExitCode),
		"duration_seconds": int64(h.DurationSeconds),
		"error":            errText,
	})
	if err != nil {
		return nil, err
	}
	return c.SignedPost("/ingest/heartbeat", body, "application/json", nil)
}

// SignObjectUpload exposes the signature for tests that verify the wire shape.
func (c *Client) SignObjectUpload(endpoint, contentSHA256 string, exp time.Time) string {
	return c.signer.SignObjectUpload(endpoint, contentSHA256, exp)
}
