package ingestclient

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type recordedRequest struct {
	Method      string
	Path        string
	Query       map[string]string
	Body        []byte
	ContentType string
	Host        string
	Headers     http.Header
}

// fakeApp records every request and answers as the app would; a handler
// override lets a test script status codes.
type fakeApp struct {
	mu       sync.Mutex
	requests []recordedRequest
	secret   string
	handler  func(w http.ResponseWriter, r *http.Request, body []byte) bool
}

func (f *fakeApp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	query := map[string]string{}
	for key := range r.URL.Query() {
		query[key] = r.URL.Query().Get(key)
	}
	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{
		Method: r.Method, Path: r.URL.Path, Query: query, Body: body,
		ContentType: r.Header.Get("Content-Type"), Host: r.Host, Headers: r.Header.Clone(),
	})
	f.mu.Unlock()
	if f.handler != nil && f.handler(w, r, body) {
		return
	}
	if f.secret != "" && strings.HasPrefix(r.URL.Path, "/ingest/") {
		sum := sha256.Sum256(body)
		svc := pdwauth.NewService([]byte(f.secret), time.Now)
		if err := svc.VerifyObjectUpload(r.URL.Path, hex.EncodeToString(sum[:]), r.URL.Query().Get("exp"), r.URL.Query().Get("sig")); err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		if r.URL.Query().Get("content_sha256") != hex.EncodeToString(sum[:]) {
			http.Error(w, "content sha mismatch", http.StatusBadRequest)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"storage_backend": "google_drive",
		"storage_key":     "key/" + strings.TrimPrefix(r.URL.Path, "/ingest/"),
		"storage_file_id": "file-1",
		"storage_url":     "",
		"token_sha256":    "abc",
	})
}

func (f *fakeApp) last() recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

func newTestClient(t *testing.T, app *fakeApp, opts ...Option) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(app)
	t.Cleanup(server.Close)
	if app.secret == "" {
		app.secret = "secret-key"
	}
	opts = append([]Option{WithSleep(func(time.Duration) {})}, opts...)
	client, err := New(server.URL, app.secret, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return client, server
}

func TestKnownAnswerSignatureMatchesPython(t *testing.T) {
	// The Python client's known-answer test pinned this exact value; the Go
	// server verifies both, so the two implementations must agree.
	client, err := New("https://app.example", "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	sig := client.SignObjectUpload("/ingest/agent-sessions/batch", strings.Repeat("ab", 32), time.Unix(1_750_000_000, 0))
	svc := pdwauth.NewService([]byte("test-secret"), time.Now)
	want := svc.SignObjectUpload("/ingest/agent-sessions/batch", strings.Repeat("ab", 32), time.Unix(1_750_000_000, 0))
	if sig != want {
		t.Fatalf("signature %q != server's %q", sig, want)
	}
}

func TestPostSignsBodyAndSendsExpectedQuery(t *testing.T) {
	app := &fakeApp{}
	client, _ := newTestClient(t, app)
	stored, err := client.UploadAgentSessionsBatch([]byte("gz"), "2026-06-19T12:34:56+00:00")
	if err != nil {
		t.Fatal(err)
	}
	if stored.StorageKey != "key/agent-sessions/batch" || stored.StorageFileID != "file-1" {
		t.Fatalf("stored = %+v", stored)
	}
	req := app.last()
	if req.Path != "/ingest/agent-sessions/batch" || req.ContentType != "application/gzip" {
		t.Fatalf("request = %+v", req)
	}
	sum := sha256.Sum256([]byte("gz"))
	if req.Query["content_sha256"] != hex.EncodeToString(sum[:]) || req.Query["exported_at"] != "2026-06-19T12:34:56+00:00" {
		t.Fatalf("query = %v", req.Query)
	}
	if req.Query["sig"] == "" || req.Query["exp"] == "" {
		t.Fatalf("missing signature fields: %v", req.Query)
	}
	if !strings.Contains(req.Headers.Get("User-Agent"), "pdw-cli") {
		t.Fatalf("User-Agent must identify the client, got %q", req.Headers.Get("User-Agent"))
	}
}

func TestJSONPayloadsAreCanonical(t *testing.T) {
	app := &fakeApp{}
	client, _ := newTestClient(t, app)
	_, err := client.UploadVoiceMemoMetadata(map[string]any{"z": 1, "a": "é"}, "2025-07-15T09:00:00", "abc")
	if err != nil {
		t.Fatal(err)
	}
	req := app.last()
	if string(req.Body) != `{"a":"\u00e9","z":1}` || req.ContentType != "application/json" {
		t.Fatalf("body = %s content-type = %s", req.Body, req.ContentType)
	}
	if req.Query["audio_content_sha256"] != "abc" || req.Query["recorded_at"] != "2025-07-15T09:00:00" {
		t.Fatalf("query = %v", req.Query)
	}
}

func TestDirectOriginKeepsThePublicHostHeader(t *testing.T) {
	app := &fakeApp{}
	server := httptest.NewServer(app)
	defer server.Close()
	app.secret = "s"
	client, err := New("https://public.example", "s", WithUploadBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.UploadAppleContactsBatch([]byte("gz"), "2026-01-01T00:00:00+00:00"); err != nil {
		t.Fatal(err)
	}
	if app.last().Host != "public.example" {
		t.Fatalf("Host = %q, want the public host", app.last().Host)
	}
	if client.EffectiveMaxUploadBytes() != DefaultMaxObjectBytes {
		t.Fatalf("direct route should lift the ceiling to the app cap")
	}
	public, _ := New("https://public.example", "s")
	if public.EffectiveMaxUploadBytes() != CloudflareMaxBodyBytes {
		t.Fatalf("public route should be capped at Cloudflare's limit")
	}
}

func TestUploadRetriesTransientStatusesAndResignsEachAttempt(t *testing.T) {
	attempts := 0
	sigs := map[string]bool{}
	app := &fakeApp{}
	app.handler = func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		attempts++
		sigs[r.URL.Query().Get("sig")] = true
		if attempts < 3 {
			w.WriteHeader(499)
			return true
		}
		return false
	}
	now := time.Now().Add(-10 * time.Second)
	client, _ := newTestClient(t, app, WithNow(func() time.Time { now = now.Add(time.Second); return now }))
	logger := &common.RecordingLogger{}
	WithLogger(logger)(client)
	if _, err := client.UploadAgentSessionsBatch([]byte("gz"), "2026-01-01T00:00:00+00:00"); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if len(sigs) != 3 {
		t.Fatalf("each retry must be re-signed with a fresh expiry, got %d distinct signatures", len(sigs))
	}
	if len(logger.Warnings) != 2 || !strings.Contains(logger.Warnings[0], "HTTP 499") {
		t.Fatalf("warnings = %v", logger.Warnings)
	}
}

func TestUploadDoesNotRetryClientRejections(t *testing.T) {
	attempts := 0
	app := &fakeApp{handler: func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		attempts++
		w.WriteHeader(413)
		return true
	}}
	client, _ := newTestClient(t, app)
	_, err := client.UploadAgentSessionsBatch([]byte("gz"), "2026-01-01T00:00:00+00:00")
	var httpErr *HTTPError
	if err == nil || !asHTTPError(err, &httpErr) || httpErr.StatusCode != 413 {
		t.Fatalf("err = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("a 413 must not be retried, got %d attempts", attempts)
	}
}

func TestUploadGivesUpAfterTheAttemptBudget(t *testing.T) {
	attempts := 0
	app := &fakeApp{handler: func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		attempts++
		w.WriteHeader(503)
		return true
	}}
	client, _ := newTestClient(t, app)
	if _, err := client.UploadAgentSessionsBatch([]byte("gz"), "2026-01-01T00:00:00+00:00"); err == nil {
		t.Fatal("expected failure")
	}
	if attempts != UploadRetryAttempts {
		t.Fatalf("attempts = %d", attempts)
	}
}

func TestPhotoFileUploadsInChunksAndVerifiesDriveChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "photo.heic")
	content := []byte(strings.Repeat("x", 25))
	os.WriteFile(path, content, 0o644)
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])

	var chunks []string
	app := &fakeApp{}
	server := httptest.NewServer(app)
	defer server.Close()
	app.secret = "s"
	app.handler = func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == photoResumableEndpoint:
			var start map[string]any
			json.Unmarshal(body, &start)
			if start["size_bytes"].(float64) != 25 || start["content_sha256"] != sha {
				w.WriteHeader(400)
				return true
			}
			json.NewEncoder(w).Encode(map[string]any{
				"storage_backend": "google_drive", "storage_key": "photos/inbox/k",
				"upload_url": server.URL + "/drive-session", "chunk_size_bytes": 10,
			})
			return true
		case r.Method == http.MethodPut && r.URL.Path == "/drive-session":
			chunks = append(chunks, r.Header.Get("Content-Range"))
			if strings.HasPrefix(r.Header.Get("Content-Range"), "bytes 20-24") {
				json.NewEncoder(w).Encode(map[string]any{"id": "drive-file", "sha256Checksum": strings.ToUpper(sha), "size": "25", "webViewLink": "https://drive/x"})
				return true
			}
			w.Header().Set("Range", "bytes=0-"+fmt.Sprint(rangeEnd(r.Header.Get("Content-Range"))))
			w.WriteHeader(308)
			return true
		}
		return false
	}
	client, err := New(server.URL, "s", WithSleep(func(time.Duration) {}))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := client.UploadPhotoFile(path, "2026-06-01T14:30:00", ".heic", "image/heic", sha)
	if err != nil {
		t.Fatal(err)
	}
	if stored.StorageFileID != "drive-file" || stored.StorageKey != "photos/inbox/k" || stored.StorageURL != "https://drive/x" {
		t.Fatalf("stored = %+v", stored)
	}
	want := []string{"bytes 0-9/25", "bytes 10-19/25", "bytes 20-24/25"}
	if strings.Join(chunks, ",") != strings.Join(want, ",") {
		t.Fatalf("chunks = %v", chunks)
	}
}

func TestPhotoFileUploadRejectsChecksumMismatchWithoutLeakingTheSession(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "photo.heic")
	os.WriteFile(path, []byte("hello"), 0o644)
	sum := sha256.Sum256([]byte("hello"))
	sha := hex.EncodeToString(sum[:])
	app := &fakeApp{}
	server := httptest.NewServer(app)
	defer server.Close()
	app.secret = "s"
	app.handler = func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		if r.Method == http.MethodPost {
			json.NewEncoder(w).Encode(map[string]any{"storage_key": "k", "upload_url": server.URL + "/secret-capability-token", "chunk_size_bytes": 100})
			return true
		}
		json.NewEncoder(w).Encode(map[string]any{"id": "f", "sha256Checksum": "deadbeef", "size": "5"})
		return true
	}
	client, _ := New(server.URL, "s", WithSleep(func(time.Duration) {}))
	_, err := client.UploadPhotoFile(path, "2026-06-01T14:30:00", ".heic", "image/heic", sha)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), "secret-capability-token") {
		t.Fatalf("error must not carry the upload URL: %v", err)
	}
}

func TestPhotoFileUploadQueriesStatusAfterADroppedChunk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "photo.heic")
	content := []byte(strings.Repeat("y", 20))
	os.WriteFile(path, content, 0o644)
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])
	puts := 0
	var ranges []string
	app := &fakeApp{}
	server := httptest.NewServer(app)
	defer server.Close()
	app.secret = "s"
	app.handler = func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		if r.Method == http.MethodPost {
			json.NewEncoder(w).Encode(map[string]any{"storage_key": "k", "upload_url": server.URL + "/session", "chunk_size_bytes": 10})
			return true
		}
		puts++
		ranges = append(ranges, r.Header.Get("Content-Range"))
		switch puts {
		case 1: // first chunk stalls: the proxy answers 503
			w.WriteHeader(503)
		case 2: // status query: Drive has nothing yet
			w.WriteHeader(308)
		case 3: // chunk 0-9 again
			w.Header().Set("Range", "bytes=0-9")
			w.WriteHeader(308)
		default:
			json.NewEncoder(w).Encode(map[string]any{"id": "f", "sha256Checksum": sha, "size": "20"})
		}
		return true
	}
	client, _ := New(server.URL, "s", WithSleep(func(time.Duration) {}))
	if _, err := client.UploadPhotoFile(path, "2026-06-01T14:30:00", ".heic", "image/heic", sha); err != nil {
		t.Fatal(err)
	}
	want := []string{"bytes 0-9/20", "bytes */20", "bytes 0-9/20", "bytes 10-19/20"}
	if strings.Join(ranges, ",") != strings.Join(want, ",") {
		t.Fatalf("ranges = %v", ranges)
	}
}

func TestHeartbeatAndSessionsSignJSONBodies(t *testing.T) {
	app := &fakeApp{}
	client, _ := newTestClient(t, app)
	if _, err := client.PostHeartbeat(Heartbeat{Pipeline: "apple_notes", Device: "porygon", RanAt: "2026-08-27T03:00:00-04:00", ExitCode: 1, DurationSeconds: 42, Error: strings.Repeat("e", 600)}); err != nil {
		t.Fatal(err)
	}
	req := app.last()
	var body map[string]any
	json.Unmarshal(req.Body, &body)
	if req.Path != "/ingest/heartbeat" || body["exit_code"].(float64) != 1 || len(body["error"].(string)) != 500 {
		t.Fatalf("heartbeat = %+v body=%v", req, body)
	}
	ack, err := client.PublishChatGPTSession("z@x", "cookie=1", "default", "Chrome")
	if err != nil {
		t.Fatal(err)
	}
	if ack["token_sha256"] != "abc" || app.last().Path != "/ingest/chatgpt/session" {
		t.Fatalf("chatgpt publish = %v %+v", ack, app.last())
	}
	if _, err := client.PublishSlackSession(SlackSession{Account: "zrl", SessionKey: "default", SessionToken: "xoxc-1", SessionCookie: "d", TeamID: "T1"}); err != nil {
		t.Fatal(err)
	}
	if app.last().Path != "/ingest/slack/session" {
		t.Fatalf("slack path = %s", app.last().Path)
	}
	if _, err := client.PublishWhoopPrivateSession(WhoopSession{Account: "z", SessionKey: "default", AccessToken: "a", RefreshToken: "r"}); err != nil {
		t.Fatal(err)
	}
	if app.last().Path != "/ingest/whoop-private/session" {
		t.Fatalf("whoop path = %s", app.last().Path)
	}
	if _, err := client.PublishHackerNewsSession(HackerNewsSession{Account: "zachlatta", SessionKey: "default", SessionToken: "user=zachlatta&tok", SourceBrowser: "Google Chrome"}); err != nil {
		t.Fatal(err)
	}
	json.Unmarshal(app.last().Body, &body)
	if app.last().Path != "/ingest/hacker-news/session" || body["session_token"] != "user=zachlatta&tok" || body["account"] != "zachlatta" || body["source_browser"] != "Google Chrome" || body["session_key"] != "default" {
		t.Fatalf("hacker news publish = %+v body=%v", app.last(), body)
	}
}

func TestResolveDirectOrigin(t *testing.T) {
	logger := &common.RecordingLogger{}
	probeOK := func(string, string) bool { return true }
	probeFail := func(string, string) bool { return false }
	ipv4 := func(host, bin string) string {
		if host == "mew-coolify" {
			return "100.64.0.9"
		}
		return ""
	}
	if got := ResolveDirectOrigin("https://app.example", "http://10.0.0.1", "", "", ipv4, probeOK, logger); got != "http://10.0.0.1" {
		t.Fatalf("explicit = %q", got)
	}
	if got := ResolveDirectOrigin("https://app.example", "", "mew-coolify", "", ipv4, probeOK, logger); got != "http://100.64.0.9" {
		t.Fatalf("tailscale = %q", got)
	}
	if got := ResolveDirectOrigin("https://app.example", "", "mew-coolify", "", ipv4, probeFail, logger); got != "" {
		t.Fatalf("unreachable should fall back, got %q", got)
	}
	if got := ResolveDirectOrigin("https://app.example", "", "", "", ipv4, probeOK, logger); got != "" {
		t.Fatalf("unconfigured = %q", got)
	}
	if got := ParseIPv4Line("fd7a::1\n100.64.0.9\n"); got != "100.64.0.9" {
		t.Fatalf("ParseIPv4Line = %q", got)
	}
}

func TestResolveConfigPrecedence(t *testing.T) {
	env := map[string]string{"PDW_API_URL": "https://env.example", "PDW_SECRET_TOKEN": "env-token", "HOME": t.TempDir(), "XDG_CONFIG_HOME": t.TempDir()}
	getenv := func(k string) string { return env[k] }
	cfg := ResolveConfig(getenv, "", "")
	if cfg.BaseURL != "https://env.example" || cfg.Token != "env-token" {
		t.Fatalf("cfg = %+v", cfg)
	}
	cfg = ResolveConfig(getenv, "https://flag.example", "flag-token")
	if cfg.BaseURL != "https://flag.example" || cfg.Token != "flag-token" {
		t.Fatalf("flag cfg = %+v", cfg)
	}
	delete(env, "PDW_API_URL")
	env["MCP_BASE_URL"] = "https://alias.example"
	if cfg := ResolveConfig(getenv, "", ""); cfg.BaseURL != "https://alias.example" {
		t.Fatalf("alias cfg = %+v", cfg)
	}
	if problem := (Config{}).Problem(); !strings.Contains(problem, "PDW_API_URL") {
		t.Fatalf("problem = %q", problem)
	}
}

func TestUploadTimeoutScalesWithBodySize(t *testing.T) {
	client, _ := New("https://app.example", "s")
	if got := client.uploadTimeout(10); got != DefaultTimeout {
		t.Fatalf("small body timeout = %v", got)
	}
	if got := client.uploadTimeout(512 * 1024 * 1024); got < 500*time.Second {
		t.Fatalf("large body timeout = %v", got)
	}
}

func asHTTPError(err error, target **HTTPError) bool {
	e, ok := err.(*HTTPError)
	if ok {
		*target = e
	}
	return ok
}

func rangeEnd(contentRange string) int {
	var start, end, total int
	fmt.Sscanf(contentRange, "bytes %d-%d/%d", &start, &end, &total)
	return end
}

func TestConfigWithEnvFallbackFillsOnlyWhatIsEmpty(t *testing.T) {
	env := map[string]string{"MCP_BASE_URL": "https://env.example", "PDW_SECRET_TOKEN": "env-token"}
	getenv := func(k string) string { return env[k] }
	got := Config{BaseURL: "https://flag.example"}.WithEnvFallback(getenv)
	if got.BaseURL != "https://flag.example" || got.Token != "env-token" {
		t.Fatalf("got %+v", got)
	}
	if got := (Config{}).WithEnvFallback(getenv); got.BaseURL != "https://env.example" {
		t.Fatalf("got %+v", got)
	}
}
