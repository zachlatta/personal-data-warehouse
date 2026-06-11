package objectstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const defaultSlackAPIBaseURL = "https://slack.com/api"

// slackFileIDPattern matches Slack file ids (slack_files.file_id): an "F"
// followed by uppercase alphanumerics, e.g. "F0123ABCDEF". The length cap
// keeps it from ever matching a Google Drive file id, which is 25+ characters.
var slackFileIDPattern = regexp.MustCompile(`^F[A-Z0-9]{6,20}$`)

// IsSlackFileID reports whether id looks like a Slack file id rather than a
// reference into the warehouse's own object storage.
func IsSlackFileID(id string) bool {
	return slackFileIDPattern.MatchString(id)
}

// SlackFileStoreOptions configures a SlackFileStore. Tokens are tried in order
// until one resolves the file, so one store can span several workspaces.
// HTTPClient and APIBaseURL exist for testing and default to http.DefaultClient
// and the public Slack API.
type SlackFileStoreOptions struct {
	Tokens     []string
	HTTPClient httpDoer
	APIBaseURL string
}

// SlackFileStore is a read-only ObjectStore over the Slack Web API. Slack
// files are never copied into the warehouse's object storage — the sync only
// records metadata (slack_files.url_private) — so reads resolve the file with
// files.info and download url_private live, using the same bearer tokens as
// the Slack sync. Write and discovery operations return ErrNotImplemented.
type SlackFileStore struct {
	tokens     []string
	client     httpDoer
	apiBaseURL string
}

// NewSlackFileStore builds a Slack-API-backed ObjectStore.
func NewSlackFileStore(opts SlackFileStoreOptions) *SlackFileStore {
	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	apiBaseURL := strings.TrimRight(opts.APIBaseURL, "/")
	if apiBaseURL == "" {
		apiBaseURL = defaultSlackAPIBaseURL
	}
	return &SlackFileStore{tokens: opts.Tokens, client: client, apiBaseURL: apiBaseURL}
}

func (s *SlackFileStore) Backend() string { return "slack" }

// slackFile is the subset of the files.info response the store reads.
type slackFile struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Mimetype   string `json:"mimetype"`
	Size       int64  `json:"size"`
	Created    int64  `json:"created"`
	URLPrivate string `json:"url_private"`
	Permalink  string `json:"permalink"`
	IsDeleted  bool   `json:"is_deleted"`
}

// fileInfo resolves a file id via files.info, trying each token until one
// finds the file. A token answering file_not_found is authoritative for its
// own workspace only, so the search continues; harder errors (invalid_auth,
// missing_scope, ...) are kept and surfaced if no token finds the file.
func (s *SlackFileStore) fileInfo(ctx context.Context, fileID string) (slackFile, string, error) {
	if fileID == "" {
		return slackFile{}, "", ErrNotFound
	}
	var firstErr error
	for _, token := range s.tokens {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			s.apiBaseURL+"/files.info?file="+url.QueryEscape(fileID), nil)
		if err != nil {
			return slackFile{}, "", err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := s.client.Do(req)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			if firstErr == nil {
				firstErr = fmt.Errorf("slack files.info: HTTP %d", resp.StatusCode)
			}
			continue
		}
		var payload struct {
			OK    bool      `json:"ok"`
			Error string    `json:"error"`
			File  slackFile `json:"file"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("slack files.info: %w", err)
			}
			continue
		}
		switch {
		case payload.OK && payload.File.IsDeleted:
			return slackFile{}, "", ErrNotFound
		case payload.OK:
			return payload.File, token, nil
		case payload.Error == "file_not_found" || payload.Error == "file_deleted":
			continue
		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("slack files.info: %s", payload.Error)
			}
		}
	}
	if firstErr != nil {
		return slackFile{}, "", firstErr
	}
	return slackFile{}, "", ErrNotFound
}

func (s *SlackFileStore) GetMetadata(ctx context.Context, ref StoredObject) (ObjectMetadata, error) {
	file, _, err := s.fileInfo(ctx, ref.StorageFileID)
	if err != nil {
		return ObjectMetadata{}, err
	}
	meta := ObjectMetadata{
		Backend:       s.Backend(),
		StorageFileID: file.ID,
		ContentType:   file.Mimetype,
		SizeBytes:     file.Size,
		Filename:      file.Name,
		StorageURL:    file.Permalink,
	}
	if file.Created > 0 {
		meta.CreatedTime = time.Unix(file.Created, 0).UTC().Format(time.RFC3339)
	}
	return meta, nil
}

func (s *SlackFileStore) GetObject(ctx context.Context, ref StoredObject) ([]byte, error) {
	file, token, err := s.fileInfo(ctx, ref.StorageFileID)
	if err != nil {
		return nil, err
	}
	if file.URLPrivate == "" {
		return nil, fmt.Errorf("slack file %s has no url_private", file.ID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, file.URLPrivate, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("slack file download: HTTP %d", resp.StatusCode)
	}
	// Slack answers unauthorized url_private fetches with an HTML login page
	// instead of an error status; surface that as an error rather than bytes.
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
		return nil, fmt.Errorf("slack file download returned HTML instead of file content; token may lack the files:read scope")
	}
	return io.ReadAll(resp.Body)
}

func (s *SlackFileStore) ObjectExists(ctx context.Context, ref StoredObject) (bool, error) {
	_, _, err := s.fileInfo(ctx, ref.StorageFileID)
	if err == ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *SlackFileStore) GetShareURL(ctx context.Context, ref StoredObject) (string, error) {
	file, _, err := s.fileInfo(ctx, ref.StorageFileID)
	if err != nil {
		return "", err
	}
	return file.Permalink, nil
}

func (s *SlackFileStore) HasBlob(context.Context, string) (bool, error) {
	return false, ErrNotImplemented
}
func (s *SlackFileStore) HasMetadata(context.Context, string) (bool, error) {
	return false, ErrNotImplemented
}
func (s *SlackFileStore) HasObject(context.Context, string, string, string) (bool, error) {
	return false, ErrNotImplemented
}
func (s *SlackFileStore) Presence(context.Context, string) (ObjectPresence, error) {
	return ObjectPresence{}, ErrNotImplemented
}
func (s *SlackFileStore) PutFile(context.Context, PutFileInput) (StoredObject, error) {
	return StoredObject{}, ErrNotImplemented
}
func (s *SlackFileStore) PutJSON(context.Context, PutJSONInput) (StoredObject, error) {
	return StoredObject{}, ErrNotImplemented
}
func (s *SlackFileStore) DeleteObject(context.Context, StoredObject) error {
	return ErrNotImplemented
}
func (s *SlackFileStore) ListObjects(context.Context, ListQuery) ([]ObjectListing, error) {
	return nil, ErrNotImplemented
}
func (s *SlackFileStore) FindObject(context.Context, ListQuery) (*ObjectListing, error) {
	return nil, ErrNotImplemented
}
func (s *SlackFileStore) MoveObject(context.Context, StoredObject, string, map[string]string) (StoredObject, error) {
	return StoredObject{}, ErrNotImplemented
}

// WithSlackFiles wraps primary so reads of Slack file references — a Slack
// file id, or an explicit storage_backend of "slack" — are served by slack,
// while everything else (including all writes and discovery) stays on primary.
func WithSlackFiles(primary, slack ObjectStore) ObjectStore {
	return &slackRoutingStore{primary: primary, slack: slack}
}

type slackRoutingStore struct {
	primary ObjectStore
	slack   ObjectStore
}

func (s *slackRoutingStore) storeFor(ref StoredObject) ObjectStore {
	if ref.StorageBackend == "slack" || IsSlackFileID(ref.StorageFileID) {
		return s.slack
	}
	return s.primary
}

func (s *slackRoutingStore) Backend() string { return s.primary.Backend() }

func (s *slackRoutingStore) GetObject(ctx context.Context, ref StoredObject) ([]byte, error) {
	return s.storeFor(ref).GetObject(ctx, ref)
}
func (s *slackRoutingStore) ObjectExists(ctx context.Context, ref StoredObject) (bool, error) {
	return s.storeFor(ref).ObjectExists(ctx, ref)
}
func (s *slackRoutingStore) GetMetadata(ctx context.Context, ref StoredObject) (ObjectMetadata, error) {
	return s.storeFor(ref).GetMetadata(ctx, ref)
}
func (s *slackRoutingStore) GetShareURL(ctx context.Context, ref StoredObject) (string, error) {
	return s.storeFor(ref).GetShareURL(ctx, ref)
}
func (s *slackRoutingStore) DeleteObject(ctx context.Context, ref StoredObject) error {
	return s.storeFor(ref).DeleteObject(ctx, ref)
}

func (s *slackRoutingStore) HasBlob(ctx context.Context, contentSHA256 string) (bool, error) {
	return s.primary.HasBlob(ctx, contentSHA256)
}
func (s *slackRoutingStore) HasMetadata(ctx context.Context, contentSHA256 string) (bool, error) {
	return s.primary.HasMetadata(ctx, contentSHA256)
}
func (s *slackRoutingStore) HasObject(ctx context.Context, kind, key, value string) (bool, error) {
	return s.primary.HasObject(ctx, kind, key, value)
}
func (s *slackRoutingStore) Presence(ctx context.Context, contentSHA256 string) (ObjectPresence, error) {
	return s.primary.Presence(ctx, contentSHA256)
}
func (s *slackRoutingStore) PutFile(ctx context.Context, in PutFileInput) (StoredObject, error) {
	return s.primary.PutFile(ctx, in)
}
func (s *slackRoutingStore) PutJSON(ctx context.Context, in PutJSONInput) (StoredObject, error) {
	return s.primary.PutJSON(ctx, in)
}
func (s *slackRoutingStore) ListObjects(ctx context.Context, q ListQuery) ([]ObjectListing, error) {
	return s.primary.ListObjects(ctx, q)
}
func (s *slackRoutingStore) FindObject(ctx context.Context, q ListQuery) (*ObjectListing, error) {
	return s.primary.FindObject(ctx, q)
}
func (s *slackRoutingStore) MoveObject(ctx context.Context, ref StoredObject, newObjectKey string, appProperties map[string]string) (StoredObject, error) {
	return s.primary.MoveObject(ctx, ref, newObjectKey, appProperties)
}
