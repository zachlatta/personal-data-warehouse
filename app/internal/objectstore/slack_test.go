package objectstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeSlackFile is one file visible to a token in the slackFake.
type fakeSlackFile struct {
	Name         string
	Mimetype     string
	Size         int64
	Created      int64
	URLPrivate   string
	Permalink    string
	Deleted      bool
	IsExternal   bool
	Mode         string
	ExternalType string
	ExternalURL  string
}

// slackFake is an httpDoer that simulates the Slack Web API: files.info plus
// the files.slack.com url_private download host. Files are scoped per token to
// simulate multiple workspaces.
type slackFake struct {
	filesByToken       map[string]map[string]fakeSlackFile // token -> file id -> file
	downloads          map[string][]byte                   // url_private -> bytes
	htmlOnDownload     bool
	redirectOnDownload int
	redirectLocation   string
	failOnDownload     bool
	infoCalls          int
	downloadCalls      int
}

func (f *slackFake) Do(req *http.Request) (*http.Response, error) {
	token := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
	if strings.HasSuffix(req.URL.Path, "/files.info") {
		f.infoCalls++
		files, ok := f.filesByToken[token]
		if !ok {
			return slackJSONResponse(map[string]any{"ok": false, "error": "invalid_auth"}), nil
		}
		file, ok := files[req.URL.Query().Get("file")]
		if !ok {
			return slackJSONResponse(map[string]any{"ok": false, "error": "file_not_found"}), nil
		}
		return slackJSONResponse(map[string]any{"ok": true, "file": map[string]any{
			"id":            req.URL.Query().Get("file"),
			"name":          file.Name,
			"mimetype":      file.Mimetype,
			"size":          file.Size,
			"created":       file.Created,
			"url_private":   file.URLPrivate,
			"permalink":     file.Permalink,
			"is_deleted":    file.Deleted,
			"is_external":   file.IsExternal,
			"mode":          file.Mode,
			"external_type": file.ExternalType,
			"external_url":  file.ExternalURL,
		}}), nil
	}
	f.downloadCalls++
	if f.failOnDownload {
		return nil, errors.New("unexpected download request sent to transport")
	}
	if _, ok := f.filesByToken[token]; !ok {
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader("no auth"))}, nil
	}
	if f.redirectOnDownload != 0 {
		loc := f.redirectLocation
		if loc == "" {
			loc = "https://external.example/redirected"
		}
		return &http.Response{
			StatusCode: f.redirectOnDownload,
			Header:     http.Header{"Location": []string{loc}},
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	}
	if f.htmlOnDownload {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
			Body:       io.NopCloser(strings.NewReader("<html>login</html>")),
		}, nil
	}
	content, ok := f.downloads[req.URL.String()]
	if !ok {
		return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader("not found"))}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/octet-stream"}},
		Body:       io.NopCloser(bytes.NewReader(content)),
	}, nil
}

func slackJSONResponse(payload map[string]any) *http.Response {
	data, _ := json.Marshal(payload)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(data)),
	}
}

func slackTestStore(fake *slackFake, tokens ...string) *SlackFileStore {
	return NewSlackFileStore(SlackFileStoreOptions{Tokens: tokens, HTTPClient: fake})
}

func slackFakeWithFile(token, id string, file fakeSlackFile) *slackFake {
	return &slackFake{
		filesByToken: map[string]map[string]fakeSlackFile{token: {id: file}},
		downloads:    map[string][]byte{file.URLPrivate: []byte("file-bytes")},
	}
}

const slackTestFileID = "F0123ABCDEF"

func slackTestFile() fakeSlackFile {
	return fakeSlackFile{
		Name:       "screenshot.png",
		Mimetype:   "image/png",
		Size:       10,
		Created:    1_700_000_000,
		URLPrivate: "https://files.slack.com/files-pri/T01-" + slackTestFileID + "/screenshot.png",
		Permalink:  "https://example.slack.com/files/U01/" + slackTestFileID + "/screenshot.png",
	}
}

func TestSlackFileStoreBackend(t *testing.T) {
	if got := slackTestStore(&slackFake{}, "tok").Backend(); got != "slack" {
		t.Fatalf("Backend() = %q", got)
	}
}

func TestSlackFileStoreGetMetadata(t *testing.T) {
	file := slackTestFile()
	store := slackTestStore(slackFakeWithFile("tok", slackTestFileID, file), "tok")
	meta, err := store.GetMetadata(context.Background(), StoredObject{StorageFileID: slackTestFileID})
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	if meta.Backend != "slack" || meta.StorageFileID != slackTestFileID {
		t.Fatalf("unexpected ref fields: %+v", meta)
	}
	if meta.ContentType != "image/png" || meta.SizeBytes != 10 || meta.Filename != "screenshot.png" {
		t.Fatalf("unexpected file fields: %+v", meta)
	}
	if meta.StorageURL != file.Permalink {
		t.Fatalf("StorageURL = %q", meta.StorageURL)
	}
	if meta.CreatedTime != "2023-11-14T22:13:20Z" {
		t.Fatalf("CreatedTime = %q", meta.CreatedTime)
	}
}

func TestSlackFileStoreGetObject(t *testing.T) {
	store := slackTestStore(slackFakeWithFile("tok", slackTestFileID, slackTestFile()), "tok")
	data, err := store.GetObject(context.Background(), StoredObject{StorageFileID: slackTestFileID})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if string(data) != "file-bytes" {
		t.Fatalf("data = %q", data)
	}
}

func TestSlackFileStoreTriesTokensInOrder(t *testing.T) {
	file := slackTestFile()
	fake := slackFakeWithFile("tok2", slackTestFileID, file)
	fake.filesByToken["tok1"] = map[string]fakeSlackFile{} // authed, but file not in this workspace
	store := slackTestStore(fake, "tok1", "tok2")
	data, err := store.GetObject(context.Background(), StoredObject{StorageFileID: slackTestFileID})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if string(data) != "file-bytes" {
		t.Fatalf("data = %q", data)
	}
}

func TestSlackFileStoreNotFound(t *testing.T) {
	fake := &slackFake{filesByToken: map[string]map[string]fakeSlackFile{"tok": {}}}
	store := slackTestStore(fake, "tok")
	if _, err := store.GetMetadata(context.Background(), StoredObject{StorageFileID: slackTestFileID}); err != ErrNotFound {
		t.Fatalf("GetMetadata err = %v", err)
	}
	if _, err := store.GetObject(context.Background(), StoredObject{StorageFileID: slackTestFileID}); err != ErrNotFound {
		t.Fatalf("GetObject err = %v", err)
	}
	if exists, err := store.ObjectExists(context.Background(), StoredObject{StorageFileID: slackTestFileID}); err != nil || exists {
		t.Fatalf("ObjectExists = %v, %v", exists, err)
	}
}

func TestSlackFileStoreDeletedFileIsNotFound(t *testing.T) {
	file := slackTestFile()
	file.Deleted = true
	store := slackTestStore(slackFakeWithFile("tok", slackTestFileID, file), "tok")
	if _, err := store.GetMetadata(context.Background(), StoredObject{StorageFileID: slackTestFileID}); err != ErrNotFound {
		t.Fatalf("GetMetadata err = %v", err)
	}
}

func TestSlackFileStoreEmptyFileIDIsNotFound(t *testing.T) {
	store := slackTestStore(&slackFake{}, "tok")
	if _, err := store.GetMetadata(context.Background(), StoredObject{}); err != ErrNotFound {
		t.Fatalf("GetMetadata err = %v", err)
	}
}

func TestSlackFileStoreSurfacesAuthErrors(t *testing.T) {
	store := slackTestStore(&slackFake{filesByToken: map[string]map[string]fakeSlackFile{}}, "bad-token")
	_, err := store.GetMetadata(context.Background(), StoredObject{StorageFileID: slackTestFileID})
	if err == nil || err == ErrNotFound || !strings.Contains(err.Error(), "invalid_auth") {
		t.Fatalf("err = %v", err)
	}
}

func TestSlackFileStoreRejectsHTMLDownload(t *testing.T) {
	fake := slackFakeWithFile("tok", slackTestFileID, slackTestFile())
	fake.htmlOnDownload = true
	store := slackTestStore(fake, "tok")
	_, err := store.GetObject(context.Background(), StoredObject{StorageFileID: slackTestFileID})
	if err == nil || !strings.Contains(err.Error(), "HTML") {
		t.Fatalf("err = %v", err)
	}
}

func TestSlackFileStoreWritesNotImplemented(t *testing.T) {
	store := slackTestStore(&slackFake{}, "tok")
	if _, err := store.PutFile(context.Background(), PutFileInput{}); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("PutFile err = %v", err)
	}
	if err := store.DeleteObject(context.Background(), StoredObject{StorageFileID: slackTestFileID}); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("DeleteObject err = %v", err)
	}
}

func TestIsSlackFileID(t *testing.T) {
	cases := map[string]bool{
		slackTestFileID:                     true,
		"F02ABC123":                         true,
		"":                                  false,
		"fid":                               false,
		"f0123abcdef":                       false, // lowercase
		"U0123ABCDEF":                       false, // user id
		"1D_16sD1JLzhtGAIbGXKy3mr33G5mC5yW": false, // Drive file id
		"FAKEDRIVEFILEID0123456789012345":   false, // too long to be a Slack id
	}
	for id, want := range cases {
		if got := IsSlackFileID(id); got != want {
			t.Errorf("IsSlackFileID(%q) = %v, want %v", id, got, want)
		}
	}
}

// stubStore records which store a routed call landed on.
type stubStore struct {
	backend string
	lastRef *StoredObject
	meta    ObjectMetadata
	content []byte
}

func (s *stubStore) Backend() string { return s.backend }
func (s *stubStore) HasBlob(context.Context, string) (bool, error) {
	return false, ErrNotImplemented
}
func (s *stubStore) HasMetadata(context.Context, string) (bool, error) {
	return false, ErrNotImplemented
}
func (s *stubStore) HasObject(context.Context, string, string, string) (bool, error) {
	return false, ErrNotImplemented
}
func (s *stubStore) Presence(context.Context, string) (ObjectPresence, error) {
	return ObjectPresence{}, ErrNotImplemented
}
func (s *stubStore) PutFile(context.Context, PutFileInput) (StoredObject, error) {
	return StoredObject{StorageBackend: s.backend}, nil
}
func (s *stubStore) PutJSON(context.Context, PutJSONInput) (StoredObject, error) {
	return StoredObject{StorageBackend: s.backend}, nil
}
func (s *stubStore) GetObject(_ context.Context, ref StoredObject) ([]byte, error) {
	s.lastRef = &ref
	return s.content, nil
}
func (s *stubStore) ObjectExists(_ context.Context, ref StoredObject) (bool, error) {
	s.lastRef = &ref
	return true, nil
}
func (s *stubStore) DeleteObject(_ context.Context, ref StoredObject) error {
	s.lastRef = &ref
	return nil
}
func (s *stubStore) GetMetadata(_ context.Context, ref StoredObject) (ObjectMetadata, error) {
	s.lastRef = &ref
	return s.meta, nil
}
func (s *stubStore) GetShareURL(_ context.Context, ref StoredObject) (string, error) {
	s.lastRef = &ref
	return s.meta.StorageURL, nil
}
func (s *stubStore) ListObjects(context.Context, ListQuery) ([]ObjectListing, error) {
	return nil, ErrNotImplemented
}
func (s *stubStore) FindObject(context.Context, ListQuery) (*ObjectListing, error) {
	return nil, ErrNotImplemented
}
func (s *stubStore) MoveObject(context.Context, StoredObject, string, map[string]string) (StoredObject, error) {
	return StoredObject{}, ErrNotImplemented
}

func TestWithSlackFilesRoutesSlackFileIDsToSlack(t *testing.T) {
	primary := &stubStore{backend: "google_drive", content: []byte("drive")}
	slack := &stubStore{backend: "slack", content: []byte("slack")}
	store := WithSlackFiles(primary, slack)

	data, err := store.GetObject(context.Background(), StoredObject{StorageFileID: slackTestFileID})
	if err != nil || string(data) != "slack" {
		t.Fatalf("slack route: data %q err %v", data, err)
	}
	if slack.lastRef == nil || primary.lastRef != nil {
		t.Fatalf("expected slack store hit only: slack %v primary %v", slack.lastRef, primary.lastRef)
	}

	data, err = store.GetObject(context.Background(), StoredObject{StorageFileID: "1D_16sD1JLzhtGAIbGXKy3mr33G5mC5yW"})
	if err != nil || string(data) != "drive" {
		t.Fatalf("drive route: data %q err %v", data, err)
	}
	if primary.lastRef == nil {
		t.Fatal("expected primary store hit")
	}
}

func TestWithSlackFilesRoutesByBackendHint(t *testing.T) {
	primary := &stubStore{backend: "google_drive"}
	slack := &stubStore{backend: "slack"}
	store := WithSlackFiles(primary, slack)
	if _, err := store.GetMetadata(context.Background(), StoredObject{StorageBackend: "slack", StorageFileID: "weird-id"}); err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	if slack.lastRef == nil || primary.lastRef != nil {
		t.Fatalf("expected slack store hit only: slack %v primary %v", slack.lastRef, primary.lastRef)
	}
}

func TestWithSlackFilesBackendAndWritesUsePrimary(t *testing.T) {
	primary := &stubStore{backend: "google_drive"}
	store := WithSlackFiles(primary, &stubStore{backend: "slack"})
	if got := store.Backend(); got != "google_drive" {
		t.Fatalf("Backend() = %q", got)
	}
	ref, err := store.PutFile(context.Background(), PutFileInput{ObjectKey: "k"})
	if err != nil || ref.StorageBackend != "google_drive" {
		t.Fatalf("PutFile ref %+v err %v", ref, err)
	}
}

func TestSlackFileStoreBlocksExternalAndDisallowedURLsWithZeroDownloadRequests(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*fakeSlackFile)
		reason string
	}{
		{
			name:   "is_external_even_with_trusted_url",
			mutate: func(f *fakeSlackFile) { f.IsExternal = true },
			reason: "blocked:external",
		},
		{
			name:   "mode_external",
			mutate: func(f *fakeSlackFile) { f.Mode = "external" },
			reason: "blocked:external_mode",
		},
		{
			name:   "mode_remote",
			mutate: func(f *fakeSlackFile) { f.Mode = "REMOTE" },
			reason: "blocked:external_mode",
		},
		{
			name:   "external_type",
			mutate: func(f *fakeSlackFile) { f.ExternalType = "gdrive" },
			reason: "blocked:external",
		},
		{
			name:   "external_url",
			mutate: func(f *fakeSlackFile) { f.ExternalURL = "https://external.example/doc" },
			reason: "blocked:external",
		},
		{
			name:   "empty_url",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "" },
			reason: "blocked:url_empty",
		},
		{
			name:   "http_scheme",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "http://files.slack.com/files-pri/T01-F01/a.png" },
			reason: "blocked:scheme",
		},
		{
			name:   "scheme_relative",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "//files.slack.com/files-pri/T01-F01/a.png" },
			reason: "blocked:scheme",
		},
		{
			name:   "unknown_host",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://external.example/files-pri/T01-F01/a.png" },
			reason: "blocked:host",
		},
		{
			name:   "subdomain_not_allowed",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://sub.files.slack.com/files-pri/T01-F01/a.png" },
			reason: "blocked:host",
		},
		{
			name:   "suffix_spoof_host",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://evil-files.slack.com/files-pri/T01-F01/a.png" },
			reason: "blocked:host",
		},
		{
			name:   "trailing_dot_host",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://files.slack.com./files-pri/T01-F01/a.png" },
			reason: "blocked:host",
		},
		{
			name:   "non_ascii_host",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://fıles.slack.com/files-pri/T01-F01/a.png" },
			reason: "blocked:host",
		},
		{
			name:   "ipv4_literal",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://127.0.0.1/files-pri/T01-F01/a.png" },
			reason: "blocked:ip_literal",
		},
		{
			name:   "ipv6_literal",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://[::1]/files-pri/T01-F01/a.png" },
			reason: "blocked:ip_literal",
		},
		{
			name:   "non_default_port",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://files.slack.com:8443/files-pri/T01-F01/a.png" },
			reason: "blocked:port",
		},
		{
			name:   "empty_port",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://files.slack.com:/files-pri/T01-F01/a.png" },
			reason: "blocked:port",
		},
		{
			name:   "leading_zero_port",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://files.slack.com:0443/files-pri/T01-F01/a.png" },
			reason: "blocked:port",
		},
		{
			name:   "userinfo_at",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://files.slack.com@external.example/a.png" },
			reason: "blocked:userinfo",
		},
		{
			name:   "backslash_ambiguity",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://files.slack.com\\@external.example/a.png" },
			reason: "blocked:url_ambiguous",
		},
		{
			name:   "percent_in_authority",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://files.slack.com%2e/files-pri/T01-F01/a.png" },
			reason: "blocked:url_ambiguous",
		},
		{
			name:   "multiple_slashes",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https:///files.slack.com/files-pri/T01-F01/a.png" },
			reason: "blocked:url_ambiguous",
		},
		{
			name:   "whitespace_in_url",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = " https://files.slack.com/files-pri/T01-F01/a.png" },
			reason: "blocked:url_chars",
		},
		{
			name:   "control_char_in_url",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://files.slack.com/files-pri/T01-F01/a.png\n" },
			reason: "blocked:url_chars",
		},
		{
			name:   "missing_path",
			mutate: func(f *fakeSlackFile) { f.URLPrivate = "https://files.slack.com" },
			reason: "blocked:url_malformed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := slackTestFile()
			tc.mutate(&file)
			fake := slackFakeWithFile("tok", slackTestFileID, file)
			fake.failOnDownload = true
			store := slackTestStore(fake, "tok")

			if _, err := store.GetMetadata(context.Background(), StoredObject{StorageFileID: slackTestFileID}); !errors.Is(err, ErrPolicyBlocked) {
				t.Fatalf("GetMetadata err = %v, want ErrPolicyBlocked", err)
			} else if !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("GetMetadata err = %q, want reason %q", err.Error(), tc.reason)
			} else if strings.Contains(err.Error(), "external.example") {
				t.Fatalf("GetMetadata err leaked URL/host: %q", err.Error())
			}

			if _, err := store.GetObject(context.Background(), StoredObject{StorageFileID: slackTestFileID}); !errors.Is(err, ErrPolicyBlocked) {
				t.Fatalf("GetObject err = %v, want ErrPolicyBlocked", err)
			} else if !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("GetObject err = %q, want reason %q", err.Error(), tc.reason)
			} else if strings.Contains(err.Error(), "external.example") {
				t.Fatalf("GetObject err leaked URL/host: %q", err.Error())
			}

			if fake.downloadCalls != 0 {
				t.Fatalf("downloadCalls = %d, want 0", fake.downloadCalls)
			}
		})
	}
}

func TestSlackFileStoreAllowsNormalizedCaseAndDefaultPort443(t *testing.T) {
	for _, tc := range []struct {
		rawURL        string
		normalizedURL string
	}{
		{
			rawURL:        "https://FILES.SLACK.COM/files-pri/T01-" + slackTestFileID + "/screenshot.png",
			normalizedURL: "https://FILES.SLACK.COM/files-pri/T01-" + slackTestFileID + "/screenshot.png",
		},
		{
			rawURL:        "HTTPS://files.slack.com:443/files-pri/T01-" + slackTestFileID + "/screenshot.png",
			normalizedURL: "https://files.slack.com:443/files-pri/T01-" + slackTestFileID + "/screenshot.png",
		},
	} {
		file := slackTestFile()
		file.Mode = "hosted"
		file.URLPrivate = tc.rawURL
		fake := slackFakeWithFile("tok", slackTestFileID, file)
		fake.downloads[tc.normalizedURL] = []byte("file-bytes")
		store := slackTestStore(fake, "tok")
		data, err := store.GetObject(context.Background(), StoredObject{StorageFileID: slackTestFileID})
		if err != nil {
			t.Fatalf("GetObject(%q): %v", tc.rawURL, err)
		}
		if string(data) != "file-bytes" {
			t.Fatalf("data = %q", data)
		}
	}
}

func TestSlackFileStoreBlocksRedirectResponsesWithoutFollowing(t *testing.T) {
	for _, code := range []int{http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		fake := slackFakeWithFile("tok", slackTestFileID, slackTestFile())
		fake.redirectOnDownload = code
		store := slackTestStore(fake, "tok")
		_, err := store.GetObject(context.Background(), StoredObject{StorageFileID: slackTestFileID})
		if !errors.Is(err, ErrPolicyBlocked) || !strings.Contains(err.Error(), "blocked:redirect") {
			t.Fatalf("status %d: err = %v, want ErrPolicyBlocked(blocked:redirect)", code, err)
		}
		if fake.downloadCalls != 1 {
			t.Fatalf("status %d: downloadCalls = %d, want 1", code, fake.downloadCalls)
		}
	}
}

func TestSlackFileStoreHTTPClientNeverFollowsRedirects(t *testing.T) {
	secondHopHits := 0
	secondHop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHopHits++
		w.WriteHeader(http.StatusOK)
	}))
	defer secondHop.Close()

	firstHopHits := 0
	firstHop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHopHits++
		http.Redirect(w, r, secondHop.URL+"/steal", http.StatusFound)
	}))
	defer firstHop.Close()

	// Exercise the store's own redirect-disabled *http.Client directly.
	store := NewSlackFileStore(SlackFileStoreOptions{Tokens: []string{"tok"}, HTTPClient: &http.Client{}})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, firstHop.URL+"/start", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := store.client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302", resp.StatusCode)
	}
	if firstHopHits != 1 || secondHopHits != 0 {
		t.Fatalf("firstHopHits=%d secondHopHits=%d, want 1 and 0", firstHopHits, secondHopHits)
	}
}
