package objectstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// httpDoer is the subset of *http.Client the Drive backend needs. Tests inject
// a fake to simulate the Drive REST API without network access.
type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

const (
	defaultDriveAPIBaseURL    = "https://www.googleapis.com/drive/v3"
	defaultDriveUploadBaseURL = "https://www.googleapis.com/upload/drive/v3"
	driveFolderMimeType       = "application/vnd.google-apps.folder"
)

var googleNativeExportMimes = map[string]string{
	"application/vnd.google-apps.document":     "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	"application/vnd.google-apps.presentation": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	"application/vnd.google-apps.spreadsheet":  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

var googleNativeExportExtensions = map[string]string{
	"application/vnd.google-apps.document":     ".docx",
	"application/vnd.google-apps.presentation": ".pptx",
	"application/vnd.google-apps.spreadsheet":  ".xlsx",
}

// GoogleNativeExportMime returns the Office Open XML export MIME type used
// when a Google native file (Docs/Slides/Sheets) is fetched through the
// object-download proxy, so callers get a .docx/.pptx/.xlsx they can hand
// straight to a Word/PowerPoint/Excel-compatible reader instead of lossy
// plain text/CSV.
func GoogleNativeExportMime(mimeType string) (string, bool) {
	exportMime, ok := googleNativeExportMimes[mimeType]
	return exportMime, ok
}

// GoogleNativeExportFilename appends the file extension matching
// GoogleNativeExportMime's export format (.docx/.pptx/.xlsx) to filename, so
// a served or downloaded export carries the right extension for its actual
// bytes. Non-native mime types and filenames that already end with the
// extension are returned unchanged.
func GoogleNativeExportFilename(mimeType, filename string) string {
	ext, ok := googleNativeExportExtensions[mimeType]
	if !ok || filename == "" || strings.HasSuffix(strings.ToLower(filename), ext) {
		return filename
	}
	return filename + ext
}

// GoogleDriveOptions configures a GoogleDriveStore. The HTTPClient must be
// authenticated for the Drive scope (see credentials.go); APIBaseURL and
// UploadBaseURL default to the public Google endpoints and exist for testing.
type GoogleDriveOptions struct {
	FolderID      string
	HTTPClient    httpDoer
	Source        string
	LegacySources []string
	AudioKind     string
	MetadataKind  string
	MaxAttempts   int
	APIBaseURL    string
	UploadBaseURL string
}

// GoogleDriveStore implements ObjectStore against the Google Drive REST API.
type GoogleDriveStore struct {
	folderID      string
	client        httpDoer
	source        string
	legacySources []string
	audioKind     string
	metadataKind  string
	maxAttempts   int
	apiBaseURL    string
	uploadBaseURL string

	folderMu    sync.Mutex
	folderCache map[string]string
}

// NewGoogleDriveStore builds a Drive-backed ObjectStore.
func NewGoogleDriveStore(opts GoogleDriveOptions) *GoogleDriveStore {
	source := opts.Source
	if source == "" {
		source = "apple_voice_memos"
	}
	audioKind := opts.AudioKind
	if audioKind == "" {
		audioKind = "voice_memo_audio"
	}
	metadataKind := opts.MetadataKind
	if metadataKind == "" {
		metadataKind = "voice_memo_metadata"
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	apiBaseURL := strings.TrimRight(opts.APIBaseURL, "/")
	if apiBaseURL == "" {
		apiBaseURL = defaultDriveAPIBaseURL
	}
	uploadBaseURL := strings.TrimRight(opts.UploadBaseURL, "/")
	if uploadBaseURL == "" {
		uploadBaseURL = defaultDriveUploadBaseURL
	}
	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &GoogleDriveStore{
		folderID:      opts.FolderID,
		client:        client,
		source:        source,
		legacySources: opts.LegacySources,
		audioKind:     audioKind,
		metadataKind:  metadataKind,
		maxAttempts:   maxAttempts,
		apiBaseURL:    apiBaseURL,
		uploadBaseURL: uploadBaseURL,
		folderCache:   map[string]string{},
	}
}

// Backend identifies this backend; matches the Python google_drive backend name.
func (s *GoogleDriveStore) Backend() string { return "google_drive" }

type driveFile struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	MimeType      string            `json:"mimeType"`
	Size          string            `json:"size"`
	CreatedTime   string            `json:"createdTime"`
	ModifiedTime  string            `json:"modifiedTime"`
	WebViewLink   string            `json:"webViewLink"`
	AppProperties map[string]string `json:"appProperties"`
	Parents       []string          `json:"parents"`
	Trashed       bool              `json:"trashed"`
}

type driveFileList struct {
	NextPageToken string      `json:"nextPageToken"`
	Files         []driveFile `json:"files"`
}

type driveError struct {
	Status int
	Body   string
}

func (e *driveError) Error() string {
	return fmt.Sprintf("drive request failed: status %d: %s", e.Status, e.Body)
}

func driveStatus(err error) int {
	var de *driveError
	if asDriveError(err, &de) {
		return de.Status
	}
	return 0
}

func asDriveError(err error, target **driveError) bool {
	for err != nil {
		if de, ok := err.(*driveError); ok {
			*target = de
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func isTransientStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// doRequest performs an HTTP request, retrying transient failures, and returns
// the response body. Non-2xx responses become *driveError.
func (s *GoogleDriveStore) doRequest(ctx context.Context, method, rawURL string, headers map[string]string, body []byte) ([]byte, error) {
	var lastErr error
	for attempt := 1; attempt <= s.maxAttempts; attempt++ {
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
		if err != nil {
			return nil, err
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			if attempt == s.maxAttempts {
				break
			}
			time.Sleep(backoff(attempt))
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			if attempt == s.maxAttempts {
				break
			}
			time.Sleep(backoff(attempt))
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}
		de := &driveError{Status: resp.StatusCode, Body: string(data)}
		if isTransientStatus(resp.StatusCode) && attempt < s.maxAttempts {
			lastErr = de
			time.Sleep(backoff(attempt))
			continue
		}
		return nil, de
	}
	return nil, lastErr
}

func backoff(attempt int) time.Duration {
	if attempt > 30 {
		attempt = 30
	}
	return time.Duration(attempt) * time.Millisecond
}

func (s *GoogleDriveStore) getJSON(ctx context.Context, rawURL string, out any) error {
	data, err := s.doRequest(ctx, http.MethodGet, rawURL, nil, nil)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func (s *GoogleDriveStore) listFiles(ctx context.Context, query string, pageSize int, pageToken string) (driveFileList, error) {
	values := url.Values{}
	values.Set("q", query)
	values.Set("pageSize", strconv.Itoa(pageSize))
	values.Set("fields", "nextPageToken,files(id,name,webViewLink,appProperties)")
	values.Set("supportsAllDrives", "true")
	values.Set("includeItemsFromAllDrives", "true")
	if pageToken != "" {
		values.Set("pageToken", pageToken)
	}
	var out driveFileList
	if err := s.getJSON(ctx, s.apiBaseURL+"/files?"+values.Encode(), &out); err != nil {
		return driveFileList{}, err
	}
	return out, nil
}

// --- dedup probes -----------------------------------------------------------

func (s *GoogleDriveStore) HasBlob(ctx context.Context, contentSHA256 string) (bool, error) {
	file, err := s.findByAppProperty(ctx, "content_sha256", contentSHA256, s.audioKind)
	if err != nil {
		return false, err
	}
	return file != nil, nil
}

func (s *GoogleDriveStore) HasMetadata(ctx context.Context, contentSHA256 string) (bool, error) {
	file, err := s.findByAppProperty(ctx, "audio_content_sha256", contentSHA256, s.metadataKind)
	if err != nil {
		return false, err
	}
	return file != nil, nil
}

func (s *GoogleDriveStore) HasObject(ctx context.Context, kind, key, value string) (bool, error) {
	file, err := s.findByAppProperty(ctx, key, value, kind)
	if err != nil {
		return false, err
	}
	return file != nil, nil
}

func (s *GoogleDriveStore) Presence(ctx context.Context, contentSHA256 string) (ObjectPresence, error) {
	escapedSHA := escapeQueryValue(contentSHA256)
	query := "trashed = false " +
		"and " + s.sourceQuery() + " " +
		fmt.Sprintf("and appProperties has { key='pdw_root_folder_id' and value='%s' } ", escapeQueryValue(s.folderID)) +
		"and (appProperties has { key='pdw_stage' and value='inbox' } or appProperties has { key='pdw_stage' and value='library' }) " +
		"and (" +
		fmt.Sprintf("(appProperties has { key='pdw_kind' and value='%s' } and appProperties has { key='content_sha256' and value='%s' }) ", escapeQueryValue(s.audioKind), escapedSHA) +
		"or " +
		fmt.Sprintf("(appProperties has { key='pdw_kind' and value='%s' } and appProperties has { key='audio_content_sha256' and value='%s' })", escapeQueryValue(s.metadataKind), escapedSHA) +
		")"
	list, err := s.listFiles(ctx, query, 10, "")
	if err != nil {
		return ObjectPresence{}, err
	}
	var presence ObjectPresence
	for _, file := range list.Files {
		switch file.AppProperties["pdw_kind"] {
		case s.audioKind:
			presence.AudioExists = true
		case s.metadataKind:
			presence.MetadataExists = true
		}
	}
	return presence, nil
}

func (s *GoogleDriveStore) findByAppProperty(ctx context.Context, key, value, kind string) (*driveFile, error) {
	query := "trashed = false " +
		"and " + s.sourceQuery() + " " +
		fmt.Sprintf("and appProperties has { key='pdw_root_folder_id' and value='%s' } ", escapeQueryValue(s.folderID)) +
		fmt.Sprintf("and appProperties has { key='pdw_kind' and value='%s' } ", escapeQueryValue(kind)) +
		"and (appProperties has { key='pdw_stage' and value='inbox' } or appProperties has { key='pdw_stage' and value='library' }) " +
		fmt.Sprintf("and appProperties has { key='%s' and value='%s' }", escapeQueryValue(key), escapeQueryValue(value))
	list, err := s.listFiles(ctx, query, 1, "")
	if err != nil {
		return nil, err
	}
	if len(list.Files) == 0 {
		return nil, nil
	}
	file := list.Files[0]
	return &file, nil
}

// --- writes -----------------------------------------------------------------

func (s *GoogleDriveStore) PutFile(ctx context.Context, in PutFileInput) (StoredObject, error) {
	if err := s.requireFolderID("PutFile"); err != nil {
		return StoredObject{}, err
	}
	kind := in.Kind
	if kind == "" {
		kind = s.audioKind
	}
	if !in.SkipExistingCheck {
		existing, err := s.findByAppProperty(ctx, "content_sha256", in.ContentSHA256, kind)
		if err != nil {
			return StoredObject{}, err
		}
		if existing != nil {
			return s.storedObject(*existing, in.ObjectKey), nil
		}
	}
	appProps := map[string]string{
		"pdw_source":         s.source,
		"pdw_kind":           kind,
		"pdw_root_folder_id": s.folderID,
		"pdw_stage":          objectStage(in.ObjectKey),
		"content_sha256":     in.ContentSHA256,
	}
	for k, v := range in.AppProperties {
		appProps[k] = v
	}
	parentID, err := s.ensureParentFolder(ctx, in.ObjectKey)
	if err != nil {
		return StoredObject{}, err
	}
	metadata := map[string]any{
		"name":          driveNameFromObjectKey(in.ObjectKey),
		"parents":       []string{parentID},
		"mimeType":      in.ContentType,
		"appProperties": appProps,
	}
	file, err := s.uploadMultipart(ctx, metadata, in.ContentType, in.Content)
	if err != nil {
		return StoredObject{}, err
	}
	return s.storedObject(file, in.ObjectKey), nil
}

func (s *GoogleDriveStore) PutJSON(ctx context.Context, in PutJSONInput) (StoredObject, error) {
	if err := s.requireFolderID("PutJSON"); err != nil {
		return StoredObject{}, err
	}
	kind := in.Kind
	if kind == "" {
		kind = s.metadataKind
	}
	if in.SourceContentSHA256 != "" && !in.SkipExistingCheck {
		existing, err := s.findByAppProperty(ctx, "audio_content_sha256", in.SourceContentSHA256, kind)
		if err != nil {
			return StoredObject{}, err
		}
		if existing != nil {
			return s.storedObject(*existing, in.ObjectKey), nil
		}
	}
	if in.SourceContentSHA256 == "" && !in.SkipExistingCheck {
		existing, err := s.findByAppProperty(ctx, "content_sha256", in.ContentSHA256, kind)
		if err != nil {
			return StoredObject{}, err
		}
		if existing != nil {
			return s.storedObject(*existing, in.ObjectKey), nil
		}
	}
	appProps := map[string]string{
		"pdw_source":         s.source,
		"pdw_kind":           kind,
		"pdw_root_folder_id": s.folderID,
		"pdw_stage":          objectStage(in.ObjectKey),
		"content_sha256":     in.ContentSHA256,
	}
	for k, v := range in.AppProperties {
		appProps[k] = v
	}
	if in.SourceContentSHA256 != "" {
		appProps["audio_content_sha256"] = in.SourceContentSHA256
	}
	parentID, err := s.ensureParentFolder(ctx, in.ObjectKey)
	if err != nil {
		return StoredObject{}, err
	}
	metadata := map[string]any{
		"name":          driveNameFromObjectKey(in.ObjectKey),
		"parents":       []string{parentID},
		"mimeType":      "application/json",
		"appProperties": appProps,
	}
	file, err := s.uploadMultipart(ctx, metadata, "application/json", in.Payload)
	if err != nil {
		return StoredObject{}, err
	}
	return s.storedObject(file, in.ObjectKey), nil
}

func (s *GoogleDriveStore) uploadMultipart(ctx context.Context, metadata map[string]any, mediaType string, content []byte) (driveFile, error) {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return driveFile{}, err
	}
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	metaHeader := textproto.MIMEHeader{}
	metaHeader.Set("Content-Type", "application/json; charset=UTF-8")
	metaPart, err := writer.CreatePart(metaHeader)
	if err != nil {
		return driveFile{}, err
	}
	if _, err := metaPart.Write(metadataJSON); err != nil {
		return driveFile{}, err
	}

	mediaHeader := textproto.MIMEHeader{}
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	mediaHeader.Set("Content-Type", mediaType)
	mediaPart, err := writer.CreatePart(mediaHeader)
	if err != nil {
		return driveFile{}, err
	}
	if _, err := mediaPart.Write(content); err != nil {
		return driveFile{}, err
	}
	if err := writer.Close(); err != nil {
		return driveFile{}, err
	}

	values := url.Values{}
	values.Set("uploadType", "multipart")
	values.Set("fields", "id,webViewLink")
	values.Set("supportsAllDrives", "true")
	headers := map[string]string{
		"Content-Type": "multipart/related; boundary=" + writer.Boundary(),
	}
	data, err := s.doRequest(ctx, http.MethodPost, s.uploadBaseURL+"/files?"+values.Encode(), headers, buf.Bytes())
	if err != nil {
		return driveFile{}, err
	}
	var file driveFile
	if err := json.Unmarshal(data, &file); err != nil {
		return driveFile{}, err
	}
	return file, nil
}

// --- reads / management -----------------------------------------------------

func (s *GoogleDriveStore) GetObject(ctx context.Context, ref StoredObject) ([]byte, error) {
	if ref.StorageFileID == "" {
		return nil, ErrNotFound
	}
	if metadata, err := s.GetMetadata(ctx, ref); err == nil {
		if exportMime, ok := GoogleNativeExportMime(metadata.ContentType); ok {
			values := url.Values{}
			values.Set("mimeType", exportMime)
			data, err := s.doRequest(ctx, http.MethodGet, s.apiBaseURL+"/files/"+url.PathEscape(ref.StorageFileID)+"/export?"+values.Encode(), nil, nil)
			if err != nil {
				if driveStatus(err) == http.StatusNotFound {
					return nil, ErrNotFound
				}
				return nil, err
			}
			return data, nil
		}
	} else if err != ErrNotFound {
		return nil, err
	}
	values := url.Values{}
	values.Set("alt", "media")
	values.Set("supportsAllDrives", "true")
	data, err := s.doRequest(ctx, http.MethodGet, s.apiBaseURL+"/files/"+url.PathEscape(ref.StorageFileID)+"?"+values.Encode(), nil, nil)
	if err != nil {
		if driveStatus(err) == http.StatusNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return data, nil
}

func (s *GoogleDriveStore) ObjectExists(ctx context.Context, ref StoredObject) (bool, error) {
	if ref.StorageFileID == "" {
		return false, nil
	}
	values := url.Values{}
	values.Set("fields", "id,trashed")
	values.Set("supportsAllDrives", "true")
	var file driveFile
	if err := s.getJSON(ctx, s.apiBaseURL+"/files/"+url.PathEscape(ref.StorageFileID)+"?"+values.Encode(), &file); err != nil {
		if driveStatus(err) == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return !file.Trashed, nil
}

func (s *GoogleDriveStore) DeleteObject(ctx context.Context, ref StoredObject) error {
	if ref.StorageFileID == "" {
		return nil
	}
	values := url.Values{}
	values.Set("supportsAllDrives", "true")
	_, err := s.doRequest(ctx, http.MethodDelete, s.apiBaseURL+"/files/"+url.PathEscape(ref.StorageFileID)+"?"+values.Encode(), nil, nil)
	if err != nil && driveStatus(err) != http.StatusNotFound {
		return err
	}
	return nil
}

func (s *GoogleDriveStore) GetMetadata(ctx context.Context, ref StoredObject) (ObjectMetadata, error) {
	if ref.StorageFileID == "" {
		return ObjectMetadata{}, ErrNotFound
	}
	values := url.Values{}
	values.Set("fields", "id,name,mimeType,size,createdTime,modifiedTime,webViewLink,appProperties")
	values.Set("supportsAllDrives", "true")
	var file driveFile
	if err := s.getJSON(ctx, s.apiBaseURL+"/files/"+url.PathEscape(ref.StorageFileID)+"?"+values.Encode(), &file); err != nil {
		if driveStatus(err) == http.StatusNotFound {
			return ObjectMetadata{}, ErrNotFound
		}
		return ObjectMetadata{}, err
	}
	var size int64
	if file.Size != "" {
		size, _ = strconv.ParseInt(file.Size, 10, 64)
	}
	url := file.WebViewLink
	if url == "" {
		url = ref.StorageURL
	}
	fileID := file.ID
	if fileID == "" {
		fileID = ref.StorageFileID
	}
	return ObjectMetadata{
		Backend:       s.Backend(),
		StorageKey:    ref.StorageKey,
		StorageFileID: fileID,
		ContentType:   file.MimeType,
		SizeBytes:     size,
		ContentSHA256: file.AppProperties["content_sha256"],
		Filename:      file.Name,
		CreatedTime:   file.CreatedTime,
		ModifiedTime:  file.ModifiedTime,
		StorageURL:    url,
	}, nil
}

func (s *GoogleDriveStore) GetShareURL(ctx context.Context, ref StoredObject) (string, error) {
	if ref.StorageURL != "" {
		return ref.StorageURL, nil
	}
	metadata, err := s.GetMetadata(ctx, ref)
	if err != nil {
		return "", err
	}
	return metadata.StorageURL, nil
}

// --- discovery / relocation -------------------------------------------------

func (s *GoogleDriveStore) ListObjects(ctx context.Context, q ListQuery) ([]ObjectListing, error) {
	if err := s.requireFolderID("ListObjects"); err != nil {
		return nil, err
	}
	query := s.objectsQuery(q)
	var listings []ObjectListing
	pageToken := ""
	for {
		list, err := s.listFiles(ctx, query, 1000, pageToken)
		if err != nil {
			return nil, err
		}
		for _, file := range list.Files {
			if file.ID == "" {
				continue
			}
			listings = append(listings, s.listingFromFile(file))
		}
		pageToken = list.NextPageToken
		if pageToken == "" {
			return listings, nil
		}
	}
}

func (s *GoogleDriveStore) FindObject(ctx context.Context, q ListQuery) (*ObjectListing, error) {
	if err := s.requireFolderID("FindObject"); err != nil {
		return nil, err
	}
	query := s.objectsQuery(q)
	list, err := s.listFiles(ctx, query, 1, "")
	if err != nil {
		return nil, err
	}
	if len(list.Files) == 0 || list.Files[0].ID == "" {
		return nil, nil
	}
	listing := s.listingFromFile(list.Files[0])
	return &listing, nil
}

func (s *GoogleDriveStore) MoveObject(ctx context.Context, ref StoredObject, newObjectKey string, appProperties map[string]string) (StoredObject, error) {
	if err := s.requireFolderID("MoveObject"); err != nil {
		return StoredObject{}, err
	}
	if ref.StorageFileID == "" {
		return StoredObject{}, ErrNotFound
	}
	if newObjectKey == "" {
		return StoredObject{}, fmt.Errorf("objectstore: MoveObject requires a non-empty new object key")
	}
	parentID, err := s.ensureParentFolder(ctx, newObjectKey)
	if err != nil {
		return StoredObject{}, err
	}
	existingValues := url.Values{}
	existingValues.Set("fields", "parents")
	existingValues.Set("supportsAllDrives", "true")
	var existing driveFile
	if err := s.getJSON(ctx, s.apiBaseURL+"/files/"+url.PathEscape(ref.StorageFileID)+"?"+existingValues.Encode(), &existing); err != nil {
		return StoredObject{}, err
	}
	var removeParents []string
	for _, parent := range existing.Parents {
		if parent != parentID {
			removeParents = append(removeParents, parent)
		}
	}
	appProps := map[string]string{"pdw_stage": objectStage(newObjectKey)}
	for k, v := range appProperties {
		appProps[k] = v
	}
	body, err := json.Marshal(map[string]any{
		"name":          driveNameFromObjectKey(newObjectKey),
		"appProperties": appProps,
	})
	if err != nil {
		return StoredObject{}, err
	}
	values := url.Values{}
	values.Set("addParents", parentID)
	if len(removeParents) > 0 {
		values.Set("removeParents", strings.Join(removeParents, ","))
	}
	values.Set("fields", "id,webViewLink,appProperties")
	values.Set("supportsAllDrives", "true")
	headers := map[string]string{"Content-Type": "application/json"}
	data, err := s.doRequest(ctx, http.MethodPatch, s.apiBaseURL+"/files/"+url.PathEscape(ref.StorageFileID)+"?"+values.Encode(), headers, body)
	if err != nil {
		return StoredObject{}, err
	}
	var file driveFile
	if err := json.Unmarshal(data, &file); err != nil {
		return StoredObject{}, err
	}
	fileID := file.ID
	if fileID == "" {
		fileID = ref.StorageFileID
	}
	return StoredObject{
		StorageBackend: s.Backend(),
		StorageKey:     newObjectKey,
		StorageFileID:  fileID,
		StorageURL:     file.WebViewLink,
	}, nil
}

func (s *GoogleDriveStore) objectsQuery(q ListQuery) string {
	query := "trashed = false " +
		"and " + s.sourceQuery() + " " +
		fmt.Sprintf("and appProperties has { key='pdw_root_folder_id' and value='%s' } ", escapeQueryValue(s.folderID)) +
		fmt.Sprintf("and appProperties has { key='pdw_kind' and value='%s' } ", escapeQueryValue(q.Kind))
	if q.Stage != "" {
		query += fmt.Sprintf("and appProperties has { key='pdw_stage' and value='%s' } ", escapeQueryValue(q.Stage))
	}
	for _, key := range sortedKeys(q.Properties) {
		query += fmt.Sprintf("and appProperties has { key='%s' and value='%s' } ", escapeQueryValue(key), escapeQueryValue(q.Properties[key]))
	}
	return strings.TrimSpace(query)
}

func (s *GoogleDriveStore) listingFromFile(file driveFile) ObjectListing {
	props := map[string]string{}
	for k, v := range file.AppProperties {
		props[k] = v
	}
	return ObjectListing{
		Ref: StoredObject{
			StorageBackend: s.Backend(),
			StorageFileID:  file.ID,
			StorageURL:     file.WebViewLink,
		},
		AppProperties: props,
		Filename:      file.Name,
	}
}

func (s *GoogleDriveStore) storedObject(file driveFile, objectKey string) StoredObject {
	return StoredObject{
		StorageBackend: s.Backend(),
		StorageKey:     objectKey,
		StorageFileID:  file.ID,
		StorageURL:     file.WebViewLink,
	}
}

func (s *GoogleDriveStore) requireFolderID(operation string) error {
	if s.folderID == "" {
		return fmt.Errorf("objectstore: google_drive %s requires a folder id", operation)
	}
	return nil
}

func (s *GoogleDriveStore) sourceQuery() string {
	sources := []string{s.source}
	seen := map[string]bool{s.source: true}
	for _, src := range s.legacySources {
		if !seen[src] {
			sources = append(sources, src)
			seen[src] = true
		}
	}
	clauses := make([]string, 0, len(sources))
	for _, src := range sources {
		clauses = append(clauses, fmt.Sprintf("appProperties has { key='pdw_source' and value='%s' }", escapeQueryValue(src)))
	}
	if len(clauses) == 1 {
		return clauses[0]
	}
	return "(" + strings.Join(clauses, " or ") + ")"
}

func (s *GoogleDriveStore) ensureParentFolder(ctx context.Context, objectKey string) (string, error) {
	parts := splitNonEmpty(objectKey, "/")
	if len(parts) > 0 {
		parts = parts[:len(parts)-1] // drop filename
	}
	parentID := s.folderID
	for _, name := range parts {
		next, err := s.ensureFolder(ctx, parentID, name)
		if err != nil {
			return "", err
		}
		parentID = next
	}
	return parentID, nil
}

func (s *GoogleDriveStore) ensureFolder(ctx context.Context, parentID, name string) (string, error) {
	cacheKey := parentID + "\x00" + name
	s.folderMu.Lock()
	if id, ok := s.folderCache[cacheKey]; ok {
		s.folderMu.Unlock()
		return id, nil
	}
	s.folderMu.Unlock()

	query := fmt.Sprintf("'%s' in parents and trashed = false and mimeType = '%s' and name = '%s'",
		escapeQueryValue(parentID), driveFolderMimeType, escapeQueryValue(name))
	list, err := s.listFiles(ctx, query, 1, "")
	if err != nil {
		return "", err
	}
	var folderID string
	if len(list.Files) > 0 && list.Files[0].ID != "" {
		folderID = list.Files[0].ID
	} else {
		metadata := map[string]any{
			"name":     name,
			"parents":  []string{parentID},
			"mimeType": driveFolderMimeType,
			"appProperties": map[string]string{
				"pdw_source":         s.source,
				"pdw_kind":           "object_storage_prefix",
				"pdw_root_folder_id": s.folderID,
			},
		}
		body, err := json.Marshal(metadata)
		if err != nil {
			return "", err
		}
		values := url.Values{}
		values.Set("fields", "id,webViewLink")
		values.Set("supportsAllDrives", "true")
		headers := map[string]string{"Content-Type": "application/json"}
		data, err := s.doRequest(ctx, http.MethodPost, s.apiBaseURL+"/files?"+values.Encode(), headers, body)
		if err != nil {
			return "", err
		}
		var file driveFile
		if err := json.Unmarshal(data, &file); err != nil {
			return "", err
		}
		folderID = file.ID
	}
	s.folderMu.Lock()
	s.folderCache[cacheKey] = folderID
	s.folderMu.Unlock()
	return folderID, nil
}
