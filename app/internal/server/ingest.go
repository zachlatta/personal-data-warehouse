package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
)

// ingestPathPrefix is the mount point for the semantic per-source upload
// endpoints. Clients POST domain payloads here; the app owns the object-store
// layout (folder ids, object keys, kinds, and pdw_* tags) so no storage
// internals leak to client devices.
const ingestPathPrefix = "/ingest/"

const (
	photoResumableEndpoint = "/ingest/photos/file/resumable"
	photoChunkSizeBytes    = 16 * 1024 * 1024
)

// ingestSourceDef describes the object-store identity for one ingestion source.
// Source and LegacySources must match the values the Dagster *_drive_ingest
// readers query by (pdw_source), or ingested objects become invisible to them.
type ingestSourceDef struct {
	source        string
	blobKind      string
	metadataKind  string
	legacySources []string
}

// ingestSourceDefs mirrors the google_drive_spec(...) the Python clients used to
// build. The blob/metadata kinds only seed the store's default kind; every
// handler passes an explicit kind, so the per-artifact kind is authoritative.
var ingestSourceDefs = map[string]ingestSourceDef{
	"agent_sessions":    {source: "agent_sessions", blobKind: "agent_sessions_blob", metadataKind: "agent_sessions_export_batch"},
	"apple_contacts":    {source: "apple_contacts", blobKind: "apple_contact_blob", metadataKind: "apple_contact_export_batch"},
	"apple_messages":    {source: "apple_messages", blobKind: "apple_message_attachment", metadataKind: "apple_message_export_batch"},
	"apple_voice_memos": {source: "apple_voice_memos", blobKind: "voice_memo_audio", metadataKind: "voice_memo_metadata", legacySources: []string{"voice_memos"}},
	"apple_notes":       {source: "apple_notes", blobKind: "apple_note_body_html", metadataKind: "apple_note_revision_metadata"},
	// One shared transport for every photo source (apple_photos now;
	// google_photos / photo_imports later): the metadata envelope carries the
	// photo source slug and the Dagster reader routes rows into that source's
	// raw table.
	"photos": {source: "photos", blobKind: "photo_file", metadataKind: "photo_metadata"},
	// Manually uploaded finance documents (statements, valuations, exports).
	"manual_finance": {source: "manual_finance", blobKind: "manual_finance_document", metadataKind: "manual_finance_metadata"},
}

// ingestBuildResult is what an artifact's build step resolves from the request's
// domain fields. The handler turns it into a PutFile/PutJSON call; the store
// adds the pdw_* tags and content_sha256 itself.
type ingestBuildResult struct {
	objectKey        string
	contentType      string
	appProperties    map[string]string
	sourceContentSHA string // PutJSON only
}

// ingestArtifact is one semantic upload endpoint backed by a single stored
// object. build receives the request query and the sha the app computed over
// the received body.
type ingestArtifact struct {
	endpoint     string
	sourceSlug   string
	kind         string
	isJSON       bool
	skipExisting bool
	build        func(q url.Values, sha string, now time.Time) (ingestBuildResult, error)
}

// ingestService wires the configured per-source object stores to the artifact
// registry and verifies upload signatures.
type ingestService struct {
	artifacts map[string]ingestArtifact
	stores    map[string]objectstore.ObjectStore
	signer    *pdwauth.Service
	maxBytes  int64
	now       func() time.Time
	logger    *slog.Logger
}

// newIngestService builds the ingestion service from config. It returns
// (nil, false) when ingestion is not configured, and skips any artifact whose
// source has no resolvable Drive folder (logging the omission).
func newIngestService(cfg config.Config, signer *pdwauth.Service, now func() time.Time, logger *slog.Logger) (*ingestService, bool, error) {
	if !cfg.IngestEnabled() {
		return nil, false, nil
	}
	conn, err := driveConnectionFromConfig(cfg)
	if err != nil {
		return nil, false, err
	}
	stores := map[string]objectstore.ObjectStore{}
	for slug, def := range ingestSourceDefs {
		folder := cfg.IngestFolderIDs[slug]
		if folder == "" {
			continue
		}
		store, err := objectstore.BuildObjectStore(objectstore.GoogleDriveSpec(
			folder, def.source, def.blobKind, def.metadataKind, def.legacySources,
			conn,
		))
		if err != nil {
			return nil, false, fmt.Errorf("build ingest store for %s: %w", slug, err)
		}
		stores[slug] = store
	}
	artifacts := map[string]ingestArtifact{}
	for _, a := range ingestArtifacts() {
		if _, ok := stores[a.sourceSlug]; !ok {
			logger.Warn("ingestion endpoint disabled: no folder configured", "endpoint", a.endpoint, "source", a.sourceSlug)
			continue
		}
		artifacts[a.endpoint] = a
	}
	if len(artifacts) == 0 {
		return nil, false, nil
	}
	return &ingestService{artifacts: artifacts, stores: stores, signer: signer, maxBytes: cfg.IngestMaxObjectBytes, now: now, logger: logger}, true, nil
}

type ingestResponse struct {
	StorageBackend string `json:"storage_backend"`
	StorageKey     string `json:"storage_key"`
	StorageFileID  string `json:"storage_file_id"`
	StorageURL     string `json:"storage_url,omitempty"`
}

type photoResumableRequest struct {
	CapturedAt    string `json:"captured_at"`
	Extension     string `json:"extension"`
	ContentType   string `json:"content_type"`
	ContentSHA256 string `json:"content_sha256"`
	SizeBytes     int64  `json:"size_bytes"`
}

type photoResumableResponse struct {
	Complete       bool   `json:"complete"`
	UploadURL      string `json:"upload_url,omitempty"`
	ChunkSizeBytes int64  `json:"chunk_size_bytes,omitempty"`
	StorageBackend string `json:"storage_backend"`
	StorageKey     string `json:"storage_key"`
	StorageFileID  string `json:"storage_file_id,omitempty"`
	StorageURL     string `json:"storage_url,omitempty"`
}

func (svc *ingestService) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		endpoint := path.Clean(r.URL.Path)
		if endpoint == photoResumableEndpoint {
			svc.handlePhotoResumable(w, r)
			return
		}
		artifact, ok := svc.artifacts[endpoint]
		if !ok {
			http.NotFound(w, r)
			return
		}
		body, actualSHA, ok := svc.readSignedBody(w, r, endpoint)
		if !ok {
			return
		}
		q := r.URL.Query()
		built, err := artifact.build(q, actualSHA, svc.now().UTC())
		if err != nil {
			svc.logger.WarnContext(r.Context(), "ingest request rejected", "endpoint", endpoint, "error", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		store := svc.stores[artifact.sourceSlug]
		var stored objectstore.StoredObject
		if artifact.isJSON {
			stored, err = store.PutJSON(r.Context(), objectstore.PutJSONInput{
				ObjectKey:           built.objectKey,
				Payload:             body,
				ContentSHA256:       actualSHA,
				SourceContentSHA256: built.sourceContentSHA,
				SkipExistingCheck:   artifact.skipExisting,
				AppProperties:       built.appProperties,
				Kind:                artifact.kind,
			})
		} else {
			stored, err = store.PutFile(r.Context(), objectstore.PutFileInput{
				ObjectKey:         built.objectKey,
				Content:           body,
				ContentSHA256:     actualSHA,
				ContentType:       built.contentType,
				SkipExistingCheck: artifact.skipExisting,
				AppProperties:     built.appProperties,
				Kind:              artifact.kind,
			})
		}
		if err != nil {
			svc.logger.ErrorContext(r.Context(), "ingest store write failed", "endpoint", endpoint, "error", err)
			http.Error(w, "object store error", http.StatusBadGateway)
			return
		}
		svc.logger.InfoContext(r.Context(), "ingest object stored", "endpoint", endpoint, "storage_key", stored.StorageKey, "bytes", len(body))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ingestResponse{
			StorageBackend: stored.StorageBackend,
			StorageKey:     stored.StorageKey,
			StorageFileID:  stored.StorageFileID,
			StorageURL:     stored.StorageURL,
		})
	})
}

func (svc *ingestService) readSignedBody(w http.ResponseWriter, r *http.Request, endpoint string) ([]byte, string, bool) {
	q := r.URL.Query()
	declaredSHA := q.Get("content_sha256")
	// The signature covers endpoint + content sha + exp, so it authorizes this
	// exact body for this exact endpoint until exp. A failure gets one generic
	// message and the sig never reaches logs.
	if err := svc.signer.VerifyObjectUpload(endpoint, declaredSHA, q.Get("exp"), q.Get("sig")); err != nil {
		svc.logger.WarnContext(r.Context(), "ingest upload link rejected", "endpoint", endpoint, "error", err)
		http.Error(w, "invalid or expired upload link", http.StatusForbidden)
		return nil, "", false
	}
	body, err := readLimited(r.Body, svc.maxBytes)
	if err == errTooLarge {
		http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
		return nil, "", false
	}
	if err != nil {
		http.Error(w, "could not read body", http.StatusBadRequest)
		return nil, "", false
	}
	actualSHA := hex.EncodeToString(sha256Sum(body))
	if actualSHA != declaredSHA {
		svc.logger.WarnContext(r.Context(), "ingest body sha mismatch", "endpoint", endpoint, "declared", declaredSHA, "actual", actualSHA)
		http.Error(w, "content_sha256 does not match body", http.StatusBadRequest)
		return nil, "", false
	}
	return body, actualSHA, true
}

var (
	sha256HexPattern      = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)
	photoExtensionPattern = regexp.MustCompile(`^\.[A-Za-z0-9]{1,16}$`)
)

func (svc *ingestService) handlePhotoResumable(w http.ResponseWriter, r *http.Request) {
	store, ok := svc.stores["photos"]
	if !ok {
		http.NotFound(w, r)
		return
	}
	resumableStore, ok := store.(objectstore.ResumableFileStore)
	if !ok {
		http.Error(w, "photo resumable uploads are unavailable", http.StatusNotImplemented)
		return
	}
	body, _, ok := svc.readSignedBody(w, r, photoResumableEndpoint)
	if !ok {
		return
	}
	var request photoResumableRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid resumable upload request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "invalid resumable upload request", http.StatusBadRequest)
		return
	}
	if !sha256HexPattern.MatchString(request.ContentSHA256) {
		http.Error(w, "invalid content_sha256", http.StatusBadRequest)
		return
	}
	request.ContentSHA256 = strings.ToLower(request.ContentSHA256)
	if request.SizeBytes <= 0 {
		http.Error(w, "size_bytes must be positive", http.StatusBadRequest)
		return
	}
	if !photoExtensionPattern.MatchString(request.Extension) {
		http.Error(w, "invalid extension", http.StatusBadRequest)
		return
	}
	capturedAt, err := parseTimestamp(request.CapturedAt)
	if err != nil {
		http.Error(w, "invalid captured_at", http.StatusBadRequest)
		return
	}
	contentType := request.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	objectKey := fmt.Sprintf(
		"photos/inbox/%04d/%02d/%s-%s%s",
		capturedAt.Year(),
		int(capturedAt.Month()),
		capturedAt.Format("2006-01-02"),
		request.ContentSHA256,
		request.Extension,
	)
	started, err := resumableStore.BeginResumableFileUpload(
		r.Context(),
		objectstore.ResumableFileUploadInput{
			ObjectKey:     objectKey,
			ContentSHA256: request.ContentSHA256,
			ContentType:   contentType,
			SizeBytes:     request.SizeBytes,
			Kind:          "photo_file",
		},
	)
	if err != nil {
		svc.logger.ErrorContext(r.Context(), "photo resumable upload initiation failed", "storage_key", objectKey, "error", err)
		http.Error(w, "object store error", http.StatusBadGateway)
		return
	}
	response := photoResumableResponse{
		StorageBackend: store.Backend(),
		StorageKey:     objectKey,
	}
	if started.Existing != nil {
		response.Complete = true
		response.StorageBackend = started.Existing.StorageBackend
		response.StorageKey = started.Existing.StorageKey
		response.StorageFileID = started.Existing.StorageFileID
		response.StorageURL = started.Existing.StorageURL
		svc.logger.InfoContext(r.Context(), "photo resumable upload dedup hit", "storage_key", response.StorageKey, "bytes", request.SizeBytes)
	} else {
		response.UploadURL = started.UploadURL
		response.ChunkSizeBytes = photoChunkSizeBytes
		svc.logger.InfoContext(r.Context(), "photo resumable upload session created", "storage_key", objectKey, "bytes", request.SizeBytes)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func sha256Sum(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

// --- object key builders (ported from the Python clients) -------------------

var ingestNonSafeRun = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// safeObjectKeyPart mirrors the Python safe_object_key_part: collapse runs of
// disallowed chars to "-", trim leading/trailing "." and "-", cap at 120, and
// fall back to "untitled".
func safeObjectKeyPart(value string) string {
	trimmed := strings.Trim(ingestNonSafeRun.ReplaceAllString(value, "-"), ".-")
	if len(trimmed) > 120 {
		trimmed = trimmed[:120]
	}
	if trimmed == "" {
		return "untitled"
	}
	return trimmed
}

// sanitizeAccountFolder normalizes the uploader-supplied account folder
// segment for manual-finance object keys (lowercase, safe key characters,
// "unsorted" when absent or fully unsafe).
func sanitizeAccountFolder(value string) string {
	part := safeObjectKeyPart(strings.ToLower(strings.TrimSpace(value)))
	if part == "untitled" {
		return "unsorted"
	}
	return part
}

// pythonSuffix mirrors pathlib.PurePath(filename).suffix: the extension
// including its dot, or "" when there is none (a leading-dot-only name, a
// trailing dot, or no dot all yield "").
func pythonSuffix(filename string) string {
	if idx := strings.LastIndexByte(filename, '/'); idx >= 0 {
		filename = filename[idx+1:]
	}
	i := strings.LastIndexByte(filename, '.')
	if i <= 0 || i == len(filename)-1 {
		return ""
	}
	return filename[i:]
}

// parseTimestampUTC parses an ISO-8601 timestamp and converts it to UTC. Used
// where the Python builder calls .astimezone(UTC) before formatting.
func parseTimestampUTC(value string) (time.Time, error) {
	t, err := parseTimestamp(value)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// parseTimestamp parses an ISO-8601 timestamp, accepting an explicit offset, a
// trailing Z, or a naive value (no offset, treated as wall-clock).
func parseTimestamp(value string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable timestamp %q", value)
}

// batchObjectKey builds the inbox batch key shared by agent-sessions and
// apple-messages:
// <prefix>/batches/YYYY/MM/<YYYYMMDDTHHMMSSZ>-<sha>.jsonl.gz
func batchObjectKey(prefix string, exportedAt time.Time, sha string) string {
	e := exportedAt.UTC()
	return fmt.Sprintf("%s/batches/%04d/%02d/%s-%s.jsonl.gz", prefix, e.Year(), int(e.Month()), e.Format("20060102T150405Z"), sha)
}

func required(q url.Values, key string) (string, error) {
	if v := q.Get(key); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("missing required field %q", key)
}

// ingestArtifacts is the registry of single-object semantic endpoints.
func ingestArtifacts() []ingestArtifact {
	batch := func(endpoint, slug, prefix, kind string) ingestArtifact {
		return ingestArtifact{
			endpoint:   endpoint,
			sourceSlug: slug,
			kind:       kind,
			build: func(q url.Values, sha string, now time.Time) (ingestBuildResult, error) {
				exportedAt, err := required(q, "exported_at")
				if err != nil {
					return ingestBuildResult{}, err
				}
				ts, err := parseTimestampUTC(exportedAt)
				if err != nil {
					return ingestBuildResult{}, err
				}
				return ingestBuildResult{
					objectKey:     batchObjectKey(prefix, ts, sha),
					contentType:   "application/gzip",
					appProperties: map[string]string{"batch_sha256": sha, "exported_at": exportedAt},
				}, nil
			},
		}
	}
	return []ingestArtifact{
		batch("/ingest/agent-sessions/batch", "agent_sessions", "agent-sessions/inbox", "agent_sessions_export_batch"),
		batch("/ingest/apple-contacts/batch", "apple_contacts", "apple-contacts/inbox", "apple_contact_export_batch"),
		batch("/ingest/apple-messages/batch", "apple_messages", "apple-messages/inbox", "apple_message_export_batch"),
		{
			endpoint:   "/ingest/apple-messages/attachment",
			sourceSlug: "apple_messages",
			kind:       "apple_message_attachment",
			build: func(q url.Values, sha string, now time.Time) (ingestBuildResult, error) {
				attachmentGUID, err := required(q, "attachment_guid")
				if err != nil {
					return ingestBuildResult{}, err
				}
				messageGUID := q.Get("message_guid")
				contentType := q.Get("content_type")
				created, err := parseTimestampUTC(q.Get("created_at"))
				if err != nil || created.Year() == 1970 {
					created = now
				}
				suffix := pythonSuffix(q.Get("filename"))
				if suffix == "" {
					suffix = ".bin"
				}
				key := fmt.Sprintf("apple-messages/inbox/attachments/%04d/%02d/%s-%s-%s%s",
					created.Year(), int(created.Month()), created.Format("2006-01-02"), safeObjectKeyPart(attachmentGUID), sha, suffix)
				return ingestBuildResult{
					objectKey:     key,
					contentType:   contentType,
					appProperties: map[string]string{"attachment_guid": attachmentGUID, "message_guid": messageGUID},
				}, nil
			},
		},
		// --- voice memos: audio blob + JSON metadata sidecar --------------
		{
			endpoint:   "/ingest/voice-memos/audio",
			sourceSlug: "apple_voice_memos",
			kind:       "voice_memo_audio",
			// The app dedups by content sha (stable), replacing the client's old
			// Drive presence probe; no probe endpoint is needed.
			build: func(q url.Values, sha string, now time.Time) (ingestBuildResult, error) {
				// recorded_at is used as wall-clock (the Python builder does NOT
				// convert to UTC), so the date in the key matches the recording.
				recordedAt, err := parseTimestamp(q.Get("recorded_at"))
				if err != nil {
					return ingestBuildResult{}, fmt.Errorf("invalid recorded_at: %w", err)
				}
				key := fmt.Sprintf("apple-voice-memos/inbox/%04d/%02d/%s-%s%s",
					recordedAt.Year(), int(recordedAt.Month()), recordedAt.Format("2006-01-02"), sha, q.Get("extension"))
				return ingestBuildResult{objectKey: key, contentType: q.Get("content_type"), appProperties: map[string]string{}}, nil
			},
		},
		{
			endpoint:   "/ingest/voice-memos/metadata",
			sourceSlug: "apple_voice_memos",
			kind:       "voice_memo_metadata",
			isJSON:     true,
			// Deduped by audio_content_sha256 (stable) via SourceContentSHA256,
			// so re-runs whose metadata json differs (uploaded_at) don't dup.
			build: func(q url.Values, sha string, now time.Time) (ingestBuildResult, error) {
				// The sidecar shares the audio's basename, which is keyed by the
				// AUDIO sha (not this JSON's sha).
				audioSHA, err := required(q, "audio_content_sha256")
				if err != nil {
					return ingestBuildResult{}, err
				}
				recordedAt, err := parseTimestamp(q.Get("recorded_at"))
				if err != nil {
					return ingestBuildResult{}, fmt.Errorf("invalid recorded_at: %w", err)
				}
				key := fmt.Sprintf("apple-voice-memos/inbox/%04d/%02d/%s-%s.json",
					recordedAt.Year(), int(recordedAt.Month()), recordedAt.Format("2006-01-02"), audioSHA)
				return ingestBuildResult{objectKey: key, sourceContentSHA: audioSHA, appProperties: map[string]string{}}, nil
			},
		},
		// --- photos: resumable file session + JSON metadata envelope ----------
		{
			endpoint:   "/ingest/photos/metadata",
			sourceSlug: "photos",
			kind:       "photo_metadata",
			isJSON:     true,
			// Deduped by the PROVENANCE sha — sha256 over
			// (source|account|native_id|role|file_sha) — not the file sha: the
			// same bytes can arrive from two photo sources and both envelopes
			// must survive, while a re-run of one source (whose envelope JSON
			// differs only in uploaded_at) must not dup.
			build: func(q url.Values, sha string, now time.Time) (ingestBuildResult, error) {
				fileSHA, err := required(q, "file_content_sha256")
				if err != nil {
					return ingestBuildResult{}, err
				}
				dedupSHA, err := required(q, "metadata_dedup_sha256")
				if err != nil {
					return ingestBuildResult{}, err
				}
				capturedAt, err := parseTimestamp(q.Get("captured_at"))
				if err != nil {
					return ingestBuildResult{}, fmt.Errorf("invalid captured_at: %w", err)
				}
				key := fmt.Sprintf("photos/inbox/%04d/%02d/%s-%s.json",
					capturedAt.Year(), int(capturedAt.Month()), capturedAt.Format("2006-01-02"), dedupSHA)
				return ingestBuildResult{
					objectKey:        key,
					sourceContentSHA: dedupSHA,
					appProperties:    map[string]string{"file_content_sha256": fileSHA},
				}, nil
			},
		},
		// --- manual finance: uploaded document blob + JSON metadata envelope
		{
			endpoint:   "/ingest/manual-finance/file",
			sourceSlug: "manual_finance",
			kind:       "manual_finance_document",
			// Deduped by content sha: re-uploading the same statement is a
			// no-op. Keys are account-keyed (the uploader's folder-per-account
			// organization) with a content-addressed suffix.
			build: func(q url.Values, sha string, now time.Time) (ingestBuildResult, error) {
				modifiedAt, err := parseTimestamp(q.Get("modified_at"))
				if err != nil {
					return ingestBuildResult{}, fmt.Errorf("invalid modified_at: %w", err)
				}
				folder := sanitizeAccountFolder(q.Get("account_folder"))
				key := fmt.Sprintf("manual-finance/inbox/%s/%s-%s%s",
					folder, modifiedAt.Format("2006-01-02"), sha, q.Get("extension"))
				return ingestBuildResult{objectKey: key, contentType: q.Get("content_type"), appProperties: map[string]string{}}, nil
			},
		},
		{
			endpoint:   "/ingest/manual-finance/metadata",
			sourceSlug: "manual_finance",
			kind:       "manual_finance_metadata",
			isJSON:     true,
			// Deduped by the provenance sha (photos pattern): a re-upload of
			// the same document claim (envelope differing only in uploaded_at
			// or original_path) must not dup.
			build: func(q url.Values, sha string, now time.Time) (ingestBuildResult, error) {
				fileSHA, err := required(q, "file_content_sha256")
				if err != nil {
					return ingestBuildResult{}, err
				}
				dedupSHA, err := required(q, "metadata_dedup_sha256")
				if err != nil {
					return ingestBuildResult{}, err
				}
				modifiedAt, err := parseTimestamp(q.Get("modified_at"))
				if err != nil {
					return ingestBuildResult{}, fmt.Errorf("invalid modified_at: %w", err)
				}
				folder := sanitizeAccountFolder(q.Get("account_folder"))
				key := fmt.Sprintf("manual-finance/inbox/%s/%s-%s.json",
					folder, modifiedAt.Format("2006-01-02"), dedupSHA)
				return ingestBuildResult{
					objectKey:        key,
					sourceContentSHA: dedupSHA,
					appProperties:    map[string]string{"file_content_sha256": fileSHA},
				}, nil
			},
		},
		// --- apple notes: body HTML + attachments + JSON revision metadata
		{
			endpoint:   "/ingest/apple-notes/body",
			sourceSlug: "apple_notes",
			kind:       "apple_note_body_html",
			// Deduped by html content sha (stable per revision body).
			build: func(q url.Values, sha string, now time.Time) (ingestBuildResult, error) {
				base, err := appleNotesRevisionBase(q)
				if err != nil {
					return ingestBuildResult{}, err
				}
				return ingestBuildResult{
					objectKey:     base + ".html",
					contentType:   "text/html",
					appProperties: map[string]string{"note_id": q.Get("note_id"), "revision_id": q.Get("revision_id")},
				}, nil
			},
		},
		{
			endpoint:   "/ingest/apple-notes/attachment",
			sourceSlug: "apple_notes",
			kind:       "apple_note_attachment",
			build: func(q url.Values, sha string, now time.Time) (ingestBuildResult, error) {
				base, err := appleNotesRevisionBase(q)
				if err != nil {
					return ingestBuildResult{}, err
				}
				attachmentID, err := required(q, "attachment_id")
				if err != nil {
					return ingestBuildResult{}, err
				}
				suffix := pythonSuffix(q.Get("filename"))
				if suffix == "" {
					suffix = ".bin"
				}
				key := fmt.Sprintf("%s/attachments/%s-%s%s", base, safeObjectKeyPart(attachmentID), sha, suffix)
				return ingestBuildResult{
					objectKey:   key,
					contentType: q.Get("content_type"),
					appProperties: map[string]string{
						"note_id":       q.Get("note_id"),
						"revision_id":   q.Get("revision_id"),
						"attachment_id": attachmentID,
					},
				}, nil
			},
		},
		{
			endpoint:     "/ingest/apple-notes/revision",
			sourceSlug:   "apple_notes",
			kind:         "apple_note_revision_metadata",
			isJSON:       true,
			skipExisting: true,
			build: func(q url.Values, sha string, now time.Time) (ingestBuildResult, error) {
				base, err := appleNotesRevisionBase(q)
				if err != nil {
					return ingestBuildResult{}, err
				}
				return ingestBuildResult{
					objectKey: base + ".json",
					appProperties: map[string]string{
						"note_id":             q.Get("note_id"),
						"revision_id":         q.Get("revision_id"),
						"note_content_sha256": q.Get("note_content_sha256"),
					},
				}, nil
			},
		},
	}
}

// appleNotesRevisionBase mirrors the Python revision_object_key_base:
// apple-notes/inbox/YYYY/MM/<safe note_id>/<revision_id> (modified_at in UTC).
func appleNotesRevisionBase(q url.Values) (string, error) {
	noteID, err := required(q, "note_id")
	if err != nil {
		return "", err
	}
	revisionID, err := required(q, "revision_id")
	if err != nil {
		return "", err
	}
	modified, err := parseTimestampUTC(q.Get("modified_at"))
	if err != nil {
		return "", fmt.Errorf("invalid modified_at: %w", err)
	}
	return fmt.Sprintf("apple-notes/inbox/%04d/%02d/%s/%s", modified.Year(), int(modified.Month()), safeObjectKeyPart(noteID), revisionID), nil
}

// errTooLarge is returned by readLimited when the body exceeds the cap.
var errTooLarge = fmt.Errorf("object exceeds size limit")

// readLimited reads up to maxBytes from r, returning errTooLarge if there are
// more bytes than the cap allows. A non-positive maxBytes means no limit.
func readLimited(r io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return io.ReadAll(r)
	}
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, errTooLarge
	}
	return data, nil
}
