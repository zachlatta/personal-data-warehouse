// Package manualfinance uploads finance documents (statements, valuation
// screenshots, fund position docs, CSV/OFX exports) through the app's
// /ingest/manual-finance endpoints. The uploader's folder-per-account
// organization is preserved as the envelope's original_path and the object
// key's account segment; no format-specific handling happens client-side.
package manualfinance

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const (
	SchemaVersion  = int64(1)
	DocumentSource = "manual"
	EvidenceSource = "manual_evidence"
)

// DocumentExtensions are the accepted file types.
var DocumentExtensions = []string{".pdf", ".png", ".jpg", ".jpeg", ".heic", ".csv", ".ofx", ".qfx", ".rtf"}

var extensionContentTypes = map[string]string{
	".pdf": "application/pdf", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
	".heic": "image/heic", ".csv": "text/csv", ".ofx": "application/x-ofx", ".qfx": "application/x-ofx", ".rtf": "text/rtf",
}

// ProvenanceDedupSHA256 is the stable dedup key for one document claim; the
// path is deliberately excluded so moving a file updates the hint instead of
// creating a second document.
func ProvenanceDedupSHA256(source, account, nativeID, fileContentSHA256 string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{source, account, nativeID, fileContentSHA256}, "|")))
	return hex.EncodeToString(sum[:])
}

// BuildDocumentMetadata builds the envelope. Fails fast on contract violations.
func BuildDocumentMetadata(source, account, filename, originalPath, mimeType string, sizeBytes int64, contentSHA256, uploadedAt, fileModifiedAt string) (map[string]any, error) {
	switch {
	case source == "":
		return nil, errors.New("source is required")
	case account == "":
		return nil, errors.New("account is required")
	case contentSHA256 == "":
		return nil, errors.New("content_sha256 is required")
	case filename == "":
		return nil, errors.New("filename is required")
	}
	return map[string]any{
		"schema_version": SchemaVersion,
		"source":         source,
		"account":        account,
		"uploaded_at":    uploadedAt,
		"file": map[string]any{
			"native_id":        contentSHA256,
			"filename":         filename,
			"original_path":    originalPath,
			"mime_type":        mimeType,
			"size_bytes":       sizeBytes,
			"content_sha256":   contentSHA256,
			"file_modified_at": fileModifiedAt,
		},
	}, nil
}

// DocumentContentType maps a path to its content type.
func DocumentContentType(path string) string {
	if known, ok := extensionContentTypes[strings.ToLower(filepath.Ext(path))]; ok {
		return known
	}
	if guessed := mime.TypeByExtension(filepath.Ext(path)); guessed != "" {
		if idx := strings.Index(guessed, ";"); idx >= 0 {
			guessed = guessed[:idx]
		}
		return guessed
	}
	return "application/octet-stream"
}

// Candidate is one file to upload.
type Candidate struct {
	Path          string
	OriginalPath  string
	AccountFolder string
}

// ResolveCandidates expands files/directories into upload candidates and the
// count of ignored (hidden or unsupported) files. original_path is relative to
// root (explicit, or each directory argument / a bare file's parent) and the
// account folder is its first directory component.
func ResolveCandidates(paths []string, root string) ([]Candidate, int, error) {
	var candidates []Candidate
	ignored := 0
	seen := map[string]bool{}
	add := func(filePath, base string) error {
		resolved, err := filepath.Abs(filePath)
		if err != nil {
			return err
		}
		if seen[resolved] {
			return nil
		}
		seen[resolved] = true
		name := filepath.Base(filePath)
		if strings.HasPrefix(name, ".") || !hasDocumentExtension(name) {
			ignored++
			return nil
		}
		baseAbs, err := filepath.Abs(base)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(baseAbs, resolved)
		if err != nil || strings.HasPrefix(relative, "..") {
			relative = name
		}
		relative = filepath.ToSlash(relative)
		parts := strings.Split(relative, "/")
		accountFolder := ""
		if len(parts) > 1 {
			accountFolder = parts[0]
		}
		candidates = append(candidates, Candidate{Path: resolved, OriginalPath: relative, AccountFolder: accountFolder})
		return nil
	}
	for _, path := range paths {
		expanded := common.ExpandUser(path)
		info, err := os.Stat(expanded)
		if err != nil {
			return nil, 0, fmt.Errorf("no such file or directory: %s", path)
		}
		if info.IsDir() {
			base := expanded
			if root != "" {
				base = common.ExpandUser(root)
			}
			var files []string
			_ = filepath.WalkDir(expanded, func(p string, d fs.DirEntry, err error) error {
				if err == nil && !d.IsDir() {
					if fi, err := d.Info(); err == nil && fi.Mode().IsRegular() {
						files = append(files, p)
					}
				}
				return nil
			})
			sort.Strings(files)
			for _, file := range files {
				if err := add(file, base); err != nil {
					return nil, 0, err
				}
			}
		} else {
			base := filepath.Dir(expanded)
			if root != "" {
				base = common.ExpandUser(root)
			}
			if err := add(expanded, base); err != nil {
				return nil, 0, err
			}
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].OriginalPath < candidates[j].OriginalPath })
	return candidates, ignored, nil
}

func hasDocumentExtension(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	for _, allowed := range DocumentExtensions {
		if ext == allowed {
			return true
		}
	}
	return false
}

// State is the per-document (content sha) upload state.
type State struct {
	db *common.StateDB
}

// DefaultStateFile is the state path.
func DefaultStateFile() string {
	return common.DefaultStateFile("manual-finance-upload-state.sqlite")
}

// OpenState opens the state for account.
func OpenState(path, account string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS upload_state (
			content_sha256 TEXT PRIMARY KEY,
			original_path TEXT NOT NULL DEFAULT '',
			complete INTEGER NOT NULL DEFAULT 0,
			last_success_at TEXT NOT NULL DEFAULT '',
			last_failure_at TEXT NOT NULL DEFAULT '',
			last_error TEXT NOT NULL DEFAULT ''
		)`,
	}, map[string]string{"schema_version": "1", "account": account}, []string{"upload_state"})
	if err != nil {
		return nil, err
	}
	return &State{db: db}, nil
}

// Close releases the database.
func (s *State) Close() error { return s.db.Close() }

// IsComplete reports whether the key was uploaded.
func (s *State) IsComplete(key string) (bool, error) {
	rows, err := common.Query(s.db.DB, "SELECT complete FROM upload_state WHERE content_sha256 = ?", key)
	if err != nil {
		return false, err
	}
	return len(rows) > 0 && rows[0].Bool("complete"), nil
}

// MarkSuccess records a completed upload.
func (s *State) MarkSuccess(key, originalPath string, now time.Time) error {
	_, err := s.db.DB.Exec(`INSERT INTO upload_state (content_sha256, original_path, complete, last_success_at, last_error)
		VALUES (?, ?, 1, ?, '')
		ON CONFLICT(content_sha256) DO UPDATE SET
			original_path = excluded.original_path, complete = 1,
			last_success_at = excluded.last_success_at, last_error = ''`, key, originalPath, common.ISOFormat(now))
	return err
}

// MarkFailure records a failed attempt.
func (s *State) MarkFailure(key, originalPath, errText string, now time.Time) error {
	_, err := s.db.DB.Exec(`INSERT INTO upload_state (content_sha256, original_path, complete, last_failure_at, last_error)
		VALUES (?, ?, 0, ?, ?)
		ON CONFLICT(content_sha256) DO UPDATE SET
			original_path = excluded.original_path,
			last_failure_at = excluded.last_failure_at, last_error = excluded.last_error`, key, originalPath, common.ISOFormat(now), errText)
	return err
}

// Summary is the run's counts.
type Summary struct {
	FilesSeen        int
	FilesIgnored     int
	FilesSelected    int
	FilesSkipped     int
	FilesUploaded    int
	MetadataUploaded int
	BytesUploaded    int64
}

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadManualFinanceDocument(content []byte, modifiedAt, accountFolder, extension, contentType string) (ingestclient.StoredObject, error)
	UploadManualFinanceMetadata(payload map[string]any, modifiedAt, accountFolder, fileContentSHA256, metadataDedupSHA256 string) (ingestclient.StoredObject, error)
}

// Runner uploads every not-yet-uploaded document.
type Runner struct {
	Account      string
	Paths        []string
	Root         string
	Client       Uploader
	Logger       common.Logger
	Now          func() time.Time
	Limit        int
	Mode         string
	State        *State
	EvidenceOnly bool
}

func (r *Runner) source() string {
	if r.EvidenceOnly {
		return EvidenceSource
	}
	return DocumentSource
}

// stateKey preserves existing statement state; evidence is a distinct
// provenance claim, so an earlier statement upload must not suppress it.
func (r *Runner) stateKey(sha string) string {
	if r.source() == DocumentSource {
		return sha
	}
	return r.source() + ":" + sha
}

// Sync runs one pass; per-file failures are collected and the first re-raised
// after the batch so the run exits non-zero while successes are recorded.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil {
		return Summary{}, errors.New("ingest client is required")
	}
	if r.Mode == "" {
		r.Mode = "incremental"
	}
	if r.Mode != "incremental" && r.Mode != "full" {
		return Summary{}, errors.New("mode must be 'full' or 'incremental'")
	}
	if len(r.Paths) == 0 {
		return Summary{}, errors.New("at least one file or directory is required")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	candidates, ignored, err := ResolveCandidates(r.Paths, r.Root)
	if err != nil {
		return Summary{}, err
	}
	r.Logger.Infof("Manual finance scan: %d document(s) found, %d ignored (hidden/unsupported)", len(candidates), ignored)

	type selection struct {
		candidate Candidate
		content   []byte
		sha       string
	}
	var selected []selection
	skipped := 0
	for _, candidate := range candidates {
		content, err := os.ReadFile(candidate.Path)
		if err != nil {
			return Summary{}, err
		}
		sha := common.BytesSHA256(content)
		if r.Mode == "incremental" && r.State != nil {
			complete, err := r.State.IsComplete(r.stateKey(sha))
			if err != nil {
				return Summary{}, err
			}
			if complete {
				skipped++
				continue
			}
		}
		selected = append(selected, selection{candidate, content, sha})
	}
	// The limit applies AFTER state selection so a capped run always makes
	// forward progress through the backlog.
	if r.Limit > 0 && len(selected) > r.Limit {
		selected = selected[:r.Limit]
	}
	r.Logger.Infof("Incremental selection: selected=%d skipped=%d", len(selected), skipped)

	var firstFailure error
	failures := 0
	summary := Summary{FilesSeen: len(candidates), FilesIgnored: ignored, FilesSelected: len(selected), FilesSkipped: skipped}
	for index, item := range selected {
		if err := r.uploadCandidate(index+1, len(selected), item.candidate, item.content, item.sha); err != nil {
			r.Logger.Warningf("Failed to upload %s: %v", item.candidate.OriginalPath, err)
			failures++
			if firstFailure == nil {
				firstFailure = err
			}
			if r.State != nil {
				_ = r.State.MarkFailure(r.stateKey(item.sha), item.candidate.OriginalPath, err.Error(), r.Now())
			}
			continue
		}
		summary.FilesUploaded++
		summary.MetadataUploaded++
		summary.BytesUploaded += int64(len(item.content))
	}
	r.Logger.Infof("Manual finance upload summary: seen=%d ignored=%d selected=%d uploaded=%d skipped=%d",
		summary.FilesSeen, summary.FilesIgnored, summary.FilesSelected, summary.FilesUploaded, summary.FilesSkipped)
	if firstFailure != nil {
		r.Logger.Warningf("Manual finance upload finished with %d failed file(s) after uploading %d; re-raising the first so the run is marked failed", failures, summary.FilesUploaded)
		return summary, firstFailure
	}
	return summary, nil
}

func (r *Runner) uploadCandidate(index, total int, candidate Candidate, content []byte, sha string) error {
	info, err := os.Stat(candidate.Path)
	if err != nil {
		return err
	}
	mtime := info.ModTime()
	modifiedAt := common.ISOFormat(common.UnixFromFloat(common.StatSeconds(mtime.Unix(), int64(mtime.Nanosecond()))))
	contentType := DocumentContentType(candidate.Path)
	r.Logger.Infof("[%d/%d] Uploading %s (%d bytes)", index, total, candidate.OriginalPath, len(content))
	extension := strings.ToLower(filepath.Ext(candidate.Path))
	if _, err := r.Client.UploadManualFinanceDocument(content, modifiedAt, candidate.AccountFolder, extension, contentType); err != nil {
		return err
	}
	envelope, err := BuildDocumentMetadata(r.source(), r.Account, filepath.Base(candidate.Path), candidate.OriginalPath, contentType, int64(len(content)), sha, common.ISOFormat(r.Now()), modifiedAt)
	if err != nil {
		return err
	}
	dedup := ProvenanceDedupSHA256(r.source(), r.Account, sha, sha)
	if _, err := r.Client.UploadManualFinanceMetadata(envelope, modifiedAt, candidate.AccountFolder, sha, dedup); err != nil {
		return err
	}
	if r.State != nil {
		return r.State.MarkSuccess(r.stateKey(sha), candidate.OriginalPath, r.Now())
	}
	return nil
}
