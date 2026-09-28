// Package muse uploads what Meta's hosted personal agent, Muse, keeps on its
// own VM: its chat transcripts (through the agent-sessions runner, as the
// `muse` tool) and its persistent workspace -- memory files, goal pages, feed
// research, podcasts, deliverables and what Zach attached in a chat.
//
// The VM has no inbound network and loses every process on restart, but its
// home directory persists and HTTPS leaves through Meta's egress proxy. So the
// uploader runs ON the VM, from a Muse hook (a Bash script the runtime polls,
// which can finish without waking the model), and posts through the app like
// every other remote-device uploader.
package muse

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const (
	// DefaultMaxInlineBytes is the largest text file that rides inline in a
	// batch record; anything larger is stored as a blob.
	DefaultMaxInlineBytes = 1 << 20
	// DefaultMaxBlobBytes is used when the caller has no route-specific
	// ceiling (the CLI passes the ingest client's effective one).
	DefaultMaxBlobBytes = 100 << 20
	// DefaultFileBatchRecords bounds records per uploaded batch.
	DefaultFileBatchRecords = 500
	// defaultBatchInlineBytes flushes a batch once its inline text grows past
	// this, so a burst of large memory files cannot build an unbounded body.
	defaultBatchInlineBytes = 16 << 20
)

// BatchUploader posts one gzipped JSONL batch.
type BatchUploader func(gzipBytes []byte, exportedAt time.Time) (ingestclient.StoredObject, error)

// BlobUploader posts one file's bytes and returns where the app stored them.
type BlobUploader func(content []byte, extension, contentType string) (ingestclient.StoredObject, error)

// Workspace snapshots the Muse home directory into muse_file records.
type Workspace struct {
	Home           string
	Account        string
	Device         string
	UploadBatch    BatchUploader
	UploadBlob     BlobUploader
	Logger         common.Logger
	State          *WorkspaceState // nil re-sends everything
	Now            func() time.Time
	Mode           string // incremental | full
	Limit          int    // changed files per run; 0 = unlimited
	MaxInlineBytes int64
	MaxBlobBytes   int64
	BatchRecords   int
}

// WorkspaceSummary is one run's counts.
type WorkspaceSummary struct {
	FilesSeen       int
	FilesChanged    int
	FilesInline     int
	FilesBlob       int
	FilesDeleted    int
	FilesTooLarge   int
	BatchesUploaded int
	LimitReached    bool
}

// skippedTopLevel are home entries that are not the agent's workspace: the
// transcripts (shipped line by line instead), Meta's product docs and static
// assets, runtime config, and anything that is only a lock or a log.
var skippedTopLevel = map[string]bool{
	"agents":       true,
	"docs":         true,
	"assets":       true,
	"config":       true,
	"runtime.lock": true,
}

// skippedDirs are relative directories never walked: the hook runtime and
// its own logs/state (which this very uploader rewrites every run), the
// browser profile (cookies and caches), bundled tool binaries, and the
// remote-access helpers (proxy plumbing, not the agent's work).
var skippedDirs = map[string]bool{
	"hooks/runtime":              true,
	"hooks/logs":                 true,
	"hooks/state":                true,
	"workspace/.hatch-browser":   true,
	"workspace/tools/vscode-cli": true,
	"workspace/remote-access":    true,
}

// skippedAnywhere are directory names skipped at any depth.
var skippedAnywhere = map[string]bool{".git": true, "node_modules": true, "__pycache__": true}

func skipDir(rel, name string) bool {
	if !strings.Contains(rel, "/") && (strings.HasPrefix(name, ".") || skippedTopLevel[name]) {
		return true
	}
	return skippedDirs[rel] || skippedAnywhere[name]
}

func skipFile(rel, name string) bool {
	if !strings.Contains(rel, "/") {
		if strings.HasPrefix(name, ".") || skippedTopLevel[name] {
			return true
		}
		if strings.HasPrefix(name, "tmate-") && strings.HasSuffix(name, ".log") {
			return true
		}
	}
	return strings.HasSuffix(name, ".lock")
}

type pendingFile struct {
	path    string
	size    int64
	mtimeNS int64
	sha     string
	deleted bool
}

type fileBatch struct {
	records     []map[string]any
	files       []pendingFile
	inlineBytes int64
}

// Sync walks the home directory once and ships what changed.
func (w *Workspace) Sync() (WorkspaceSummary, error) {
	if w.UploadBatch == nil || w.UploadBlob == nil {
		return WorkspaceSummary{}, errors.New("batch and blob uploaders are required")
	}
	if w.Now == nil {
		w.Now = func() time.Time { return time.Now().UTC() }
	}
	if w.Mode == "" {
		w.Mode = "incremental"
	}
	if w.MaxInlineBytes <= 0 {
		w.MaxInlineBytes = DefaultMaxInlineBytes
	}
	if w.MaxBlobBytes <= 0 {
		w.MaxBlobBytes = DefaultMaxBlobBytes
	}
	if w.BatchRecords <= 0 {
		w.BatchRecords = DefaultFileBatchRecords
	}
	home := filepath.Clean(w.Home)
	if info, err := os.Stat(home); err != nil || !info.IsDir() {
		return WorkspaceSummary{}, errors.New("muse home " + home + " is not a directory")
	}
	known := map[string]FileProgress{}
	if w.State != nil {
		var err error
		if known, err = w.State.All(); err != nil {
			return WorkspaceSummary{}, err
		}
	}

	summary := WorkspaceSummary{}
	batch := &fileBatch{}
	seen := map[string]bool{}
	walkErr := filepath.WalkDir(home, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path == home {
				return err
			}
			return nil
		}
		if path == home {
			return nil
		}
		rel, relErr := filepath.Rel(home, path)
		if relErr != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if skipDir(rel, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() || skipFile(rel, d.Name()) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		summary.FilesSeen++
		seen[rel] = true
		prior, hadPrior := known[rel]
		if w.Mode == "incremental" && hadPrior && prior.SizeBytes == info.Size() && prior.MtimeNS == info.ModTime().UnixNano() {
			return nil
		}
		if info.Size() > w.MaxBlobBytes {
			summary.FilesTooLarge++
			w.Logger.Warningf("Deferring %s: %s is above the %s upload ceiling", rel, common.FormatBytes(info.Size()), common.FormatBytes(w.MaxBlobBytes))
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			w.Logger.Warningf("Skipping unreadable %s: %v", rel, err)
			return nil
		}
		if bytes.HasPrefix(content, []byte("\x7fELF")) {
			return nil // an executable is a tool, not the agent's work
		}
		sha := common.BytesSHA256(content)
		file := pendingFile{path: rel, size: info.Size(), mtimeNS: info.ModTime().UnixNano(), sha: sha}
		if w.Mode == "incremental" && hadPrior && prior.ContentSHA256 == sha {
			// Touched, not changed: remember the new stat so it is not hashed again.
			if w.State != nil {
				return w.State.Record(file.path, file.size, file.mtimeNS, file.sha, w.Now())
			}
			return nil
		}
		if w.Limit > 0 && summary.FilesChanged >= w.Limit {
			summary.LimitReached = true
			return filepath.SkipAll
		}
		record, inline, err := w.fileRecord(rel, info, content, sha)
		if err != nil {
			return err
		}
		summary.FilesChanged++
		if inline {
			summary.FilesInline++
		} else {
			summary.FilesBlob++
		}
		return w.add(batch, record, file, &summary)
	})
	if walkErr != nil {
		return summary, walkErr
	}
	if !summary.LimitReached && w.State != nil {
		var gone []string
		for path := range known {
			if !seen[path] {
				gone = append(gone, path)
			}
		}
		sort.Strings(gone)
		for _, path := range gone {
			summary.FilesDeleted++
			record := w.envelope(map[string]any{
				"path": path, "content_sha256": "", "size_bytes": int64(0), "modified_at": common.ISOFormat(w.Now()),
				"mime_type": "", "is_text": false, "content_text": "", "storage_backend": "", "storage_key": "",
				"storage_file_id": "", "storage_url": "", "deleted": true,
			})
			if err := w.add(batch, record, pendingFile{path: path, deleted: true}, &summary); err != nil {
				return summary, err
			}
		}
	}
	if err := w.flush(batch, &summary); err != nil {
		return summary, err
	}
	w.Logger.Infof("Muse workspace upload summary: seen=%d changed=%d inline=%d blobs=%d deleted=%d too_large=%d batches=%d",
		summary.FilesSeen, summary.FilesChanged, summary.FilesInline, summary.FilesBlob, summary.FilesDeleted, summary.FilesTooLarge, summary.BatchesUploaded)
	return summary, nil
}

func (w *Workspace) fileRecord(rel string, info fs.FileInfo, content []byte, sha string) (map[string]any, bool, error) {
	name := filepath.Base(rel)
	mimeType := common.GuessMimeType(name)
	isText := int64(len(content)) <= w.MaxInlineBytes && utf8.Valid(content) && !bytes.Contains(content, []byte{0})
	if mimeType == "" {
		if isText {
			mimeType = "text/plain"
		} else {
			mimeType = "application/octet-stream"
		}
	}
	record := map[string]any{
		"path":           rel,
		"content_sha256": sha,
		"size_bytes":     info.Size(),
		"modified_at":    common.ISOFormat(info.ModTime().UTC()),
		"mime_type":      mimeType,
		"is_text":        isText,
		"content_text":   "",
		"deleted":        false,
	}
	stored := ingestclient.StoredObject{}
	if isText {
		record["content_text"] = string(content)
	} else {
		var err error
		stored, err = w.UploadBlob(content, strings.ToLower(filepath.Ext(name)), mimeType)
		if err != nil {
			return nil, false, err
		}
	}
	record["storage_backend"] = stored.StorageBackend
	record["storage_key"] = stored.StorageKey
	record["storage_file_id"] = stored.StorageFileID
	record["storage_url"] = stored.StorageURL
	return w.envelope(record), isText, nil
}

func (w *Workspace) envelope(record map[string]any) map[string]any {
	return map[string]any{
		"schema_version": int64(1),
		"source":         "agent_sessions",
		"account":        w.Account,
		"device":         w.Device,
		"exported_at":    common.ISOFormat(w.Now()),
		"record_type":    "muse_file",
		"record":         record,
	}
}

func (w *Workspace) add(batch *fileBatch, record map[string]any, file pendingFile, summary *WorkspaceSummary) error {
	batch.records = append(batch.records, record)
	batch.files = append(batch.files, file)
	if text, ok := record["record"].(map[string]any)["content_text"].(string); ok {
		batch.inlineBytes += int64(len(text))
	}
	if len(batch.records) >= w.BatchRecords || batch.inlineBytes >= defaultBatchInlineBytes {
		return w.flush(batch, summary)
	}
	return nil
}

// flush uploads the batch, then commits every file in it: state only moves
// once the warehouse has the records, so a failed upload re-sends next run.
func (w *Workspace) flush(batch *fileBatch, summary *WorkspaceSummary) error {
	if len(batch.records) == 0 {
		return nil
	}
	encoded, err := common.GzipJSONL(batch.records)
	if err != nil {
		return err
	}
	stored, err := w.UploadBatch(encoded, w.Now())
	if err != nil {
		return err
	}
	if w.State != nil {
		for _, file := range batch.files {
			if file.deleted {
				err = w.State.Forget(file.path)
			} else {
				err = w.State.Record(file.path, file.size, file.mtimeNS, file.sha, w.Now())
			}
			if err != nil {
				return err
			}
		}
	}
	summary.BatchesUploaded++
	w.Logger.Infof("Uploaded Muse workspace batch %s with %d files", stored.StorageKey, len(batch.records))
	*batch = fileBatch{}
	return nil
}
