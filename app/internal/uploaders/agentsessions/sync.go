package agentsessions

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultBatchSize is the maximum lines per uploaded batch.
const DefaultBatchSize = 20000

// ErrUploadBlocked is returned when the network/preflight guard refuses the
// run; the CLI reports it as a deliberate skip, not a failure.
type ErrUploadBlocked struct{ Reason string }

func (e *ErrUploadBlocked) Error() string { return e.Reason }

// Summary is the run's counts.
type Summary struct {
	FilesSeen         int
	FilesWithNewLines int
	LinesSelected     int
	LinesSkipped      int
	BatchesUploaded   int
	LimitReached      bool
}

// BatchUploader posts one gzipped batch; the ingest client satisfies it.
type BatchUploader func(gzipBytes []byte, exportedAt time.Time) (ingestclient.StoredObject, error)

// Runner ships new transcript lines as gzipped JSONL batches.
type Runner struct {
	Account           string
	Device            string
	Dirs              Dirs
	Upload            BatchUploader
	Logger            common.Logger
	State             *State // nil disables incremental state
	Now               func() time.Time
	Mode              string // "incremental" | "full"
	Limit             int    // 0 = unlimited
	BatchSize         int
	BeforeUploadCheck func() string

	checkedNetwork bool
}

type pendingBatch struct {
	batchSize int
	records   []map[string]any
	pending   map[string][2]int64 // path -> (offset, line_no) of its last buffered line
	order     []string
	batches   int
}

func newPendingBatch(size int) *pendingBatch {
	return &pendingBatch{batchSize: size, pending: map[string][2]int64{}}
}

func (b *pendingBatch) add(record map[string]any, path string, offset, lineNo int64) {
	b.records = append(b.records, record)
	if _, seen := b.pending[path]; !seen {
		b.order = append(b.order, path)
	}
	b.pending[path] = [2]int64{offset, lineNo}
}

func (b *pendingBatch) full() bool { return len(b.records) >= b.batchSize }

func (b *pendingBatch) reset() {
	b.records = nil
	b.pending = map[string][2]int64{}
	b.order = nil
	b.batches++
}

// Sync runs one pass.
func (r *Runner) Sync() (Summary, error) {
	if r.Upload == nil {
		return Summary{}, errors.New("batch uploader is required")
	}
	if r.Mode == "" {
		r.Mode = "incremental"
	}
	if r.Mode != "incremental" && r.Mode != "full" {
		return Summary{}, fmt.Errorf("mode must be 'full' or 'incremental'")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.BatchSize < 1 {
		r.BatchSize = DefaultBatchSize
	}
	files := Discover(r.Dirs)
	r.Logger.Infof("Discovered %d agent session transcript file(s)", len(files))
	// One buffer spanning all files so many small transcripts (OpenClaw can
	// have thousands of ~5-line sessions) coalesce into full-size batches
	// instead of one tiny upload per file. A file's offset is only committed
	// once the batch carrying its lines is uploaded, so a crash re-ships the
	// un-acknowledged tail (ingest dedupes by primary key).
	batch := newPendingBatch(r.BatchSize)
	remaining := r.Limit
	summary := Summary{FilesSeen: len(files)}
	for _, file := range files {
		if r.Limit > 0 && remaining <= 0 {
			summary.LimitReached = true
			break
		}
		result, ok, err := r.readFile(file, batch, remaining)
		if err != nil {
			return summary, err
		}
		if !ok {
			continue
		}
		if result.selected > 0 {
			summary.FilesWithNewLines++
		}
		summary.LinesSelected += result.selected
		summary.LinesSkipped += result.skipped
		if r.Limit > 0 {
			remaining -= result.selected
		}
		if result.deferred {
			summary.LimitReached = true
			break
		}
	}
	if err := r.flush(batch); err != nil {
		return summary, err
	}
	summary.BatchesUploaded = batch.batches
	suffix := ""
	if summary.LimitReached {
		suffix = " (run limit reached; remaining lines deferred to next run)"
	}
	r.Logger.Infof("Agent sessions upload summary: files=%d new=%d lines=%d skipped=%d batches=%d%s",
		summary.FilesSeen, summary.FilesWithNewLines, summary.LinesSelected, summary.LinesSkipped, summary.BatchesUploaded, suffix)
	return summary, nil
}

type fileResult struct {
	selected int
	skipped  int
	deferred bool
}

func (r *Runner) readFile(file SessionFile, batch *pendingBatch, remaining int) (fileResult, bool, error) {
	info, err := os.Stat(file.Path)
	if err != nil {
		return fileResult{}, false, nil
	}
	size := info.Size()
	startOffset, startLine, err := r.startPosition(file.Path, size)
	if err != nil {
		return fileResult{}, false, err
	}
	if size <= startOffset {
		return fileResult{}, true, nil
	}
	handle, err := os.Open(file.Path)
	if err != nil {
		return fileResult{}, false, nil
	}
	defer handle.Close()
	if _, err := handle.Seek(startOffset, 0); err != nil {
		return fileResult{}, false, nil
	}
	data := make([]byte, size-startOffset)
	n, _ := readFull(handle, data)
	data = data[:n]

	offset := startOffset
	lineNo := startLine
	result := fileResult{}
	for _, raw := range completeLines(data) {
		if r.Limit > 0 && remaining-result.selected <= 0 {
			result.deferred = true
			break
		}
		offset += int64(len(raw))
		seq := lineNo
		lineNo++
		text := strings.TrimSpace(string(bytes.ToValidUTF8(raw, []byte("�"))))
		if text == "" {
			result.skipped++
			continue
		}
		var parsed map[string]any
		if err := json.Unmarshal([]byte(text), &parsed); err != nil || parsed == nil {
			if err != nil && !looksLikeJSONObject(text) {
				r.Logger.Warningf("Skipping unparseable line %d in %s", seq, file.Path)
			} else if err != nil {
				r.Logger.Warningf("Skipping unparseable line %d in %s", seq, file.Path)
			}
			result.skipped++
			continue
		}
		batch.add(r.envelope(file, seq, parsed), file.Path, offset, lineNo)
		result.selected++
		if batch.full() {
			if err := r.flush(batch); err != nil {
				return result, true, err
			}
		}
	}
	if result.selected == 0 && offset != startOffset {
		// Consumed only skipped/blank lines; advance so we do not reprocess
		// them next run. (Files with selected lines commit via flush.)
		if err := r.commit(file.Path, offset, lineNo); err != nil {
			return result, true, err
		}
	}
	return result, true, nil
}

func looksLikeJSONObject(text string) bool { return strings.HasPrefix(text, "{") }

func readFull(handle *os.File, data []byte) (int, error) {
	total := 0
	for total < len(data) {
		n, err := handle.Read(data[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			break
		}
	}
	return total, nil
}

func (r *Runner) flush(batch *pendingBatch) error {
	if len(batch.records) == 0 {
		return nil
	}
	if err := r.runNetworkCheck(); err != nil {
		return err
	}
	stored, err := r.uploadBatch(batch.records)
	if err != nil {
		return err
	}
	// Commit only after the upload succeeds; pending offsets correspond to
	// lines whose records are in this just-uploaded batch.
	for _, path := range batch.order {
		progress := batch.pending[path]
		if err := r.commit(path, progress[0], progress[1]); err != nil {
			return err
		}
	}
	r.Logger.Infof("Uploaded agent-sessions batch %s with %d records", stored.StorageKey, len(batch.records))
	batch.reset()
	return nil
}

func (r *Runner) startPosition(path string, size int64) (int64, int64, error) {
	if r.Mode != "incremental" || r.State == nil {
		return 0, 0, nil
	}
	progress, err := r.State.ProgressFor(path)
	if err != nil {
		return 0, 0, err
	}
	if progress.UploadedOffset > size {
		// File was truncated/rotated; re-read from the beginning.
		return 0, 0, nil
	}
	return progress.UploadedOffset, progress.UploadedLines, nil
}

func (r *Runner) commit(path string, offset, lineNo int64) error {
	if r.State == nil {
		return nil
	}
	return r.State.RecordProgress(path, offset, lineNo, r.Now())
}

func (r *Runner) runNetworkCheck() error {
	if r.BeforeUploadCheck == nil || r.checkedNetwork {
		return nil
	}
	r.checkedNetwork = true
	if reason := r.BeforeUploadCheck(); reason != "" {
		return &ErrUploadBlocked{Reason: reason}
	}
	return nil
}

func (r *Runner) uploadBatch(records []map[string]any) (ingestclient.StoredObject, error) {
	encoded, err := common.GzipJSONL(records)
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	return r.Upload(encoded, r.Now())
}

func (r *Runner) envelope(file SessionFile, seq int64, line map[string]any) map[string]any {
	return map[string]any{
		"schema_version": int64(1),
		"source":         "agent_sessions",
		"account":        r.Account,
		"device":         r.Device,
		"exported_at":    common.ISOFormat(r.Now()),
		"record_type":    file.Tool + "_event",
		"record": map[string]any{
			"tool":       file.Tool,
			"session_id": file.SessionID,
			"seq":        seq,
			"line":       line,
		},
	}
}

// completeLines returns only newline-terminated lines (each including its
// trailing \n). A trailing partial line (a transcript being written) is left
// for the next run.
func completeLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for {
		idx := bytes.IndexByte(data[start:], '\n')
		if idx < 0 {
			break
		}
		lines = append(lines, data[start:start+idx+1])
		start += idx + 1
	}
	return lines
}
