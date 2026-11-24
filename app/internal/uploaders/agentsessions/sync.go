package agentsessions

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"syscall"
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
	// FilesUnchanged counts transcripts (and the OpenClaw store) a
	// SkipUnchanged pass passed over because nothing about them moved.
	FilesUnchanged int
	// LatestModTime is the newest non-future real modification time seen
	// across all discovered transcripts this pass. Zero when no files exist
	// or all mtimes are in the future.
	LatestModTime time.Time
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
	// SkipUnchanged is the resident watcher's mode (`--watch`): a transcript
	// whose size, mtime and inode are what they were when a previous pass left
	// it fully shipped is passed over without a state lookup or a read, and
	// idle passes log nothing.
	SkipUnchanged bool
	// MtimeSettleWindow guards the OpenClaw store against a write in the same
	// mtime tick as the read: a source modified more recently than
	// Now()-MtimeSettleWindow is not marked unchanged until its mtime settles.
	MtimeSettleWindow time.Duration
	// Observe posts each tool's newest event (contract C11); nil disables it.
	Observe ingestclient.ObservationPoster

	checkedNetwork bool
	lastObserved   time.Time
	// shipped is each source's signature as of the last pass that left it
	// fully uploaded; pass holds this pass's candidates until it succeeds.
	shipped map[string]fileSignature
	pass    map[string]fileSignature
	// progress caches file_state in memory after a single bulk LoadAll so a
	// restarted watcher does not issue thousands of per-file SQLite SELECTs.
	progress map[string]FileProgress
	// backfill holds this pass's signatures for caught-up file_state rows that
	// predate signature tracking.
	backfill  map[string]fileSignature
	argonMeta argonMetaCache
}

// fileSignature is what a watcher compares between passes. An append moves
// the size; an in-place rewrite moves modTime; an atomic rename replacement
// moves inode; a SQLite write moves the database's or its WAL's mtime.
// latestModTime is always a real UnixNano timestamp (the max across all paths
// in signatureOf) so settle checks never inspect a hashed/overflowed modTime.
type fileSignature struct {
	size          int64
	modTime       int64
	inode         uint64
	latestModTime int64
}

func signatureFromInfo(info fs.FileInfo) fileSignature {
	mt := info.ModTime().UnixNano()
	return fileSignature{
		size:          info.Size(),
		modTime:       mt,
		inode:         fileInode(info),
		latestModTime: mt,
	}
}

func fileInode(info fs.FileInfo) uint64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return st.Ino
	}
	return 0
}

// signatureOf combines a database and its sidecars into one signature.
func signatureOf(paths ...string) (fileSignature, bool) {
	var sig fileSignature
	found := false
	for i, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			if i == 0 {
				return fileSignature{}, false
			}
			continue
		}
		s := signatureFromInfo(info)
		if !found || s.latestModTime > sig.latestModTime {
			sig.latestModTime = s.latestModTime
		}
		found = true
		sig.size = sig.size*31 + s.size
		sig.modTime = sig.modTime*31 + s.modTime
		sig.inode = sig.inode*31 + s.inode
	}
	return sig, found
}

// futureMtimeTolerance allows minor clock skew when computing LatestModTime
// for idle-backoff decisions while ignoring bogus far-future mtimes.
const futureMtimeTolerance = 5 * time.Second

func (r *Runner) recordLatestModTime(summary *Summary, sig fileSignature) {
	if sig.latestModTime <= 0 {
		return
	}
	mt := time.Unix(0, sig.latestModTime).UTC()
	if mt.After(r.Now().Add(futureMtimeTolerance)) {
		return
	}
	if mt.After(summary.LatestModTime) {
		summary.LatestModTime = mt
	}
}

// unchanged reports whether key still has the signature it was shipped at.
func (r *Runner) unchanged(key string, sig fileSignature) bool {
	if !r.SkipUnchanged {
		return false
	}
	shipped, ok := r.shipped[key]
	return ok && shipped == sig
}

func (r *Runner) isSettled(sig fileSignature) bool {
	if r.MtimeSettleWindow <= 0 {
		return true
	}
	mod := time.Unix(0, sig.latestModTime)
	return r.Now().Sub(mod) >= r.MtimeSettleWindow
}

// caughtUp records that key was read to its end this pass; it becomes
// skippable once the pass's uploads succeed and its mtime has settled.
func (r *Runner) caughtUp(key string, sig fileSignature) {
	if r.SkipUnchanged && r.isSettled(sig) {
		r.pass[key] = sig
	}
}

type pendingProgress struct {
	offset int64
	lineNo int64
	sig    fileSignature
}

type pendingBatch struct {
	batchSize int
	records   []map[string]any
	pending   map[string]pendingProgress // path -> last buffered line progress + pre-read sig
	order     []string
	batches   int
}

func newPendingBatch(size int) *pendingBatch {
	return &pendingBatch{batchSize: size, pending: map[string]pendingProgress{}}
}

func (b *pendingBatch) add(record map[string]any, path string, offset, lineNo int64, sig fileSignature) {
	b.records = append(b.records, record)
	if _, seen := b.pending[path]; !seen {
		b.order = append(b.order, path)
	}
	b.pending[path] = pendingProgress{offset: offset, lineNo: lineNo, sig: sig}
}

func (b *pendingBatch) full() bool { return len(b.records) >= b.batchSize }

func (b *pendingBatch) reset() {
	b.records = nil
	b.pending = map[string]pendingProgress{}
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
	if r.SkipUnchanged && r.shipped == nil {
		r.shipped = map[string]fileSignature{}
	}
	if r.Mode == "incremental" && r.State != nil && r.progress == nil {
		loaded, err := r.State.LoadAll()
		if err != nil {
			return Summary{}, err
		}
		r.progress = loaded
	}
	r.pass = map[string]fileSignature{}
	r.backfill = map[string]fileSignature{}
	files := Discover(r.Dirs)
	r.observeNewest(files)
	if !r.SkipUnchanged {
		r.Logger.Infof("Discovered %d agent session transcript file(s)", len(files))
	}
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
		result, ok, err := r.readFile(file, batch, remaining, &summary)
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
	if !summary.LimitReached {
		if err := r.readOpenClawStore(batch, &remaining, &summary); err != nil {
			return summary, err
		}
	}
	if err := r.flush(batch); err != nil {
		return summary, err
	}
	if len(r.backfill) > 0 {
		if err := r.State.BackfillSignatures(r.backfill); err != nil {
			return summary, err
		}
		for path, sig := range r.backfill {
			p := r.progress[path]
			p.LastMtimeNs = sig.modTime
			p.LastInode = sig.inode
			r.progress[path] = p
		}
	}
	summary.BatchesUploaded = batch.batches
	for key, sig := range r.pass {
		r.shipped[key] = sig
	}
	if r.SkipUnchanged && summary.LinesSelected == 0 && summary.LinesSkipped == 0 {
		return summary, nil
	}
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

func (r *Runner) readFile(file SessionFile, batch *pendingBatch, remaining int, summary *Summary) (fileResult, bool, error) {
	sig := file.sig
	r.recordLatestModTime(summary, sig)
	if r.unchanged(file.Path, sig) {
		summary.FilesUnchanged++
		return fileResult{}, false, nil
	}
	size := sig.size
	startOffset, startLine := r.startPosition(file.Path, sig)
	if size <= startOffset {
		if p, ok := r.progress[file.Path]; ok && p.LastMtimeNs == 0 {
			r.backfill[file.Path] = sig
		}
		r.caughtUp(file.Path, sig)
		return fileResult{}, true, nil
	}
	if file.Tool == ArgonTool {
		file.Session = r.argonMeta.sessionMeta(file.Path, file.SessionID)
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
	shortRead := int64(n) < size-startOffset
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
		parsed, blank := parseLine(raw)
		if parsed == nil {
			if !blank {
				r.Logger.Warningf("Skipping unparseable line %d in %s", seq, file.Path)
			}
			result.skipped++
			continue
		}
		batch.add(r.envelope(file, seq, parsed), file.Path, offset, lineNo, sig)
		result.selected++
		if batch.full() {
			if err := r.flush(batch); err != nil {
				return result, true, err
			}
		}
	}
	if !result.deferred && !shortRead {
		// Read to the end (a trailing partial line stays in the size, so
		// finishing it moves the signature and brings the file back).
		r.caughtUp(file.Path, sig)
	}
	if result.selected == 0 && offset != startOffset {
		// Consumed only skipped/blank lines; advance so we do not reprocess
		// them next run. (Files with selected lines commit via flush.)
		if err := r.commit(file.Path, offset, lineNo, sig); err != nil {
			return result, true, err
		}
	}
	return result, true, nil
}

// parseLine is the one rule for which transcript lines become events: a
// complete line, invalid UTF-8 replaced, trimmed, that decodes to a non-null
// JSON object. blank reports an empty line, which is skipped silently. The
// Muse session manifest counts with this same function, so its counts are
// the warehouse's by construction.
func parseLine(raw []byte) (parsed map[string]any, blank bool) {
	text := strings.TrimSpace(string(bytes.ToValidUTF8(raw, []byte("�"))))
	if text == "" {
		return nil, true
	}
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		return nil, false
	}
	return parsed, false
}

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
		if err := r.commit(path, progress.offset, progress.lineNo, progress.sig); err != nil {
			return err
		}
	}
	r.Logger.Infof("Uploaded agent-sessions batch %s with %d records", stored.StorageKey, len(batch.records))
	batch.reset()
	return nil
}

// startPosition is where to resume a source. sig is the zero signature for an
// OpenClaw store session, whose cursor is a seq rather than a byte offset.
func (r *Runner) startPosition(path string, sig fileSignature) (offset, line int64) {
	if r.Mode != "incremental" || r.State == nil {
		return 0, 0
	}
	progress := r.progress[path]
	if progress.UploadedOffset > sig.size {
		// File was truncated/rotated; re-read from the beginning.
		return 0, 0
	}
	if progress.LastInode != 0 && sig.inode != 0 && progress.LastInode != sig.inode {
		// File was atomically replaced (rename/recreate); re-read from the beginning.
		return 0, 0
	}
	if progress.UploadedOffset == sig.size && sig.size > 0 && progress.LastMtimeNs != 0 && progress.LastMtimeNs != sig.modTime {
		// Same-size in-place rewrite; re-read from the beginning.
		return 0, 0
	}
	return progress.UploadedOffset, progress.UploadedLines
}

// commit records what was shipped and the signature taken before it was read.
func (r *Runner) commit(path string, offset, lineNo int64, sig fileSignature) error {
	if r.State == nil {
		return nil
	}
	if err := r.State.RecordProgress(path, offset, lineNo, sig, r.Now()); err != nil {
		return err
	}
	if r.progress != nil {
		r.progress[path] = FileProgress{
			UploadedOffset: offset,
			UploadedLines:  lineNo,
			LastMtimeNs:    sig.modTime,
			LastInode:      sig.inode,
		}
	}
	return nil
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
		"record":         r.record(file, seq, line),
	}
}

func (r *Runner) record(file SessionFile, seq int64, line map[string]any) map[string]any {
	record := map[string]any{
		"tool":       file.Tool,
		"session_id": file.SessionID,
		"seq":        seq,
		"line":       line,
	}
	if file.Session != nil {
		record["session"] = file.Session
	}
	return record
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
