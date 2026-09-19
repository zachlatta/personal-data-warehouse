package photos

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// PhotoSource is the envelope `source` slug the Dagster reader routes on.
const PhotoSource = "apple_photos"

// Some rows in Photos.sqlite can never be exported through PhotoKit (burst
// stack members, records whose iCloud original is gone, ...). Retrying them
// every 30 minutes forever burned the run's export budget and, because the
// first failure was re-raised, kept every scheduled run red, so a handful of
// dead assets hid the health of the ~12k that upload fine. Failing files are
// therefore backed off exponentially, and after MaxFatalAttempts they stop
// failing the run: they are reported loudly in the summary instead.
const (
	MaxFatalAttempts = 5
	RetryBackoffBase = 30 * time.Minute
	MaxRetryBackoff  = 7 * 24 * time.Hour
	// 2 ** 12 * 30min is far past the cap; bound the shift so the delay math
	// cannot overflow for a file that has been failing for years.
	MaxBackoffDoublings = 12
)

// RetryDelay is the exponential backoff for a file that has failed
// failureCount times.
func RetryDelay(failureCount int64) time.Duration {
	if failureCount <= 0 {
		return 0
	}
	doublings := failureCount
	if doublings > MaxBackoffDoublings {
		doublings = MaxBackoffDoublings
	}
	delay := RetryBackoffBase * (1 << (doublings - 1))
	if delay > MaxRetryBackoff {
		return MaxRetryBackoff
	}
	return delay
}

// Summary is the run's counts.
type Summary struct {
	AssetsSeen       int
	FilesSeen        int
	FilesSelected    int
	FilesSkipped     int
	FilesExported    int
	FilesUploaded    int
	MetadataUploaded int
	BytesExported    int64
	BytesUploaded    int64
	FilesDeferred    int
	FilesFailed      int
}

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadPhotoFile(path, capturedAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error)
	UploadPhotoMetadata(payload map[string]any, capturedAt, fileContentSHA256, metadataDedupSHA256 string) (ingestclient.StoredObject, error)
}

// Runner snapshots the library DB, resolves original-resource candidates,
// exports each selected resource through PhotoKit with iCloud network access
// enabled, and uploads its complete bytes through the app's shared photo
// endpoints (blob + envelope). The local originals/ cache is never a source
// of truth.
type Runner struct {
	Account           string
	LibraryPath       string
	Client            Uploader
	Logger            common.Logger
	Now               func() time.Time
	Limit             int // 0 means no limit
	Mode              string
	State             *State
	BeforeUploadCheck func() string
	Exporter          Exporter
}

type failedCandidate struct {
	candidate Candidate
	err       error
}

type deferredCandidate struct {
	candidate Candidate
	retryAt   time.Time
}

// Sync runs one pass. A still-retryable per-file failure is returned after
// the whole batch so the run exits non-zero for the status helper, while
// every success stays recorded in the state file.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil {
		return Summary{}, errors.New("ingest_client is required")
	}
	if r.Mode == "" {
		r.Mode = "incremental"
	}
	if r.Mode != "incremental" && r.Mode != "full" {
		return Summary{}, errors.New("mode must be 'full' or 'incremental'")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.Exporter == nil {
		r.Exporter = &PhotoKitExporter{}
	}
	libraryPath := common.ExpandUser(r.LibraryPath)
	r.Logger.Infof("Scanning Apple Photos library at %s", libraryPath)
	workingDir, err := os.MkdirTemp("", "pdw-photos-")
	if err != nil {
		return Summary{}, err
	}
	defer os.RemoveAll(workingDir)
	snapshot, err := SnapshotStore(libraryPath, workingDir)
	if err != nil {
		return Summary{}, err
	}
	candidates, err := Scan(snapshot)
	if err != nil {
		return Summary{}, err
	}
	return r.syncCandidates(candidates, filepath.Join(workingDir, "exports"))
}

func (r *Runner) syncCandidates(candidates []Candidate, exportDir string) (Summary, error) {
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		return Summary{}, err
	}
	assets := map[string]bool{}
	edited := map[string]bool{}
	for _, candidate := range candidates {
		assets[candidate.NativeID] = true
		if candidate.Role == "original" && common.ToInt(candidate.AppleRecord["adjustments_state"]) != 0 {
			edited[candidate.NativeID] = true
		}
	}
	assetsSeen := len(assets)
	r.Logger.Infof("Apple Photos inventory: assets=%d original_resources=%d (full bytes exported via PhotoKit)", assetsSeen, len(candidates))
	// v1 uploads originals only; edited renditions (Photos' adjusted output
	// under resources/renders) are a known follow-up. Keep the count visible
	// so the gap never reads as complete coverage.
	if len(edited) > 0 {
		r.Logger.Infof("%d asset(s) have Photos adjustments; edited renditions are not uploaded yet (originals only)", len(edited))
	}

	var selected []Candidate
	var deferred []deferredCandidate
	stateSkipped := 0
	for _, candidate := range candidates {
		if r.Mode == "incremental" {
			complete, err := r.isStateComplete(candidate)
			if err != nil {
				return Summary{}, err
			}
			if complete {
				stateSkipped++
				continue
			}
		}
		retryAt, deferredNow, err := r.retryNotBefore(candidate)
		if err != nil {
			return Summary{}, err
		}
		if deferredNow {
			deferred = append(deferred, deferredCandidate{candidate, retryAt})
			continue
		}
		selected = append(selected, candidate)
	}
	// The limit applies AFTER state selection (unlike voice memos) so a
	// capped run always makes forward progress through the backlog instead
	// of re-considering the same already-complete head of the list.
	// Backed-off failures are dropped before the limit too, so broken files
	// never consume slots that working ones need.
	if r.Limit > 0 && len(selected) > r.Limit {
		selected = selected[:r.Limit]
	}
	r.Logger.Infof("Incremental selection: selected=%d skipped=%d deferred=%d", len(selected), stateSkipped, len(deferred))
	if len(deferred) > 0 {
		r.logDeferred(deferred)
	}
	summary := Summary{
		AssetsSeen:    assetsSeen,
		FilesSeen:     len(candidates),
		FilesSelected: len(selected),
		FilesSkipped:  stateSkipped,
		FilesDeferred: len(deferred),
	}
	if len(selected) > 0 && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping photo upload: %s", reason)
			return summary, nil
		}
	}

	// Per-file failures are collected, not raised mid-batch (voice-memos
	// pattern): successes are recorded in upload_state so the next run
	// resumes past them, and a still-retryable failure re-raises at the end
	// so the run exits non-zero for the status helper.
	var failures []failedCandidate
	for index, candidate := range selected {
		r.Logger.Infof("[%d/%d] Exporting full original %s (%s) through PhotoKit", index+1, len(selected), candidate.Filename, candidate.Role)
		exported, err := r.Exporter.Export(candidate, exportDir)
		if err != nil {
			r.Logger.Warningf("Failed to upload %s: %s", candidate.Filename, err)
			failures = append(failures, failedCandidate{candidate, err})
			continue
		}
		summary.FilesExported++
		summary.BytesExported += exported.SizeBytes
		err = r.uploadCandidate(index+1, len(selected), candidate, exported)
		if removeErr := os.Remove(exported.Path); removeErr != nil && !os.IsNotExist(removeErr) {
			r.Logger.Warningf("Could not remove temporary PhotoKit export %s: %s", exported.Path, removeErr)
		}
		if err != nil {
			r.Logger.Warningf("Failed to upload %s: %s", candidate.Filename, err)
			failures = append(failures, failedCandidate{candidate, err})
			continue
		}
		summary.FilesUploaded++
		summary.MetadataUploaded++
		summary.BytesUploaded += exported.SizeBytes
	}

	retryable, err := r.recordFailures(failures)
	if err != nil {
		return summary, err
	}
	summary.FilesFailed = len(failures)
	r.Logger.Infof("Photo upload summary: assets=%d original_resources=%d selected=%d exported=%d (%s) uploaded=%d (%s) skipped=%d deferred=%d failed=%d",
		summary.AssetsSeen, summary.FilesSeen, summary.FilesSelected, summary.FilesExported, formatBytes(summary.BytesExported),
		summary.FilesUploaded, formatBytes(summary.BytesUploaded), summary.FilesSkipped, summary.FilesDeferred, summary.FilesFailed)
	if len(retryable) > 0 {
		r.Logger.Warningf("Photo upload finished with %d failed file(s) after uploading %d; re-raising the first so the run is marked failed (successful uploads are recorded, so the next run resumes past them)",
			len(failures), summary.FilesUploaded)
		return summary, retryable[0]
	}
	if len(failures) > 0 {
		var names []string
		for i, failure := range failures {
			if i >= 5 {
				break
			}
			names = append(names, fmt.Sprintf("%s (%s)", failure.candidate.Filename, failure.err))
		}
		r.Logger.Warningf("%d file(s) have now failed %d+ times and no longer fail the run; they retry on backoff (up to %d days) and stay visible as failed= in this summary: %s",
			len(failures), MaxFatalAttempts, int(MaxRetryBackoff.Hours()/24), strings.Join(names, ", "))
	}
	return summary, nil
}

// recordFailures persists each failed attempt and returns the ones that
// must fail the run.
//
// A failure keeps failing the run until the same file has been attempted
// MaxFatalAttempts times AND an upload has succeeded since that streak began.
// The second condition is what keeps a real outage (revoked Photos access,
// dead network) loudly red instead of quietly "green with failures": if
// nothing has succeeded since the failures started, every failure is still
// treated as fatal no matter how many attempts it has.
func (r *Runner) recordFailures(failures []failedCandidate) ([]error, error) {
	var retryable []error
	if r.State == nil {
		for _, failure := range failures {
			retryable = append(retryable, failure.err)
		}
		return retryable, nil
	}
	now := r.Now()
	latestSuccess, hasSuccess, err := r.State.LatestSuccessAt()
	if err != nil {
		return nil, err
	}
	for _, failure := range failures {
		entry, err := r.State.MarkFailure(SourceTypeAssetFile, failure.candidate.StateID(), failure.candidate.Fingerprint(), failure.err.Error(), now)
		if err != nil {
			return nil, err
		}
		firstFailure, hasFirst := common.TryParseISO(entry.FirstFailureAt)
		proven := hasSuccess && hasFirst && !latestSuccess.Before(firstFailure)
		if entry.FailureCount < MaxFatalAttempts || !proven {
			retryable = append(retryable, failure.err)
		}
	}
	return retryable, nil
}

func (r *Runner) isStateComplete(candidate Candidate) (bool, error) {
	if r.State == nil {
		return false, nil
	}
	return r.State.IsComplete(SourceTypeAssetFile, candidate.StateID(), candidate.Fingerprint())
}

// retryNotBefore is the moment a previously failed candidate may be
// attempted again; deferred is false when it may run now.
func (r *Runner) retryNotBefore(candidate Candidate) (retryAt time.Time, deferred bool, err error) {
	if r.State == nil {
		return time.Time{}, false, nil
	}
	entry, ok, err := r.State.EntryFor(SourceTypeAssetFile, candidate.StateID())
	if err != nil || !ok || entry.FailureCount <= 0 {
		return time.Time{}, false, err
	}
	if entry.Fingerprint != candidate.Fingerprint() {
		// The asset changed in Photos: a different file to upload, so the
		// old streak says nothing about it.
		return time.Time{}, false, nil
	}
	lastFailure, ok := common.TryParseISO(entry.LastFailureAt)
	if !ok {
		return time.Time{}, false, nil
	}
	retryAt = lastFailure.Add(RetryDelay(entry.FailureCount))
	if r.Now().Before(retryAt) {
		return retryAt, true, nil
	}
	return time.Time{}, false, nil
}

func (r *Runner) logDeferred(deferred []deferredCandidate) {
	earliest := deferred[0].retryAt
	var names []string
	for i, item := range deferred {
		if item.retryAt.Before(earliest) {
			earliest = item.retryAt
		}
		if i < 5 {
			names = append(names, item.candidate.Filename)
		}
	}
	r.Logger.Warningf("Deferred %d previously failed file(s) still in retry backoff (earliest retry %s): %s",
		len(deferred), common.ISOFormat(earliest), strings.Join(names, ", "))
}

func (r *Runner) uploadCandidate(index, total int, candidate Candidate, exported ExportedFile) error {
	contentSHA256, err := common.FileSHA256(exported.Path)
	if err != nil {
		return err
	}
	capturedAt := candidate.CapturedAt
	if capturedAt == "" {
		capturedAt = r.Now().UTC().Format("2006-01-02T15:04:05")
	}
	r.Logger.Infof("[%d/%d] Uploading %s (%s, %s)", index, total, exported.Filename, candidate.Role, formatBytes(exported.SizeBytes))
	stored, err := r.Client.UploadPhotoFile(exported.Path, capturedAt, exported.Extension, exported.MimeType, contentSHA256)
	if err != nil {
		return err
	}
	envelope, err := BuildPhotoMetadata(Envelope{
		Source:          PhotoSource,
		Account:         r.Account,
		NativeID:        candidate.NativeID,
		Role:            candidate.Role,
		Filename:        exported.Filename,
		MimeType:        exported.MimeType,
		SizeBytes:       exported.SizeBytes,
		ContentSHA256:   contentSHA256,
		UploadedAt:      common.ISOFormat(r.Now()),
		Width:           candidate.Width,
		Height:          candidate.Height,
		CapturedAt:      capturedAt,
		CaptureTZOffset: candidate.CaptureTZOffset,
		CameraMake:      candidate.CameraMake,
		CameraModel:     candidate.CameraModel,
		RecordKey:       "apple_record",
		Record:          candidate.AppleRecord,
	})
	if err != nil {
		return err
	}
	dedup := ProvenanceDedupSHA256(PhotoSource, r.Account, candidate.NativeID, candidate.Role, contentSHA256)
	if _, err := r.Client.UploadPhotoMetadata(envelope, capturedAt, contentSHA256, dedup); err != nil {
		return err
	}
	if r.State != nil {
		return r.State.MarkSuccess(SourceTypeAssetFile, candidate.StateID(), candidate.Fingerprint(), r.Now(), contentSHA256, stored.StorageKey)
	}
	return nil
}

// formatBytes mirrors the Python format_bytes ("123 B", "1.5 KiB", ...).
func formatBytes(count int64) string {
	return common.FormatBytes(count)
}
