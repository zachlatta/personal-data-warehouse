package voicememos

import (
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// PartialLocalDurationToleranceSeconds is how far the locally materialized
// audio may fall short of the recording's full duration before the file is
// treated as still downloading from iCloud.
const PartialLocalDurationToleranceSeconds = 5.0

// Summary is the run's counts (VoiceMemosUploadSummary).
type Summary struct {
	RecordingsSeen     int
	RecordingsSkipped  int
	RecordingsUploaded int
	MetadataUploaded   int
	BytesSeen          int64
	BytesUploaded      int64
	BytesSkipped       int64
	RecordingsSelected int
	RecordingsDeferred int
}

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadVoiceMemoAudio(content []byte, recordedAt, extension, contentType string) (ingestclient.StoredObject, error)
	UploadVoiceMemoMetadata(payload map[string]any, recordedAt, audioContentSHA256 string) (ingestclient.StoredObject, error)
}

// AppKicker makes sure the Voice Memos store is still being fed before the
// scan; the runner calls it first, before trusting what it scanned.
type AppKicker func(recordingsPath string, logger common.Logger) AppKick

// Runner uploads recordings through the app (VoiceMemosUploadRunner).
type Runner struct {
	Account           string
	RecordingsPath    string
	Extensions        []string
	Client            Uploader
	Logger            common.Logger
	Now               func() time.Time
	Limit             int // 0 means no limit
	Workers           int
	Mode              string // "full" or "incremental"
	State             *State
	MinFileAgeSeconds int
	BeforeUploadCheck func() string
	// MaxUploadBytes is the route's ceiling; 0 means unbounded. The CLI sets
	// it from the client's EffectiveMaxUploadBytes.
	MaxUploadBytes int64
	// EnsureApp defaults to EnsureVoiceMemosAppRunning against the process
	// environment; tests inject a no-op.
	EnsureApp AppKicker

	stateMu sync.Mutex
}

type recordingResult struct {
	uploaded         int
	skipped          int
	metadataUploaded int
	bytesUploaded    int64
	bytesSkipped     int64
	audioPresent     bool
	metadataPresent  bool
}

// Sync runs one pass.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil {
		return Summary{}, errors.New("ingest_client is required")
	}
	if r.Mode == "" {
		r.Mode = "full"
	}
	if r.Mode != "full" && r.Mode != "incremental" {
		return Summary{}, errors.New("mode must be 'full' or 'incremental'")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.Workers < 1 {
		r.Workers = 1
	}
	if r.MinFileAgeSeconds < 0 {
		r.MinFileAgeSeconds = 0
	}
	if r.Logger == nil {
		r.Logger = &common.RecordingLogger{}
	}
	root := common.ExpandUser(r.RecordingsPath)
	// Before trusting what the store contains, make sure the store is still
	// being fed: with voicememod and the app both quit, macOS stops pulling
	// iCloud changes and every run reads a healthy selected=0.
	kick := r.EnsureApp
	if kick == nil {
		kick = func(path string, logger common.Logger) AppKick {
			return EnsureVoiceMemosAppRunning(path, logger, os.Getenv, "", nil)
		}
	}
	kick(root, r.Logger)
	r.Logger.Infof("Scanning Voice Memos in %s for extensions: %s", root, strings.Join(r.Extensions, ", "))
	candidates, err := ScanCandidates(root, r.Extensions)
	if err != nil {
		return Summary{}, err
	}
	if r.Mode == "incremental" {
		return r.syncIncremental(candidates)
	}
	return r.syncFull(candidates)
}

func (r *Runner) syncFull(candidates []Candidate) (Summary, error) {
	if r.Limit > 0 && len(candidates) > r.Limit {
		candidates = candidates[:r.Limit]
	}
	seen := len(candidates)
	var bytesSeen int64
	for _, c := range candidates {
		bytesSeen += c.SizeBytes
	}
	var partial, kept []Candidate
	for _, c := range candidates {
		if IsPartiallyMaterialized(c) {
			partial = append(partial, c)
		} else {
			kept = append(kept, c)
		}
	}
	for _, c := range partial {
		r.Logger.Warningf("Deferring %s because Voice Memos reports %.2fs total but only %.2fs local audio", c.Filename, floatOrZero(c.Duration), floatOrZero(c.LocalDuration))
	}
	recordings := make([]Recording, 0, len(kept))
	for _, c := range kept {
		recording, err := RecordingFromCandidate(c)
		if err != nil {
			return Summary{}, err
		}
		recordings = append(recordings, recording)
	}
	r.Logger.Infof("Found %d Voice Memos recordings totaling %s", seen, common.FormatBytes(bytesSeen))
	if len(recordings) > 0 && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping Voice Memos upload: %s", reason)
			return Summary{
				RecordingsSeen: seen, BytesSeen: bytesSeen,
				RecordingsSelected: len(recordings), RecordingsDeferred: len(recordings) + len(partial),
			}, nil
		}
	}
	r.Logger.Infof("Uploading with %d worker(s)", r.Workers)
	results, errs := r.uploadAll(recordings)
	summary := Summary{RecordingsSeen: seen, BytesSeen: bytesSeen, RecordingsSelected: len(recordings), RecordingsDeferred: len(partial)}
	for _, err := range errs {
		if err != nil {
			return summary, err
		}
	}
	for _, result := range results {
		summary.RecordingsUploaded += result.uploaded
		summary.RecordingsSkipped += result.skipped
		summary.MetadataUploaded += result.metadataUploaded
		summary.BytesUploaded += result.bytesUploaded
		summary.BytesSkipped += result.bytesSkipped
	}
	r.Logger.Infof("Voice Memos upload summary: seen=%d (%s), uploaded=%d (%s), skipped=%d (%s), metadata=%d",
		summary.RecordingsSeen, common.FormatBytes(summary.BytesSeen), summary.RecordingsUploaded, common.FormatBytes(summary.BytesUploaded),
		summary.RecordingsSkipped, common.FormatBytes(summary.BytesSkipped), summary.MetadataUploaded)
	return summary, nil
}

func (r *Runner) syncIncremental(candidates []Candidate) (Summary, error) {
	if r.Limit > 0 && len(candidates) > r.Limit {
		candidates = candidates[:r.Limit]
	}
	var bytesSeen int64
	for _, c := range candidates {
		bytesSeen += c.SizeBytes
	}
	r.Logger.Infof("Found %d Voice Memos recordings totaling %s", len(candidates), common.FormatBytes(bytesSeen))
	var selected []Candidate
	stateSkipped, ageDeferred, partialDeferred, oversizeDeferred := 0, 0, 0, 0
	now := r.Now()
	for _, c := range candidates {
		if r.MinFileAgeSeconds > 0 && now.Sub(c.FileModifiedAt).Seconds() < float64(r.MinFileAgeSeconds) {
			ageDeferred++
			continue
		}
		if r.MaxUploadBytes > 0 && c.SizeBytes > r.MaxUploadBytes {
			oversizeDeferred++
			r.Logger.Warningf("Deferring %s (%s): exceeds the %s upload ceiling for the current route", c.Filename, common.FormatBytes(c.SizeBytes), common.FormatBytes(r.MaxUploadBytes))
			continue
		}
		if IsPartiallyMaterialized(c) {
			partialDeferred++
			r.Logger.Warningf("Deferring %s because Voice Memos reports %.2fs total but only %.2fs local audio", c.Filename, floatOrZero(c.Duration), floatOrZero(c.LocalDuration))
			continue
		}
		if r.isStateComplete(c) {
			stateSkipped++
			continue
		}
		selected = append(selected, c)
	}
	deferred := ageDeferred + partialDeferred + oversizeDeferred
	r.Logger.Infof("Incremental selection: selected=%d skipped=%d deferred=%d", len(selected), stateSkipped, deferred)
	completeBytes := func() int64 {
		var total int64
		for _, c := range candidates {
			if r.isStateComplete(c) {
				total += c.SizeBytes
			}
		}
		return total
	}
	if len(selected) == 0 {
		return Summary{
			RecordingsSeen: len(candidates), RecordingsSkipped: stateSkipped, BytesSeen: bytesSeen,
			BytesSkipped: completeBytes(), RecordingsDeferred: deferred,
		}, nil
	}
	if r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping Voice Memos upload: %s", reason)
			return Summary{
				RecordingsSeen: len(candidates), RecordingsSkipped: stateSkipped, BytesSeen: bytesSeen,
				BytesSkipped: completeBytes(), RecordingsSelected: len(selected), RecordingsDeferred: len(selected) + deferred,
			}, nil
		}
	}
	r.Logger.Infof("Uploading with %d worker(s)", r.Workers)
	recordings := make([]Recording, 0, len(selected))
	for _, c := range selected {
		recording, err := RecordingFromCandidate(c)
		if err != nil {
			return Summary{}, err
		}
		recordings = append(recordings, recording)
	}
	// Per-file failures are collected, not returned mid-batch: one memo that
	// errors must not abort the rest. Successful uploads are recorded in the
	// state, so the next run resumes past them; the first error is returned
	// once at the end so the run still exits non-zero.
	results, errs := r.uploadAll(recordings)
	var failures []error
	summary := Summary{RecordingsSeen: len(candidates), RecordingsSkipped: stateSkipped, BytesSeen: bytesSeen, RecordingsSelected: len(selected), RecordingsDeferred: deferred}
	for i, err := range errs {
		if err != nil {
			r.Logger.Warningf("Failed to upload %s: %s", recordings[i].Filename, err)
			failures = append(failures, err)
			continue
		}
		summary.RecordingsUploaded += results[i].uploaded
		summary.RecordingsSkipped += results[i].skipped
		summary.MetadataUploaded += results[i].metadataUploaded
		summary.BytesUploaded += results[i].bytesUploaded
		summary.BytesSkipped += results[i].bytesSkipped
	}
	summary.BytesSkipped += completeBytes()
	r.Logger.Infof("Voice Memos upload summary: seen=%d (%s), selected=%d, uploaded=%d (%s), skipped=%d (%s), deferred=%d, metadata=%d",
		summary.RecordingsSeen, common.FormatBytes(summary.BytesSeen), summary.RecordingsSelected, summary.RecordingsUploaded,
		common.FormatBytes(summary.BytesUploaded), summary.RecordingsSkipped, common.FormatBytes(summary.BytesSkipped),
		summary.RecordingsDeferred, summary.MetadataUploaded)
	if len(failures) > 0 {
		r.Logger.Warningf("Voice Memos upload finished with %d failed recording(s) after uploading %d; re-raising the first so the run is marked failed (successful uploads are recorded, so the next run resumes past them)", len(failures), summary.RecordingsUploaded)
		return summary, failures[0]
	}
	return summary, nil
}

// uploadAll runs syncCandidate over the recordings with Workers goroutines,
// returning per-recording results and errors in input order.
func (r *Runner) uploadAll(recordings []Recording) ([]recordingResult, []error) {
	results := make([]recordingResult, len(recordings))
	errs := make([]error, len(recordings))
	workers := r.Workers
	if len(recordings) <= 1 {
		workers = 1
	}
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, workers)
	for i, recording := range recordings {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(i int, recording Recording) {
			defer wg.Done()
			defer func() { <-semaphore }()
			results[i], errs[i] = r.syncCandidate(i+1, len(recordings), recording)
		}(i, recording)
	}
	wg.Wait()
	return results, errs
}

func (r *Runner) isStateComplete(c Candidate) bool {
	if r.State == nil {
		return false
	}
	entry, ok := r.State.EntryFor(c)
	return ok && entry.Complete() && entry.Matches(c)
}

func (r *Runner) syncCandidate(index, total int, recording Recording) (recordingResult, error) {
	result, err := r.syncRecording(index, total, recording)
	if r.State != nil {
		r.stateMu.Lock()
		defer r.stateMu.Unlock()
		if err != nil {
			r.State.MarkFailure(recording.Candidate, recording.ContentSHA256, err.Error(), r.Now())
		} else {
			r.State.MarkSuccess(recording.Candidate, recording.ContentSHA256, result.audioPresent, result.metadataPresent, r.Now())
		}
	}
	return result, err
}

func (r *Runner) syncRecording(index, total int, recording Recording) (recordingResult, error) {
	// The app owns object keys, kinds, and pdw_* tags and dedups by content
	// sha, so we always send and let the app collapse duplicates.
	recordedAt := common.ISOFormat(recording.RecordedAt)
	r.Logger.Infof("[%d/%d] upload %s (%s, sha256=%s) -> app", index, total, recording.Filename, common.FormatBytes(recording.SizeBytes), common.ShortSHA256(recording.ContentSHA256))
	content, err := os.ReadFile(recording.Path)
	if err != nil {
		return recordingResult{}, err
	}
	if _, err := r.Client.UploadVoiceMemoAudio(content, recordedAt, recording.Extension, recording.ContentType); err != nil {
		return recordingResult{}, err
	}
	payload := BuildMetadata(r.Account, recording, r.Now())
	if _, err := r.Client.UploadVoiceMemoMetadata(payload, recordedAt, recording.ContentSHA256); err != nil {
		return recordingResult{}, err
	}
	return recordingResult{uploaded: 1, metadataUploaded: 1, bytesUploaded: recording.SizeBytes, audioPresent: true, metadataPresent: true}, nil
}

// BuildMetadata is the recording's JSON sidecar, byte-identical (through
// common.CanonicalJSON) to the Python build_metadata.
func BuildMetadata(account string, recording Recording, uploadedAt time.Time) map[string]any {
	payload := map[string]any{
		"recording_id":     recording.RecordingID,
		"title":            recording.Title,
		"original_path":    recording.Path,
		"filename":         recording.Filename,
		"extension":        recording.Extension,
		"content_type":     recording.ContentType,
		"size_bytes":       recording.SizeBytes,
		"content_sha256":   recording.ContentSHA256,
		"file_created_at":  common.ISOFormat(recording.FileCreatedAt),
		"file_modified_at": common.ISOFormat(recording.FileModifiedAt),
		"recorded_at":      common.ISOFormat(recording.RecordedAt),
	}
	if recording.Duration != nil {
		payload["duration_seconds"] = *recording.Duration
	}
	if recording.LocalDuration != nil {
		payload["local_duration_seconds"] = *recording.LocalDuration
	}
	return map[string]any{
		"schema_version": int64(1),
		"source":         "apple_voice_memos",
		"account":        account,
		"uploaded_at":    common.ISOFormat(uploadedAt),
		"recording":      payload,
	}
}

// IsPartiallyMaterialized reports whether Voice Memos says the recording is
// longer than the audio it has downloaded locally, i.e. iCloud is still
// delivering it and the file on disk is incomplete.
func IsPartiallyMaterialized(c Candidate) bool {
	if c.Duration == nil || c.LocalDuration == nil {
		return false
	}
	if *c.Duration > 0 && *c.LocalDuration <= 0 {
		return true
	}
	return *c.Duration > *c.LocalDuration+PartialLocalDurationToleranceSeconds
}

func floatOrZero(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}
