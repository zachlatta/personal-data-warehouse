package androidcallrecordings

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultSettleAge applies to both BCR files before parsing or uploading.
const DefaultSettleAge = 10 * time.Second

// DefaultLimit is the recordings sent per run, newest first, so a first run
// over a long history leaves the newest memo waiting behind nothing.
const DefaultLimit = 50

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadAndroidCallRecordingsAudio(path, recordedAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error)
	UploadAndroidCallRecordingsMetadata(payload map[string]any, recordedAt, audioContentSHA256, metadataDedupSHA256 string) (ingestclient.StoredObject, error)
}

// Summary is one run's counts.
type Summary struct {
	NewestFinishedAt string
	Seen             int
	AudioUploaded    int
	MetadataUploaded int
	Skipped          int // already landed, unchanged
	InProgress       int // a pair is still being saved
	Unsettled        int // finished, but its file changed seconds ago
	Missing          int // finished, but no audio file on the phone
	Remaining        int // waiting for a later run (the limit)
	Deferred         bool
}

// Runner sends every finished recording the ledger has not seen land.
type Runner struct {
	RecorderDir       string
	Device            string
	Account           string // warehouse account for this device
	Client            Uploader
	State             *State
	Logger            common.Logger
	Now               func() time.Time
	SettleAge         time.Duration
	Limit             int
	BeforeUploadCheck func() string
}

type pending struct {
	recording Recording
	audioSHA  string
	sendAudio bool
	payload   map[string]any
	dedupSHA  string
}

// Sync runs one pass. It stops at the first failed upload -- the network or
// the app being down fails every recording alike -- and returns an error
// without the identifying filename, so the heartbeat reads failing and the
// recording is retried next run.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil || r.State == nil {
		return Summary{}, errors.New("client and state are required")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.Logger == nil {
		r.Logger = &common.RecordingLogger{}
	}
	if r.SettleAge <= 0 {
		r.SettleAge = DefaultSettleAge
	}
	if r.Limit <= 0 {
		r.Limit = DefaultLimit
	}
	recordings, err := readRecordings(r.RecorderDir, r.Now(), r.SettleAge)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{Seen: len(recordings)}
	now := r.Now()
	var queue []pending
	for _, recording := range recordings {
		switch {
		case recording.State != CompletedState:
			summary.InProgress++
			continue
		case !recording.FileExists || recording.SizeBytes == 0:
			summary.Missing++
			r.Logger.Warningf("Skipping empty audio")
			continue
		case now.Sub(recording.ModifiedAt) < r.SettleAge || now.Sub(recording.MetadataModifiedAt) < r.SettleAge:
			summary.Unsettled++
			continue
		}
		ended := recording.CreatedAt.Add(time.Duration(recording.DurationMS) * time.Millisecond)
		if stamp, ok := recording.Fields["ended_at"].(string); ok {
			if parsed, err := time.Parse(time.RFC3339Nano, stamp); err == nil {
				ended = parsed
			}
		}
		if stamp := common.ISOFormat(ended); stamp > summary.NewestFinishedAt {
			summary.NewestFinishedAt = stamp
		}
		entry, known, err := r.State.entry(recording.UUID)
		if err != nil {
			return summary, err
		}
		audioSHA := entry.AudioSHA256
		if !known || entry.SizeBytes != recording.SizeBytes || entry.ModifiedNS != recording.ModifiedAt.UnixNano() || audioSHA == "" {
			if audioSHA, err = common.FileSHA256(recording.Path); err != nil {
				return summary, fmt.Errorf("hash audio failed")
			}
		}
		payload := BuildMetadata(r.Account, r.Device, recording, audioSHA, now)
		dedupSHA := MetadataDedupSHA256(payload)
		if known && entry.AudioSHA256 == audioSHA && entry.MetadataSHA256 == dedupSHA {
			summary.Skipped++
			if entry.SizeBytes != recording.SizeBytes || entry.ModifiedNS != recording.ModifiedAt.UnixNano() {
				// Touched but unchanged: remember the new stat so it is not re-hashed.
				if err := r.State.record(recording, audioSHA, dedupSHA, now); err != nil {
					return summary, err
				}
			}
			continue
		}
		queue = append(queue, pending{
			recording: recording, audioSHA: audioSHA, payload: payload, dedupSHA: dedupSHA,
			sendAudio: !known || entry.AudioSHA256 != audioSHA,
		})
	}
	if len(queue) > r.Limit {
		summary.Remaining = len(queue) - r.Limit
		queue = queue[:r.Limit]
	}
	if len(queue) > 0 && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping Android call recordings upload: %s", reason)
			summary.Deferred = true
			summary.Remaining += len(queue)
			return summary, nil
		}
	}
	for i, p := range queue {
		recordedAt := common.ISOFormat(p.recording.CreatedAt)
		r.Logger.Infof("[%d/%d] upload sha256=%s", i+1, len(queue), common.ShortSHA256(p.audioSHA))
		if p.sendAudio {
			if _, err := r.Client.UploadAndroidCallRecordingsAudio(p.recording.Path, recordedAt, p.recording.Extension, p.recording.ContentType, p.audioSHA); err != nil {
				return summary, fmt.Errorf("upload audio failed")
			}
			summary.AudioUploaded++
		}
		if _, err := r.Client.UploadAndroidCallRecordingsMetadata(p.payload, recordedAt, p.audioSHA, p.dedupSHA); err != nil {
			return summary, fmt.Errorf("upload sidecar failed")
		}
		summary.MetadataUploaded++
		if err := r.State.record(p.recording, p.audioSHA, p.dedupSHA, r.Now()); err != nil {
			return summary, err
		}
	}
	return summary, nil
}

// BuildMetadata is a recording's JSON sidecar (schema_version 1), which
// personal_data_warehouse.android_call_recordings_drive_ingest maps onto
// base_android_call_recordings.recordings.
func BuildMetadata(account, device string, r Recording, audioSHA string, uploadedAt time.Time) map[string]any {
	recording := map[string]any{
		"recording_id":     r.UUID,
		"title":            r.Title,
		"recorded_at":      common.ISOFormat(r.CreatedAt),
		"duration_ms":      r.DurationMS,
		"duration_seconds": float64(r.DurationMS) / 1000,
		"filename":         r.UUID + r.Extension,
		"extension":        r.Extension,
		"content_type":     r.ContentType,
		"size_bytes":       r.SizeBytes,
		"content_sha256":   audioSHA,
		"file_modified_at": common.ISOFormat(r.ModifiedAt),
		"recording_state":  r.State,
		"original_path":    r.StoredPath,
	}
	var fields map[string]any
	encoded, _ := json.Marshal(r.Fields)
	json.Unmarshal(encoded, &fields)
	for key, value := range fields {
		recording[key] = value
	}

	return map[string]any{
		"schema_version": int64(1),
		"source":         "android_call_recordings",
		"account":        account,
		"device":         device,
		"uploaded_at":    common.ISOFormat(uploadedAt),
		"recording":      recording,
		"bcr_raw_json":   string(r.BCR),
	}
}

// MetadataDedupSHA256 is the sidecar's identity: its canonical JSON without
// uploaded_at, so a re-send of an unchanged sidecar dedups at the app while a
// renamed or edited recording lands.
func MetadataDedupSHA256(payload map[string]any) string {
	identity := make(map[string]any, len(payload))
	for key, value := range payload {
		if key != "uploaded_at" {
			identity[key] = value
		}
	}
	encoded, err := common.CanonicalJSON(identity)
	if err != nil {
		// Every value here is a string, number, bool or map of them.
		panic(fmt.Sprintf("BCR sidecar is not JSON: %v", err))
	}
	return common.BytesSHA256(encoded)
}
