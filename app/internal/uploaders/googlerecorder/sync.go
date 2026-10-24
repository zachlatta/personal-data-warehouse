package googlerecorder

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultSettleAge is how long a finished recording's file must sit unchanged
// before it is sent. Recorder marks a recording complete when it has written
// it, so this only covers the moment between the two; the phone's service
// runs the uploader within seconds of Recorder's database changing, and a
// short memo is meant to be a transcript ~30 s after it ends.
const DefaultSettleAge = 5 * time.Second

// DefaultLimit is the recordings sent per run, newest first, so a first run
// over a long history leaves the newest memo waiting behind nothing.
const DefaultLimit = 50

// signedOutAccount is recording_meta.account when Recorder has no Google
// account signed in.
const signedOutAccount = "RECORDER_DEFAULT_ACCOUNT"

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadGoogleRecorderAudio(path, recordedAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error)
	UploadGoogleRecorderMetadata(payload map[string]any, recordedAt, audioContentSHA256, metadataDedupSHA256 string) (ingestclient.StoredObject, error)
}

// Summary is one run's counts.
type Summary struct {
	Seen             int
	AudioUploaded    int
	MetadataUploaded int
	Skipped          int // already landed, unchanged
	InProgress       int // Recorder is still recording or saving it
	Unsettled        int // finished, but its file changed seconds ago
	Missing          int // finished, but no audio file on the phone
	Remaining        int // waiting for a later run (the limit)
	Deferred         bool
}

// Runner sends every finished recording the ledger has not seen land.
type Runner struct {
	RecorderDir       string
	ScratchDir        string // where the database copy is made
	Device            string
	Account           string // used when Recorder has no account signed in
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
// naming the recording, so the run's heartbeat reads failing and the
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
	scratch := r.ScratchDir
	if scratch == "" {
		dir, err := os.MkdirTemp("", "pdw-google-recorder-")
		if err != nil {
			return Summary{}, err
		}
		defer os.RemoveAll(dir)
		scratch = dir
	}
	recordings, err := ReadRecordings(r.RecorderDir, scratch)
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
			r.Logger.Warningf("Recorder lists %s (%q) as finished but its audio is not at %s", recording.UUID, recording.Title, recording.Path)
			continue
		case now.Sub(recording.ModifiedAt) < r.SettleAge:
			summary.Unsettled++
			continue
		}
		entry, known, err := r.State.entry(recording.UUID)
		if err != nil {
			return summary, err
		}
		audioSHA := entry.AudioSHA256
		if !known || entry.SizeBytes != recording.SizeBytes || entry.ModifiedNS != recording.ModifiedAt.UnixNano() || audioSHA == "" {
			if audioSHA, err = common.FileSHA256(recording.Path); err != nil {
				return summary, fmt.Errorf("%s: hash audio: %w", recording.UUID, err)
			}
		}
		payload := BuildMetadata(r.accountFor(recording), r.Device, recording, audioSHA, now)
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
			r.Logger.Warningf("Skipping Google Recorder upload: %s", reason)
			summary.Deferred = true
			summary.Remaining += len(queue)
			return summary, nil
		}
	}
	for i, p := range queue {
		recordedAt := common.ISOFormat(p.recording.CreatedAt)
		r.Logger.Infof("[%d/%d] upload %s %q (%s, sha256=%s) -> app", i+1, len(queue), p.recording.UUID, p.recording.Title,
			common.FormatBytes(p.recording.SizeBytes), common.ShortSHA256(p.audioSHA))
		if p.sendAudio {
			if _, err := r.Client.UploadGoogleRecorderAudio(p.recording.Path, recordedAt, ".m4a", "audio/mp4", p.audioSHA); err != nil {
				return summary, fmt.Errorf("%s: upload audio: %w", p.recording.UUID, err)
			}
			summary.AudioUploaded++
		}
		if _, err := r.Client.UploadGoogleRecorderMetadata(p.payload, recordedAt, p.audioSHA, p.dedupSHA); err != nil {
			return summary, fmt.Errorf("%s: upload sidecar: %w", p.recording.UUID, err)
		}
		summary.MetadataUploaded++
		if err := r.State.record(p.recording, p.audioSHA, p.dedupSHA, r.Now()); err != nil {
			return summary, err
		}
	}
	return summary, nil
}

func (r *Runner) accountFor(recording Recording) string {
	if recording.Account != "" && recording.Account != signedOutAccount {
		return recording.Account
	}
	if r.Account != "" {
		return r.Account
	}
	return signedOutAccount
}

// BuildMetadata is a recording's JSON sidecar (schema_version 1), which
// personal_data_warehouse.google_recorder_drive_ingest maps onto
// base_google_recorder.recordings.
func BuildMetadata(account, device string, r Recording, audioSHA string, uploadedAt time.Time) map[string]any {
	recording := map[string]any{
		"recording_id":     r.UUID,
		"title":            r.Title,
		"recorded_at":      common.ISOFormat(r.CreatedAt),
		"duration_ms":      r.DurationMS,
		"duration_seconds": float64(r.DurationMS) / 1000,
		"filename":         r.UUID + ".m4a",
		"extension":        ".m4a",
		"content_type":     "audio/mp4",
		"size_bytes":       r.SizeBytes,
		"content_sha256":   audioSHA,
		"file_modified_at": common.ISOFormat(r.ModifiedAt),
		"recording_state":  r.State,
		"original_path":    r.StoredPath,
	}
	if r.LocationName != "" || r.Latitude != nil || r.Longitude != nil {
		location := map[string]any{"name": r.LocationName}
		if r.Latitude != nil {
			location["latitude"] = *r.Latitude
		}
		if r.Longitude != nil {
			location["longitude"] = *r.Longitude
		}
		if r.PlaceID != "" {
			location["place_id"] = r.PlaceID
		}
		recording["location"] = location
	}
	if r.ParentUUID != "" {
		recording["parent_uuid"] = r.ParentUUID
	}
	var config map[string]any
	if json.Unmarshal([]byte(r.RecordingConfig), &config) == nil && config != nil {
		recording["recording_config"] = config
	}
	return map[string]any{
		"schema_version": int64(1),
		"source":         "google_recorder",
		"account":        account,
		"device":         device,
		"uploaded_at":    common.ISOFormat(uploadedAt),
		"recording":      recording,
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
		panic(fmt.Sprintf("google recorder sidecar is not JSON: %v", err))
	}
	return common.BytesSHA256(encoded)
}
