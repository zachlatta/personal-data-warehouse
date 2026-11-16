package googlerecorder

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/recordingupload"
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
type Summary = recordingupload.Summary

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
	scratch := r.ScratchDir
	if scratch == "" {
		dir, err := os.MkdirTemp("", "pdw-google-recorder-")
		if err != nil {
			return Summary{}, err
		}
		defer os.RemoveAll(dir)
		scratch = dir
	}
	shared := recordingupload.Runner[Recording]{State: r.State, Now: r.Now, SettleAge: r.SettleAge, Limit: r.Limit, BeforeUploadCheck: r.BeforeUploadCheck, Logger: r.Logger,
		Read: func() ([]recordingupload.Item[Recording], int, error) {
			records, err := ReadRecordings(r.RecorderDir, scratch)
			items := make([]recordingupload.Item[Recording], 0, len(records))
			for _, v := range records {
				items = append(items, recordingupload.Item[Recording]{Key: v.UUID, Path: v.Path, SizeBytes: v.SizeBytes, ModifiedAt: v.ModifiedAt, SettleAt: v.ModifiedAt, Ready: v.State == CompletedState, Exists: v.FileExists, Value: v})
			}
			return items, 0, err
		},
		BuildMetadata: func(v Recording, sha string, now time.Time) map[string]any {
			return BuildMetadata(r.accountFor(v), r.Device, v, sha, now)
		},
		UploadAudio: func(v Recording, sha string) error {
			_, err := r.Client.UploadGoogleRecorderAudio(v.Path, common.ISOFormat(v.CreatedAt), ".m4a", "audio/mp4", sha)
			return err
		},
		UploadMetadata: func(v Recording, p map[string]any, sha, dedup string) error {
			_, err := r.Client.UploadGoogleRecorderMetadata(p, common.ISOFormat(v.CreatedAt), sha, dedup)
			return err
		},
		Error: func(v recordingupload.Item[Recording], stage string, err error) error {
			return fmt.Errorf("%s: %s: %w", v.Key, stage, err)
		},
		LogMissing: func(v recordingupload.Item[Recording]) {
			r.Logger.Warningf("Recorder lists %s (%q) as finished but its audio is not at %s", v.Key, v.Value.Title, v.Path)
		},
		LogUpload: func(i, n int, v recordingupload.Item[Recording], sha string) {
			r.Logger.Infof("[%d/%d] upload %s %q (%s, sha256=%s) -> app", i, n, v.Key, v.Value.Title, common.FormatBytes(v.SizeBytes), common.ShortSHA256(sha))
		},
		LogDeferral: func(reason string) { r.Logger.Warningf("Skipping Google Recorder upload: %s", reason) },
	}
	return shared.Sync()
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

var MetadataDedupSHA256 = recordingupload.MetadataDedupSHA256
