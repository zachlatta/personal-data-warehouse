package androidcallrecordings

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/recordingupload"
)

const DefaultSettleAge = 10 * time.Second
const DefaultLimit = 50

type Uploader interface {
	UploadAndroidCallRecordingsAudio(string, string, string, string, string) (ingestclient.StoredObject, error)
	UploadAndroidCallRecordingsMetadata(map[string]any, string, string, string) (ingestclient.StoredObject, error)
}
type Summary struct {
	Seen             int
	AudioUploaded    int
	MetadataUploaded int
	Skipped          int
	Unsettled        int
	Remaining        int
	Invalid          int
	Deferred         bool
	NewestFinishedAt string
}
type Runner struct {
	RecorderDir, Device, Account string
	Client                       Uploader
	State                        *State
	Logger                       common.Logger
	Now                          func() time.Time
	SettleAge                    time.Duration
	Limit                        int
	BeforeUploadCheck            func() string
}

func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil || r.State == nil {
		return Summary{}, errors.New("client and state are required")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.SettleAge <= 0 {
		r.SettleAge = DefaultSettleAge
	}
	shared := recordingupload.Runner[Recording]{State: r.State, Now: r.Now, SettleAge: r.SettleAge, Limit: r.Limit, BeforeUploadCheck: r.BeforeUploadCheck, Logger: r.Logger,
		Read: func() ([]recordingupload.Item[Recording], int, error) {
			records, invalid, err := readRecordings(r.RecorderDir, r.Now(), r.SettleAge)
			var items []recordingupload.Item[Recording]
			for _, v := range records {
				settle := v.ModifiedAt
				if v.MetadataModifiedAt.After(settle) {
					settle = v.MetadataModifiedAt
				}
				items = append(items, recordingupload.Item[Recording]{Key: v.ID, Path: v.Path, SizeBytes: v.SizeBytes, ModifiedAt: v.ModifiedAt, SettleAt: settle, EndedAt: v.EndedAt, Ready: true, Exists: true, Value: v})
			}
			return items, invalid, err
		},
		BuildMetadata: func(v Recording, sha string, now time.Time) map[string]any {
			return BuildMetadata(r.Account, r.Device, v, sha, now)
		},
		UploadAudio: func(v Recording, sha string) error {
			_, err := r.Client.UploadAndroidCallRecordingsAudio(v.Path, common.ISOFormat(v.CreatedAt), v.Extension, v.ContentType, sha)
			return err
		},
		UploadMetadata: func(v Recording, p map[string]any, sha, dedup string) error {
			_, err := r.Client.UploadAndroidCallRecordingsMetadata(p, common.ISOFormat(v.CreatedAt), sha, dedup)
			return err
		},
	}
	s, err := shared.Sync()
	return Summary{Seen: s.Seen, AudioUploaded: s.AudioUploaded, MetadataUploaded: s.MetadataUploaded, Skipped: s.Skipped, Unsettled: s.Unsettled, Remaining: s.Remaining, Invalid: s.Invalid, Deferred: s.Deferred, NewestFinishedAt: s.NewestFinishedAt}, err
}
func BuildMetadata(account, device string, r Recording, audioSHA string, uploadedAt time.Time) map[string]any {
	recording := map[string]any{
		"recording_id":     r.ID,
		"title":            r.Title,
		"recorded_at":      common.ISOFormat(r.CreatedAt),
		"duration_ms":      r.DurationMS,
		"duration_seconds": float64(r.DurationMS) / 1000,
		"filename":         r.ID + r.Extension,
		"extension":        r.Extension,
		"content_type":     r.ContentType,
		"size_bytes":       r.SizeBytes,
		"content_sha256":   audioSHA,
		"file_modified_at": common.ISOFormat(r.ModifiedAt),
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
