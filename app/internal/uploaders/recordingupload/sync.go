// Package recordingupload owns the ledger and serial queue for paired audio uploads.
package recordingupload

import (
	"errors"
	"fmt"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type Item[T any] struct {
	Key, Path                     string
	SizeBytes                     int64
	ModifiedAt, SettleAt, EndedAt time.Time
	Ready, Exists                 bool
	Value                         T
}
type Summary struct {
	Seen             int
	AudioUploaded    int
	MetadataUploaded int
	Skipped          int
	InProgress       int
	Unsettled        int
	Missing          int
	Remaining        int
	Invalid          int
	Deferred         bool
	NewestFinishedAt string
}
type Runner[T any] struct {
	Read              func() ([]Item[T], int, error)
	State             *State
	Now               func() time.Time
	SettleAge         time.Duration
	Limit             int
	BeforeUploadCheck func() string
	BuildMetadata     func(T, string, time.Time) map[string]any
	UploadAudio       func(T, string) error
	UploadMetadata    func(T, map[string]any, string, string) error
	Logger            common.Logger
	// A source may retain its existing diagnostic spelling; the default is
	// privacy-safe and never includes source identities, paths or provider errors.
	Error       func(Item[T], string, error) error
	LogUpload   func(int, int, Item[T], string)
	LogMissing  func(Item[T])
	LogDeferral func(string)
}
type pending[T any] struct {
	item               Item[T]
	audioSHA, dedupSHA string
	payload            map[string]any
	sendAudio          bool
}

func (r *Runner[T]) Sync() (summary Summary, err error) {
	if r.State == nil || r.Read == nil || r.UploadAudio == nil || r.UploadMetadata == nil {
		return summary, errors.New("reader, client and state are required")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.Logger == nil {
		r.Logger = &common.RecordingLogger{}
	}
	if r.Limit <= 0 {
		r.Limit = 50
	}
	if r.Error == nil {
		r.Error = func(_ Item[T], stage string, _ error) error { return fmt.Errorf("%s failed", stage) }
	}
	defer func() {
		if err == nil && summary.Invalid > 0 {
			err = fmt.Errorf("%d invalid recordings skipped", summary.Invalid)
		}
	}()
	items, invalid, readErr := r.Read()
	if readErr != nil {
		return summary, readErr
	}
	summary.Seen = len(items) + invalid
	summary.Invalid = invalid
	now := r.Now()
	var queue []pending[T]
	for _, item := range items {
		switch {
		case !item.Ready:
			summary.InProgress++
			continue
		case !item.Exists || item.SizeBytes == 0:
			summary.Missing++
			if r.LogMissing != nil {
				r.LogMissing(item)
			}
			continue
		case now.Sub(item.SettleAt) < r.SettleAge:
			summary.Unsettled++
			continue
		}
		if !item.EndedAt.IsZero() {
			if stamp := common.ISOFormat(item.EndedAt); stamp > summary.NewestFinishedAt {
				summary.NewestFinishedAt = stamp
			}
		}
		entry, known, e := r.State.entry(item.Key)
		if e != nil {
			return summary, r.Error(item, "read ledger", e)
		}
		sha := entry.AudioSHA256
		if !known || entry.SizeBytes != item.SizeBytes || entry.ModifiedNS != item.ModifiedAt.UnixNano() || sha == "" {
			sha, e = common.FileSHA256(item.Path)
			if e != nil {
				return summary, r.Error(item, "hash audio", e)
			}
		}
		payload := r.BuildMetadata(item.Value, sha, now)
		dedup := MetadataDedupSHA256(payload)
		if known && entry.AudioSHA256 == sha && entry.MetadataSHA256 == dedup {
			summary.Skipped++
			if entry.SizeBytes != item.SizeBytes || entry.ModifiedNS != item.ModifiedAt.UnixNano() {
				if e := r.State.record(item.Key, item.SizeBytes, item.ModifiedAt, sha, dedup, now); e != nil {
					return summary, r.Error(item, "write ledger", e)
				}
			}
			continue
		}
		queue = append(queue, pending[T]{item: item, audioSHA: sha, dedupSHA: dedup, payload: payload, sendAudio: !known || entry.AudioSHA256 != sha})
	}
	if len(queue) > r.Limit {
		summary.Remaining = len(queue) - r.Limit
		queue = queue[:r.Limit]
	}
	if len(queue) > 0 && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			summary.Deferred = true
			summary.Remaining += len(queue)
			if r.LogDeferral != nil {
				r.LogDeferral(reason)
			}
			return summary, nil
		}
	}
	for i, p := range queue {
		if r.LogUpload != nil {
			r.LogUpload(i+1, len(queue), p.item, p.audioSHA)
		} else {
			r.Logger.Infof("[%d/%d] upload sha256=%s", i+1, len(queue), common.ShortSHA256(p.audioSHA))
		}
		if p.sendAudio {
			if e := r.UploadAudio(p.item.Value, p.audioSHA); e != nil {
				return summary, r.Error(p.item, "upload audio", e)
			}
			summary.AudioUploaded++
		}
		if e := r.UploadMetadata(p.item.Value, p.payload, p.audioSHA, p.dedupSHA); e != nil {
			return summary, r.Error(p.item, "upload sidecar", e)
		}
		summary.MetadataUploaded++
		if e := r.State.record(p.item.Key, p.item.SizeBytes, p.item.ModifiedAt, p.audioSHA, p.dedupSHA, r.Now()); e != nil {
			return summary, r.Error(p.item, "write ledger", e)
		}
	}
	return summary, nil
}

// MetadataDedupSHA256 excludes only the upload time and never mutates its input.
func MetadataDedupSHA256(payload map[string]any) string {
	identity := make(map[string]any, len(payload))
	for k, v := range payload {
		if k != "uploaded_at" {
			identity[k] = v
		}
	}
	encoded, err := common.CanonicalJSON(identity)
	if err != nil {
		panic(fmt.Sprintf("recording sidecar is not JSON: %v", err))
	}
	return common.BytesSHA256(encoded)
}
