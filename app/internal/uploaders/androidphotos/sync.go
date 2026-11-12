package androidphotos

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/photos"
)

// PhotoSource is the photo source slug every envelope carries; the warehouse
// routes it to base_android_photos.files (PHOTO_SOURCE_RELATIONS).
const PhotoSource = "android_photos"

// RecordKey names the source's raw record block in the envelope.
const RecordKey = "android_record"

// DefaultSettleAge is how long a file must sit unchanged before it is sent.
// The camera writes a shot under a hidden pending name and renames it when
// done, and MediaStore lists only finished files, so this covers an editor
// rewriting a file in place.
const DefaultSettleAge = 5 * time.Second

// DefaultLimit is the files sent per run, newest first: a first run over the
// whole library leaves the newest shot waiting behind nothing, and a run of
// ~10 MB photos stays well inside the service's deadline.
const DefaultLimit = 25

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadPhotoFile(path, capturedAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error)
	UploadPhotoMetadata(payload map[string]any, capturedAt, fileContentSHA256, metadataDedupSHA256 string) (ingestclient.StoredObject, error)
}

// Summary is one run's counts.
type Summary struct {
	Seen      int
	Uploaded  int
	Skipped   int // already landed, unchanged
	Unsettled int // changed in the last few seconds
	Missing   int // listed by MediaStore but gone from storage, or empty
	Motion    int // motion-photo videos sent (role live_video)
	// MotionUnreadable counts motion photos whose XMP points at bytes that are
	// not an MP4: the still is sent, the video is not, and the log names it.
	MotionUnreadable int
	Remaining        int // waiting for a later run (the limit)
	Deferred         bool
}

// Runner sends every gallery file the ledger has not seen land.
type Runner struct {
	MediaRoot         string // the primary volume's backing directory
	Device            string
	Account           string
	Client            Uploader
	State             *State
	Logger            common.Logger
	Now               func() time.Time
	SettleAge         time.Duration
	Limit             int
	BeforeUploadCheck func() string
	// NetworkCheck runs first, before MediaStore is read or a file hashed: a
	// non-empty reason defers the whole run. The CLI makes it Wi-Fi only.
	NetworkCheck func() string
	List         func() ([]MediaItem, error) // MediaStore; replaced in tests
	// Observe posts the gallery's own newest arrival (contract C11); nil
	// disables it.
	Observe ingestclient.ObservationPoster
}

type pending struct {
	item      MediaItem
	path      string
	size      int64
	mtimeNS   int64
	sha       string
	sendStill bool // false: the still landed before videos were sent; send only its video
}

func isJPEG(item MediaItem) bool { return strings.EqualFold(item.MimeType, "image/jpeg") }

// Sync runs one pass, newest shot first. It stops at the first failed upload
// (the network or the app being down fails every file alike) and returns an
// error naming the file, so the heartbeat reads failing and the file is
// retried next run.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil || r.State == nil {
		return Summary{}, errors.New("client and state are required")
	}
	if r.Account == "" {
		return Summary{}, errors.New("account is required")
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
	if r.MediaRoot == "" {
		r.MediaRoot = DefaultMediaRoot
	}
	if r.NetworkCheck != nil {
		if reason := r.NetworkCheck(); reason != "" {
			r.Logger.Infof("Android photo upload deferred: %s", reason)
			return Summary{Deferred: true}, nil
		}
	}
	list := r.List
	if list == nil {
		list = ListMedia
	}
	items, err := list()
	if err != nil {
		return Summary{}, err
	}
	// After the Wi-Fi gate, which keeps MediaStore unread off Wi-Fi: the C11
	// observation's interval allows for days away from Wi-Fi.
	r.observeNewest(items)
	sort.SliceStable(items, func(i, j int) bool { return items[i].TakenAt.After(items[j].TakenAt) })
	summary := Summary{Seen: len(items)}
	now := r.Now()
	var queue []pending
	for _, item := range items {
		path := filepath.Join(r.MediaRoot, filepath.FromSlash(item.RelativePath))
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			summary.Missing++
			continue
		}
		if err != nil {
			return summary, fmt.Errorf("%s: %w", item.RelativePath, err)
		}
		if info.Size() == 0 {
			// Nothing to send; an empty file is not one about to be written,
			// so it must not ask the service for a quick re-run.
			summary.Missing++
			continue
		}
		if now.Sub(info.ModTime()) < r.SettleAge {
			summary.Unsettled++
			continue
		}
		p := pending{item: item, path: path, size: info.Size(), mtimeNS: info.ModTime().UnixNano()}
		entry, known, err := r.State.entry(item.RelativePath)
		if err != nil {
			return summary, err
		}
		motionDone := known && (entry.MotionChecked || !isJPEG(item))
		if known && entry.SizeBytes == p.size && entry.ModifiedNS == p.mtimeNS {
			if motionDone {
				summary.Skipped++
				continue
			}
			p.sha = entry.ContentSHA256
		} else {
			if p.sha, err = common.FileSHA256(path); err != nil {
				return summary, fmt.Errorf("%s: hash: %w", item.RelativePath, err)
			}
			if known && entry.ContentSHA256 == p.sha && motionDone {
				// Touched but unchanged: remember the new stat so it is not re-hashed.
				summary.Skipped++
				if err := r.State.record(item.RelativePath, p.size, p.mtimeNS, p.sha, entry.MotionChecked, entry.MotionSHA256, now); err != nil {
					return summary, err
				}
				continue
			}
		}
		p.sendStill = !known || entry.ContentSHA256 != p.sha
		queue = append(queue, p)
		if len(queue) > r.Limit {
			break
		}
	}
	if len(queue) > r.Limit {
		// Hashing stops one past the limit: the exact backlog is not worth
		// reading every remaining file for.
		summary.Remaining = len(queue) - r.Limit
		queue = queue[:r.Limit]
	}
	if len(queue) > 0 && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping Android photo upload: %s", reason)
			summary.Deferred = true
			summary.Remaining += len(queue)
			return summary, nil
		}
	}
	for i, p := range queue {
		what := "upload"
		if !p.sendStill {
			what = "motion video of"
		}
		r.Logger.Infof("[%d/%d] %s %s (%s, sha256=%s) -> app", i+1, len(queue), what, p.item.RelativePath,
			common.FormatBytes(p.size), common.ShortSHA256(p.sha))
		motionSHA, err := r.upload(p, &summary)
		if err != nil {
			return summary, fmt.Errorf("%s: %w", p.item.RelativePath, err)
		}
		if p.sendStill {
			summary.Uploaded++
		}
		if err := r.State.record(p.item.RelativePath, p.size, p.mtimeNS, p.sha, true, motionSHA, r.Now()); err != nil {
			return summary, err
		}
	}
	return summary, nil
}

// upload sends the file (unless only its video is owed) and, for a motion
// photo, the video inside it as the same photo's live_video rendition. It
// returns the video's sha, or "" for a still without one.
func (r *Runner) upload(p pending, summary *Summary) (string, error) {
	var facts ExifFacts
	var blocks jpegBlocks
	if isJPEG(p.item) {
		var err error
		if blocks, err = readJPEGBlocks(p.path); err != nil {
			return "", fmt.Errorf("read jpeg: %w", err)
		}
		if blocks.exif != nil {
			facts = parseTIFF(blocks.exif)
		}
	}
	capturedAt, _ := CaptureTime(p.item.TakenAt, facts.OffsetTimeOriginal)
	if p.sendStill {
		mimeType := p.item.MimeType
		if mimeType == "" {
			mimeType = common.GuessMimeType(p.item.Filename)
		}
		if _, err := r.Client.UploadPhotoFile(p.path, capturedAt, strings.ToLower(filepath.Ext(p.item.Filename)), mimeType, p.sha); err != nil {
			return "", fmt.Errorf("upload file: %w", err)
		}
		envelope, err := BuildEnvelope(r.Account, r.Device, p.item, facts, p.sha, p.size, r.Now())
		if err != nil {
			return "", err
		}
		dedup := photos.ProvenanceDedupSHA256(PhotoSource, r.Account, p.item.RelativePath, "original", p.sha)
		if _, err := r.Client.UploadPhotoMetadata(envelope, capturedAt, p.sha, dedup); err != nil {
			return "", fmt.Errorf("upload metadata: %w", err)
		}
	}
	video, ok := FindMotionVideo(blocks.xmp, p.size)
	if !ok {
		return "", nil
	}
	tmp, err := os.CreateTemp("", "pdw-motion-*.mp4")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)
	videoSHA, err := ExtractMotionVideo(p.path, video, tmpPath)
	if err != nil {
		r.Logger.Warningf("%s: motion photo video not sent: %v", p.item.RelativePath, err)
		summary.MotionUnreadable++
		return "", nil
	}
	if _, err := r.Client.UploadPhotoFile(tmpPath, capturedAt, ".mp4", "video/mp4", videoSHA); err != nil {
		return "", fmt.Errorf("upload motion video: %w", err)
	}
	envelope, err := BuildMotionEnvelope(r.Account, r.Device, p.item, facts, video, p.sha, videoSHA, r.Now())
	if err != nil {
		return "", err
	}
	dedup := photos.ProvenanceDedupSHA256(PhotoSource, r.Account, p.item.RelativePath, "live_video", videoSHA)
	if _, err := r.Client.UploadPhotoMetadata(envelope, capturedAt, videoSHA, dedup); err != nil {
		return "", fmt.Errorf("upload motion video metadata: %w", err)
	}
	summary.Motion++
	return videoSHA, nil
}

var utcOffset = regexp.MustCompile(`^[+-](0\d|1[0-4]):[0-5]\d$`)

// CaptureTime is the envelope's captured_at and capture_tz_offset. The
// warehouse reads captured_at as a wall clock in capture_tz_offset, so with
// the camera's EXIF offset it is the local wall clock; without one (videos,
// screenshots) it is the UTC wall clock with no offset, which is the right
// instant and claims no zone.
func CaptureTime(takenAt time.Time, exifOffset string) (string, string) {
	const layout = "2006-01-02T15:04:05.000"
	if !utcOffset.MatchString(exifOffset) {
		return takenAt.UTC().Format(layout), ""
	}
	parsed, err := time.Parse("-07:00", exifOffset)
	if err != nil {
		return takenAt.UTC().Format(layout), ""
	}
	_, seconds := parsed.Zone()
	return takenAt.In(time.FixedZone(exifOffset, seconds)).Format(layout), exifOffset
}

// BuildEnvelope is a file's photo metadata envelope (the shared photo
// contract in photos.BuildPhotoMetadata) with MediaStore's row and the
// camera's EXIF facts as its android_record.
func BuildEnvelope(account, device string, item MediaItem, facts ExifFacts, sha string, size int64, uploadedAt time.Time) (map[string]any, error) {
	capturedAt, offset := CaptureTime(item.TakenAt, facts.OffsetTimeOriginal)
	return photos.BuildPhotoMetadata(photos.Envelope{
		Source:          PhotoSource,
		Account:         account,
		NativeID:        item.RelativePath,
		Role:            "original",
		Filename:        item.Filename,
		MimeType:        item.MimeType,
		SizeBytes:       size,
		ContentSHA256:   sha,
		UploadedAt:      common.ISOFormat(uploadedAt),
		Width:           item.Width,
		Height:          item.Height,
		CapturedAt:      capturedAt,
		CaptureTZOffset: offset,
		CameraMake:      facts.Make,
		CameraModel:     facts.Model,
		FileModifiedAt:  common.ISOFormat(item.ModifiedAt),
		RecordKey:       RecordKey,
		Record:          androidRecord(device, item, facts),
	})
}

// BuildMotionEnvelope is the envelope of a motion photo's video: the same
// native id as its still (which is what attaches it to the still's photo),
// role live_video, and where in the still's file it was found.
func BuildMotionEnvelope(account, device string, item MediaItem, facts ExifFacts, video MotionVideo, stillSHA, videoSHA string, uploadedAt time.Time) (map[string]any, error) {
	capturedAt, offset := CaptureTime(item.TakenAt, facts.OffsetTimeOriginal)
	record := androidRecord(device, item, facts)
	record["media_type"] = "video"
	record["motion_photo"] = map[string]any{
		"format":                    video.Format,
		"offset":                    video.Offset,
		"length":                    video.Length,
		"presentation_timestamp_us": video.PresentationTimestampUs,
		"still_content_sha256":      stillSHA,
	}
	return photos.BuildPhotoMetadata(photos.Envelope{
		Source:          PhotoSource,
		Account:         account,
		NativeID:        item.RelativePath,
		Role:            "live_video",
		Filename:        strings.TrimSuffix(item.Filename, filepath.Ext(item.Filename)) + ".mp4",
		MimeType:        "video/mp4",
		SizeBytes:       video.Length,
		ContentSHA256:   videoSHA,
		UploadedAt:      common.ISOFormat(uploadedAt),
		CapturedAt:      capturedAt,
		CaptureTZOffset: offset,
		CameraMake:      facts.Make,
		CameraModel:     facts.Model,
		FileModifiedAt:  common.ISOFormat(item.ModifiedAt),
		RecordKey:       RecordKey,
		Record:          record,
	})
}

// androidRecord is MediaStore's row and the camera's EXIF facts.
func androidRecord(device string, item MediaItem, facts ExifFacts) map[string]any {
	record := map[string]any{
		"device":              device,
		"media_store_id":      item.MediaID,
		"relative_path":       item.RelativePath,
		"media_type":          map[bool]string{true: "video", false: "image"}[item.IsVideo],
		"owner_package_name":  item.OwnerPackage,
		"is_favorite":         item.IsFavorite,
		"orientation":         item.Orientation,
		"duration_ms":         item.DurationMS,
		"date_taken":          common.ISOFormat(item.TakenAt),
		"date_taken_source":   map[bool]string{true: "date_added", false: "datetaken"}[item.TakenAtFromAdded],
		"generation_modified": item.GenerationModified,
	}
	if facts.DateTimeOriginal != "" {
		record["exif_datetime_original"] = facts.DateTimeOriginal
	}
	if facts.OffsetTimeOriginal != "" {
		record["exif_offset_time_original"] = facts.OffsetTimeOriginal
	}
	if facts.Latitude != nil && facts.Longitude != nil {
		// Top-level latitude/longitude: the identity layer reads GPS from any
		// source record block that carries both.
		record["latitude"] = *facts.Latitude
		record["longitude"] = *facts.Longitude
	}
	return record
}

// observeNewest posts the newest gallery file by date_added (when it reached
// the phone) with its relative path, the native id the warehouse stores as
// source_native_id, so the detector asks whether that exact file landed.
func (r *Runner) observeNewest(items []MediaItem) {
	var newest MediaItem
	for _, item := range items {
		if item.AddedAt.After(newest.AddedAt) {
			newest = item
		}
	}
	ingestclient.Observe(r.Observe, r.Logger, "android_photos", r.Account, newest.AddedAt, newest.RelativePath,
		fmt.Sprintf("MediaStore: the newest date_added of %d gallery files", len(items)), r.Now())
}
