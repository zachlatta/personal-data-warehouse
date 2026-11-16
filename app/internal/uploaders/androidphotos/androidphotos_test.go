package androidphotos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/photos"
)

// --- MediaStore ------------------------------------------------------------

const mediaRow = "Row: %d _id=%d, datetaken=%s, date_added=1794089746, date_modified=1794089749, mime_type=%s, media_type=%d, _size=7555900, width=6128, height=8160, orientation=0, duration=%s, is_favorite=%d, owner_package_name=%s, generation_modified=12714, _data=%s"

func row(n, id int, taken, mime string, mediaType int, duration string, favorite int, owner, data string) string {
	return fmt.Sprintf(mediaRow, n, id, taken, mime, mediaType, duration, favorite, owner, data)
}

func TestParseMediaRowsReadsEveryColumnAndKeepsAPathWithCommasWhole(t *testing.T) {
	out := strings.Join([]string{
		row(0, 1622, "1794089746101", "image/jpeg", 1, "NULL", 1, "com.google.android.GoogleCamera", "/storage/emulated/0/DCIM/Camera/PXL_20261107_221546101.jpg"),
		row(1, 1700, "1794000000000", "video/mp4", 3, "61000", 0, "NULL", "/storage/emulated/0/Movies/Trip, day 2, final=yes.mp4"),
		"",
	}, "\n")
	items, err := ParseMediaRows(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	photo := items[0]
	if photo.RelativePath != "DCIM/Camera/PXL_20261107_221546101.jpg" || photo.Filename != "PXL_20261107_221546101.jpg" {
		t.Fatalf("path = %q / %q", photo.RelativePath, photo.Filename)
	}
	if !photo.TakenAt.Equal(time.UnixMilli(1794089746101)) || photo.TakenAtFromAdded {
		t.Fatalf("taken = %s (from added %v)", photo.TakenAt, photo.TakenAtFromAdded)
	}
	if photo.MediaID != 1622 || photo.MimeType != "image/jpeg" || photo.IsVideo || photo.SizeBytes != 7555900 ||
		photo.Width != 6128 || photo.Height != 8160 || !photo.IsFavorite || photo.OwnerPackage != "com.google.android.GoogleCamera" ||
		photo.GenerationModified != 12714 || !photo.ModifiedAt.Equal(time.Unix(1794089749, 0)) {
		t.Fatalf("photo = %+v", photo)
	}
	video := items[1]
	if video.RelativePath != "Movies/Trip, day 2, final=yes.mp4" || !video.IsVideo || video.DurationMS != 61000 || video.OwnerPackage != "" {
		t.Fatalf("video = %+v", video)
	}
}

func TestParseMediaRowsFallsBackToDateAddedWithoutATakenTime(t *testing.T) {
	items, err := ParseMediaRows(row(0, 5, "NULL", "image/png", 1, "NULL", 0, "com.android.systemui", "/storage/emulated/0/Pictures/Screenshots/Screenshot_20261029.png"))
	if err != nil {
		t.Fatal(err)
	}
	if !items[0].TakenAt.Equal(time.Unix(1794089746, 0)) || !items[0].TakenAtFromAdded {
		t.Fatalf("taken = %s from added %v", items[0].TakenAt, items[0].TakenAtFromAdded)
	}
}

func TestParseMediaRowsSkipsOtherVolumesAndHiddenFiles(t *testing.T) {
	out := strings.Join([]string{
		row(0, 1, "1", "image/jpeg", 1, "NULL", 0, "x", "/storage/1234-ABCD/DCIM/Camera/a.jpg"),
		row(1, 2, "1", "image/jpeg", 1, "NULL", 0, "x", "/storage/emulated/0/DCIM/Camera/.pending-1794089746-PXL.jpg"),
	}, "\n")
	items, err := ParseMediaRows(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("items = %+v, want none", items)
	}
}

func TestParseMediaRowsReadsNoResultAsEmptyAndAnythingElseAsAnError(t *testing.T) {
	if items, err := ParseMediaRows("No result found.\n"); err != nil || len(items) != 0 {
		t.Fatalf("items=%v err=%v", items, err)
	}
	if _, err := ParseMediaRows("Error while accessing provider:media\njava.lang.SecurityException: nope\n"); err == nil {
		t.Fatal("a refused query must fail loud, not read as an empty library")
	}
}

// --- EXIF ------------------------------------------------------------------

type tiffTag struct {
	id    uint16
	typ   uint16
	count uint32
	data  []byte
}

func asciiTag(id uint16, value string) tiffTag {
	data := append([]byte(value), 0)
	return tiffTag{id: id, typ: exifTypeASCII, count: uint32(len(data)), data: data}
}

func rationalTag(id uint16, values ...[2]uint32) tiffTag {
	var buf bytes.Buffer
	for _, v := range values {
		_ = binary.Write(&buf, binary.LittleEndian, v[0])
		_ = binary.Write(&buf, binary.LittleEndian, v[1])
	}
	return tiffTag{id: id, typ: exifTypeRational, count: uint32(len(values)), data: buf.Bytes()}
}

// buildTIFF lays out IFD0, then the Exif and GPS IFDs, each followed by its
// out-of-line values, little-endian. Pointers to the sub-IFDs are added here.
func buildTIFF(ifd0, exif, gps []tiffTag) []byte {
	order := binary.LittleEndian
	size := func(tags []tiffTag) int {
		n := 2 + 12*len(tags) + 4
		for _, tag := range tags {
			if len(tag.data) > 4 {
				n += len(tag.data)
			}
		}
		return n
	}
	pointer := func(id uint16) tiffTag { return tiffTag{id: id, typ: exifTypeLong, count: 1, data: make([]byte, 4)} }
	ifd0 = append(append([]tiffTag{}, ifd0...), pointer(tagExifIFD), pointer(tagGPSIFD))
	ifd0At := 8
	exifAt := ifd0At + size(ifd0)
	gpsAt := exifAt + size(exif)
	order.PutUint32(ifd0[len(ifd0)-2].data, uint32(exifAt))
	order.PutUint32(ifd0[len(ifd0)-1].data, uint32(gpsAt))
	out := []byte{'I', 'I', 42, 0, 8, 0, 0, 0}
	write := func(tags []tiffTag, at int) {
		extra := at + 2 + 12*len(tags) + 4
		var block, values bytes.Buffer
		_ = binary.Write(&block, order, uint16(len(tags)))
		for _, tag := range tags {
			_ = binary.Write(&block, order, tag.id)
			_ = binary.Write(&block, order, tag.typ)
			_ = binary.Write(&block, order, tag.count)
			field := make([]byte, 4)
			if len(tag.data) <= 4 {
				copy(field, tag.data)
			} else {
				order.PutUint32(field, uint32(extra+values.Len()))
				values.Write(tag.data)
			}
			block.Write(field)
		}
		block.Write([]byte{0, 0, 0, 0})
		out = append(out, block.Bytes()...)
		out = append(out, values.Bytes()...)
	}
	write(ifd0, ifd0At)
	write(exif, exifAt)
	write(gps, gpsAt)
	return out
}

// jpegWith wraps TIFF bytes in a JPEG: SOI, an APP0, the APP1 Exif segment,
// then start of scan and some "image data" the reader must never reach.
func jpegWith(tiff []byte) []byte {
	var buf bytes.Buffer
	buf.Write([]byte{0xFF, 0xD8})
	buf.Write([]byte{0xFF, 0xE0, 0, 16, 'J', 'F', 'I', 'F', 0, 1, 1, 0, 0, 1, 0, 1, 0, 0})
	payload := append([]byte("Exif\x00\x00"), tiff...)
	buf.Write([]byte{0xFF, 0xE1})
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(payload)+2))
	buf.Write(payload)
	buf.Write([]byte{0xFF, 0xDA, 0, 2})
	buf.Write(bytes.Repeat([]byte{0xAB}, 64))
	buf.Write([]byte{0xFF, 0xD9})
	return buf.Bytes()
}

func pixelJPEG(t *testing.T, gps bool) []byte {
	t.Helper()
	var gpsTags []tiffTag
	if gps {
		gpsTags = []tiffTag{
			asciiTag(tagGPSLatitudeRef, "N"),
			rationalTag(tagGPSLatitude, [2]uint32{12, 1}, [2]uint32{30, 1}, [2]uint32{1500, 100}),
			asciiTag(tagGPSLongitudeRef, "W"),
			rationalTag(tagGPSLongitude, [2]uint32{45, 1}, [2]uint32{15, 1}, [2]uint32{0, 1}),
		}
	}
	return jpegWith(buildTIFF(
		[]tiffTag{asciiTag(tagMake, "Google"), asciiTag(tagModel, "Pixel 11 Pro")},
		[]tiffTag{asciiTag(tagDateTimeOriginal, "2026:11:07 17:15:46"), asciiTag(tagOffsetTimeOriginal, "-05:00")},
		gpsTags,
	))
}

func TestReadJPEGExifReadsCameraOffsetAndGPS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "PXL.jpg")
	if err := os.WriteFile(path, pixelJPEG(t, true), 0o600); err != nil {
		t.Fatal(err)
	}
	facts, err := ReadJPEGExif(path)
	if err != nil {
		t.Fatal(err)
	}
	if facts.Make != "Google" || facts.Model != "Pixel 11 Pro" || facts.DateTimeOriginal != "2026:11:07 17:15:46" || facts.OffsetTimeOriginal != "-05:00" {
		t.Fatalf("facts = %+v", facts)
	}
	if facts.Latitude == nil || math.Abs(*facts.Latitude-(12+30.0/60+15.0/3600)) > 1e-9 {
		t.Fatalf("latitude = %v", facts.Latitude)
	}
	if facts.Longitude == nil || math.Abs(*facts.Longitude-(-(45+15.0/60))) > 1e-9 {
		t.Fatalf("longitude = %v", facts.Longitude)
	}
}

func TestReadJPEGExifIsEmptyForAFileWithoutExif(t *testing.T) {
	dir := t.TempDir()
	plain := filepath.Join(dir, "plain.jpg")
	_ = os.WriteFile(plain, []byte{0xFF, 0xD8, 0xFF, 0xDA, 0, 2, 1, 2, 3, 0xFF, 0xD9}, 0o600)
	notJPEG := filepath.Join(dir, "shot.png")
	_ = os.WriteFile(notJPEG, []byte("\x89PNG\r\n\x1a\n"), 0o600)
	for _, path := range []string{plain, notJPEG} {
		facts, err := ReadJPEGExif(path)
		if err != nil || facts != (ExifFacts{}) {
			t.Fatalf("%s: facts=%+v err=%v", path, facts, err)
		}
	}
}

func TestCaptureTimeIsTheLocalWallClockWithTheCamerasOffsetElseUTC(t *testing.T) {
	taken := time.UnixMilli(1794089746101).UTC() // 2026-11-07T22:15:46.101Z
	at, offset := CaptureTime(taken, "-05:00")
	if at != "2026-11-07T17:15:46.101" || offset != "-05:00" {
		t.Fatalf("with offset: %q %q", at, offset)
	}
	at, offset = CaptureTime(taken, "")
	if at != "2026-11-07T22:15:46.101" || offset != "" {
		t.Fatalf("without offset: %q %q", at, offset)
	}
	at, offset = CaptureTime(taken, "garbage")
	if at != "2026-11-07T22:15:46.101" || offset != "" {
		t.Fatalf("bad offset: %q %q", at, offset)
	}
}

// --- runner ----------------------------------------------------------------

type sentFile struct {
	path, capturedAt, extension, contentType, sha string
}

type sentEnvelope struct {
	payload    map[string]any
	capturedAt string
	sha, dedup string
}

type fakeUploader struct {
	files     []sentFile
	envelopes []sentEnvelope
	failOn    string // fail the file upload whose path ends with this
}

func (f *fakeUploader) UploadPhotoFile(path, capturedAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error) {
	if f.failOn != "" && strings.HasSuffix(path, f.failOn) {
		return ingestclient.StoredObject{}, errors.New("app unreachable")
	}
	f.files = append(f.files, sentFile{path, capturedAt, extension, contentType, contentSHA256})
	return ingestclient.StoredObject{}, nil
}

func (f *fakeUploader) UploadPhotoMetadata(payload map[string]any, capturedAt, fileContentSHA256, metadataDedupSHA256 string) (ingestclient.StoredObject, error) {
	f.envelopes = append(f.envelopes, sentEnvelope{payload, capturedAt, fileContentSHA256, metadataDedupSHA256})
	return ingestclient.StoredObject{}, nil
}

type library struct {
	t     *testing.T
	root  string
	items []MediaItem
	now   time.Time
}

func newLibrary(t *testing.T) *library {
	return &library{t: t, root: t.TempDir(), now: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
}

func (l *library) add(relative, mime string, taken time.Time, content []byte) {
	l.t.Helper()
	path := filepath.Join(l.root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		l.t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		l.t.Fatal(err)
	}
	old := l.now.Add(-time.Hour)
	_ = os.Chtimes(path, old, old)
	l.items = append(l.items, MediaItem{
		MediaID: int64(len(l.items) + 1), RelativePath: relative, Filename: filepath.Base(relative),
		MimeType: mime, IsVideo: strings.HasPrefix(mime, "video/"), SizeBytes: int64(len(content)),
		Width: 4000, Height: 3000, OwnerPackage: "com.google.android.GoogleCamera", TakenAt: taken, ModifiedAt: old,
	})
}

func (l *library) runner(t *testing.T, client Uploader, state *State) *Runner {
	return &Runner{
		MediaRoot: l.root, Device: "pichu", Account: "zach@example.com", Client: client, State: state,
		Now:  func() time.Time { return l.now },
		List: func() ([]MediaItem, error) { return append([]MediaItem(nil), l.items...), nil },
	}
}

func openState(t *testing.T) *State {
	t.Helper()
	state, err := OpenState(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	return state
}

func TestSyncSendsNewestFirstWithTheSharedPhotoEnvelope(t *testing.T) {
	lib := newLibrary(t)
	older := time.Date(2026, 11, 5, 16, 42, 53, 0, time.UTC)
	newer := time.UnixMilli(1794089746101).UTC()
	lib.add("Pictures/Screenshots/Screenshot_20261105.png", "image/png", older, []byte("png bytes"))
	lib.add("DCIM/Camera/PXL_20261107_221546101.jpg", "image/jpeg", newer, pixelJPEG(t, true))
	client := &fakeUploader{}
	summary, err := lib.runner(t, client, openState(t)).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Seen != 2 || summary.Uploaded != 2 {
		t.Fatalf("summary = %+v", summary)
	}
	if !strings.HasSuffix(client.files[0].path, "PXL_20261107_221546101.jpg") || client.files[0].extension != ".jpg" ||
		client.files[0].contentType != "image/jpeg" || client.files[0].capturedAt != "2026-11-07T17:15:46.101" {
		t.Fatalf("first file = %+v", client.files[0])
	}
	envelope := client.envelopes[0].payload
	file := envelope["file"].(map[string]any)
	if envelope["source"] != "android_photos" || envelope["account"] != "zach@example.com" ||
		file["native_id"] != "DCIM/Camera/PXL_20261107_221546101.jpg" || file["role"] != "original" ||
		file["captured_at"] != "2026-11-07T17:15:46.101" || file["capture_tz_offset"] != "-05:00" ||
		file["camera_make"] != "Google" || file["camera_model"] != "Pixel 11 Pro" || file["content_sha256"] != client.files[0].sha {
		t.Fatalf("envelope = %+v", envelope)
	}
	record := envelope["android_record"].(map[string]any)
	if record["device"] != "pichu" || record["relative_path"] != "DCIM/Camera/PXL_20261107_221546101.jpg" ||
		record["owner_package_name"] != "com.google.android.GoogleCamera" || record["latitude"] == nil || record["longitude"] == nil {
		t.Fatalf("record = %+v", record)
	}
	wantDedup := photos.ProvenanceDedupSHA256("android_photos", "zach@example.com", "DCIM/Camera/PXL_20261107_221546101.jpg", "original", client.files[0].sha)
	if client.envelopes[0].dedup != wantDedup {
		t.Fatalf("dedup = %s, want %s", client.envelopes[0].dedup, wantDedup)
	}
	// The screenshot has no EXIF: UTC wall clock, no offset, no camera.
	shot := client.envelopes[1].payload["file"].(map[string]any)
	if shot["captured_at"] != "2026-11-05T16:42:53.000" || shot["capture_tz_offset"] != "" || shot["camera_make"] != "" {
		t.Fatalf("screenshot = %+v", shot)
	}
	if _, ok := client.envelopes[1].payload["android_record"].(map[string]any)["latitude"]; ok {
		t.Fatal("no GPS must mean no latitude key")
	}
}

func TestSyncSendsAFileOnceAndAgainOnlyWhenItsBytesChange(t *testing.T) {
	lib := newLibrary(t)
	lib.add("DCIM/Camera/PXL_1.jpg", "image/jpeg", lib.now.Add(-2*time.Hour), []byte("v1"))
	state := openState(t)
	first := &fakeUploader{}
	if _, err := lib.runner(t, first, state).Sync(); err != nil || len(first.files) != 1 {
		t.Fatalf("first run: files=%d err=%v", len(first.files), err)
	}
	// Unchanged: nothing sent, nothing re-read.
	second := &fakeUploader{}
	summary, err := lib.runner(t, second, state).Sync()
	if err != nil || len(second.files) != 0 || summary.Skipped != 1 {
		t.Fatalf("second run: %+v files=%d err=%v", summary, len(second.files), err)
	}
	// Touched with the same bytes: still nothing sent.
	path := filepath.Join(lib.root, "DCIM/Camera/PXL_1.jpg")
	touched := lib.now.Add(-30 * time.Minute)
	_ = os.Chtimes(path, touched, touched)
	third := &fakeUploader{}
	if _, err := lib.runner(t, third, state).Sync(); err != nil || len(third.files) != 0 {
		t.Fatalf("touched run: files=%d err=%v", len(third.files), err)
	}
	// Edited in place (an editor saving over it): the new bytes go.
	_ = os.WriteFile(path, []byte("v2 edited"), 0o600)
	edited := lib.now.Add(-20 * time.Minute)
	_ = os.Chtimes(path, edited, edited)
	fourth := &fakeUploader{}
	if _, err := lib.runner(t, fourth, state).Sync(); err != nil || len(fourth.files) != 1 {
		t.Fatalf("edited run: files=%d err=%v", len(fourth.files), err)
	}
	if fourth.files[0].sha == first.files[0].sha {
		t.Fatal("an edit must send the new bytes")
	}
}

func TestSyncWaitsForAFileStillBeingWrittenAndSkipsOneThatIsGoneOrEmpty(t *testing.T) {
	lib := newLibrary(t)
	lib.add("DCIM/Camera/PXL_fresh.jpg", "image/jpeg", lib.now, []byte("fresh"))
	fresh := filepath.Join(lib.root, "DCIM/Camera/PXL_fresh.jpg")
	_ = os.Chtimes(fresh, lib.now.Add(-time.Second), lib.now.Add(-time.Second))
	lib.add("DCIM/Camera/PXL_gone.jpg", "image/jpeg", lib.now, []byte("gone"))
	_ = os.Remove(filepath.Join(lib.root, "DCIM/Camera/PXL_gone.jpg"))
	lib.add("DCIM/Camera/PXL_empty.jpg", "image/jpeg", lib.now, nil)
	client := &fakeUploader{}
	summary, err := lib.runner(t, client, openState(t)).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Unsettled != 1 || summary.Missing != 2 || len(client.files) != 0 {
		t.Fatalf("summary = %+v files=%d", summary, len(client.files))
	}
}

func TestSyncSendsAtMostTheLimitAndReportsTheRest(t *testing.T) {
	lib := newLibrary(t)
	for i := 0; i < 5; i++ {
		lib.add("DCIM/Camera/PXL_"+string(rune('a'+i))+".jpg", "image/jpeg", lib.now.Add(-time.Duration(10-i)*time.Hour), []byte{byte(i)})
	}
	client := &fakeUploader{}
	runner := lib.runner(t, client, openState(t))
	runner.Limit = 2
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.files) != 2 || summary.Remaining == 0 {
		t.Fatalf("summary = %+v files=%d", summary, len(client.files))
	}
	if !strings.HasSuffix(client.files[0].path, "PXL_e.jpg") || !strings.HasSuffix(client.files[1].path, "PXL_d.jpg") {
		t.Fatalf("newest first: %s, %s", client.files[0].path, client.files[1].path)
	}
}

func TestSyncStopsAtTheFirstFailedUploadAndRetriesItNextRun(t *testing.T) {
	lib := newLibrary(t)
	lib.add("DCIM/Camera/PXL_new.jpg", "image/jpeg", lib.now.Add(-time.Hour), []byte("new"))
	lib.add("DCIM/Camera/PXL_old.jpg", "image/jpeg", lib.now.Add(-2*time.Hour), []byte("old"))
	state := openState(t)
	failing := &fakeUploader{failOn: "PXL_new.jpg"}
	if _, err := lib.runner(t, failing, state).Sync(); err == nil || !strings.Contains(err.Error(), "DCIM/Camera/PXL_new.jpg") {
		t.Fatalf("err = %v, want the failed file named", err)
	}
	if len(failing.files) != 0 {
		t.Fatalf("nothing after the failure may be sent: %+v", failing.files)
	}
	retry := &fakeUploader{}
	if _, err := lib.runner(t, retry, state).Sync(); err != nil || len(retry.files) != 2 {
		t.Fatalf("retry: files=%d err=%v", len(retry.files), err)
	}
}

func TestSyncDefersWhenTheAppIsUnreachable(t *testing.T) {
	lib := newLibrary(t)
	lib.add("DCIM/Camera/PXL_1.jpg", "image/jpeg", lib.now.Add(-time.Hour), []byte("x"))
	client := &fakeUploader{}
	runner := lib.runner(t, client, openState(t))
	runner.BeforeUploadCheck = func() string { return "app unreachable" }
	summary, err := runner.Sync()
	if err != nil || !summary.Deferred || summary.Remaining != 1 || len(client.files) != 0 {
		t.Fatalf("summary=%+v files=%d err=%v", summary, len(client.files), err)
	}
}

func TestRunDryRunListsWithoutSendingOrRecording(t *testing.T) {
	lib := newLibrary(t)
	lib.add("DCIM/Camera/PXL_1.jpg", "image/jpeg", time.Now().Add(-time.Hour), pixelJPEG(t, false))
	old := time.Now().Add(-time.Hour)
	_ = os.Chtimes(filepath.Join(lib.root, "DCIM/Camera/PXL_1.jpg"), old, old)
	saved := queryMediaStore
	t.Cleanup(func() { queryMediaStore = saved })
	queryMediaStore = func() (string, error) {
		return row(0, 1, "1794089746101", "image/jpeg", 1, "NULL", 0, "com.google.android.GoogleCamera", PrimaryVolumePath+"DCIM/Camera/PXL_1.jpg"), nil
	}
	stateFile := filepath.Join(t.TempDir(), "state.sqlite")
	var stdout, stderr bytes.Buffer
	env := map[string]string{"PDW_INGEST_PROJECT_DIR": t.TempDir()}
	code := Run([]string{"--dry-run", "--media-root", lib.root, "--account", "zach@example.com", "--device", "pichu", "--state-file", stateFile},
		strings.NewReader(""), &stdout, &stderr, func(k string) string { return env[k] }, ingestclient.Config{})
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "would send original envelope for DCIM/Camera/PXL_1.jpg (camera Google Pixel 11 Pro, offset \"-05:00\")") ||
		!strings.Contains(stdout.String(), "uploaded=1") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	if _, err := os.Stat(stateFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a dry run must not create the ledger: %v", err)
	}
}

func TestRunRequiresAnAccount(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := map[string]string{"PDW_INGEST_PROJECT_DIR": t.TempDir()}
	code := Run([]string{"--dry-run", "--state-file", filepath.Join(t.TempDir(), "s.sqlite")},
		strings.NewReader(""), &stdout, &stderr, func(k string) string { return env[k] }, ingestclient.Config{})
	if code != common.ExitFailure || !strings.Contains(stderr.String(), "ANDROID_PHOTOS_ACCOUNT") {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
}

func TestSyncOffWiFiDefersBeforeReadingMediaStoreOrAnyFile(t *testing.T) {
	lib := newLibrary(t)
	lib.add("DCIM/Camera/PXL_1.jpg", "image/jpeg", lib.now.Add(-time.Hour), []byte("x"))
	client := &fakeUploader{}
	runner := lib.runner(t, client, openState(t))
	listed := false
	runner.List = func() ([]MediaItem, error) { listed = true; return lib.items, nil }
	runner.NetworkCheck = func() string { return "device is not on Wi-Fi (Wi-Fi only)" }
	summary, err := runner.Sync()
	if err != nil || !summary.Deferred || listed || len(client.files) != 0 {
		t.Fatalf("summary=%+v listed=%v files=%d err=%v", summary, listed, len(client.files), err)
	}
	// Back on Wi-Fi, the same run sends it.
	runner.NetworkCheck = func() string { return "" }
	if _, err := runner.Sync(); err != nil || len(client.files) != 1 {
		t.Fatalf("on Wi-Fi: files=%d err=%v", len(client.files), err)
	}
}

// --- motion photos -----------------------------------------------------------

// motionXMP is the Container directory Pixel Camera writes: the primary JPEG,
// a gain map, then the video, last in the file.
func motionXMP(gainMap, video int) string {
	return fmt.Sprintf(`<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF><rdf:Description
 GCamera:MotionPhoto="1" GCamera:MotionPhotoVersion="1" GCamera:MotionPhotoPresentationTimestampUs="1196528">
<Container:Directory><rdf:Seq>
<rdf:li rdf:parseType="Resource"><Container:Item
  Item:Semantic="Primary"
  Item:Mime="image/jpeg"/></rdf:li>
<rdf:li rdf:parseType="Resource"><Container:Item
  Item:Length="%d"
  Item:Mime="image/jpeg"
  Item:Semantic="GainMap"/></rdf:li>
<rdf:li rdf:parseType="Resource"><Container:Item
  Item:Mime="video/mp4"
  Item:Semantic="MotionPhoto"
  Item:Length="%d"
  Item:Padding="0"/></rdf:li>
</rdf:Seq></Container:Directory></rdf:Description></rdf:RDF></x:xmpmeta>`, gainMap, video)
}

func fakeMP4(n int) []byte {
	video := append([]byte{0, 0, 0, 24}, []byte("ftypisom")...)
	return append(video, bytes.Repeat([]byte{0x42}, n-len(video))...)
}

// motionJPEG is a JPEG with EXIF and XMP before the image data, then a gain
// map and the video appended after the end-of-image marker.
func motionJPEG(t *testing.T, xmp string, video []byte) []byte {
	t.Helper()
	base := pixelJPEG(t, false)
	// Insert the XMP APP1 after SOI.
	payload := append([]byte(xmpSignature), []byte(xmp)...)
	var seg bytes.Buffer
	seg.Write([]byte{0xFF, 0xE1})
	_ = binary.Write(&seg, binary.BigEndian, uint16(len(payload)+2))
	seg.Write(payload)
	out := append([]byte{}, base[:2]...)
	out = append(out, seg.Bytes()...)
	out = append(out, base[2:]...)
	out = append(out, bytes.Repeat([]byte{0xEE}, 100)...) // the gain map
	return append(out, video...)
}

func TestFindMotionVideoReadsTheContainerDirectory(t *testing.T) {
	video := fakeMP4(5000)
	file := motionJPEG(t, motionXMP(100, len(video)), video)
	got, ok := FindMotionVideo([]byte(motionXMP(100, len(video))), int64(len(file)))
	if !ok || got.Offset != int64(len(file)-len(video)) || got.Length != int64(len(video)) ||
		got.Format != "motion_photo" || got.PresentationTimestampUs != 1196528 {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
	// A video that is not the last item: the items after it are skipped over.
	xmp := `<Container:Item Item:Semantic="Primary" Item:Mime="image/jpeg"/>
<Container:Item Item:Semantic="MotionPhoto" Item:Mime="video/mp4" Item:Length="300" Item:Padding="8"/>
<Container:Item Item:Semantic="Depth" Item:Mime="image/jpeg" Item:Length="50"/>`
	got, ok = FindMotionVideo([]byte(xmp), 10000)
	if !ok || got.Offset != 10000-50-8-300 || got.Length != 300 {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestFindMotionVideoReadsTheOlderMicroVideoTags(t *testing.T) {
	got, ok := FindMotionVideo([]byte(`GCamera:MicroVideo="1" GCamera:MicroVideoVersion="1" GCamera:MicroVideoOffset="4000" GCamera:MicroVideoPresentationTimestampUs="5"`), 9000)
	if !ok || got.Offset != 5000 || got.Length != 4000 || got.Format != "micro_video" || got.PresentationTimestampUs != 5 {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestFindMotionVideoIgnoresAStill(t *testing.T) {
	for _, xmp := range []string{"", `<Container:Item Item:Semantic="Primary" Item:Mime="image/jpeg"/>`, `GCamera:MicroVideo="0"`} {
		if got, ok := FindMotionVideo([]byte(xmp), 1000); ok {
			t.Fatalf("%q: got %+v", xmp, got)
		}
	}
	// A length past the start of the file is not a video.
	if got, ok := FindMotionVideo([]byte(motionXMP(0, 5000)), 1000); ok {
		t.Fatalf("oversized: got %+v", got)
	}
}

func TestSyncSendsAMotionPhotosVideoAsTheSamePhotosLiveVideo(t *testing.T) {
	lib := newLibrary(t)
	video := fakeMP4(5000)
	lib.add("DCIM/Camera/PXL_20261106_214733378.MP.jpg", "image/jpeg", lib.now.Add(-time.Hour),
		motionJPEG(t, motionXMP(100, len(video)), video))
	client := &fakeUploader{}
	summary, err := lib.runner(t, client, openState(t)).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Uploaded != 1 || summary.Motion != 1 || len(client.files) != 2 || len(client.envelopes) != 2 {
		t.Fatalf("summary=%+v files=%d envelopes=%d", summary, len(client.files), len(client.envelopes))
	}
	still, motion := client.envelopes[0].payload["file"].(map[string]any), client.envelopes[1].payload["file"].(map[string]any)
	if still["role"] != "original" || motion["role"] != "live_video" || motion["native_id"] != still["native_id"] {
		t.Fatalf("still=%+v motion=%+v", still, motion)
	}
	if motion["filename"] != "PXL_20261106_214733378.MP.mp4" || motion["mime_type"] != "video/mp4" ||
		motion["size_bytes"] != int64(len(video)) || motion["captured_at"] != still["captured_at"] {
		t.Fatalf("motion = %+v", motion)
	}
	if client.files[1].extension != ".mp4" || client.files[1].contentType != "video/mp4" || client.files[1].sha != common.BytesSHA256(video) {
		t.Fatalf("video file = %+v, want the appended bytes exactly", client.files[1])
	}
	wantDedup := photos.ProvenanceDedupSHA256("android_photos", "zach@example.com", "DCIM/Camera/PXL_20261106_214733378.MP.jpg", "live_video", common.BytesSHA256(video))
	if client.envelopes[1].dedup != wantDedup {
		t.Fatalf("dedup = %s, want %s", client.envelopes[1].dedup, wantDedup)
	}
	block := client.envelopes[1].payload["android_record"].(map[string]any)["motion_photo"].(map[string]any)
	if block["format"] != "motion_photo" || block["length"] != int64(len(video)) || block["still_content_sha256"] != client.files[0].sha {
		t.Fatalf("motion_photo = %+v", block)
	}
	// The extracted copy does not outlive the run.
	if leftovers, _ := filepath.Glob(filepath.Join(os.TempDir(), "pdw-motion-*.mp4")); len(leftovers) != 0 {
		t.Fatalf("temp videos left behind: %v", leftovers)
	}
}

func TestSyncSendsOnlyTheVideoForAStillThatLandedBeforeVideosWereSent(t *testing.T) {
	lib := newLibrary(t)
	video := fakeMP4(4000)
	lib.add("DCIM/Camera/PXL_old.MP.jpg", "image/jpeg", lib.now.Add(-time.Hour), motionJPEG(t, motionXMP(100, len(video)), video))
	lib.add("DCIM/Camera/PXL_plain.jpg", "image/jpeg", lib.now.Add(-2*time.Hour), pixelJPEG(t, false))
	// A ledger written by the first release: no motion columns at all.
	statePath := filepath.Join(t.TempDir(), "state.sqlite")
	old, err := common.OpenStateDB(statePath, []string{`CREATE TABLE IF NOT EXISTS landed (
		native_id TEXT PRIMARY KEY, size_bytes INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,
		content_sha256 TEXT NOT NULL, landed_at TEXT NOT NULL DEFAULT '')`}, map[string]string{"schema_version": "1"}, []string{"landed"})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range lib.items {
		path := filepath.Join(lib.root, item.RelativePath)
		info, _ := os.Stat(path)
		sha, _ := common.FileSHA256(path)
		if _, err := old.DB.Exec(`INSERT INTO landed VALUES (?, ?, ?, ?, '')`, item.RelativePath, info.Size(), info.ModTime().UnixNano(), sha); err != nil {
			t.Fatal(err)
		}
	}
	old.Close()
	state, err := OpenState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	client := &fakeUploader{}
	summary, err := lib.runner(t, client, state).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Uploaded != 0 || summary.Motion != 1 || len(client.files) != 1 || client.files[0].contentType != "video/mp4" {
		t.Fatalf("summary=%+v files=%+v", summary, client.files)
	}
	// Both are now checked: a second run sends nothing.
	again := &fakeUploader{}
	if summary, err := lib.runner(t, again, state).Sync(); err != nil || len(again.files) != 0 || summary.Skipped != 2 {
		t.Fatalf("second run: %+v files=%d err=%v", summary, len(again.files), err)
	}
}

func TestSyncSendsTheStillWhenTheMotionVideoIsUnreadable(t *testing.T) {
	lib := newLibrary(t)
	notVideo := bytes.Repeat([]byte{0x11}, 3000)
	lib.add("DCIM/Camera/PXL_bad.MP.jpg", "image/jpeg", lib.now.Add(-time.Hour), motionJPEG(t, motionXMP(100, len(notVideo)), notVideo))
	client := &fakeUploader{}
	summary, err := lib.runner(t, client, openState(t)).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Uploaded != 1 || summary.Motion != 0 || summary.MotionUnreadable != 1 || len(client.files) != 1 {
		t.Fatalf("summary=%+v files=%d", summary, len(client.files))
	}
}

// C11: the observation names the newest arrival by its relative path, the
// native id the warehouse stores, so the detector asks whether that file landed.
func TestSyncObservesTheGallerysNewestArrival(t *testing.T) {
	lib := newLibrary(t)
	lib.add("DCIM/Camera/PXL_1.jpg", "image/jpeg", lib.now.Add(-2*time.Hour), []byte("a"))
	lib.add("Pictures/Screenshots/Screenshot_2.png", "image/png", lib.now.Add(-3*time.Hour), []byte("b"))
	lib.items[0].AddedAt = lib.now.Add(-2 * time.Hour)
	lib.items[1].AddedAt = lib.now.Add(-time.Hour) // added last, though taken earlier
	runner := lib.runner(t, &fakeUploader{}, openState(t))
	var observed []ingestclient.SourceObservation
	runner.Observe = func(o ingestclient.SourceObservation) (map[string]any, error) {
		observed = append(observed, o)
		return nil, errors.New("a failed post never fails the upload")
	}
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 1 || observed[0].Pipeline != "android_photos" || observed[0].Scope != "zach@example.com" ||
		observed[0].NewestID != "Pictures/Screenshots/Screenshot_2.png" ||
		observed[0].NewestAt != common.ISOFormat(lib.now.Add(-time.Hour)) {
		t.Fatalf("observed = %+v", observed)
	}
}
