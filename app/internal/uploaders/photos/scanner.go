// Package photos is the Go port of the Apple Photos uploader
// (personal_data_warehouse_photos): it snapshots the library's Photos.sqlite,
// resolves every user-library asset to the original PhotoKit resource(s) to
// export, exports each one through the native helper app with iCloud access,
// and uploads the complete bytes plus the cross-source metadata envelope
// through the app's photo ingest endpoints.
//
// Every wire payload, fingerprint and dedup key is byte-identical to what the
// Python uploader produced: the warehouse dedups on sha256 of canonical JSON
// and the state file keys on the fingerprint, so a one-byte drift would
// re-upload the whole library on the first Go run. testdata/golden.jsonl is
// the Python output the tests pin this against.
package photos

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// CocoaEpochUnixOffset is 2001-01-01T00:00:00Z as Unix seconds; Core Data
// stores timestamps relative to it.
const CocoaEpochUnixOffset = 978307200

// GPSUnsetSentinel is how Photos stores "no GPS" in both latitude and longitude.
const GPSUnsetSentinel = -180.0

// KindSubtypeLivePhoto is ZASSET.ZKINDSUBTYPE for a Live Photo still.
const KindSubtypeLivePhoto = 2

// ProbeTimeout bounds how long opening the Photos store may block inside
// macOS TCC before the run is failed loudly.
const ProbeTimeout = 15 * time.Second

var utiMimeTypes = map[string]string{
	"public.heic":               "image/heic",
	"public.heif":               "image/heif",
	"public.jpeg":               "image/jpeg",
	"public.png":                "image/png",
	"public.tiff":               "image/tiff",
	"org.webmproject.webp":      "image/webp",
	"com.compuserve.gif":        "image/gif",
	"com.adobe.raw-image":       "image/x-adobe-dng",
	"com.apple.quicktime-movie": "video/quicktime",
	"public.mpeg-4":             "video/mp4",
}

var extensionMimeTypes = map[string]string{
	".heic": "image/heic",
	".heif": "image/heif",
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".png":  "image/png",
	".tif":  "image/tiff",
	".tiff": "image/tiff",
	".webp": "image/webp",
	".gif":  "image/gif",
	".dng":  "image/x-adobe-dng",
	".mov":  "video/quicktime",
	".mp4":  "video/mp4",
}

// SchemaError reports a Photos.sqlite without the tables the scanner reads.
type SchemaError struct{ Missing []string }

func (e *SchemaError) Error() string {
	return "Unsupported Apple Photos schema: missing " + strings.Join(e.Missing, ", ")
}

// Candidate is one original PhotoKit resource claim.
type Candidate struct {
	NativeID          string // the still asset's ZUUID for BOTH roles (this attaches the .mov)
	Role              string // original | live_video
	AssetKind         string // image | video (of the parent asset)
	Filename          string
	Extension         string
	MimeType          string
	ExpectedSizeBytes int64
	Width             int64
	Height            int64
	CapturedAt        string // local wall-clock ISO, "" when unknown
	CaptureTZOffset   string // "+HH:MM" / "-HH:MM" / ""
	CameraMake        string
	CameraModel       string
	AppleRecord       map[string]any
}

// Fingerprint is the stable Photos-metadata fingerprint the state file keys
// on: "photokit-v2|" + sha256 of the canonical {version, role, apple_record}.
func (c Candidate) Fingerprint() string {
	return "photokit-v2|" + common.JSONSHA256(map[string]any{
		"version":      int64(2),
		"role":         c.Role,
		"apple_record": c.AppleRecord,
	})
}

// StateID is the upload_state source_id: "<native_id>|<role>".
func (c Candidate) StateID() string {
	return c.NativeID + "|" + c.Role
}

// probeOpenable fails fast when opening the Photos library would hang.
//
// Unlike ~/Library/Messages (where a launchd process without Full Disk
// Access gets an immediate EPERM), open(2) on the Photos-library files can
// BLOCK indefinitely inside macOS TCC. A hung run holds the uploader lock
// forever, never writes a heartbeat exit code, and reads as healthy. Probe
// the open in a goroutine and convert a stall into the same loud permission
// error the other uploaders raise.
func probeOpenable(path string, timeout time.Duration) error {
	outcome := make(chan error, 1)
	go func() {
		file, err := os.Open(path)
		if err == nil {
			file.Close()
		}
		outcome <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case err := <-outcome:
		if err != nil {
			return &common.PermissionError{Path: path, Err: err}
		}
		return nil
	case <-ctx.Done():
		return &common.PermissionError{Path: path, Err: fmt.Errorf(
			"opening %s blocked for %.0fs; macOS TCC is stalling this process (see the Apple Photos section of AGENTS.md)",
			path, timeout.Seconds())}
	}
}

// SnapshotStore copies the library's Photos.sqlite into destinationDir
// (never reading the live file directly: WAL churn from Photos.app would
// tear reads) and returns the snapshot path.
func SnapshotStore(libraryPath, destinationDir string) (string, error) {
	source := filepath.Join(common.ExpandUser(libraryPath), "database", "Photos.sqlite")
	destination := filepath.Join(destinationDir, "Photos.sqlite")
	if err := probeOpenable(source, ProbeTimeout); err != nil {
		return "", err
	}
	if err := common.SnapshotSQLite(source, destination); err != nil {
		return "", err
	}
	return destination, nil
}

const candidateQuery = `
SELECT
    a.ZUUID AS uuid,
    a.ZDIRECTORY AS directory,
    a.ZFILENAME AS filename,
    a.ZKIND AS kind,
    a.ZKINDSUBTYPE AS kind_subtype,
    a.ZUNIFORMTYPEIDENTIFIER AS uti,
    a.ZDATECREATED AS date_created,
    a.ZADDEDDATE AS added_date,
    a.ZMODIFICATIONDATE AS modification_date,
    a.ZWIDTH AS width,
    a.ZHEIGHT AS height,
    a.ZLATITUDE AS latitude,
    a.ZLONGITUDE AS longitude,
    a.ZFAVORITE AS favorite,
    a.ZHIDDEN AS hidden,
    a.ZADJUSTMENTSSTATE AS adjustments_state,
    aa.ZORIGINALFILENAME AS original_filename,
    aa.ZORIGINALFILESIZE AS original_file_size,
    aa.ZTIMEZONEOFFSET AS timezone_offset,
    aa.ZINFERREDTIMEZONEOFFSET AS inferred_timezone_offset,
    aa.ZTIMEZONENAME AS timezone_name,
    aa.ZEXIFTIMESTAMPSTRING AS exif_timestamp_string,
    ea.ZCAMERAMAKE AS camera_make,
    ea.ZCAMERAMODEL AS camera_model,
    ea.ZLENSMODEL AS lens_model
FROM ZASSET a
LEFT JOIN ZADDITIONALASSETATTRIBUTES aa ON aa.ZASSET = a.Z_PK
LEFT JOIN ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = a.Z_PK
WHERE a.ZTRASHEDSTATE = 0
  -- Bundle-scoped rows are transient syndicated/shared records stored in
  -- Photos.sqlite but are not user-library PHAssets. PhotoKit cannot fetch
  -- or export them.
  AND COALESCE(a.ZBUNDLESCOPE, 0) = 0
ORDER BY a.ZDATECREATED DESC
`

// Scan returns newest-first original-resource candidates for every
// non-trashed, user-library asset in a Photos.sqlite snapshot.
func Scan(snapshotPath string) ([]Candidate, error) {
	db, err := common.OpenSQLite(snapshotPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tables, err := common.TableNames(db)
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, required := range []string{"ZADDITIONALASSETATTRIBUTES", "ZASSET", "ZEXTENDEDATTRIBUTES"} {
		if !tables[required] {
			missing = append(missing, required)
		}
	}
	if len(missing) > 0 {
		return nil, &SchemaError{Missing: missing}
	}
	rows, err := common.Query(db, candidateQuery)
	if err != nil {
		return nil, err
	}
	var candidates []Candidate
	for _, row := range rows {
		candidates = append(candidates, candidatesForAsset(row)...)
	}
	return candidates, nil
}

func candidatesForAsset(row common.Row) []Candidate {
	uuid := strOr(row.Get("uuid"))
	filename := strOr(row.Get("filename"))
	if uuid == "" || filename == "" {
		return nil
	}
	assetKind := "image"
	if common.ToInt(row.Get("kind")) == 1 {
		assetKind = "video"
	}
	tzSeconds := firstInt(row.Get("timezone_offset"), row.Get("inferred_timezone_offset"))
	capturedAt, tzOffset := wallClock(row.Get("date_created"), tzSeconds)
	record := appleRecord(row)

	originalFilename := strOr(row.Get("original_filename"))
	if originalFilename == "" {
		originalFilename = filename
	}
	extension := suffix(originalFilename)
	candidates := []Candidate{fileCandidate(row, uuid, "original", assetKind, originalFilename, extension,
		mimeType(strOr(row.Get("uti")), extension), capturedAt, tzOffset, record)}

	if assetKind == "image" && common.ToInt(row.Get("kind_subtype")) == KindSubtypeLivePhoto {
		// PhotoKit exposes the original motion component as a paired-video
		// resource. It is uploaded under the SAME native id, which lets the
		// identity layer attach it to the still's asset.
		liveName := stem(originalFilename) + ".MOV"
		candidates = append(candidates, fileCandidate(row, uuid, "live_video", assetKind, liveName, ".mov",
			"video/quicktime", capturedAt, tzOffset, record))
	}
	return candidates
}

func fileCandidate(row common.Row, nativeID, role, assetKind, filename, extension, mime, capturedAt, tzOffset string, record map[string]any) Candidate {
	var width, height, expected int64
	if role == "original" {
		width = common.ToInt(row.Get("width"))
		height = common.ToInt(row.Get("height"))
		expected = common.ToInt(row.Get("original_file_size"))
	}
	return Candidate{
		NativeID:          nativeID,
		Role:              role,
		AssetKind:         assetKind,
		Filename:          filename,
		Extension:         extension,
		MimeType:          mime,
		ExpectedSizeBytes: expected,
		Width:             width,
		Height:            height,
		CapturedAt:        capturedAt,
		CaptureTZOffset:   tzOffset,
		CameraMake:        strOr(row.Get("camera_make")),
		CameraModel:       strOr(row.Get("camera_model")),
		AppleRecord:       record,
	}
}

// appleRecord is the archival raw payload: everything the scanner read.
func appleRecord(row common.Row) map[string]any {
	record := map[string]any{
		"uuid":                    strOr(row.Get("uuid")),
		"kind":                    common.ToInt(row.Get("kind")),
		"kind_subtype":            common.ToInt(row.Get("kind_subtype")),
		"uti":                     strOr(row.Get("uti")),
		"directory":               strOr(row.Get("directory")),
		"filename":                strOr(row.Get("filename")),
		"original_filename":       strOr(row.Get("original_filename")),
		"date_created":            isoUTC(row.Get("date_created")),
		"added_date":              isoUTC(row.Get("added_date")),
		"modification_date":       isoUTC(row.Get("modification_date")),
		"timezone_name":           strOr(row.Get("timezone_name")),
		"timezone_offset_seconds": firstInt(row.Get("timezone_offset"), row.Get("inferred_timezone_offset")),
		"exif_timestamp_string":   strOr(row.Get("exif_timestamp_string")),
		"width":                   common.ToInt(row.Get("width")),
		"height":                  common.ToInt(row.Get("height")),
		"original_file_size":      common.ToInt(row.Get("original_file_size")),
		"favorite":                common.ToInt(row.Get("favorite")),
		"hidden":                  common.ToInt(row.Get("hidden")),
		"adjustments_state":       common.ToInt(row.Get("adjustments_state")),
		"camera_make":             strOr(row.Get("camera_make")),
		"camera_model":            strOr(row.Get("camera_model")),
		"lens_model":              strOr(row.Get("lens_model")),
	}
	latitude, latOK := gpsValue(row.Get("latitude"))
	longitude, lonOK := gpsValue(row.Get("longitude"))
	if latOK && lonOK {
		record["latitude"] = latitude
		record["longitude"] = longitude
	}
	return record
}

func gpsValue(value any) (float64, bool) {
	if value == nil {
		return 0, false
	}
	number := common.ToFloat(value)
	if number == GPSUnsetSentinel {
		return 0, false
	}
	return number, true
}

// firstInt mirrors _first_int: int() of the first non-NULL value, else 0.
func firstInt(values ...any) int64 {
	for _, value := range values {
		if value != nil {
			return common.ToInt(value)
		}
	}
	return 0
}

// strOr mirrors str(value or ""): NULL, 0 and "" all read as "".
func strOr(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case int64:
		if v == 0 {
			return ""
		}
	case float64:
		if v == 0 {
			return ""
		}
	}
	return common.PyStr(value)
}

func cocoaTime(value any) (time.Time, bool) {
	if value == nil {
		return time.Time{}, false
	}
	return common.UnixFromFloat(common.ToFloat(value) + CocoaEpochUnixOffset), true
}

func isoUTC(value any) string {
	moment, ok := cocoaTime(value)
	if !ok {
		return ""
	}
	return common.ISOFormat(moment)
}

// wallClock returns (local wall-clock ISO without offset, "+HH:MM") for the
// capture.
func wallClock(dateCreated any, tzSeconds int64) (string, string) {
	moment, ok := cocoaTime(dateCreated)
	if !ok {
		return "", ""
	}
	local := moment.Add(time.Duration(tzSeconds) * time.Second)
	sign := "+"
	if tzSeconds < 0 {
		sign = "-"
	}
	magnitude := tzSeconds
	if magnitude < 0 {
		magnitude = -magnitude
	}
	offset := fmt.Sprintf("%s%02d:%02d", sign, magnitude/3600, (magnitude%3600)/60)
	return local.Format("2006-01-02T15:04:05"), offset
}

// suffix mirrors Path(filename).suffix.lower().
func suffix(filename string) string {
	return strings.ToLower(suffixRaw(filepath.Base(filename)))
}

// stem mirrors Path(filename).stem.
func stem(filename string) string {
	base := filepath.Base(filename)
	return strings.TrimSuffix(base, suffixRaw(base))
}

// suffixRaw is pathlib's suffix: the last dot onward, unless it is the
// leading character (a dotfile has no suffix).
func suffixRaw(base string) string {
	i := strings.LastIndex(base, ".")
	if i <= 0 {
		return ""
	}
	return base[i:]
}

func mimeType(uti, extension string) string {
	if mime, ok := utiMimeTypes[uti]; ok {
		return mime
	}
	if mime, ok := extensionMimeTypes[extension]; ok {
		return mime
	}
	return "application/octet-stream"
}
