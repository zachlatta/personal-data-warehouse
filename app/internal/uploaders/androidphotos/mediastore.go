package androidphotos

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// PrimaryVolumePath is where MediaStore's _data paths for the primary
// volume start. Root reads the same files under DefaultMediaRoot, the
// storage below the FUSE view, so a read never goes through MediaProvider.
const PrimaryVolumePath = "/storage/emulated/0/"

// DefaultMediaRoot is the primary volume's backing directory for user 0.
const DefaultMediaRoot = "/data/media/0"

// GalleryFolders are the top-level folders whose images and videos are the
// phone's photo library: the camera (DCIM), screenshots and images apps
// save (Pictures), and recorded or saved videos (Movies). The same set a
// gallery app shows; app caches, downloads and chat media folders are not
// photos Zach took or kept.
var GalleryFolders = []string{"DCIM/", "Pictures/", "Movies/"}

// mediaProjection lists MediaStore's columns with _data LAST: every other
// value is a number, a MIME type or a package name, none of which contains
// ", ", so a `content query` row splits safely on ", " up to _data, and
// _data (a path, which may contain anything) runs to the end of the line.
var mediaProjection = []string{
	"_id", "datetaken", "date_added", "date_modified", "mime_type", "media_type", "_size",
	"width", "height", "orientation", "duration", "is_favorite", "owner_package_name",
	"generation_modified", "_data",
}

// MediaItem is one image or video MediaStore lists in a gallery folder.
type MediaItem struct {
	MediaID            int64
	RelativePath       string // e.g. DCIM/Camera/PXL_20261107_221546101.jpg: the native id
	Filename           string
	MimeType           string
	IsVideo            bool
	SizeBytes          int64
	Width              int64
	Height             int64
	Orientation        int64
	DurationMS         int64
	IsFavorite         bool
	OwnerPackage       string
	GenerationModified int64
	TakenAt            time.Time // datetaken (UTC); falls back to date_added
	TakenAtFromAdded   bool      // true when MediaStore had no datetaken
	ModifiedAt         time.Time // date_modified (whole seconds)
}

// queryMediaStore runs Android's `content query` against MediaStore and
// returns its text output. Replaced in tests.
var queryMediaStore = func() (string, error) {
	folders := make([]string, 0, len(GalleryFolders))
	for _, folder := range GalleryFolders {
		folders = append(folders, fmt.Sprintf("relative_path LIKE '%s%%'", folder))
	}
	where := "media_type IN (1,3) AND is_pending=0 AND is_trashed=0 AND volume_name='external_primary' AND (" +
		strings.Join(folders, " OR ") + ")"
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("content", "query", "--uri", "content://media/external/file",
		"--projection", strings.Join(mediaProjection, ":"), "--where", where)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("content query MediaStore: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// ListMedia queries MediaStore for every image and video in a gallery folder.
func ListMedia() ([]MediaItem, error) {
	out, err := queryMediaStore()
	if err != nil {
		return nil, err
	}
	return ParseMediaRows(out)
}

// ParseMediaRows parses `content query` output ("Row: N k=v, k=v, ...").
// "No result found." is an empty library, not an error; any other line that
// is not a row means the provider refused the query, which must fail loud
// rather than read as an empty library.
func ParseMediaRows(out string) ([]MediaItem, error) {
	var items []MediaItem
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" || strings.TrimSpace(line) == "No result found." {
			continue
		}
		if !strings.HasPrefix(line, "Row: ") {
			return nil, fmt.Errorf("unexpected MediaStore output: %q", line)
		}
		item, ok, err := parseMediaRow(line)
		if err != nil {
			return nil, err
		}
		if ok {
			items = append(items, item)
		}
	}
	return items, nil
}

func parseMediaRow(line string) (MediaItem, bool, error) {
	dataAt := strings.Index(line, ", _data=")
	if dataAt < 0 {
		return MediaItem{}, false, fmt.Errorf("MediaStore row has no _data: %q", line)
	}
	data := line[dataAt+len(", _data="):]
	head := line[len("Row: "):dataAt]
	// Drop the row number.
	if space := strings.IndexByte(head, ' '); space >= 0 {
		head = head[space+1:]
	}
	fields := map[string]string{}
	for _, pair := range strings.Split(head, ", ") {
		key, value, found := strings.Cut(pair, "=")
		if !found {
			return MediaItem{}, false, fmt.Errorf("MediaStore row field %q has no value: %q", pair, line)
		}
		if value == "NULL" {
			value = ""
		}
		fields[key] = value
	}
	if !strings.HasPrefix(data, PrimaryVolumePath) {
		// Another volume (an SD card or USB drive) is not the phone's library.
		return MediaItem{}, false, nil
	}
	relative := strings.TrimPrefix(data, PrimaryVolumePath)
	filename := relative
	if slash := strings.LastIndexByte(relative, '/'); slash >= 0 {
		filename = relative[slash+1:]
	}
	if filename == "" || strings.HasPrefix(filename, ".") {
		// A hidden file is a camera's or an app's work in progress.
		return MediaItem{}, false, nil
	}
	item := MediaItem{
		MediaID:            atoi(fields["_id"]),
		RelativePath:       relative,
		Filename:           filename,
		MimeType:           fields["mime_type"],
		IsVideo:            fields["media_type"] == "3",
		SizeBytes:          atoi(fields["_size"]),
		Width:              atoi(fields["width"]),
		Height:             atoi(fields["height"]),
		Orientation:        atoi(fields["orientation"]),
		DurationMS:         atoi(fields["duration"]),
		IsFavorite:         fields["is_favorite"] == "1",
		OwnerPackage:       fields["owner_package_name"],
		GenerationModified: atoi(fields["generation_modified"]),
		ModifiedAt:         time.Unix(atoi(fields["date_modified"]), 0).UTC(),
	}
	if taken := atoi(fields["datetaken"]); taken > 0 {
		item.TakenAt = time.UnixMilli(taken).UTC()
	} else {
		item.TakenAt = time.Unix(atoi(fields["date_added"]), 0).UTC()
		item.TakenAtFromAdded = true
	}
	return item, true, nil
}

func atoi(value string) int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return n
}
