// Package timelapsefiles is the filename grammar of the screen timelapse: the
// periodic whole-screen images and accessibility trees the capture loops in the
// sysadmin repository write, once a minute, on crobat (the Mac), for joltik
// (the iPhone, imaged by whichever Mac shares its network) and on pixel11pro
// (the Android phone, which captures its own screen).
//
// A capture's name is all the metadata it carries: who wrote it, when, which
// display, and whether it is pixels or a tree. The uploader and the app's
// ingest endpoint both read it through this package, so the two cannot drift.
package timelapsefiles

import (
	"regexp"
	"strconv"
	"time"
)

// Kind is what a capture file holds.
type Kind string

const (
	Screenshot        Kind = "screenshot"
	AccessibilityTree Kind = "accessibility_tree"
)

// File is one recognised capture file.
type File struct {
	Kind Kind
	// DisplayIndex is 1-based for a screenshot and 0 for a tree, which
	// describes every display at once.
	DisplayIndex int
	// LocalStamp is the capture loop's wall-clock stamp, exactly as written
	// ("2026-10-22_14-00-54"), without the UTC offset some loops append.
	LocalStamp string
	Extension  string
	MimeType   string
	wall       time.Time // LocalStamp's fields, in UTC only as a carrier
	offset     *time.Duration
}

// The capture loops write a temp file and rename it, and the Mac's tree is
// gzipped in place; anything not exactly matching is skipped, never guessed.
// A loop that knows its UTC offset appends it to the stamp ("-0400"): the
// Pixel's frames are uploaded by a Mac that may be in another zone.
var filenamePattern = regexp.MustCompile(
	`^(periodic-screenshot|phone-screenshot|phone-ax)-` +
		`(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})` +
		`([+-](?:0\d|1[0-4])[0-5]\d)?` +
		`(?:-display-([1-9][0-9]?))?` +
		`(\.ax\.json\.gz|\.heic|\.png|\.avif|\.json|\.xml\.gz)$`,
)

const stampLayout = "2006-01-02_15-04-05"

// Parse recognises a capture file by name; ok is false for anything else.
func Parse(name string) (File, bool) {
	m := filenamePattern.FindStringSubmatch(name)
	if m == nil {
		return File{}, false
	}
	prefix, stamp, offset, display, ext := m[1], m[2], m[3], m[4], m[5]
	wall, err := time.Parse(stampLayout, stamp)
	if err != nil {
		return File{}, false
	}
	file := File{LocalStamp: stamp, Extension: ext, wall: wall}
	if offset != "" {
		zoned, err := time.Parse("-0700", offset)
		if err != nil {
			return File{}, false
		}
		_, seconds := zoned.Zone()
		d := time.Duration(seconds) * time.Second
		file.offset = &d
	}
	switch {
	case prefix == "periodic-screenshot" && ext == ".heic":
		file.Kind, file.MimeType, file.DisplayIndex = Screenshot, "image/heic", 1
		if display != "" {
			file.DisplayIndex, _ = strconv.Atoi(display)
		}
	case prefix == "periodic-screenshot" && ext == ".ax.json.gz" && display == "":
		file.Kind, file.MimeType = AccessibilityTree, "application/gzip"
	case prefix == "phone-screenshot" && ext == ".png" && display == "":
		file.Kind, file.MimeType, file.DisplayIndex = Screenshot, "image/png", 1
	case prefix == "phone-screenshot" && ext == ".avif" && display == "":
		file.Kind, file.MimeType, file.DisplayIndex = Screenshot, "image/avif", 1
	case prefix == "phone-ax" && ext == ".json" && display == "":
		file.Kind, file.MimeType = AccessibilityTree, "application/json"
	case prefix == "phone-ax" && ext == ".xml.gz" && display == "":
		// Android's uiautomator dump, gzipped.
		file.Kind, file.MimeType = AccessibilityTree, "application/gzip"
	default:
		return File{}, false
	}
	return file, true
}

// Offset is the UTC offset the stamp carries, when the loop wrote one.
func (f File) Offset() (time.Duration, bool) {
	if f.offset == nil {
		return 0, false
	}
	return *f.offset, true
}

// CapturedAt is the instant of the stamp. A stamp that carries its offset names
// exactly that instant; otherwise it is read in the capture host's zone. When
// the wall time occurs twice (the fall-back hour) the candidate nearest the
// file's write time wins, since the loop writes within seconds of its stamp.
// The files of one moment share a stamp and are written seconds apart, so they
// resolve to the same instant: (device, captured_at) is what links a tree to
// its screenshots. (The local stamp alone would not: 01:30 on a fall-back day
// names two moments an hour apart.)
func (f File) CapturedAt(loc *time.Location, written time.Time) time.Time {
	w := f.wall
	if f.offset != nil {
		return w.Add(-*f.offset).UTC()
	}
	base := time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), w.Second(), 0, loc)
	best := base
	for _, shift := range []time.Duration{-time.Hour, time.Hour} {
		candidate := base.Add(shift)
		if candidate.In(loc).Format(stampLayout) != f.LocalStamp {
			continue
		}
		if absDuration(candidate.Sub(written)) < absDuration(best.Sub(written)) {
			best = candidate
		}
	}
	return best.UTC()
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
