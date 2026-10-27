package timelapsefiles

import (
	"testing"
	"time"
)

func TestParseRecognisesEveryCaptureFileShape(t *testing.T) {
	cases := []struct {
		name    string
		kind    Kind
		display int
		stamp   string
		mime    string
	}{
		{"periodic-screenshot-2026-10-22_14-00-54-display-1.heic", Screenshot, 1, "2026-10-22_14-00-54", "image/heic"},
		{"periodic-screenshot-2026-10-22_14-00-54-display-2.heic", Screenshot, 2, "2026-10-22_14-00-54", "image/heic"},
		// Single-display frames from before 2026-05-23 carry no display suffix.
		{"periodic-screenshot-2026-05-20_21-52-22.heic", Screenshot, 1, "2026-05-20_21-52-22", "image/heic"},
		{"periodic-screenshot-2026-10-22_14-00-54.ax.json.gz", AccessibilityTree, 0, "2026-10-22_14-00-54", "application/gzip"},
		{"phone-screenshot-2026-10-18_22-39-13.png", Screenshot, 1, "2026-10-18_22-39-13", "image/png"},
		{"phone-ax-2026-10-18_22-39-13.json", AccessibilityTree, 0, "2026-10-18_22-39-13", "application/json"},
		// The Pixel writes WebP (on-phone pdw compression) or AVIF frames and
		// gzipped uiautomator XML, stamped with the phone's own UTC offset.
		{"phone-screenshot-2026-10-23_21-09-00-0400.avif", Screenshot, 1, "2026-10-23_21-09-00", "image/avif"},
		{"phone-screenshot-2026-10-27_08-09-01-0700.webp", Screenshot, 1, "2026-10-27_08-09-01", "image/webp"},
		{"phone-screenshot-2026-10-27_08-09-01-0700.jpg", Screenshot, 1, "2026-10-27_08-09-01", "image/jpeg"},
		{"phone-ax-2026-10-23_21-09-00-0400.xml.gz", AccessibilityTree, 0, "2026-10-23_21-09-00", "application/gzip"},
		{"phone-screenshot-2026-10-23_18-09-00+0530.avif", Screenshot, 1, "2026-10-23_18-09-00", "image/avif"},
	}
	for _, tc := range cases {
		got, ok := Parse(tc.name)
		if !ok {
			t.Fatalf("Parse(%q) did not recognise the file", tc.name)
		}
		if got.Kind != tc.kind || got.DisplayIndex != tc.display || got.LocalStamp != tc.stamp || got.MimeType != tc.mime {
			t.Fatalf("Parse(%q) = %+v", tc.name, got)
		}
	}
}

func TestParseIgnoresEverythingElse(t *testing.T) {
	for _, name := range []string{
		"periodic-screenshot-2026-10-22_14-00-54.ax.json",     // not yet gzipped
		"periodic-screenshot-2026-10-22_14-00-54.ax.json.tmp", // still being written
		"periodic-screenshot-2026-10-22_14-00-54-display-1.heic.tmp",
		".DS_Store",
		"Screenshot 2026-10-22 at 14.00.54.png", // a deliberate screenshot, not the timelapse
		"phone-screenshot-2026-13-18_22-39-13.png",
		"phone-ax-2026-10-18_22-39-13.json.gz",
		"periodic-screenshot-2026-10-22_14-00-54-display-0.heic",
		"phone-ax-2026-10-23_21-09-00-0400.xml",             // not yet gzipped
		"phone-screenshot-2026-10-23_21-09-00-0400.png.tmp", // still being encoded
		"phone-screenshot-2026-10-23_21-09-00-0400.avif.tmp",
		"phone-screenshot-2026-10-23_21-09-00-2500.avif", // no such offset
		"phone-screenshot-2026-10-23_21-09-00-04.avif",
	} {
		if got, ok := Parse(name); ok {
			t.Fatalf("Parse(%q) = %+v, want unrecognised", name, got)
		}
	}
}

func TestTheFilesOfOneMomentResolveToOneInstant(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	shot, _ := Parse("periodic-screenshot-2026-11-01_01-30-00-display-2.heic")
	tree, _ := Parse("periodic-screenshot-2026-11-01_01-30-00.ax.json.gz")
	// The tree is gzipped a few seconds after the screenshot lands.
	shotAt := shot.CapturedAt(ny, time.Date(2026, 11, 1, 6, 30, 1, 0, time.UTC))
	treeAt := tree.CapturedAt(ny, time.Date(2026, 11, 1, 6, 30, 4, 0, time.UTC))
	if !shotAt.Equal(treeAt) {
		t.Fatalf("screenshot %s and tree %s of one moment must share captured_at", shotAt, treeAt)
	}
}

func TestCapturedAtReadsTheStampInTheCaptureHostsZone(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	file, _ := Parse("periodic-screenshot-2026-10-22_14-00-54-display-1.heic")
	got := file.CapturedAt(ny, time.Date(2026, 10, 22, 18, 0, 55, 0, time.UTC))
	if want := time.Date(2026, 10, 22, 18, 0, 54, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("captured_at = %s, want %s", got, want)
	}
}

func TestCapturedAtUsesTheWriteTimeToPlaceTheRepeatedFallBackHour(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	// 01:30 on 2026-11-01 happens twice in New York: 05:30Z (EDT) and 06:30Z (EST).
	file, _ := Parse("periodic-screenshot-2026-11-01_01-30-00-display-1.heic")
	first := file.CapturedAt(ny, time.Date(2026, 11, 1, 5, 30, 2, 0, time.UTC))
	second := file.CapturedAt(ny, time.Date(2026, 11, 1, 6, 30, 2, 0, time.UTC))
	if want := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC); !first.Equal(want) {
		t.Fatalf("first 01:30 = %s, want %s", first, want)
	}
	if want := time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC); !second.Equal(want) {
		t.Fatalf("second 01:30 = %s, want %s", second, want)
	}
}

func TestCapturedAtTrustsAnOffsetTheStampCarries(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	// The phone stamps its own offset: a frame taken in California and
	// uploaded by a Mac in New York is still placed at the instant it was taken.
	file, ok := Parse("phone-screenshot-2026-10-23_18-09-00-0700.avif")
	if !ok {
		t.Fatal("not recognised")
	}
	got := file.CapturedAt(ny, time.Date(2026, 10, 24, 1, 15, 0, 0, time.UTC))
	if want := time.Date(2026, 10, 24, 1, 9, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("captured_at = %s, want %s", got, want)
	}
	if offset, ok := file.Offset(); !ok || offset != -7*time.Hour {
		t.Fatalf("Offset() = %s, %t", offset, ok)
	}
	if _, ok := mustParse(t, "phone-screenshot-2026-10-18_22-39-13.png").Offset(); ok {
		t.Fatal("a stamp with no offset must not report one")
	}
}

func TestAnOffsetStampNamesEachFallBackMomentOnItsOwn(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	// Both 01:30s of 2026-11-01, with write times that would mislead the
	// mtime rule: the offset decides, not the write time.
	edt := mustParse(t, "phone-screenshot-2026-11-01_01-30-00-0400.avif")
	est := mustParse(t, "phone-screenshot-2026-11-01_01-30-00-0500.avif")
	written := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	if got, want := edt.CapturedAt(ny, written), time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("EDT 01:30 = %s, want %s", got, want)
	}
	if got, want := est.CapturedAt(ny, written), time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("EST 01:30 = %s, want %s", got, want)
	}
}

func mustParse(t *testing.T, name string) File {
	t.Helper()
	file, ok := Parse(name)
	if !ok {
		t.Fatalf("Parse(%q) did not recognise the file", name)
	}
	return file
}
