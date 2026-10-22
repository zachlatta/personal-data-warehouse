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
