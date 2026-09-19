package common

import (
	"math"
	"testing"
	"time"
)

// The expected strings below were produced by CPython 3.12
// (json.dumps(..., sort_keys=True, separators=(",", ":")) and repr()).

func TestCanonicalJSONMatchesPythonDumps(t *testing.T) {
	value := map[string]any{
		"z":     []any{1, 2.5, "é", nil, true},
		"a":     map[string]any{"nested": "quote\"back\\slash\nnewline\ttab\x01"},
		"emoji": "\U0001F642",
		"empty": map[string]any{},
		"list":  []any{},
		"big":   int64(1234567890123),
		"del":   "\x7f",
	}
	got, err := CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":{"nested":"quote\"back\\slash\nnewline\ttab\u0001"},"big":1234567890123,"del":"\u007f","emoji":"\ud83d\ude42","empty":{},"list":[],"z":[1,2.5,"\u00e9",null,true]}`
	if string(got) != want {
		t.Fatalf("CanonicalJSON =\n%s\nwant\n%s", got, want)
	}
}

func TestIndentedJSONMatchesPythonIndent2(t *testing.T) {
	got, err := IndentedJSON(map[string]any{"b": []any{1, map[string]any{}}, "a": "x"})
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"a\": \"x\",\n  \"b\": [\n    1,\n    {}\n  ]\n}"
	if string(got) != want {
		t.Fatalf("IndentedJSON =\n%s\nwant\n%s", got, want)
	}
}

func TestFloatReprMatchesPython(t *testing.T) {
	cases := map[float64]string{
		0:                      "0.0",
		1:                      "1.0",
		-2:                     "-2.0",
		2.5:                    "2.5",
		0.1:                    "0.1",
		1e16:                   "1e+16",
		1e15:                   "1000000000000000.0",
		123456789012345680:     "1.2345678901234568e+17",
		0.0001:                 "0.0001",
		0.00001:                "1e-05",
		700000000.123456:       "700000000.123456",
		45.5:                   "45.5",
		-122.6:                 "-122.6",
		1.5e-7:                 "1.5e-07",
		786157423.5:            "786157423.5",
		math.Copysign(0, -1):   "-0.0",
		9007199254740993.0:     "9007199254740992.0",
		0.30000000000000004:    "0.30000000000000004",
		1234567890.0987654321:  "1234567890.0987654",
		1e22:                   "1e+22",
		12345678901234567890.0: "1.2345678901234567e+19",
	}
	for input, want := range cases {
		if got := FloatRepr(input); got != want {
			t.Errorf("FloatRepr(%v) = %q, want %q", input, got, want)
		}
	}
}

func TestISOFormatMatchesPythonIsoformat(t *testing.T) {
	whole := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	if got := ISOFormat(whole); got != "2026-05-21T12:00:00+00:00" {
		t.Fatalf("ISOFormat whole = %q", got)
	}
	micro := time.Date(2026, 5, 21, 12, 0, 0, 123456789, time.UTC)
	if got := ISOFormat(micro); got != "2026-05-21T12:00:00.123456+00:00" {
		t.Fatalf("ISOFormat micro = %q", got)
	}
	if got := ISOFormat(UnixEpoch); got != "1970-01-01T00:00:00+00:00" {
		t.Fatalf("ISOFormat epoch = %q", got)
	}
}

func TestFromFloatSecondsRoundsHalfEvenToMicroseconds(t *testing.T) {
	// CPython 3.12: datetime.fromtimestamp(1.0000005, tz=UTC).microsecond == 1
	// and 1.0000015 -> 1 (the doubles are not exact halves; modf then round).
	if got := UnixFromFloat(1.0000005).Nanosecond(); got != 1000 {
		t.Fatalf("rounding 1.0000005: got %d ns", got)
	}
	if got := UnixFromFloat(1.0000015).Nanosecond(); got != 1000 {
		t.Fatalf("rounding 1.0000015: got %d ns", got)
	}
	// Apple nanosecond timestamps: 2026-05-21T12:00:00Z is 801057600 s after 2001.
	ns := int64(801057600) * 1_000_000_000
	if got := AppleTimestamp(ns); !got.Equal(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("AppleTimestamp(ns) = %v", got)
	}
	if got := AppleTimestamp(int64(801057600)); !got.Equal(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("AppleTimestamp(seconds) = %v", got)
	}
	if got := AppleTimestamp(0); !got.Equal(UnixEpoch) {
		t.Fatalf("AppleTimestamp(0) = %v", got)
	}
}

func TestNotesDatetimeShapes(t *testing.T) {
	if got := NotesDatetime("2026-05-21T12:00:00+00:00"); got != time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC) {
		t.Fatalf("iso: %v", got)
	}
	if got := NotesDatetime("2026-05-21T12:00:00Z"); got != time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC) {
		t.Fatalf("iso Z: %v", got)
	}
	if got := NotesDatetime(float64(801057600)); got != time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC) {
		t.Fatalf("cocoa: %v", got)
	}
	if got := NotesDatetime(float64(1779451200)); got != time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC) {
		t.Fatalf("unix: %v", got)
	}
	if got := NotesDatetime(nil); got != UnixEpoch {
		t.Fatalf("nil: %v", got)
	}
}
