package common

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// AppleEpoch is 2001-01-01T00:00:00Z, the zero of Core Data / Cocoa timestamps.
var AppleEpoch = time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)

// UnixEpoch is the warehouse's "absent" timestamp.
var UnixEpoch = time.Unix(0, 0).UTC()

// ISOFormat renders an aware UTC datetime as Python's isoformat() does:
// "2006-01-02T15:04:05+00:00", with ".ffffff" only when the microsecond is
// non-zero. Sub-microsecond precision is dropped first, because a Python
// datetime never carried it.
func ISOFormat(t time.Time) string {
	t = t.UTC()
	base := t.Format("2006-01-02T15:04:05")
	micro := t.Nanosecond() / 1000
	if micro != 0 {
		base += fmt.Sprintf(".%06d", micro)
	}
	return base + "+00:00"
}

// FromFloatSeconds converts a float count of seconds relative to epoch to a
// UTC time the way Python's datetime.fromtimestamp / timedelta do: modf, then
// round the fraction to the nearest microsecond, half to even.
func FromFloatSeconds(seconds float64, epoch time.Time) time.Time {
	whole, frac := math.Modf(seconds)
	micro := int64(math.RoundToEven(frac * 1e6))
	sec := int64(whole)
	if micro >= 1_000_000 {
		sec++
		micro -= 1_000_000
	} else if micro <= -1_000_000 {
		sec--
		micro += 1_000_000
	}
	return epoch.Add(time.Duration(sec)*time.Second + time.Duration(micro)*time.Microsecond).UTC()
}

// UnixFromFloat is FromFloatSeconds against the Unix epoch
// (datetime.fromtimestamp(x, tz=UTC)).
func UnixFromFloat(seconds float64) time.Time {
	return FromFloatSeconds(seconds, UnixEpoch)
}

// StatSeconds reproduces the float st_mtime/st_birthtime CPython builds from a
// (sec, nsec) pair: sec + nsec*1e-9 in double arithmetic.
func StatSeconds(sec, nsec int64) float64 {
	return float64(sec) + float64(nsec)*1e-9
}

// AppleTimestamp mirrors the Apple Messages scanner's parse_apple_timestamp:
// nanoseconds (or seconds) since 2001 to UTC, the epoch for <= 0.
func AppleTimestamp(value any) time.Time {
	number := ToFloat(value)
	if number <= 0 {
		return UnixEpoch
	}
	seconds := number
	if number > 10_000_000_000 {
		seconds = number / 1_000_000_000
	}
	return FromFloatSeconds(seconds, AppleEpoch)
}

// AppleSeconds converts a Cocoa seconds value (REAL or INTEGER) to UTC, or
// reports ok=false for NULL/empty/unparseable. Mirrors the Apple Contacts
// scanner's _apple_datetime.
func AppleSeconds(value any) (time.Time, bool) {
	if value == nil {
		return time.Time{}, false
	}
	if s, ok := value.(string); ok {
		if s == "" {
			return time.Time{}, false
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return time.Time{}, false
		}
		return FromFloatSeconds(f, AppleEpoch), true
	}
	return FromFloatSeconds(ToFloat(value), AppleEpoch), true
}

// ParseISO parses the ISO-8601 shapes Python's datetime.fromisoformat accepts
// for the values the uploaders store ("Z" is accepted as +00:00, a missing
// offset means UTC). It returns the Unix epoch for an unparseable value.
func ParseISO(text string) time.Time {
	t, ok := TryParseISO(text)
	if !ok {
		return UnixEpoch
	}
	return t
}

// TryParseISO is ParseISO with an explicit ok flag.
func TryParseISO(text string) (time.Time, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return time.Time{}, false
	}
	text = strings.Replace(text, "Z", "+00:00", 1)
	layouts := []string{
		"2006-01-02T15:04:05.999999999-07:00",
		"2006-01-02T15:04:05-07:00",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, text); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// NotesDatetime mirrors the Apple Notes scanner's parse_datetime: a number
// under 1e9 is Cocoa seconds, larger is Unix seconds, text is ISO-8601 (or a
// number spelled as text).
func NotesDatetime(value any) time.Time {
	switch v := value.(type) {
	case nil:
		return UnixEpoch
	case time.Time:
		return v.UTC()
	case int64, int, int32, float64, float32:
		number := ToFloat(v)
		if number <= 0 {
			return UnixEpoch
		}
		if number < 1_000_000_000 {
			return FromFloatSeconds(number, AppleEpoch)
		}
		return UnixFromFloat(number)
	default:
		text := strings.TrimSpace(fmt.Sprint(v))
		if text == "" {
			return UnixEpoch
		}
		if t, ok := TryParseISO(text); ok {
			return t
		}
		if f, err := strconv.ParseFloat(text, 64); err == nil {
			return NotesDatetime(f)
		}
		return UnixEpoch
	}
}

// ToFloat converts a scanned SQLite/JSON scalar to float64, 0 when it cannot.
func ToFloat(value any) float64 {
	switch v := value.(type) {
	case nil:
		return 0
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0
		}
		return f
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// ToInt mirrors the scanners' int_value: int(value), falling back to
// int(float(value)), 0 when neither parses.
func ToInt(value any) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		s := strings.TrimSpace(v)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f)
		}
		return 0
	default:
		return 0
	}
}
