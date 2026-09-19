// Package common holds the plumbing every local uploader shares: the JSON and
// time formatting that must stay byte-compatible with the Python uploaders
// they replaced (the warehouse dedups on sha256 of these bytes, and every
// incremental state file keys on them), SQLite access, the run lock, the
// network guard, and account resolution.
package common

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// CanonicalJSON encodes v exactly as Python's
// json.dumps(v, sort_keys=True, separators=(",", ":")) would (ensure_ascii on,
// keys sorted by code point, floats in repr() form). Fingerprints and dedup
// keys are sha256 over these bytes, so a one-byte difference from the Python
// encoder would re-upload every record on the first Go run.
//
// Supported values: nil, bool, string, int/int64/uint*, float64/float32,
// []any, []string, []map[string]any, map[string]any, and types implementing
// JSONValuer.
func CanonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := writePyJSON(&buf, v, "", ""); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// IndentedJSON encodes v like json.dumps(v, sort_keys=True, indent=2): the
// shape the JSON state files were written in, so a state file written by
// either implementation reads the same to both.
func IndentedJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := writePyJSON(&buf, v, "  ", ""); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// JSONSHA256 is sha256 over CanonicalJSON(v), hex encoded: the fingerprint
// every uploader's incremental state compares.
func JSONSHA256(v any) string {
	data, err := CanonicalJSON(v)
	if err != nil {
		panic(fmt.Sprintf("common.JSONSHA256: %v", err))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// JSONValuer lets a value choose how it is encoded.
type JSONValuer interface {
	JSONValue() any
}

func writePyJSON(buf *bytes.Buffer, v any, indent, current string) error {
	switch value := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if value {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case string:
		writePyString(buf, value)
	case int:
		buf.WriteString(strconv.FormatInt(int64(value), 10))
	case int64:
		buf.WriteString(strconv.FormatInt(value, 10))
	case int32:
		buf.WriteString(strconv.FormatInt(int64(value), 10))
	case uint:
		buf.WriteString(strconv.FormatUint(uint64(value), 10))
	case uint64:
		buf.WriteString(strconv.FormatUint(value, 10))
	case float64:
		buf.WriteString(FloatRepr(value))
	case float32:
		buf.WriteString(FloatRepr(float64(value)))
	case JSONValuer:
		return writePyJSON(buf, value.JSONValue(), indent, current)
	case []string:
		items := make([]any, len(value))
		for i, s := range value {
			items[i] = s
		}
		return writePyJSON(buf, items, indent, current)
	case []map[string]any:
		items := make([]any, len(value))
		for i, m := range value {
			items[i] = m
		}
		return writePyJSON(buf, items, indent, current)
	case []any:
		if len(value) == 0 {
			buf.WriteString("[]")
			return nil
		}
		buf.WriteByte('[')
		inner := current + indent
		for i, item := range value {
			if i > 0 {
				buf.WriteByte(',')
			}
			if indent != "" {
				buf.WriteByte('\n')
				buf.WriteString(inner)
			}
			if err := writePyJSON(buf, item, indent, inner); err != nil {
				return err
			}
		}
		if indent != "" {
			buf.WriteByte('\n')
			buf.WriteString(current)
		}
		buf.WriteByte(']')
	case map[string]any:
		if len(value) == 0 {
			buf.WriteString("{}")
			return nil
		}
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		inner := current + indent
		for i, key := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			if indent != "" {
				buf.WriteByte('\n')
				buf.WriteString(inner)
			}
			writePyString(buf, key)
			buf.WriteByte(':')
			if indent != "" {
				buf.WriteByte(' ')
			}
			if err := writePyJSON(buf, value[key], indent, inner); err != nil {
				return err
			}
		}
		if indent != "" {
			buf.WriteByte('\n')
			buf.WriteString(current)
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("common.CanonicalJSON: unsupported value of type %T", v)
	}
	return nil
}

// writePyString mirrors json.encoder.py_encode_basestring_ascii: everything
// outside printable ASCII (space..tilde) is \uXXXX-escaped, astral code points
// as surrogate pairs, and the short escapes are used for the usual controls.
func writePyString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		i += size
		switch r {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		default:
			if r >= ' ' && r <= '~' {
				buf.WriteRune(r)
			} else if r < 0x10000 {
				fmt.Fprintf(buf, `\u%04x`, r)
			} else {
				v := r - 0x10000
				fmt.Fprintf(buf, `\u%04x\u%04x`, 0xd800|(v>>10), 0xdc00|(v&0x3ff))
			}
		}
	}
	buf.WriteByte('"')
}

// FloatRepr formats f the way Python's float repr() does: the shortest digits
// that round-trip, in fixed notation when the decimal exponent is in
// [-4, 16), else in exponent notation with a sign and at least two digits.
func FloatRepr(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	if f == 0 {
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	// strconv 'e' with precision -1 gives the shortest round-tripping digits.
	sci := strconv.FormatFloat(f, 'e', -1, 64)
	neg := strings.HasPrefix(sci, "-")
	if neg {
		sci = sci[1:]
	}
	mantissa, expText, _ := strings.Cut(sci, "e")
	exp, _ := strconv.Atoi(expText)
	digits := strings.Replace(mantissa, ".", "", 1)
	var out string
	if exp >= -4 && exp < 16 {
		decpt := exp + 1
		switch {
		case decpt <= 0:
			out = "0." + strings.Repeat("0", -decpt) + digits
		case decpt >= len(digits):
			out = digits + strings.Repeat("0", decpt-len(digits)) + ".0"
		default:
			out = digits[:decpt] + "." + digits[decpt:]
		}
	} else {
		m := digits[:1]
		if len(digits) > 1 {
			m += "." + digits[1:]
		}
		sign := "+"
		if exp < 0 {
			sign = "-"
			exp = -exp
		}
		out = fmt.Sprintf("%se%s%02d", m, sign, exp)
	}
	if neg {
		return "-" + out
	}
	return out
}
