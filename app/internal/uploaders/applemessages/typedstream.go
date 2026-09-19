package applemessages

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// A minimal reader for Apple's typedstream (NSArchiver) format, enough to
// unarchive the NSAttributedString a Messages row keeps in attributedBody and
// pull its text out. It follows the python-typedstream library the Python
// uploader used, including its shared-string and shared-object numbering,
// because a reference in the stream is an index into those tables.

const (
	tsTagInteger2      = -127
	tsTagInteger4      = -126
	tsTagFloatingPoint = -125
	tsTagNew           = -124
	tsTagNil           = -123
	tsTagEndOfObject   = -122
	tsFirstTag         = -128
	tsLastTag          = -111
	tsFirstReference   = tsLastTag + 1
)

var errInvalidTypedstream = errors.New("invalid typedstream")

// tsClass is one class in an object's superclass chain.
type tsClass struct {
	Name    string
	Version int64
	Super   *tsClass
}

// tsGroup is one type-prefixed group of values inside an object.
type tsGroup struct {
	Encodings []string
	Values    []any
}

// tsObject is a literally stored object: its class and its typed groups.
type tsObject struct {
	Class  *tsClass
	Groups []tsGroup
}

type tsRefKind int

const (
	tsRefCString tsRefKind = iota
	tsRefClass
	tsRefObject
)

type tsSharedEntry struct {
	kind  tsRefKind
	value any
}

type tsReader struct {
	data          []byte
	pos           int
	little        bool
	sharedStrings [][]byte
	sharedObjects []tsSharedEntry
}

func (r *tsReader) readExact(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.data) {
		return nil, fmt.Errorf("%w: attempted to read %d bytes at %d of %d", errInvalidTypedstream, n, r.pos, len(r.data))
	}
	out := r.data[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

func (r *tsReader) atEOF() bool { return r.pos >= len(r.data) }

func (r *tsReader) readHead() (int, error) {
	b, err := r.readExact(1)
	if err != nil {
		return 0, err
	}
	return int(int8(b[0])), nil
}

func isTag(head int) bool { return head >= tsFirstTag && head <= tsLastTag }

func (r *tsReader) readInteger(head int, signed bool) (int64, error) {
	if !isTag(head) {
		if signed {
			return int64(head), nil
		}
		return int64(head & 0xff), nil
	}
	switch head {
	case tsTagInteger2:
		raw, err := r.readExact(2)
		if err != nil {
			return 0, err
		}
		var u uint16
		if r.little {
			u = binary.LittleEndian.Uint16(raw)
		} else {
			u = binary.BigEndian.Uint16(raw)
		}
		if signed {
			return int64(int16(u)), nil
		}
		return int64(u), nil
	case tsTagInteger4:
		raw, err := r.readExact(4)
		if err != nil {
			return 0, err
		}
		var u uint32
		if r.little {
			u = binary.LittleEndian.Uint32(raw)
		} else {
			u = binary.BigEndian.Uint32(raw)
		}
		if signed {
			return int64(int32(u)), nil
		}
		return int64(u), nil
	default:
		return 0, fmt.Errorf("%w: invalid head tag in this context: %d", errInvalidTypedstream, head)
	}
}

func (r *tsReader) readIntegerNext(signed bool) (int64, error) {
	head, err := r.readHead()
	if err != nil {
		return 0, err
	}
	return r.readInteger(head, signed)
}

func (r *tsReader) readHeader() error {
	raw, err := r.readExact(2)
	if err != nil {
		return err
	}
	version, signatureLength := int(raw[0]), int(raw[1])
	if version != 4 {
		return fmt.Errorf("Invalid streamer version: %d", version)
	}
	if signatureLength != 11 {
		return fmt.Errorf("%w: signature length %d", errInvalidTypedstream, signatureLength)
	}
	signature, err := r.readExact(signatureLength)
	if err != nil {
		return err
	}
	switch string(signature) {
	case "streamtyped":
		r.little = true
	case "typedstream":
		r.little = false
	default:
		return fmt.Errorf("%w: signature %q", errInvalidTypedstream, signature)
	}
	if _, err := r.readIntegerNext(false); err != nil { // system version
		return err
	}
	return nil
}

func (r *tsReader) readFloat(head int) (float64, error) {
	if head == tsTagFloatingPoint {
		raw, err := r.readExact(4)
		if err != nil {
			return 0, err
		}
		var u uint32
		if r.little {
			u = binary.LittleEndian.Uint32(raw)
		} else {
			u = binary.BigEndian.Uint32(raw)
		}
		return float64(math.Float32frombits(u)), nil
	}
	v, err := r.readInteger(head, true)
	return float64(v), err
}

func (r *tsReader) readDouble(head int) (float64, error) {
	if head == tsTagFloatingPoint {
		raw, err := r.readExact(8)
		if err != nil {
			return 0, err
		}
		var u uint64
		if r.little {
			u = binary.LittleEndian.Uint64(raw)
		} else {
			u = binary.BigEndian.Uint64(raw)
		}
		return math.Float64frombits(u), nil
	}
	v, err := r.readInteger(head, true)
	return float64(v), err
}

// readUnsharedString reads a length-prefixed string; ok=false for nil.
func (r *tsReader) readUnsharedString(head int) ([]byte, bool, error) {
	if head == tsTagNil {
		return nil, false, nil
	}
	length, err := r.readInteger(head, false)
	if err != nil {
		return nil, false, err
	}
	data, err := r.readExact(int(length))
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

// readSharedString reads a literal (appending it to the table) or a reference.
func (r *tsReader) readSharedString(head int) ([]byte, bool, error) {
	switch head {
	case tsTagNil:
		return nil, false, nil
	case tsTagNew:
		next, err := r.readHead()
		if err != nil {
			return nil, false, err
		}
		s, ok, err := r.readUnsharedString(next)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("%w: literal shared string cannot be nil", errInvalidTypedstream)
		}
		r.sharedStrings = append(r.sharedStrings, s)
		return s, true, nil
	default:
		number, err := r.readInteger(head, true)
		if err != nil {
			return nil, false, err
		}
		index := int(number) - tsFirstReference
		if index < 0 || index >= len(r.sharedStrings) {
			return nil, false, fmt.Errorf("%w: shared string reference %d out of range", errInvalidTypedstream, index)
		}
		return r.sharedStrings[index], true, nil
	}
}

func (r *tsReader) lookupObject(head int, kind tsRefKind) (any, error) {
	number, err := r.readInteger(head, true)
	if err != nil {
		return nil, err
	}
	index := int(number) - tsFirstReference
	if index < 0 || index >= len(r.sharedObjects) {
		return nil, fmt.Errorf("%w: object reference %d out of range", errInvalidTypedstream, index)
	}
	entry := r.sharedObjects[index]
	if entry.kind != kind {
		return nil, fmt.Errorf("%w: object reference type mismatch", errInvalidTypedstream)
	}
	return entry.value, nil
}

// readClass reads a superclass chain, assigning object numbers in stream order.
func (r *tsReader) readClass(head int) (*tsClass, error) {
	var singles []tsClass
	for head == tsTagNew {
		nameHead, err := r.readHead()
		if err != nil {
			return nil, err
		}
		name, ok, err := r.readSharedString(nameHead)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: class name cannot be nil", errInvalidTypedstream)
		}
		version, err := r.readIntegerNext(true)
		if err != nil {
			return nil, err
		}
		singles = append(singles, tsClass{Name: string(name), Version: version})
		head, err = r.readHead()
		if err != nil {
			return nil, err
		}
	}
	var super *tsClass
	if head != tsTagNil {
		value, err := r.lookupObject(head, tsRefClass)
		if err != nil {
			return nil, err
		}
		super = value.(*tsClass)
	}
	classes := make([]*tsClass, len(singles))
	next := super
	for i := len(singles) - 1; i >= 0; i-- {
		classes[i] = &tsClass{Name: singles[i].Name, Version: singles[i].Version, Super: next}
		next = classes[i]
	}
	for _, class := range classes {
		r.sharedObjects = append(r.sharedObjects, tsSharedEntry{kind: tsRefClass, value: class})
	}
	if len(classes) == 0 {
		return super, nil
	}
	return classes[0], nil
}

func (r *tsReader) readObject(head int) (any, error) {
	switch head {
	case tsTagNil:
		return nil, nil
	case tsTagNew:
		placeholder := len(r.sharedObjects)
		r.sharedObjects = append(r.sharedObjects, tsSharedEntry{kind: tsRefObject})
		classHead, err := r.readHead()
		if err != nil {
			return nil, err
		}
		class, err := r.readClass(classHead)
		if err != nil {
			return nil, err
		}
		if class == nil {
			return nil, fmt.Errorf("%w: object class cannot be nil", errInvalidTypedstream)
		}
		obj := &tsObject{Class: class}
		r.sharedObjects[placeholder] = tsSharedEntry{kind: tsRefObject, value: obj}
		for {
			next, err := r.readHead()
			if err != nil {
				return nil, err
			}
			if next == tsTagEndOfObject {
				break
			}
			group, err := r.readTypedValues(next)
			if err != nil {
				return nil, err
			}
			obj.Groups = append(obj.Groups, group)
		}
		return obj, nil
	default:
		return r.lookupObject(head, tsRefObject)
	}
}

func (r *tsReader) readTypedValues(head int) (tsGroup, error) {
	encoding, ok, err := r.readSharedString(head)
	if err != nil {
		return tsGroup{}, err
	}
	if !ok || len(encoding) == 0 {
		return tsGroup{}, fmt.Errorf("%w: nil or empty type encoding", errInvalidTypedstream)
	}
	encodings, err := splitEncodings(string(encoding))
	if err != nil {
		return tsGroup{}, err
	}
	group := tsGroup{Encodings: encodings}
	for _, enc := range encodings {
		value, err := r.readValue(enc)
		if err != nil {
			return tsGroup{}, err
		}
		group.Values = append(group.Values, value)
	}
	return group, nil
}

func (r *tsReader) readValue(encoding string) (any, error) {
	switch {
	case encoding == "B":
		raw, err := r.readExact(1)
		if err != nil {
			return nil, err
		}
		return raw[0] != 0, nil
	case encoding == "C":
		raw, err := r.readExact(1)
		if err != nil {
			return nil, err
		}
		return int64(raw[0]), nil
	case encoding == "c":
		raw, err := r.readExact(1)
		if err != nil {
			return nil, err
		}
		return int64(int8(raw[0])), nil
	case encoding == "S" || encoding == "I" || encoding == "L" || encoding == "Q":
		return r.readIntegerNext(false)
	case encoding == "s" || encoding == "i" || encoding == "l" || encoding == "q":
		return r.readIntegerNext(true)
	case encoding == "f":
		head, err := r.readHead()
		if err != nil {
			return nil, err
		}
		return r.readFloat(head)
	case encoding == "d":
		head, err := r.readHead()
		if err != nil {
			return nil, err
		}
		return r.readDouble(head)
	case encoding == "*":
		head, err := r.readHead()
		if err != nil {
			return nil, err
		}
		switch head {
		case tsTagNil:
			return nil, nil
		case tsTagNew:
			next, err := r.readHead()
			if err != nil {
				return nil, err
			}
			s, ok, err := r.readSharedString(next)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("%w: literal C string cannot be nil", errInvalidTypedstream)
			}
			r.sharedObjects = append(r.sharedObjects, tsSharedEntry{kind: tsRefCString, value: s})
			return s, nil
		default:
			return r.lookupObject(head, tsRefCString)
		}
	case encoding == "%" || encoding == ":":
		head, err := r.readHead()
		if err != nil {
			return nil, err
		}
		s, ok, err := r.readSharedString(head)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		return s, nil
	case encoding == "+":
		head, err := r.readHead()
		if err != nil {
			return nil, err
		}
		s, ok, err := r.readUnsharedString(head)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		return s, nil
	case encoding == "#":
		head, err := r.readHead()
		if err != nil {
			return nil, err
		}
		return r.readClass(head)
	case encoding == "@":
		head, err := r.readHead()
		if err != nil {
			return nil, err
		}
		return r.readObject(head)
	case encoding == "!":
		return nil, nil
	case strings.HasPrefix(encoding, "["):
		length, element, err := parseArrayEncoding(encoding)
		if err != nil {
			return nil, err
		}
		if element == "C" || element == "c" {
			raw, err := r.readExact(length)
			if err != nil {
				return nil, err
			}
			return append([]byte(nil), raw...), nil
		}
		values := make([]any, 0, length)
		for i := 0; i < length; i++ {
			value, err := r.readValue(element)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	case strings.HasPrefix(encoding, "{"):
		fields, err := parseStructEncoding(encoding)
		if err != nil {
			return nil, err
		}
		values := make([]any, 0, len(fields))
		for _, field := range fields {
			value, err := r.readValue(field)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("%w: unknown type encoding %q", errInvalidTypedstream, encoding)
	}
}

func endOfEncoding(encoding string, start int) (int, error) {
	depth := 0
	for i := start; i < len(encoding); i++ {
		c := encoding[i]
		switch {
		case c == '(' || c == '[' || c == '{':
			depth++
		case depth > 0:
			if c == ')' || c == ']' || c == '}' {
				depth--
			}
			if depth == 0 {
				return i + 1, nil
			}
		default:
			return i + 1, nil
		}
	}
	return 0, fmt.Errorf("%w: incomplete type encoding %q", errInvalidTypedstream, encoding)
}

func splitEncodings(encoding string) ([]string, error) {
	var out []string
	start := 0
	for start < len(encoding) {
		end, err := endOfEncoding(encoding, start)
		if err != nil {
			return nil, err
		}
		out = append(out, encoding[start:end])
		start = end
	}
	return out, nil
}

func parseArrayEncoding(encoding string) (int, string, error) {
	if !strings.HasPrefix(encoding, "[") || !strings.HasSuffix(encoding, "]") {
		return 0, "", fmt.Errorf("%w: bad array encoding %q", errInvalidTypedstream, encoding)
	}
	i := 1
	for i < len(encoding)-1 && encoding[i] >= '0' && encoding[i] <= '9' {
		i++
	}
	length, err := strconv.Atoi(encoding[1:i])
	if err != nil || i == len(encoding)-1 {
		return 0, "", fmt.Errorf("%w: bad array encoding %q", errInvalidTypedstream, encoding)
	}
	return length, encoding[i : len(encoding)-1], nil
}

func parseStructEncoding(encoding string) ([]string, error) {
	if !strings.HasPrefix(encoding, "{") || !strings.HasSuffix(encoding, "}") {
		return nil, fmt.Errorf("%w: bad struct encoding %q", errInvalidTypedstream, encoding)
	}
	inner := encoding[1 : len(encoding)-1]
	nested := strings.Index(inner, "{")
	searchIn := inner
	if nested >= 0 {
		searchIn = inner[:nested]
	}
	if eq := strings.Index(searchIn, "="); eq >= 0 {
		inner = inner[eq+1:]
	}
	return splitEncodings(inner)
}

// unarchiveTypedstream decodes the single root value of an archive.
func unarchiveTypedstream(data []byte) (any, error) {
	r := &tsReader{data: data}
	if err := r.readHeader(); err != nil {
		return nil, err
	}
	var roots []tsGroup
	for !r.atEOF() {
		head, err := r.readHead()
		if err != nil {
			return nil, err
		}
		group, err := r.readTypedValues(head)
		if err != nil {
			return nil, err
		}
		roots = append(roots, group)
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("%w: archive contains %d root values", errInvalidTypedstream, len(roots))
	}
	if len(roots[0].Values) != 1 {
		return nil, fmt.Errorf("%w: root is a group of %d values", errInvalidTypedstream, len(roots[0].Values))
	}
	return roots[0].Values[0], nil
}

// DecodeAttributedBody unarchives a Messages attributedBody blob and returns
// the string of its (mutable) attributed string, "" when the archive holds no
// decodable string.
func DecodeAttributedBody(data []byte) (string, error) {
	root, err := unarchiveTypedstream(data)
	if err != nil {
		return "", err
	}
	text, _ := archivedString(root)
	return text, nil
}

// archivedString mirrors the Python body.archived_string walk over the
// unarchived object graph.
func archivedString(value any) (string, bool) {
	switch v := value.(type) {
	case *tsObject:
		name := ""
		if v.Class != nil {
			name = v.Class.Name
		}
		switch name {
		case "NSAttributedString", "NSMutableAttributedString":
			if len(v.Groups) > 0 {
				return archivedString(groupValue(v.Groups[0]))
			}
		case "NSString", "NSMutableString":
			if len(v.Groups) > 0 {
				if raw, ok := groupValue(v.Groups[0]).([]byte); ok {
					return string(raw), true
				}
			}
		}
		return "", false
	case string:
		return v, true
	default:
		return "", false
	}
}

func groupValue(group tsGroup) any {
	if len(group.Values) == 1 {
		return group.Values[0]
	}
	return group
}
