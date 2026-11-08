package androidphotos

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
)

// ExifFacts are the capture facts MediaStore does not carry: the camera, the
// UTC offset the shot was taken at, and where. MediaStore stopped exposing
// latitude/longitude in Android 10; the file's own EXIF still has them.
type ExifFacts struct {
	Make               string
	Model              string
	DateTimeOriginal   string // "2006:01:02 15:04:05", local to the offset
	OffsetTimeOriginal string // "-05:00"
	Latitude           *float64
	Longitude          *float64
}

const (
	tagMake               = 0x010F
	tagModel              = 0x0110
	tagExifIFD            = 0x8769
	tagGPSIFD             = 0x8825
	tagDateTimeOriginal   = 0x9003
	tagOffsetTimeOriginal = 0x9011
	tagGPSLatitudeRef     = 0x0001
	tagGPSLatitude        = 0x0002
	tagGPSLongitudeRef    = 0x0003
	tagGPSLongitude       = 0x0004

	exifTypeASCII    = 2
	exifTypeShort    = 3
	exifTypeLong     = 4
	exifTypeRational = 5
)

// ReadJPEGExif reads the EXIF block of a JPEG. A file with no EXIF returns
// empty facts and no error; only an unreadable file is an error. Nothing
// here is a judgement: it copies the fields the camera wrote.
func ReadJPEGExif(path string) (ExifFacts, error) {
	blocks, err := readJPEGBlocks(path)
	if err != nil || blocks.exif == nil {
		return ExifFacts{}, err
	}
	return parseTIFF(blocks.exif), nil
}

// jpegBlocks are the APP1 payloads before the image data: the TIFF bytes of
// the EXIF block and the XMP packet.
type jpegBlocks struct {
	exif []byte
	xmp  []byte
}

const xmpSignature = "http://ns.adobe.com/xap/1.0/\x00"

// readJPEGBlocks reads a JPEG's APP1 blocks. A file that is not a JPEG, or
// whose markers are damaged, has none; only an unopenable file is an error.
func readJPEGBlocks(path string) (jpegBlocks, error) {
	file, err := os.Open(path)
	if err != nil {
		return jpegBlocks{}, err
	}
	defer file.Close()
	blocks, _ := jpegAPP1Blocks(bufio.NewReader(file))
	return blocks, nil
}

// jpegAPP1Blocks walks the markers up to the start of scan, keeping the
// first EXIF and the first XMP APP1 segment.
func jpegAPP1Blocks(r *bufio.Reader) (jpegBlocks, error) {
	var blocks jpegBlocks
	var soi [2]byte
	if _, err := io.ReadFull(r, soi[:]); err != nil || soi != [2]byte{0xFF, 0xD8} {
		return blocks, errors.New("not a JPEG")
	}
	for {
		b, err := r.ReadByte()
		if err != nil {
			return blocks, err
		}
		if b != 0xFF {
			return blocks, errors.New("corrupt JPEG marker")
		}
		marker, err := r.ReadByte()
		for err == nil && marker == 0xFF { // fill bytes
			marker, err = r.ReadByte()
		}
		if err != nil {
			return blocks, err
		}
		if marker == 0xD9 || marker == 0xDA { // end of image, start of scan
			return blocks, nil
		}
		if marker == 0x01 || (marker >= 0xD0 && marker <= 0xD7) {
			continue // standalone markers carry no length
		}
		var lengthBytes [2]byte
		if _, err := io.ReadFull(r, lengthBytes[:]); err != nil {
			return blocks, err
		}
		length := int(binary.BigEndian.Uint16(lengthBytes[:])) - 2
		if length < 0 {
			return blocks, errors.New("corrupt JPEG segment length")
		}
		segment := make([]byte, length)
		if _, err := io.ReadFull(r, segment); err != nil {
			return blocks, err
		}
		if marker != 0xE1 {
			continue
		}
		switch {
		case blocks.exif == nil && len(segment) > 6 && string(segment[:6]) == "Exif\x00\x00":
			blocks.exif = segment[6:]
		case blocks.xmp == nil && strings.HasPrefix(string(segment), xmpSignature):
			blocks.xmp = segment[len(xmpSignature):]
		}
	}
}

type tiffReader struct {
	data  []byte
	order binary.ByteOrder
}

type ifdEntry struct {
	typ   uint16
	count uint32
	value []byte // the 4-byte value/offset field
}

func parseTIFF(data []byte) ExifFacts {
	var facts ExifFacts
	if len(data) < 8 {
		return facts
	}
	t := tiffReader{data: data}
	switch string(data[:2]) {
	case "II":
		t.order = binary.LittleEndian
	case "MM":
		t.order = binary.BigEndian
	default:
		return facts
	}
	ifd0 := t.readIFD(t.order.Uint32(data[4:8]))
	facts.Make = t.ascii(ifd0[tagMake])
	facts.Model = t.ascii(ifd0[tagModel])
	if entry, ok := ifd0[tagExifIFD]; ok {
		exif := t.readIFD(t.long(entry))
		facts.DateTimeOriginal = t.ascii(exif[tagDateTimeOriginal])
		facts.OffsetTimeOriginal = t.ascii(exif[tagOffsetTimeOriginal])
	}
	if entry, ok := ifd0[tagGPSIFD]; ok {
		gps := t.readIFD(t.long(entry))
		lat, latOK := t.degrees(gps[tagGPSLatitude], t.ascii(gps[tagGPSLatitudeRef]))
		lon, lonOK := t.degrees(gps[tagGPSLongitude], t.ascii(gps[tagGPSLongitudeRef]))
		// 0,0 is what a camera writes when it had no fix.
		if latOK && lonOK && !(lat == 0 && lon == 0) {
			facts.Latitude, facts.Longitude = &lat, &lon
		}
	}
	return facts
}

func (t tiffReader) readIFD(offset uint32) map[uint16]ifdEntry {
	entries := map[uint16]ifdEntry{}
	start := int(offset)
	if offset == 0 || start+2 > len(t.data) {
		return entries
	}
	count := int(t.order.Uint16(t.data[start : start+2]))
	for i := 0; i < count; i++ {
		at := start + 2 + i*12
		if at+12 > len(t.data) {
			break
		}
		raw := t.data[at : at+12]
		entries[t.order.Uint16(raw[0:2])] = ifdEntry{
			typ:   t.order.Uint16(raw[2:4]),
			count: t.order.Uint32(raw[4:8]),
			value: raw[8:12],
		}
	}
	return entries
}

// bytesOf is an entry's payload: inline when it fits in four bytes.
func (t tiffReader) bytesOf(e ifdEntry, size int) []byte {
	total := int(e.count) * size
	if total <= 0 {
		return nil
	}
	if total <= 4 {
		return e.value[:total]
	}
	offset := int(t.order.Uint32(e.value))
	if offset < 0 || offset+total > len(t.data) || total > 1<<16 {
		return nil
	}
	return t.data[offset : offset+total]
}

func (t tiffReader) ascii(e ifdEntry) string {
	if e.typ != exifTypeASCII {
		return ""
	}
	return strings.TrimSpace(strings.TrimRight(string(t.bytesOf(e, 1)), "\x00"))
}

func (t tiffReader) long(e ifdEntry) uint32 {
	switch e.typ {
	case exifTypeLong:
		return t.order.Uint32(e.value)
	case exifTypeShort:
		return uint32(t.order.Uint16(e.value))
	}
	return 0
}

func (t tiffReader) degrees(e ifdEntry, ref string) (float64, bool) {
	if e.typ != exifTypeRational || e.count != 3 {
		return 0, false
	}
	raw := t.bytesOf(e, 8)
	if len(raw) != 24 {
		return 0, false
	}
	var parts [3]float64
	for i := range parts {
		num := t.order.Uint32(raw[i*8 : i*8+4])
		den := t.order.Uint32(raw[i*8+4 : i*8+8])
		if den == 0 {
			return 0, false
		}
		parts[i] = float64(num) / float64(den)
	}
	value := parts[0] + parts[1]/60 + parts[2]/3600
	if ref == "S" || ref == "W" {
		value = -value
	}
	return value, true
}
