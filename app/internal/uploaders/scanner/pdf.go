package scanner

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"image/color"
	"image/jpeg"
	"math/bits"
	"strings"
	"time"
)

// A scan's pages arrive as the scanner made them: a JPEG per page in color or
// gray, a single-strip CCITT G4 TIFF per page in black and white. A PDF can
// carry both encodings unchanged (DCTDecode and CCITTFaxDecode), so the
// document is the scanner's own bytes in a PDF wrapper, never re-encoded.

// Page is one scanned page.
type Page struct {
	// Format is the WS-Scan format the bytes are in: "exif" or "tiff-single-g4".
	Format string
	Data   []byte
	// DPI is the resolution the page was scanned at (used when the image
	// itself does not say).
	DPI int
}

type pdfImage struct {
	width, height int
	dpiX, dpiY    float64
	dict          string
	stream        []byte
}

func pageImage(p Page) (pdfImage, error) {
	switch p.Format {
	case "exif", "jfif", "jpeg":
		return jpegImage(p)
	case "tiff-single-g4":
		return g4Image(p)
	}
	return pdfImage{}, fmt.Errorf("unsupported page format %q", p.Format)
}

func jpegImage(p Page) (pdfImage, error) {
	cfg, err := jpeg.DecodeConfig(bytes.NewReader(p.Data))
	if err != nil {
		return pdfImage{}, fmt.Errorf("page is not a readable JPEG: %w", err)
	}
	space := "/DeviceRGB"
	switch cfg.ColorModel {
	case color.GrayModel, color.Gray16Model:
		space = "/DeviceGray"
	case color.CMYKModel:
		return pdfImage{}, fmt.Errorf("CMYK JPEG pages are not supported")
	}
	dpi := float64(max(p.DPI, 1))
	return pdfImage{
		width: cfg.Width, height: cfg.Height, dpiX: dpi, dpiY: dpi,
		dict:   fmt.Sprintf("/ColorSpace %s /BitsPerComponent 8 /Filter /DCTDecode", space),
		stream: p.Data,
	}, nil
}

// g4Image lifts the G4 strip out of a single-strip TIFF. A PDF's CCITT
// decoder reads bits most significant first; a TIFF with FillOrder 2 (Brother
// sends them so) stores them least significant first, so those bytes are
// bit-reversed.
func g4Image(p Page) (pdfImage, error) {
	t, err := parseTIFF(p.Data)
	if err != nil {
		return pdfImage{}, err
	}
	if t.compression != 4 {
		return pdfImage{}, fmt.Errorf("TIFF page is compression %d, not CCITT G4", t.compression)
	}
	if len(t.stripOffsets) != 1 || len(t.stripCounts) != 1 {
		return pdfImage{}, fmt.Errorf("TIFF page has %d strips; only single-strip G4 is supported", len(t.stripOffsets))
	}
	start, n := int(t.stripOffsets[0]), int(t.stripCounts[0])
	if start < 0 || n <= 0 || start+n > len(p.Data) {
		return pdfImage{}, fmt.Errorf("TIFF strip lies outside the file")
	}
	strip := append([]byte(nil), p.Data[start:start+n]...)
	if t.fillOrder == 2 {
		for i, b := range strip {
			strip[i] = bits.Reverse8(b)
		}
	}
	blackIs1 := "false"
	if t.photometric == 1 {
		blackIs1 = "true"
	}
	dpiX, dpiY := t.xRes, t.yRes
	if dpiX <= 0 {
		dpiX = float64(max(p.DPI, 1))
	}
	if dpiY <= 0 {
		dpiY = dpiX
	}
	return pdfImage{
		width: t.width, height: t.height, dpiX: dpiX, dpiY: dpiY,
		dict: fmt.Sprintf("/ColorSpace /DeviceGray /BitsPerComponent 1 /Filter /CCITTFaxDecode /DecodeParms << /K -1 /Columns %d /Rows %d /BlackIs1 %s >>",
			t.width, t.height, blackIs1),
		stream: strip,
	}, nil
}

type tiffInfo struct {
	width, height            int
	compression, photometric int
	fillOrder                int
	stripOffsets             []uint32
	stripCounts              []uint32
	xRes, yRes               float64
}

func parseTIFF(data []byte) (tiffInfo, error) {
	if len(data) < 8 {
		return tiffInfo{}, fmt.Errorf("TIFF page is truncated")
	}
	var order binary.ByteOrder
	switch string(data[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return tiffInfo{}, fmt.Errorf("page is not a TIFF")
	}
	if order.Uint16(data[2:4]) != 42 {
		return tiffInfo{}, fmt.Errorf("page is not a TIFF")
	}
	ifd := int(order.Uint32(data[4:8]))
	if ifd+2 > len(data) {
		return tiffInfo{}, fmt.Errorf("TIFF directory lies outside the file")
	}
	count := int(order.Uint16(data[ifd : ifd+2]))
	t := tiffInfo{fillOrder: 1}
	typeSize := map[uint16]int{1: 1, 2: 1, 3: 2, 4: 4, 5: 8}
	values := func(typ uint16, n uint32, field []byte) ([]uint32, error) {
		size := typeSize[typ]
		if size == 0 || typ == 5 || typ == 2 {
			return nil, nil
		}
		total := size * int(n)
		raw := field
		if total > 4 {
			off := int(order.Uint32(field))
			if off < 0 || off+total > len(data) {
				return nil, fmt.Errorf("TIFF value lies outside the file")
			}
			raw = data[off : off+total]
		}
		out := make([]uint32, n)
		for i := range out {
			switch size {
			case 1:
				out[i] = uint32(raw[i])
			case 2:
				out[i] = uint32(order.Uint16(raw[2*i:]))
			case 4:
				out[i] = order.Uint32(raw[4*i:])
			}
		}
		return out, nil
	}
	rational := func(field []byte) float64 {
		off := int(order.Uint32(field))
		if off < 0 || off+8 > len(data) {
			return 0
		}
		num, den := order.Uint32(data[off:]), order.Uint32(data[off+4:])
		if den == 0 {
			return 0
		}
		return float64(num) / float64(den)
	}
	for i := 0; i < count; i++ {
		e := ifd + 2 + 12*i
		if e+12 > len(data) {
			return tiffInfo{}, fmt.Errorf("TIFF directory is truncated")
		}
		tag, typ, n := order.Uint16(data[e:]), order.Uint16(data[e+2:]), order.Uint32(data[e+4:])
		field := data[e+8 : e+12]
		vals, err := values(typ, n, field)
		if err != nil {
			return tiffInfo{}, err
		}
		first := 0
		if len(vals) > 0 {
			first = int(vals[0])
		}
		switch tag {
		case 256:
			t.width = first
		case 257:
			t.height = first
		case 259:
			t.compression = first
		case 262:
			t.photometric = first
		case 266:
			t.fillOrder = first
		case 273:
			t.stripOffsets = vals
		case 279:
			t.stripCounts = vals
		case 282:
			t.xRes = rational(field)
		case 283:
			t.yRes = rational(field)
		}
	}
	if t.width <= 0 || t.height <= 0 {
		return tiffInfo{}, fmt.Errorf("TIFF page has no size")
	}
	return t, nil
}

// BuildPDF writes the pages, in order, as one PDF; each page is the size of
// its scan.
func BuildPDF(pages []Page, title string, created time.Time) ([]byte, error) {
	if len(pages) == 0 {
		return nil, fmt.Errorf("a scan has no pages")
	}
	images := make([]pdfImage, len(pages))
	for i, p := range pages {
		img, err := pageImage(p)
		if err != nil {
			return nil, fmt.Errorf("page %d: %w", i+1, err)
		}
		images[i] = img
	}
	var buf bytes.Buffer
	var offsets []int
	object := func(body string, stream []byte) int {
		offsets = append(offsets, buf.Len())
		n := len(offsets)
		fmt.Fprintf(&buf, "%d 0 obj\n", n)
		if stream == nil {
			buf.WriteString(body + "\nendobj\n")
		} else {
			fmt.Fprintf(&buf, "<< %s /Length %d >>\nstream\n", body, len(stream))
			buf.Write(stream)
			buf.WriteString("\nendstream\nendobj\n")
		}
		return n
	}
	buf.WriteString("%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
	// Fixed numbering: 1 catalog, 2 page tree, 3 info, then three objects
	// per page (image, content, page).
	const firstPage = 4
	kids := make([]string, len(images))
	for i := range images {
		kids[i] = fmt.Sprintf("%d 0 R", firstPage+3*i+2)
	}
	object("<< /Type /Catalog /Pages 2 0 R >>", nil)
	object(fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", strings.Join(kids, " "), len(images)), nil)
	object(fmt.Sprintf("<< /Title %s /Producer (PDW scanner) /CreationDate (D:%s) >>",
		pdfString(title), created.UTC().Format("20060102150405Z")), nil)
	for _, img := range images {
		w := float64(img.width) * 72 / img.dpiX
		h := float64(img.height) * 72 / img.dpiY
		imageObj := object(fmt.Sprintf("/Type /XObject /Subtype /Image /Width %d /Height %d %s", img.width, img.height, img.dict), img.stream)
		content := []byte(fmt.Sprintf("q %.2f 0 0 %.2f 0 0 cm /Im0 Do Q", w, h))
		contentObj := object("", content)
		object(fmt.Sprintf("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 %.2f %.2f] /Resources << /XObject << /Im0 %d 0 R >> >> /Contents %d 0 R >>",
			w, h, imageObj, contentObj), nil)
	}
	xref := buf.Len()
	fmt.Fprintf(&buf, "xref\n0 %d\n0000000000 65535 f \n", len(offsets)+1)
	for _, off := range offsets {
		fmt.Fprintf(&buf, "%010d 00000 n \n", off)
	}
	fmt.Fprintf(&buf, "trailer\n<< /Size %d /Root 1 0 R /Info 3 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(offsets)+1, xref)
	return buf.Bytes(), nil
}

// pdfString escapes a literal string; non-ASCII is dropped (titles here are
// generated, not typed).
func pdfString(s string) string {
	var b strings.Builder
	b.WriteByte('(')
	for _, r := range s {
		switch {
		case r == '(' || r == ')' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r >= 0x20 && r < 0x7f:
			b.WriteRune(r)
		}
	}
	b.WriteByte(')')
	return b.String()
}
