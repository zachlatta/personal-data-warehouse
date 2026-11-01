package scanner

import (
	"bytes"
	"fmt"
	"image/color"
	"image/jpeg"
	"strings"
	"time"
)

// A scan's pages arrive as the scanner made them, a JPEG per page in color or
// gray. A PDF carries JPEG unchanged (DCTDecode), so the document is the
// scanner's own bytes in a PDF wrapper, never re-encoded.

// Page is one scanned page.
type Page struct {
	// Format is the encoding of Data: "exif" (JPEG).
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
