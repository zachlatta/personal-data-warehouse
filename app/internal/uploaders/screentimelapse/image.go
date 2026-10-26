package screentimelapse

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
)

// ImageCodec decodes and re-encodes a screenshot in its own format: the app
// reads a file's type from its name, so a boxed frame keeps the format it was
// captured in.
type ImageCodec interface {
	Decode(content []byte, mimeType string) (image.Image, error)
	Encode(img image.Image, mimeType string) ([]byte, error)
}

// SystemCodec handles PNG in Go and HEIC and AVIF with macOS's sips, which
// reads and writes both; every host that runs this uploader is a Mac.
type SystemCodec struct {
	Sips string // default /usr/bin/sips
}

var sipsFormats = map[string]string{"image/heic": "heic", "image/avif": "avif"}

func (c SystemCodec) Decode(content []byte, mimeType string) (image.Image, error) {
	if mimeType == "image/png" {
		return png.Decode(bytes.NewReader(content))
	}
	format, ok := sipsFormats[mimeType]
	if !ok {
		return nil, fmt.Errorf("no codec for %s", mimeType)
	}
	out, err := c.convert(content, format, "png")
	if err != nil {
		return nil, err
	}
	return png.Decode(bytes.NewReader(out))
}

func (c SystemCodec) Encode(img image.Image, mimeType string) ([]byte, error) {
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, err
	}
	if mimeType == "image/png" {
		return buf.Bytes(), nil
	}
	format, ok := sipsFormats[mimeType]
	if !ok {
		return nil, fmt.Errorf("no codec for %s", mimeType)
	}
	return c.convert(buf.Bytes(), "png", format)
}

func (c SystemCodec) convert(content []byte, from, to string) ([]byte, error) {
	sips := c.Sips
	if sips == "" {
		sips = "/usr/bin/sips"
	}
	dir, err := os.MkdirTemp("", "pdw-screen-timelapse-redact-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	in, out := filepath.Join(dir, "in."+from), filepath.Join(dir, "out."+to)
	if err := os.WriteFile(in, content, 0o600); err != nil {
		return nil, err
	}
	if output, err := exec.Command(sips, "-s", "format", to, in, "--out", out).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("sips %s to %s: %w: %s", from, to, err, bytes.TrimSpace(output))
	}
	return os.ReadFile(out)
}

var boxColor = color.RGBA{R: 255, A: 255}

// fillBoxes paints each rectangle solid red over a copy of img.
func fillBoxes(img image.Image, boxes []image.Rectangle) *image.RGBA {
	out := image.NewRGBA(img.Bounds())
	draw.Draw(out, out.Bounds(), img, img.Bounds().Min, draw.Src)
	for _, r := range boxes {
		draw.Draw(out, r.Add(img.Bounds().Min), &image.Uniform{C: boxColor}, image.Point{}, draw.Src)
	}
	return out
}
