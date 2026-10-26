package screentimelapse

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"testing"
)

// The real codec, on the Macs that run the uploader: a boxed frame survives
// the round trip through sips in each format a Mac or the Android phone writes.
func TestSystemCodecRedrawsHEICAndAVIF(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 64, 32))
	for i := range src.Pix {
		src.Pix[i] = 0xff // white
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, src); err != nil {
		t.Fatal(err)
	}
	codec := SystemCodec{}
	for _, mime := range []string{"image/heic", "image/avif"} {
		encoded, err := codec.Encode(src, mime)
		if err != nil {
			t.Fatalf("%s: %v", mime, err)
		}
		decoded, err := codec.Decode(encoded, mime)
		if err != nil {
			t.Fatalf("%s: %v", mime, err)
		}
		boxed, err := codec.Encode(fillBoxes(decoded, []image.Rectangle{image.Rect(8, 8, 40, 24)}), mime)
		if err != nil {
			t.Fatalf("%s: %v", mime, err)
		}
		again, err := codec.Decode(boxed, mime)
		if err != nil {
			t.Fatalf("%s: %v", mime, err)
		}
		if again.Bounds().Size() != src.Bounds().Size() {
			t.Fatalf("%s: size %v after the round trip", mime, again.Bounds().Size())
		}
		r, g, b, _ := again.At(24, 16).RGBA()
		if r>>8 < 200 || g>>8 > 60 || b>>8 > 60 {
			t.Fatalf("%s: box pixel = %v, want red", mime, color.RGBA{uint8(r >> 8), uint8(g >> 8), uint8(b >> 8), 255})
		}
		r, g, b, _ = again.At(56, 4).RGBA()
		if r>>8 < 200 || g>>8 < 200 || b>>8 < 200 {
			t.Fatalf("%s: pixel outside the box changed", mime)
		}
	}
}
