// Package imaging holds the pure-Go image helpers shared by the uploaders and
// the server, so a frame is scaled the same way wherever it is scaled.
package imaging

import "image"

// ScaleBilinear resizes src to dstW x dstH with bilinear sampling into an
// opaque RGBA image (alpha is dropped). RGBA/NRGBA sources take a direct
// pixel path; any other image type goes through At().
func ScaleBilinear(src image.Image, dstW, dstH int) *image.RGBA {
	b := src.Bounds()
	srcW, srcH := b.Dx(), b.Dy()
	dst := image.NewRGBA(image.Rect(0, 0, dstW, dstH))
	if dstW <= 0 || dstH <= 0 || srcW <= 0 || srcH <= 0 {
		return dst
	}
	var srcPix []uint8
	var srcStride int
	switch s := src.(type) {
	case *image.RGBA:
		srcPix, srcStride = s.Pix, s.Stride
	case *image.NRGBA:
		srcPix, srcStride = s.Pix, s.Stride
	}
	denomX := max(1, dstW-1)
	denomY := max(1, dstH-1)
	for y := 0; y < dstH; y++ {
		sy := float64(y) * float64(max(0, srcH-1)) / float64(denomY)
		y0 := int(sy)
		y1 := min(srcH-1, y0+1)
		fy := int((sy - float64(y0)) * 256)
		ify := 256 - fy
		for x := 0; x < dstW; x++ {
			sx := float64(x) * float64(max(0, srcW-1)) / float64(denomX)
			x0 := int(sx)
			x1 := min(srcW-1, x0+1)
			fx := int((sx - float64(x0)) * 256)
			ifx := 256 - fx
			w00 := ifx * ify
			w10 := fx * ify
			w01 := ifx * fy
			w11 := fx * fy
			di := y*dst.Stride + x*4
			if srcPix != nil {
				i00 := (y0-b.Min.Y)*srcStride + (x0-b.Min.X)*4
				i10 := (y0-b.Min.Y)*srcStride + (x1-b.Min.X)*4
				i01 := (y1-b.Min.Y)*srcStride + (x0-b.Min.X)*4
				i11 := (y1-b.Min.Y)*srcStride + (x1-b.Min.X)*4
				dst.Pix[di+0] = uint8((int(srcPix[i00+0])*w00 + int(srcPix[i10+0])*w10 + int(srcPix[i01+0])*w01 + int(srcPix[i11+0])*w11) >> 16)
				dst.Pix[di+1] = uint8((int(srcPix[i00+1])*w00 + int(srcPix[i10+1])*w10 + int(srcPix[i01+1])*w01 + int(srcPix[i11+1])*w11) >> 16)
				dst.Pix[di+2] = uint8((int(srcPix[i00+2])*w00 + int(srcPix[i10+2])*w10 + int(srcPix[i01+2])*w01 + int(srcPix[i11+2])*w11) >> 16)
				dst.Pix[di+3] = 255
			} else {
				r00, g00, b00, _ := src.At(b.Min.X+x0, b.Min.Y+y0).RGBA()
				r10, g10, b10, _ := src.At(b.Min.X+x1, b.Min.Y+y0).RGBA()
				r01, g01, b01, _ := src.At(b.Min.X+x0, b.Min.Y+y1).RGBA()
				r11, g11, b11, _ := src.At(b.Min.X+x1, b.Min.Y+y1).RGBA()
				dst.Pix[di+0] = uint8(((int(r00>>8)*w00 + int(r10>>8)*w10 + int(r01>>8)*w01 + int(r11>>8)*w11) >> 16))
				dst.Pix[di+1] = uint8(((int(g00>>8)*w00 + int(g10>>8)*w10 + int(g01>>8)*w01 + int(g11>>8)*w11) >> 16))
				dst.Pix[di+2] = uint8(((int(b00>>8)*w00 + int(b10>>8)*w10 + int(b01>>8)*w01 + int(b11>>8)*w11) >> 16))
				dst.Pix[di+3] = 255
			}
		}
	}
	return dst
}
