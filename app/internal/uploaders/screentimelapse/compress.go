package screentimelapse

import (
	"bytes"
	_ "embed"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

//go:embed webp_android.jar
var embeddedWebpJar []byte

const (
	// DefaultCompressQuality is the lossy WebP/JPEG quality used when
	// --compress-png is enabled (measured on pichu: 9.2x smaller at full
	// 2410x1080 resolution, ~37 KB/frame average, ~126 ms CPU/frame).
	DefaultCompressQuality = 75

	// DefaultCompressMaxEdge caps the longest image edge when > 0; 0 keeps
	// the screenshot's native resolution (2410x1080 on pichu).
	DefaultCompressMaxEdge = 0

	defaultAndroidAppProcess = "/system/bin/app_process64"
	androidHelperClass       = "com.zachlatta.pdw.screentimelapse.PdwPngCompress"
	defaultAndroidDumpsys    = "/system/bin/dumpsys"
)

// CompressedImage holds the transcoded screenshot bytes and its target
// filename extension and MIME type.
type CompressedImage struct {
	Content   []byte
	Extension string
	MimeType  string
}

// PNGCompressor transcodes a PNG screenshot (after any card-number redaction)
// in memory before upload.
type PNGCompressor interface {
	CompressPNG(raw []byte) (CompressedImage, error)
}

// DefaultPNGCompressor uses Android's native Skia WEBP_LOSSY encoder (via the
// embedded DEX helper and /system/bin/app_process64) when running on Android,
// and falls back to pure-Go JPEG encoding elsewhere.
type DefaultPNGCompressor struct {
	Quality        int
	MaxEdge        int
	AppProcessPath string
}

func (c *DefaultPNGCompressor) quality() int {
	if c == nil || c.Quality <= 0 || c.Quality > 100 {
		return DefaultCompressQuality
	}
	return c.Quality
}

func (c *DefaultPNGCompressor) maxEdge() int {
	if c == nil || c.MaxEdge < 0 {
		return DefaultCompressMaxEdge
	}
	return c.MaxEdge
}

func (c *DefaultPNGCompressor) appProcess() string {
	if c != nil && c.AppProcessPath != "" {
		return c.AppProcessPath
	}
	return defaultAndroidAppProcess
}

// CompressPNG transcodes PNG bytes to WebP (on Android) or JPEG (pure-Go fallback
// when not running on Android). On Android, a Skia helper failure is returned
// directly rather than silently falling back to JPEG.
func (c *DefaultPNGCompressor) CompressPNG(raw []byte) (CompressedImage, error) {
	if appProc := c.appProcess(); isExecutable(appProc) {
		return c.compressAndroidWebP(appProc, raw)
	}
	return c.compressPureGoJPEG(raw)
}

func (c *DefaultPNGCompressor) compressAndroidWebP(appProc string, raw []byte) (CompressedImage, error) {
	jarPath, err := ensureEmbeddedJar()
	if err != nil {
		return CompressedImage{}, err
	}
	tmpDir, err := os.MkdirTemp("", "pdw-webp-")
	if err != nil {
		return CompressedImage{}, err
	}
	defer os.RemoveAll(tmpDir)

	inPath := filepath.Join(tmpDir, "in.png")
	if err := os.WriteFile(inPath, raw, 0o600); err != nil {
		return CompressedImage{}, err
	}
	outPath := filepath.Join(tmpDir, "out.webp")
	cmd := exec.Command(
		appProc,
		"/",
		androidHelperClass,
		strconv.Itoa(c.quality()),
		strconv.Itoa(c.maxEdge()),
		inPath,
		outPath,
	)
	cmd.Env = append(os.Environ(), "CLASSPATH="+jarPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return CompressedImage{}, fmt.Errorf("app_process64 %s: %w (%s)", androidHelperClass, err, strings.TrimSpace(string(out)))
	}
	webp, err := os.ReadFile(outPath)
	if err != nil {
		return CompressedImage{}, err
	}
	if len(webp) < 12 || string(webp[:4]) != "RIFF" || string(webp[8:12]) != "WEBP" {
		return CompressedImage{}, fmt.Errorf("invalid WebP output (%d bytes)", len(webp))
	}
	return CompressedImage{
		Content:   webp,
		Extension: ".webp",
		MimeType:  "image/webp",
	}, nil
}

func (c *DefaultPNGCompressor) compressPureGoJPEG(raw []byte) (CompressedImage, error) {
	img, err := png.Decode(bytes.NewReader(raw))
	if err != nil {
		return CompressedImage{}, fmt.Errorf("decode png: %w", err)
	}
	maxEdge := c.maxEdge()
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	maxDim := w
	if h > maxDim {
		maxDim = h
	}
	if maxEdge > 0 && maxDim > maxEdge {
		nw := (w * maxEdge) / maxDim
		if nw < 1 {
			nw = 1
		}
		nh := (h * maxEdge) / maxDim
		if nh < 1 {
			nh = 1
		}
		img = scaleBilinear(img, nw, nh)
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: c.quality()}); err != nil {
		return CompressedImage{}, fmt.Errorf("encode jpeg: %w", err)
	}
	return CompressedImage{
		Content:   buf.Bytes(),
		Extension: ".jpg",
		MimeType:  "image/jpeg",
	}, nil
}

func ensureEmbeddedJar() (string, error) {
	sum := sha256Hex(embeddedWebpJar)[:16]
	jarPath := filepath.Join(os.TempDir(), "pdw-screen-timelapse-"+sum+".jar")
	if info, err := os.Stat(jarPath); err == nil && info.Size() == int64(len(embeddedWebpJar)) {
		return jarPath, nil
	}
	tmp := jarPath + ".tmp"
	if err := os.WriteFile(tmp, embeddedWebpJar, 0o600); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, jarPath); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return jarPath, nil
}

func isExecutable(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return info.Mode()&0o111 != 0
}

func scaleBilinear(src image.Image, dstW, dstH int) *image.RGBA {
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

// CheckAndroidWiFi inspects `dumpsys connectivity` output and returns a non-empty
// reason when the active default network is not Wi-Fi.
func CheckAndroidWiFi(dumpsysOutput string) string {
	var netID string
	for _, line := range strings.Split(dumpsysOutput, "\n") {
		if idx := strings.Index(line, "Active default network:"); idx >= 0 {
			fields := strings.Fields(line[idx+len("Active default network:"):])
			if len(fields) == 0 || fields[0] == "none" || fields[0] == "null" {
				return "device has no active default network (--wifi-only)"
			}
			netID = "network{" + fields[0] + "}"
			break
		}
	}
	if netID == "" {
		return ""
	}
	marker := "NetworkAgentInfo{" + netID
	for _, line := range strings.Split(dumpsysOutput, "\n") {
		if strings.Contains(line, marker) {
			if strings.Contains(line, "ni{WIFI") {
				return ""
			}
			return "device is not on Wi-Fi (--wifi-only)"
		}
	}
	return "device default network transport is not Wi-Fi (--wifi-only)"
}

// AndroidWiFiGuard checks `/system/bin/dumpsys connectivity` on Android devices
// when --wifi-only / SCREEN_TIMELAPSE_WIFI_ONLY is enabled.
func AndroidWiFiGuard() string {
	if !isExecutable(defaultAndroidDumpsys) {
		return ""
	}
	out, err := exec.Command(defaultAndroidDumpsys, "connectivity").Output()
	if err != nil {
		return ""
	}
	return CheckAndroidWiFi(string(out))
}
