package screentimelapse

import (
	"bytes"
	_ "embed"
	"fmt"
	"image/jpeg"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/imaging"
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
		img = imaging.ScaleBilinear(img, nw, nh)
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
