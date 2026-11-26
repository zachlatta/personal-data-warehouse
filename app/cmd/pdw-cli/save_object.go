package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type getObjectEnvelope struct {
	StorageFileID string `json:"storage_file_id"`
	Exists        bool   `json:"exists"`
	ContentType   string `json:"content_type"`
	SizeBytes     int64  `json:"size_bytes"`
	ContentSHA256 string `json:"content_sha256"`
	Filename      string `json:"filename"`
	DownloadURL   string `json:"download_url"`
	Error         string `json:"error"`
}

type saveObjectSummary struct {
	Saved         string `json:"saved"`
	StorageFileID string `json:"storage_file_id"`
	ContentType   string `json:"content_type"`
	ConvertedFrom string `json:"converted_from,omitempty"`
	SizeBytes     int64  `json:"size_bytes"`
	SourceSHA256  string `json:"source_sha256"`
}

var errNoImageConverter = errors.New("no image converter found on PATH")

// convertImageFn is overridden in unit tests.
var convertImageFn = convertImageToTarget

var saveObjectHTTPClient = &http.Client{Timeout: 120 * time.Second}

// saveGetObjectResponse downloads the signed download_url from a get_object
// response, verifies content_sha256, and converts mismatched image formats
// (HEIC/HEIF/AVIF/WebP/PNG/JPEG) when the destination path ends in .png, .jpg
// or .jpeg. If no converter is available on PATH, the original bytes are saved
// under their real extension with a clear hint on stderr.
func saveGetObjectResponse(raw json.RawMessage, destPath string, stdout, stderr io.Writer) int {
	var env getObjectEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		fmt.Fprintf(stderr, "pdw call: could not decode get_object response: %v\n", err)
		return 1
	}
	if env.Error != "" {
		fmt.Fprintf(stderr, "pdw call: %s\n", env.Error)
		return 1
	}
	if !env.Exists || env.DownloadURL == "" {
		fmt.Fprintf(stderr, "pdw call: object %q not found or has no download_url\n", env.StorageFileID)
		return 1
	}
	req, err := http.NewRequest(http.MethodGet, env.DownloadURL, nil)
	if err != nil {
		fmt.Fprintf(stderr, "pdw call: invalid download_url: %v\n", err)
		return 1
	}
	req.Header.Set("User-Agent", "pdw-cli/1.0")
	resp, err := saveObjectHTTPClient.Do(req)
	if err != nil {
		fmt.Fprintf(stderr, "pdw call: download failed: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		fmt.Fprintf(stderr, "pdw call: download HTTP %d: %s\n", resp.StatusCode, strings.TrimSpace(string(body)))
		return 1
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(stderr, "pdw call: reading download body failed: %v\n", err)
		return 1
	}
	sum := sha256.Sum256(data)
	gotSHA := hex.EncodeToString(sum[:])
	if env.ContentSHA256 != "" && !strings.EqualFold(env.ContentSHA256, gotSHA) {
		fmt.Fprintf(stderr, "pdw call: sha256 mismatch (expected %s, got %s)\n", env.ContentSHA256, gotSHA)
		return 1
	}

	if dir := filepath.Dir(destPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintf(stderr, "pdw call: create directory %s: %v\n", dir, err)
			return 1
		}
	}

	outPath := destPath
	outContentType := env.ContentType
	convertedFrom := ""
	outBytes := data
	ext := strings.ToLower(filepath.Ext(destPath))
	srcMIME, srcExt := detectSourceImageFormat(env.ContentType, env.Filename)
	_, targetMIME, wantsConvert := targetImageFormat(ext)
	if srcMIME != "" && wantsConvert && srcMIME != targetMIME {
		converted, convMIME, err := convertImageFn(data, srcMIME, srcExt, ext)
		if errors.Is(err, errNoImageConverter) {
			outPath = replaceExt(destPath, srcExt)
			outContentType = srcMIME
			fmt.Fprintf(stderr, "pdw call: warning: no converter on PATH for %s -> %s; saved original bytes to %s (install ImageMagick `magick` or `ffmpeg`, or use preview_storage_file_id)\n", srcMIME, ext, outPath)
		} else if err != nil {
			fmt.Fprintf(stderr, "pdw call: %s conversion to %s failed: %v\n", srcMIME, ext, err)
			return 1
		} else {
			convertedFrom = srcMIME
			outContentType = convMIME
			outBytes = converted
		}
	}

	if err := os.WriteFile(outPath, outBytes, 0o644); err != nil {
		fmt.Fprintf(stderr, "pdw call: write %s: %v\n", outPath, err)
		return 1
	}

	summary := saveObjectSummary{
		Saved:         outPath,
		StorageFileID: env.StorageFileID,
		ContentType:   outContentType,
		ConvertedFrom: convertedFrom,
		SizeBytes:     int64(len(outBytes)),
		SourceSHA256:  gotSHA,
	}
	encoded, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Fprintln(stdout, string(encoded))
	return 0
}

func detectSourceImageFormat(contentType, filename string) (mime, ext string) {
	ct := strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	switch ct {
	case "image/heic":
		return "image/heic", ".heic"
	case "image/heif":
		return "image/heif", ".heif"
	case "image/avif":
		return "image/avif", ".avif"
	case "image/webp":
		return "image/webp", ".webp"
	case "image/png":
		return "image/png", ".png"
	case "image/jpeg", "image/jpg":
		return "image/jpeg", ".jpg"
	}
	switch strings.ToLower(filepath.Ext(filename)) {
	case ".heic":
		return "image/heic", ".heic"
	case ".heif":
		return "image/heif", ".heif"
	case ".avif":
		return "image/avif", ".avif"
	case ".webp":
		return "image/webp", ".webp"
	case ".png":
		return "image/png", ".png"
	case ".jpg", ".jpeg":
		return "image/jpeg", ".jpg"
	}
	return "", ""
}

func targetImageFormat(ext string) (format, mime string, ok bool) {
	switch ext {
	case ".png":
		return "png", "image/png", true
	case ".jpg", ".jpeg":
		return "jpeg", "image/jpeg", true
	default:
		return "", "", false
	}
}

func replaceExt(path, newExt string) string {
	old := filepath.Ext(path)
	if old == "" {
		return path + newExt
	}
	return strings.TrimSuffix(path, old) + newExt
}

func convertImageToTarget(srcBytes []byte, srcMIME, srcExt, ext string) ([]byte, string, error) {
	targetFormat, targetMIME, ok := targetImageFormat(ext)
	if !ok {
		return nil, "", fmt.Errorf("unsupported target extension %q", ext)
	}
	// Pure-Go path for PNG <-> JPEG so no external binary is needed.
	if srcMIME == "image/png" || srcMIME == "image/jpeg" {
		img, _, err := image.Decode(bytes.NewReader(srcBytes))
		if err != nil {
			return nil, "", fmt.Errorf("decode %s: %w", srcMIME, err)
		}
		var buf bytes.Buffer
		if targetFormat == "png" {
			if err := png.Encode(&buf, img); err != nil {
				return nil, "", err
			}
		} else {
			if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 90}); err != nil {
				return nil, "", err
			}
		}
		return buf.Bytes(), targetMIME, nil
	}

	dir, err := os.MkdirTemp("", "pdw-img-convert-")
	if err != nil {
		return nil, "", err
	}
	defer os.RemoveAll(dir)
	inPath := filepath.Join(dir, "in"+srcExt)
	outPath := filepath.Join(dir, "out."+targetFormat)
	if err := os.WriteFile(inPath, srcBytes, 0o600); err != nil {
		return nil, "", err
	}

	if runtime.GOOS == "darwin" && (srcMIME == "image/heic" || srcMIME == "image/heif" || srcMIME == "image/avif") {
		if sips, err := exec.LookPath("sips"); err == nil {
			if out, runErr := exec.Command(sips, "-s", "format", targetFormat, inPath, "--out", outPath).CombinedOutput(); runErr != nil {
				return nil, "", fmt.Errorf("sips: %w: %s", runErr, bytes.TrimSpace(out))
			}
			converted, readErr := os.ReadFile(outPath)
			return converted, targetMIME, readErr
		}
	}
	if magick, err := exec.LookPath("magick"); err == nil {
		if out, runErr := exec.Command(magick, inPath, outPath).CombinedOutput(); runErr != nil {
			return nil, "", fmt.Errorf("magick: %w: %s", runErr, bytes.TrimSpace(out))
		}
		converted, readErr := os.ReadFile(outPath)
		return converted, targetMIME, readErr
	}
	if (srcMIME == "image/heic" || srcMIME == "image/heif") {
		if convert, err := exec.LookPath("heif-convert"); err == nil {
			if out, runErr := exec.Command(convert, inPath, outPath).CombinedOutput(); runErr != nil {
				return nil, "", fmt.Errorf("heif-convert: %w: %s", runErr, bytes.TrimSpace(out))
			}
			converted, readErr := os.ReadFile(outPath)
			return converted, targetMIME, readErr
		}
	}
	if srcMIME == "image/webp" && targetFormat == "png" {
		if dwebp, err := exec.LookPath("dwebp"); err == nil {
			if out, runErr := exec.Command(dwebp, inPath, "-o", outPath).CombinedOutput(); runErr != nil {
				return nil, "", fmt.Errorf("dwebp: %w: %s", runErr, bytes.TrimSpace(out))
			}
			converted, readErr := os.ReadFile(outPath)
			return converted, targetMIME, readErr
		}
	}
	if srcMIME == "image/avif" {
		if avifdec, err := exec.LookPath("avifdec"); err == nil {
			if out, runErr := exec.Command(avifdec, inPath, outPath).CombinedOutput(); runErr != nil {
				return nil, "", fmt.Errorf("avifdec: %w: %s", runErr, bytes.TrimSpace(out))
			}
			converted, readErr := os.ReadFile(outPath)
			return converted, targetMIME, readErr
		}
	}
	if ffmpeg, err := exec.LookPath("ffmpeg"); err == nil {
		if out, runErr := exec.Command(ffmpeg, "-y", "-i", inPath, outPath).CombinedOutput(); runErr != nil {
			return nil, "", fmt.Errorf("ffmpeg: %w: %s", runErr, bytes.TrimSpace(out))
		}
		converted, readErr := os.ReadFile(outPath)
		return converted, targetMIME, readErr
	}
	return nil, "", fmt.Errorf("%w for %s", errNoImageConverter, srcMIME)
}
