package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveGetObjectDownloadsVerifiesAndConvertsHEIC(t *testing.T) {
	heicBytes := []byte("fake-heic-payload")
	sum := sha256.Sum256(heicBytes)
	wantSHA := hex.EncodeToString(sum[:])
	pngBytes := []byte("\x89PNG\r\n\x1a\nconverted")

	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "image/heic")
		_, _ = w.Write(heicBytes)
	}))
	defer srv.Close()

	origConvert := convertImageFn
	var convertedExt, convertedMIME string
	convertImageFn = func(data []byte, srcMIME, srcExt, ext string) ([]byte, string, error) {
		if !bytes.Equal(data, heicBytes) {
			t.Fatalf("converter saw wrong bytes")
		}
		convertedMIME = srcMIME
		convertedExt = ext
		return pngBytes, "image/png", nil
	}
	t.Cleanup(func() { convertImageFn = origConvert })

	env, _ := json.Marshal(getObjectEnvelope{
		StorageFileID: "fid-heic",
		Exists:        true,
		ContentType:   "image/heic",
		SizeBytes:     int64(len(heicBytes)),
		ContentSHA256: wantSHA,
		Filename:      "frame.heic",
		DownloadURL:   srv.URL + "/objects/fid-heic",
	})
	dest := filepath.Join(t.TempDir(), "out", "frame.png")
	var stdout, stderr bytes.Buffer
	code := saveGetObjectResponse(env, dest, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, stderr.String())
	}
	if !strings.HasPrefix(gotUA, "pdw-cli/") {
		t.Fatalf("expected descriptive User-Agent, got %q", gotUA)
	}
	if convertedMIME != "image/heic" || convertedExt != ".png" {
		t.Fatalf("expected image/heic -> .png conversion, got %q -> %q", convertedMIME, convertedExt)
	}
	gotBytes, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read saved file: %v", err)
	}
	if !bytes.Equal(gotBytes, pngBytes) {
		t.Fatalf("saved bytes mismatch")
	}
	var summary saveObjectSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}
	if summary.ConvertedFrom != "image/heic" || summary.ContentType != "image/png" || summary.SourceSHA256 != wantSHA {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func TestSaveGetObjectConvertsAVIFAndFallsBackToRealExtWhenNoConverter(t *testing.T) {
	avifBytes := []byte("fake-avif-payload")
	sum := sha256.Sum256(avifBytes)
	wantSHA := hex.EncodeToString(sum[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/avif")
		_, _ = w.Write(avifBytes)
	}))
	defer srv.Close()

	origConvert := convertImageFn
	convertImageFn = func(data []byte, srcMIME, srcExt, ext string) ([]byte, string, error) {
		return nil, "", fmt.Errorf("%w for %s", errNoImageConverter, srcMIME)
	}
	t.Cleanup(func() { convertImageFn = origConvert })

	env, _ := json.Marshal(getObjectEnvelope{
		StorageFileID: "fid-avif",
		Exists:        true,
		ContentType:   "image/avif",
		SizeBytes:     int64(len(avifBytes)),
		ContentSHA256: wantSHA,
		Filename:      "frame.avif",
		DownloadURL:   srv.URL + "/objects/fid-avif",
	})
	dir := t.TempDir()
	dest := filepath.Join(dir, "frame.png")
	var stdout, stderr bytes.Buffer
	code := saveGetObjectResponse(env, dest, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit 0 fallback, got %d: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "saved original bytes") {
		t.Fatalf("expected fallback warning on stderr, got %q", stderr.String())
	}
	fallbackPath := filepath.Join(dir, "frame.avif")
	gotBytes, err := os.ReadFile(fallbackPath)
	if err != nil {
		t.Fatalf("expected fallback file at %s: %v", fallbackPath, err)
	}
	if !bytes.Equal(gotBytes, avifBytes) {
		t.Fatalf("fallback bytes mismatch")
	}
	var summary saveObjectSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}
	if summary.Saved != fallbackPath || summary.ContentType != "image/avif" || summary.ConvertedFrom != "" {
		t.Fatalf("unexpected fallback summary: %+v", summary)
	}
}

func TestSaveGetObjectConvertsPNGToJPEGInPureGo(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 16, 16))
	img.Set(0, 0, color.RGBA{R: 200, G: 100, B: 50, A: 255})
	var pngBuf bytes.Buffer
	if err := png.Encode(&pngBuf, img); err != nil {
		t.Fatal(err)
	}
	pngBytes := pngBuf.Bytes()
	sum := sha256.Sum256(pngBytes)
	wantSHA := hex.EncodeToString(sum[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes)
	}))
	defer srv.Close()

	env, _ := json.Marshal(getObjectEnvelope{
		StorageFileID: "fid-png",
		Exists:        true,
		ContentType:   "image/png",
		SizeBytes:     int64(len(pngBytes)),
		ContentSHA256: wantSHA,
		Filename:      "shot.png",
		DownloadURL:   srv.URL + "/objects/fid-png",
	})
	dest := filepath.Join(t.TempDir(), "shot.jpg")
	var stdout, stderr bytes.Buffer
	code := saveGetObjectResponse(env, dest, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, stderr.String())
	}
	saved, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jpeg.DecodeConfig(bytes.NewReader(saved)); err != nil {
		t.Fatalf("saved file is not valid JPEG: %v", err)
	}
}

func TestSaveGetObjectRejectsSHA256Mismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("corrupted"))
	}))
	defer srv.Close()

	env, _ := json.Marshal(getObjectEnvelope{
		StorageFileID: "fid-bad",
		Exists:        true,
		ContentType:   "image/png",
		ContentSHA256: "0000000000000000000000000000000000000000000000000000000000000000",
		DownloadURL:   srv.URL,
	})
	dest := filepath.Join(t.TempDir(), "bad.png")
	var stdout, stderr bytes.Buffer
	code := saveGetObjectResponse(env, dest, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "sha256 mismatch") {
		t.Fatalf("expected sha256 mismatch error, got code=%d stderr=%q", code, stderr.String())
	}
}
