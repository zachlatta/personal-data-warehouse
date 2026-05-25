package selfupdate_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/selfupdate"
)

// --- version compare -------------------------------------------------------

func TestShouldUpdate(t *testing.T) {
	cases := []struct {
		current, latest string
		want            bool
		wantErr         bool
	}{
		{"v0.1.0", "v0.2.0", true, false},
		{"v0.2.0", "v0.2.0", false, false},
		{"v0.2.1", "v0.2.0", false, false},
		{"0.1.0", "v0.2.0", true, false}, // tolerate missing v
		{"v0.1.0", "v0.1.10", true, false},
		{"v1.0.0", "v0.99.99", false, false},
		// Releases are tagged "pdw-cli/v0.1.0" in this repo so they don't
		// collide with other release series; ShouldUpdate must tolerate that.
		{"v0.1.0", "pdw-cli/v0.2.0", true, false},
		{"pdw-cli/v0.2.0", "pdw-cli/v0.2.0", false, false},
		// dev/unset current always wants update so users on local builds can
		// pull a real release.
		{"dev", "v0.1.0", true, false},
		{"", "v0.1.0", true, false},
		// Garbage latest is an error so we never overwrite with nothing.
		{"v0.1.0", "not-a-version", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.current+"->"+tc.latest, func(t *testing.T) {
			got, err := selfupdate.ShouldUpdate(tc.current, tc.latest)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (got=%v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("ShouldUpdate(%q,%q) = %v, want %v", tc.current, tc.latest, got, tc.want)
			}
		})
	}
}

// --- LatestRelease ---------------------------------------------------------

func TestLatestReleaseParsesGitHubResponse(t *testing.T) {
	var gotPath string
	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotUA = r.Header.Get("User-Agent")
		_, _ = io.WriteString(w, `{
			"tag_name": "v1.2.3",
			"assets": [
				{"name":"pdw-cli_v1.2.3_darwin_arm64.tar.gz","browser_download_url":"https://example/a.tar.gz","size":1234},
				{"name":"SHA256SUMS","browser_download_url":"https://example/SHA256SUMS","size":64}
			]
		}`)
	}))
	defer srv.Close()

	c := selfupdate.NewClient("octo/repo")
	c.SetBaseURL(srv.URL)
	rel, err := c.LatestRelease(context.Background())
	if err != nil {
		t.Fatalf("LatestRelease: %v", err)
	}
	if gotPath != "/repos/octo/repo/releases/latest" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotUA == "" {
		t.Fatal("User-Agent must be set (GitHub requires it)")
	}
	if rel.Version != "v1.2.3" {
		t.Fatalf("version = %q", rel.Version)
	}
	if len(rel.Assets) != 2 {
		t.Fatalf("assets = %d", len(rel.Assets))
	}
}

func TestLatestReleaseSurfacesGitHubErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"message":"Not Found"}`)
	}))
	defer srv.Close()
	c := selfupdate.NewClient("octo/repo")
	c.SetBaseURL(srv.URL)
	_, err := c.LatestRelease(context.Background())
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("err = %v", err)
	}
}

// --- AssetFor --------------------------------------------------------------

func TestReleaseAssetForPicksMatchingPlatform(t *testing.T) {
	rel := selfupdate.Release{
		Version: "v1.0.0",
		Assets: []selfupdate.Asset{
			{Name: "pdw-cli_v1.0.0_linux_amd64.tar.gz", DownloadURL: "https://x/linux-amd64"},
			{Name: "pdw-cli_v1.0.0_linux_arm64.tar.gz", DownloadURL: "https://x/linux-arm64"},
			{Name: "pdw-cli_v1.0.0_darwin_amd64.tar.gz", DownloadURL: "https://x/darwin-amd64"},
			{Name: "pdw-cli_v1.0.0_darwin_arm64.tar.gz", DownloadURL: "https://x/darwin-arm64"},
			{Name: "SHA256SUMS", DownloadURL: "https://x/sums"},
		},
	}
	cases := []struct {
		goos, goarch, want string
	}{
		{"linux", "amd64", "https://x/linux-amd64"},
		{"linux", "arm64", "https://x/linux-arm64"},
		{"darwin", "amd64", "https://x/darwin-amd64"},
		{"darwin", "arm64", "https://x/darwin-arm64"},
	}
	for _, tc := range cases {
		t.Run(tc.goos+"_"+tc.goarch, func(t *testing.T) {
			a, ok := rel.AssetFor(tc.goos, tc.goarch)
			if !ok {
				t.Fatalf("no asset for %s/%s", tc.goos, tc.goarch)
			}
			if a.DownloadURL != tc.want {
				t.Fatalf("download url = %q, want %q", a.DownloadURL, tc.want)
			}
		})
	}
}

func TestReleaseAssetForUnknownPlatformReturnsFalse(t *testing.T) {
	rel := selfupdate.Release{Assets: []selfupdate.Asset{{Name: "pdw-cli_v1.0.0_linux_amd64.tar.gz"}}}
	if _, ok := rel.AssetFor("windows", "386"); ok {
		t.Fatal("expected no asset for windows/386")
	}
}

func TestReleaseChecksumAsset(t *testing.T) {
	rel := selfupdate.Release{Assets: []selfupdate.Asset{
		{Name: "pdw-cli_v1.0.0_linux_amd64.tar.gz"},
		{Name: "SHA256SUMS", DownloadURL: "https://x/sums"},
	}}
	a, ok := rel.ChecksumAsset()
	if !ok || a.DownloadURL != "https://x/sums" {
		t.Fatalf("ChecksumAsset = %+v ok=%v", a, ok)
	}
}

// --- DownloadAndVerify -----------------------------------------------------

func TestDownloadAndVerifyExtractsBinaryFromTarGz(t *testing.T) {
	bin := []byte("#!/bin/echo pretend binary\nbody bytes\n")
	tgz := tarGzFile(t, "pdw-cli", bin)
	sum := sha256.Sum256(tgz)
	assetName := "pdw-cli_v1.0.0_" + runtime.GOOS + "_" + runtime.GOARCH + ".tar.gz"
	sums := fmt.Sprintf("%s  %s\nfeed face  unrelated\n", hex.EncodeToString(sum[:]), assetName)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/asset":
			_, _ = w.Write(tgz)
		case "/sums":
			_, _ = io.WriteString(w, sums)
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	rel := selfupdate.Release{
		Version: "v1.0.0",
		Assets: []selfupdate.Asset{
			{Name: assetName, DownloadURL: srv.URL + "/asset"},
			{Name: "SHA256SUMS", DownloadURL: srv.URL + "/sums"},
		},
	}
	c := selfupdate.NewClient("octo/repo")
	got, err := c.FetchBinary(context.Background(), rel, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("FetchBinary: %v", err)
	}
	if !bytes.Equal(got, bin) {
		t.Fatalf("binary mismatch:\ngot=%q\nwant=%q", got, bin)
	}
}

func TestDownloadAndVerifyFailsOnChecksumMismatch(t *testing.T) {
	bin := []byte("real bytes")
	tgz := tarGzFile(t, "pdw-cli", bin)
	assetName := "pdw-cli_v1.0.0_" + runtime.GOOS + "_" + runtime.GOARCH + ".tar.gz"
	wrong := strings.Repeat("0", 64) // 64 hex chars but wrong value
	sums := wrong + "  " + assetName + "\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/asset" {
			_, _ = w.Write(tgz)
		} else {
			_, _ = io.WriteString(w, sums)
		}
	}))
	defer srv.Close()

	rel := selfupdate.Release{
		Version: "v1.0.0",
		Assets: []selfupdate.Asset{
			{Name: assetName, DownloadURL: srv.URL + "/asset"},
			{Name: "SHA256SUMS", DownloadURL: srv.URL + "/sums"},
		},
	}
	c := selfupdate.NewClient("octo/repo")
	_, err := c.FetchBinary(context.Background(), rel, runtime.GOOS, runtime.GOARCH)
	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "checksum") {
		t.Fatalf("err = %v, want checksum error", err)
	}
}

func TestDownloadAndVerifyRequiresChecksumAsset(t *testing.T) {
	assetName := "pdw-cli_v1.0.0_" + runtime.GOOS + "_" + runtime.GOARCH + ".tar.gz"
	rel := selfupdate.Release{
		Version: "v1.0.0",
		Assets:  []selfupdate.Asset{{Name: assetName, DownloadURL: "http://x"}},
	}
	_, err := selfupdate.NewClient("octo/repo").FetchBinary(context.Background(), rel, runtime.GOOS, runtime.GOARCH)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "checksum") {
		t.Fatalf("err = %v", err)
	}
}

func TestDownloadAndVerifyRequiresPlatformAsset(t *testing.T) {
	rel := selfupdate.Release{
		Version: "v1.0.0",
		Assets:  []selfupdate.Asset{{Name: "SHA256SUMS", DownloadURL: "http://x"}},
	}
	_, err := selfupdate.NewClient("octo/repo").FetchBinary(context.Background(), rel, "linux", "amd64")
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "no asset") {
		t.Fatalf("err = %v", err)
	}
}

// --- ApplyUpdate -----------------------------------------------------------

func TestApplyUpdateReplacesFileAtomically(t *testing.T) {
	dir := t.TempDir()
	current := filepath.Join(dir, "pdw-cli")
	if err := os.WriteFile(current, []byte("OLD"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := selfupdate.ApplyUpdate(current, []byte("NEW BINARY")); err != nil {
		t.Fatalf("ApplyUpdate: %v", err)
	}
	got, err := os.ReadFile(current)
	if err != nil {
		t.Fatalf("read replaced: %v", err)
	}
	if string(got) != "NEW BINARY" {
		t.Fatalf("contents = %q", got)
	}
	st, err := os.Stat(current)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if runtime.GOOS != "windows" && st.Mode().Perm()&0o100 == 0 {
		t.Fatalf("not executable after replace: %v", st.Mode().Perm())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("leftover files: %v", entries)
	}
}

func TestApplyUpdateRefusesEmptyPath(t *testing.T) {
	if err := selfupdate.ApplyUpdate("", []byte("x")); err == nil {
		t.Fatal("expected error")
	}
}

func TestApplyUpdateRefusesEmptyBinary(t *testing.T) {
	if err := selfupdate.ApplyUpdate(filepath.Join(t.TempDir(), "pdw-cli"), nil); err == nil {
		t.Fatal("expected error")
	}
}

// --- AssetName helper ------------------------------------------------------

func TestAssetNameForFormat(t *testing.T) {
	got := selfupdate.AssetName("v1.2.3", "linux", "arm64")
	if got != "pdw-cli_v1.2.3_linux_arm64.tar.gz" {
		t.Fatalf("AssetName = %q", got)
	}
}

// --- helpers ---------------------------------------------------------------

func tarGzFile(t *testing.T, name string, contents []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(contents))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(contents); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

var _ = errors.New // keep errors import if tests evolve
