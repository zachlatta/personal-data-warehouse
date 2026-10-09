package clirelease

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testTag = "pdw-cli/v0.0.900-sha.abc1234"

// fakeGitHub stands in for api.github.com and github.com: one release whose
// assets are a real tarball per platform plus a matching SHA256SUMS.
type fakeGitHub struct {
	srv        *httptest.Server
	apiCalls   atomic.Int32
	assets     map[string][]byte
	failAPI    atomic.Bool
	extraAsset string
}

func newFakeGitHub(t *testing.T, binary []byte) *fakeGitHub {
	t.Helper()
	f := &fakeGitHub{assets: map[string][]byte{}}
	version := strings.TrimPrefix(testTag, "pdw-cli/")
	var sums strings.Builder
	for _, p := range []string{"linux_amd64", "linux_arm64", "darwin_amd64", "darwin_arm64"} {
		name := "pdw-cli_" + version + "_" + p + ".tar.gz"
		f.assets[name] = tarball(t, binary)
		sum := sha256.Sum256(f.assets[name])
		fmt.Fprintf(&sums, "%s  %s\n", hex.EncodeToString(sum[:]), name)
	}
	f.assets["SHA256SUMS"] = []byte(sums.String())
	f.extraAsset = "notes.txt"
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/repos/"+Repo+"/releases/latest":
			f.apiCalls.Add(1)
			if r.Header.Get("User-Agent") == "" {
				http.Error(w, "user agent required", http.StatusForbidden)
				return
			}
			if f.failAPI.Load() {
				http.Error(w, "rate limited", http.StatusForbidden)
				return
			}
			type asset struct {
				Name string `json:"name"`
				URL  string `json:"browser_download_url"`
				Size int    `json:"size"`
			}
			var assets []asset
			for name, body := range f.assets {
				assets = append(assets, asset{Name: name, URL: "https://github.com/x/" + name, Size: len(body)})
			}
			assets = append(assets, asset{Name: f.extraAsset, URL: "https://github.com/x/notes", Size: 1})
			_ = json.NewEncoder(w).Encode(map[string]any{"tag_name": testTag, "assets": assets})
		case strings.HasPrefix(r.URL.Path, "/"+Repo+"/releases/download/"+testTag+"/"):
			name := strings.TrimPrefix(r.URL.Path, "/"+Repo+"/releases/download/"+testTag+"/")
			body, ok := f.assets[name]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func tarball(t *testing.T, binary []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "pdw-cli", Mode: 0o755, Size: int64(len(binary))}); err != nil {
		t.Fatal(err)
	}
	_, _ = tw.Write(binary)
	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes()
}

type fixture struct {
	gh    *fakeGitHub
	app   *httptest.Server
	clock *time.Time
}

func newFixture(t *testing.T, binary []byte) *fixture {
	t.Helper()
	gh := newFakeGitHub(t, binary)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	fx := &fixture{gh: gh, clock: &now}
	mux := http.NewServeMux()
	proxy := New(Options{
		GitHubAPI:      gh.srv.URL,
		GitHubDownload: gh.srv.URL,
		Now:            func() time.Time { return *fx.clock },
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	app := httptest.NewServer(mux)
	t.Cleanup(app.Close)
	Register(mux, proxy, app.URL)
	fx.app = app
	return fx
}

func get(t *testing.T, url string) (int, http.Header, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, body
}

func TestLatestReleaseIsServedWithDownloadURLsOnThisServer(t *testing.T) {
	fx := newFixture(t, []byte("BIN"))
	code, _, body := get(t, fx.app.URL+LatestPath)
	if code != http.StatusOK {
		t.Fatalf("latest: %d %s", code, body)
	}
	var rel struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name string `json:"name"`
			URL  string `json:"browser_download_url"`
			Size int64  `json:"size"`
		} `json:"assets"`
	}
	if err := json.Unmarshal(body, &rel); err != nil {
		t.Fatal(err)
	}
	if rel.TagName != testTag {
		t.Fatalf("tag = %q", rel.TagName)
	}
	if len(rel.Assets) != 5 {
		t.Fatalf("want 4 tarballs + SHA256SUMS, got %+v", rel.Assets)
	}
	for _, a := range rel.Assets {
		if a.Name == fx.gh.extraAsset {
			t.Fatalf("an asset outside the allow-list was advertised: %+v", a)
		}
		if want := fx.app.URL + DownloadPrefix + testTag + "/" + a.Name; a.URL != want {
			t.Fatalf("download url = %q, want %q", a.URL, want)
		}
		if a.Size <= 0 {
			t.Fatalf("size missing for %s", a.Name)
		}
	}
	// The JSON must be greppable by install.sh's sed, which reads "tag_name":"...".
	if !strings.Contains(string(body), `"tag_name":"`+testTag+`"`) {
		t.Fatalf("tag_name not on one line: %s", body)
	}
}

func TestLatestReleaseIsCachedAndServedStaleWhenGitHubRefuses(t *testing.T) {
	fx := newFixture(t, []byte("BIN"))
	for i := 0; i < 3; i++ {
		if code, _, body := get(t, fx.app.URL+LatestPath); code != http.StatusOK {
			t.Fatalf("latest: %d %s", code, body)
		}
	}
	if n := fx.gh.apiCalls.Load(); n != 1 {
		t.Fatalf("GitHub API calls = %d, want 1 inside the cache window", n)
	}
	*fx.clock = fx.clock.Add(latestTTL + time.Second)
	fx.gh.failAPI.Store(true)
	code, _, body := get(t, fx.app.URL+LatestPath)
	if code != http.StatusOK || !strings.Contains(string(body), testTag) {
		t.Fatalf("stale fallback: %d %s", code, body)
	}
	if n := fx.gh.apiCalls.Load(); n != 2 {
		t.Fatalf("GitHub API calls = %d, want a refresh attempt after the window", n)
	}
}

func TestLatestReleaseFailsLoudWithNothingCached(t *testing.T) {
	fx := newFixture(t, []byte("BIN"))
	fx.gh.failAPI.Store(true)
	code, _, body := get(t, fx.app.URL+LatestPath)
	if code != http.StatusBadGateway {
		t.Fatalf("latest with GitHub down: %d %s", code, body)
	}
}

func TestAssetsAreStreamedFromTheGitHubRelease(t *testing.T) {
	fx := newFixture(t, []byte("BIN"))
	for name, want := range fx.gh.assets {
		code, header, body := get(t, fx.app.URL+DownloadPrefix+testTag+"/"+name)
		if code != http.StatusOK {
			t.Fatalf("%s: %d %s", name, code, body)
		}
		if !bytes.Equal(body, want) {
			t.Fatalf("%s: body differs", name)
		}
		if !strings.Contains(header.Get("Cache-Control"), "immutable") {
			t.Fatalf("%s: cache-control %q", name, header.Get("Cache-Control"))
		}
	}
}

func TestTheProxyIsNotOpen(t *testing.T) {
	fx := newFixture(t, []byte("BIN"))
	version := strings.TrimPrefix(testTag, "pdw-cli/")
	for _, path := range []string{
		DownloadPrefix + testTag + "/notes.txt",
		DownloadPrefix + testTag + "/pdw-cli_" + version + "_windows_amd64.tar.gz",
		DownloadPrefix + testTag + "/pdw-cli_v0.0.1_linux_amd64.tar.gz", // version disagrees with tag
		DownloadPrefix + "other/v1.0.0/SHA256SUMS",
		DownloadPrefix + "pdw-cli/v1.0.0/../../evil/SHA256SUMS",
		DownloadPrefix + "SHA256SUMS",
	} {
		code, _, _ := get(t, fx.app.URL+path)
		if code != http.StatusNotFound {
			t.Fatalf("%s: %d, want 404", path, code)
		}
	}
}

// The installer an agent pipes to sh, run for real against the proxy: it
// must install a checksum-verified binary while talking only to this server.
func TestTheServedInstallerInstallsAVerifiedBinaryFromThisServerOnly(t *testing.T) {
	for _, tool := range []string{"sh", "curl", "tar"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("installer supports linux and darwin")
	}
	binary := []byte("#!/bin/sh\necho pdw-from-proxy\n")
	fx := newFixture(t, binary)

	code, _, script := get(t, fx.app.URL+InstallScriptPath)
	if code != http.StatusOK {
		t.Fatalf("install.sh: %d %s", code, script)
	}
	for _, banned := range []string{"github.com", "githubusercontent.com"} {
		if strings.Contains(string(script), banned) {
			t.Fatalf("installer still names %s", banned)
		}
	}
	if !strings.Contains(string(script), "'"+fx.app.URL+"'") {
		t.Fatalf("installer does not carry this server's URL")
	}

	home := t.TempDir()
	dir := filepath.Join(home, "bin")
	cmd := exec.Command("sh")
	cmd.Stdin = bytes.NewReader(script)
	cmd.Env = []string{"HOME=" + home, "PATH=" + os.Getenv("PATH"), "SHELL=/bin/sh", "PDW_INSTALL_DIR=" + dir}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("installer failed: %v\n%s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(dir, "pdw"))
	if err != nil || !bytes.Equal(got, binary) {
		t.Fatalf("installed binary = %q, %v\n%s", got, err, out)
	}

	// A tampered SHA256SUMS fails the install.
	fx.gh.assets["SHA256SUMS"] = bytes.Repeat([]byte("0"), 64)
	fx.gh.assets["SHA256SUMS"] = append(fx.gh.assets["SHA256SUMS"], []byte("  "+AssetName(strings.TrimPrefix(testTag, "pdw-cli/"), runtime.GOOS, runtime.GOARCH)+"\n")...)
	dir2 := filepath.Join(home, "bin2")
	cmd = exec.Command("sh")
	cmd.Stdin = bytes.NewReader(script)
	cmd.Env = []string{"HOME=" + home, "PATH=" + os.Getenv("PATH"), "SHELL=/bin/sh", "PDW_INSTALL_DIR=" + dir2}
	out, err = cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "checksum mismatch") {
		t.Fatalf("tampered checksum accepted: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(dir2, "pdw")); err == nil {
		t.Fatal("a binary was installed despite the checksum mismatch")
	}
}

func TestThePinnedVersionInstallSkipsTheMetadataCall(t *testing.T) {
	for _, tool := range []string{"sh", "curl", "tar"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}
	binary := []byte("pinned")
	fx := newFixture(t, binary)
	_, _, script := get(t, fx.app.URL+InstallScriptPath)
	home := t.TempDir()
	cmd := exec.Command("sh")
	cmd.Stdin = bytes.NewReader(script)
	cmd.Env = []string{"HOME=" + home, "PATH=" + os.Getenv("PATH"), "SHELL=/bin/sh", "PDW_INSTALL_DIR=" + home, "PDW_VERSION=" + testTag}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("pinned install failed: %v\n%s", err, out)
	}
	if n := fx.gh.apiCalls.Load(); n != 0 {
		t.Fatalf("pinned install called the release API %d times", n)
	}
}
