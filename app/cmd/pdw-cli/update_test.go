package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestVersionPrintsBuildVersion(t *testing.T) {
	prev := version
	t.Cleanup(func() { version = prev })
	version = "v9.9.9"

	var stdout, stderr bytes.Buffer
	code := run([]string{"version"}, strings.NewReader(""), &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "v9.9.9") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestVersionDoesNotRequireEnv(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"version"}, strings.NewReader(""), &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("expected version to work without env, exit=%d stderr=%s", code, stderr.String())
	}
}

func TestUpdateCheckReportsNewerVersionWithoutWriting(t *testing.T) {
	prev := version
	t.Cleanup(func() { version = prev })
	version = "v0.0.1"

	target := filepath.Join(t.TempDir(), "pdw-cli")
	if err := os.WriteFile(target, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}

	api := newGitHubStub(t, "octo/repo", "v0.2.0", nil, "")
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"update", "--check", "--github-api", api.URL, "--repo", "octo/repo", "--target", target},
		strings.NewReader(""), &stdout, &stderr, func(string) string { return "" },
	)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "v0.2.0") {
		t.Fatalf("stdout missing latest version: %s", stdout.String())
	}
	if got, _ := os.ReadFile(target); string(got) != "OLD" {
		t.Fatalf("--check should not write; target = %q", got)
	}
}

func TestUpdateCheckReportsUpToDate(t *testing.T) {
	prev := version
	t.Cleanup(func() { version = prev })
	version = "v0.2.0"

	target := filepath.Join(t.TempDir(), "pdw-cli")
	if err := os.WriteFile(target, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}

	api := newGitHubStub(t, "octo/repo", "v0.2.0", nil, "")
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"update", "--check", "--github-api", api.URL, "--repo", "octo/repo", "--target", target},
		strings.NewReader(""), &stdout, &stderr, func(string) string { return "" },
	)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	low := strings.ToLower(stdout.String())
	if !strings.Contains(low, "up to date") && !strings.Contains(low, "already") {
		t.Fatalf("stdout should say up to date: %s", stdout.String())
	}
}

func TestUpdateReplacesBinary(t *testing.T) {
	prev := version
	t.Cleanup(func() { version = prev })
	version = "v0.0.1"

	target := filepath.Join(t.TempDir(), "pdw-cli")
	if err := os.WriteFile(target, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}

	newBinary := []byte("NEW REAL BINARY BYTES")
	api := newGitHubStub(t, "octo/repo", "v1.0.0", newBinary, "")

	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"update", "--github-api", api.URL, "--repo", "octo/repo", "--target", target},
		strings.NewReader(""), &stdout, &stderr, func(string) string { return "" },
	)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, newBinary) {
		t.Fatalf("target not updated: %q", got)
	}
	if !strings.Contains(stdout.String(), "v1.0.0") {
		t.Fatalf("stdout missing version: %s", stdout.String())
	}
}

func TestUpdateNoOpWhenAlreadyCurrent(t *testing.T) {
	prev := version
	t.Cleanup(func() { version = prev })
	version = "v1.0.0"

	target := filepath.Join(t.TempDir(), "pdw-cli")
	if err := os.WriteFile(target, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}

	api := newGitHubStub(t, "octo/repo", "v1.0.0", []byte("NEW"), "")
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"update", "--github-api", api.URL, "--repo", "octo/repo", "--target", target},
		strings.NewReader(""), &stdout, &stderr, func(string) string { return "" },
	)
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if got, _ := os.ReadFile(target); string(got) != "OLD" {
		t.Fatalf("expected no replace, got %q", got)
	}
	low := strings.ToLower(stdout.String())
	if !strings.Contains(low, "up to date") && !strings.Contains(low, "already") {
		t.Fatalf("stdout should say up to date: %s", stdout.String())
	}
}

func TestUpdateForceReplacesEvenWhenCurrent(t *testing.T) {
	prev := version
	t.Cleanup(func() { version = prev })
	version = "v1.0.0"

	target := filepath.Join(t.TempDir(), "pdw-cli")
	if err := os.WriteFile(target, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	newBinary := []byte("FORCE REINSTALLED")
	api := newGitHubStub(t, "octo/repo", "v1.0.0", newBinary, "")
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"update", "--force", "--github-api", api.URL, "--repo", "octo/repo", "--target", target},
		strings.NewReader(""), &stdout, &stderr, func(string) string { return "" },
	)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	got, _ := os.ReadFile(target)
	if !bytes.Equal(got, newBinary) {
		t.Fatalf("force should replace; target=%q", got)
	}
}

func TestUpdatePropagatesChecksumMismatch(t *testing.T) {
	prev := version
	t.Cleanup(func() { version = prev })
	version = "v0.0.1"

	target := filepath.Join(t.TempDir(), "pdw-cli")
	if err := os.WriteFile(target, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	api := newGitHubStub(t, "octo/repo", "v1.0.0", []byte("NEW"), strings.Repeat("0", 64))

	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"update", "--github-api", api.URL, "--repo", "octo/repo", "--target", target},
		strings.NewReader(""), &stdout, &stderr, func(string) string { return "" },
	)
	if code == 0 {
		t.Fatal("expected non-zero exit on checksum mismatch")
	}
	if !strings.Contains(strings.ToLower(stderr.String()), "checksum") {
		t.Fatalf("stderr missing checksum: %s", stderr.String())
	}
	if got, _ := os.ReadFile(target); string(got) != "OLD" {
		t.Fatalf("target must not be replaced on checksum mismatch: %q", got)
	}
}

// newGitHubStub spins up a fake GitHub API + asset host serving a release
// with the given tag containing a tarball of binaryBody for the current OS/arch
// and a SHA256SUMS file. If overrideChecksum is non-empty, it's published as
// the tarball's checksum instead of the real one (used to simulate corruption).
func newGitHubStub(t *testing.T, repo, tag string, binaryBody []byte, overrideChecksum string) *httptest.Server {
	t.Helper()
	if binaryBody == nil {
		binaryBody = []byte("placeholder binary\n")
	}
	assetName := fmt.Sprintf("pdw-cli_%s_%s_%s.tar.gz", tag, runtime.GOOS, runtime.GOARCH)
	tarball := makeTarGz(t, "pdw-cli", binaryBody)
	sum := sha256.Sum256(tarball)
	hexSum := hex.EncodeToString(sum[:])
	if overrideChecksum != "" {
		hexSum = overrideChecksum
	}
	sumsBody := hexSum + "  " + assetName + "\n"

	mux := http.NewServeMux()
	var srvURL string
	mux.HandleFunc("/repos/"+repo+"/releases/latest", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{
			"tag_name": %q,
			"assets": [
				{"name": %q, "browser_download_url": %q, "size": %d},
				{"name": "SHA256SUMS", "browser_download_url": %q, "size": %d}
			]
		}`, tag, assetName, srvURL+"/dl/"+assetName, len(tarball), srvURL+"/dl/SHA256SUMS", len(sumsBody))
	})
	mux.HandleFunc("/dl/"+assetName, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(tarball)
	})
	mux.HandleFunc("/dl/SHA256SUMS", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, sumsBody)
	})
	srv := httptest.NewServer(mux)
	srvURL = srv.URL
	t.Cleanup(srv.Close)
	return srv
}

func makeTarGz(t *testing.T, name string, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
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
