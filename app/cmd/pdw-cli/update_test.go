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

	"github.com/zachlatta/personal-data-warehouse/app/internal/clirelease"
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

	api := newReleaseStub(t, "v0.2.0", nil, "")
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"--base-url", api.URL, "update", "--check", "--target", target},
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

	api := newReleaseStub(t, "v0.2.0", nil, "")
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"--base-url", api.URL, "update", "--check", "--target", target},
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
	api := newReleaseStub(t, "v1.0.0", newBinary, "")

	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"--base-url", api.URL, "update", "--target", target},
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

	api := newReleaseStub(t, "v1.0.0", []byte("NEW"), "")
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"--base-url", api.URL, "update", "--target", target},
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
	api := newReleaseStub(t, "v1.0.0", newBinary, "")
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"--base-url", api.URL, "update", "--force", "--target", target},
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
	api := newReleaseStub(t, "v1.0.0", []byte("NEW"), strings.Repeat("0", 64))

	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"--base-url", api.URL, "update", "--target", target},
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

// newReleaseStub stands up the warehouse app's real release proxy
// (internal/clirelease) in front of a fake GitHub serving one release, tagged
// pdw-cli/<tag>, with a tarball of binaryBody for the current OS/arch and a
// SHA256SUMS file. If overrideChecksum is non-empty, it's published as the
// tarball's checksum instead of the real one (used to simulate corruption).
// It returns the app server: the CLI never talks to GitHub itself.
func newReleaseStub(t *testing.T, tag string, binaryBody []byte, overrideChecksum string) *httptest.Server {
	t.Helper()
	if binaryBody == nil {
		binaryBody = []byte("placeholder binary\n")
	}
	fullTag := "pdw-cli/" + tag
	assetName := fmt.Sprintf("pdw-cli_%s_%s_%s.tar.gz", tag, runtime.GOOS, runtime.GOARCH)
	tarball := makeTarGz(t, "pdw-cli", binaryBody)
	sum := sha256.Sum256(tarball)
	hexSum := hex.EncodeToString(sum[:])
	if overrideChecksum != "" {
		hexSum = overrideChecksum
	}
	sumsBody := hexSum + "  " + assetName + "\n"

	github := http.NewServeMux()
	github.HandleFunc("/repos/"+clirelease.Repo+"/releases/latest", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{
			"tag_name": %q,
			"assets": [
				{"name": %q, "browser_download_url": "https://github.invalid/a", "size": %d},
				{"name": "SHA256SUMS", "browser_download_url": "https://github.invalid/s", "size": %d}
			]
		}`, fullTag, assetName, len(tarball), len(sumsBody))
	})
	download := "/" + clirelease.Repo + "/releases/download/" + fullTag + "/"
	github.HandleFunc(download+assetName, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(tarball)
	})
	github.HandleFunc(download+"SHA256SUMS", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, sumsBody)
	})
	upstream := httptest.NewServer(github)
	t.Cleanup(upstream.Close)

	appMux := http.NewServeMux()
	app := httptest.NewServer(appMux)
	t.Cleanup(app.Close)
	clirelease.Register(appMux, clirelease.New(clirelease.Options{GitHubAPI: upstream.URL, GitHubDownload: upstream.URL}), app.URL)
	return app
}

// With no --base-url, update reads the release from the same app every
// other command talks to: PDW_API_URL, then the saved config.
func TestUpdateUsesTheConfiguredWarehouseURL(t *testing.T) {
	prev := version
	t.Cleanup(func() { version = prev })
	version = "v0.0.1"

	target := filepath.Join(t.TempDir(), "pdw")
	if err := os.WriteFile(target, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	api := newReleaseStub(t, "v0.3.0", []byte("FROM ENV URL"), "")
	env := map[string]string{"HOME": t.TempDir(), "PDW_API_URL": api.URL + "/"}
	var stdout, stderr bytes.Buffer
	code := run([]string{"update", "--target", target}, strings.NewReader(""), &stdout, &stderr, func(k string) string { return env[k] })
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	if got, _ := os.ReadFile(target); string(got) != "FROM ENV URL" {
		t.Fatalf("target = %q", got)
	}
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
