// Package selfupdate fetches pdw-cli release artifacts from GitHub Releases,
// verifies them against the published SHA256SUMS, and atomically replaces the
// running binary. The release artifact layout it expects:
//
//   pdw-cli_<version>_<goos>_<goarch>.tar.gz   (contains a single "pdw-cli" file)
//   SHA256SUMS                                 (lines of "<hex>  <filename>")
//
// Version strings use the GitHub tag form (e.g. "v1.2.3"). The package is
// platform-agnostic; the calling command supplies runtime.GOOS / runtime.GOARCH.
package selfupdate

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// DefaultGitHubAPI is the public GitHub REST API root.
const DefaultGitHubAPI = "https://api.github.com"

const (
	binaryName     = "pdw-cli"
	checksumFile   = "SHA256SUMS"
	assetExtension = ".tar.gz"
)

// Release is the slimmed-down view of a GitHub release.
type Release struct {
	Version string
	Assets  []Asset
}

// Asset is one downloadable release artifact.
type Asset struct {
	Name        string
	DownloadURL string
	Size        int64
}

// Client talks to GitHub Releases.
type Client struct {
	repo    string
	baseURL string
	http    *http.Client
	ua      string
}

// NewClient returns a Client for the given "<owner>/<repo>".
func NewClient(repo string) *Client {
	return &Client{
		repo:    repo,
		baseURL: DefaultGitHubAPI,
		http:    &http.Client{Timeout: 60 * time.Second},
		ua:      binaryName + "/selfupdate",
	}
}

// SetBaseURL overrides the GitHub API root. Used by tests.
func (c *Client) SetBaseURL(u string) { c.baseURL = strings.TrimRight(u, "/") }

// SetHTTPClient overrides the underlying *http.Client.
func (c *Client) SetHTTPClient(h *http.Client) {
	if h != nil {
		c.http = h
	}
}

// SetUserAgent overrides the User-Agent header.
func (c *Client) SetUserAgent(ua string) {
	if ua != "" {
		c.ua = ua
	}
}

// LatestRelease fetches /repos/<repo>/releases/latest.
func (c *Client) LatestRelease(ctx context.Context) (Release, error) {
	url := c.baseURL + "/repos/" + c.repo + "/releases/latest"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Release{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", c.ua)
	resp, err := c.http.Do(req)
	if err != nil {
		return Release{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Release{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Release{}, fmt.Errorf("github releases: http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name string `json:"name"`
			URL  string `json:"browser_download_url"`
			Size int64  `json:"size"`
		} `json:"assets"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return Release{}, fmt.Errorf("decode release: %w", err)
	}
	rel := Release{Version: payload.TagName}
	for _, a := range payload.Assets {
		rel.Assets = append(rel.Assets, Asset{Name: a.Name, DownloadURL: a.URL, Size: a.Size})
	}
	return rel, nil
}

// AssetName returns the conventional artifact name for a platform.
func AssetName(version, goos, goarch string) string {
	return binaryName + "_" + version + "_" + goos + "_" + goarch + assetExtension
}

// AssetFor returns the artifact matching the given goos/goarch suffix.
func (r Release) AssetFor(goos, goarch string) (Asset, bool) {
	suffix := "_" + goos + "_" + goarch + assetExtension
	for _, a := range r.Assets {
		if strings.HasSuffix(a.Name, suffix) {
			return a, true
		}
	}
	return Asset{}, false
}

// ChecksumAsset returns the SHA256SUMS asset.
func (r Release) ChecksumAsset() (Asset, bool) {
	for _, a := range r.Assets {
		if a.Name == checksumFile {
			return a, true
		}
	}
	return Asset{}, false
}

// FetchBinary downloads the platform tarball, verifies it against
// SHA256SUMS, extracts the single pdw-cli entry, and returns the bytes.
func (c *Client) FetchBinary(ctx context.Context, rel Release, goos, goarch string) ([]byte, error) {
	platformAsset, ok := rel.AssetFor(goos, goarch)
	if !ok {
		return nil, fmt.Errorf("no asset for %s/%s in release %s", goos, goarch, rel.Version)
	}
	sumsAsset, ok := rel.ChecksumAsset()
	if !ok {
		return nil, fmt.Errorf("release %s has no SHA256SUMS asset (checksum required)", rel.Version)
	}

	tarball, err := c.downloadBytes(ctx, platformAsset.DownloadURL)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", platformAsset.Name, err)
	}
	sums, err := c.downloadBytes(ctx, sumsAsset.DownloadURL)
	if err != nil {
		return nil, fmt.Errorf("download SHA256SUMS: %w", err)
	}

	wantHex, ok := lookupChecksum(string(sums), platformAsset.Name)
	if !ok {
		return nil, fmt.Errorf("checksum for %s not found in SHA256SUMS", platformAsset.Name)
	}
	got := sha256.Sum256(tarball)
	if !strings.EqualFold(hex.EncodeToString(got[:]), wantHex) {
		return nil, fmt.Errorf("checksum mismatch for %s: got %s want %s", platformAsset.Name, hex.EncodeToString(got[:]), wantHex)
	}

	bin, err := extractBinary(tarball)
	if err != nil {
		return nil, fmt.Errorf("extract %s: %w", binaryName, err)
	}
	return bin, nil
}

func (c *Client) downloadBytes(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.ua)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func lookupChecksum(sums, name string) (string, bool) {
	for _, line := range strings.Split(sums, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		// SHA256SUMS lines: "<hex>  <filename>" (or "<hex> *<filename>" with binary marker).
		fname := strings.TrimPrefix(fields[len(fields)-1], "*")
		if fname == name {
			return fields[0], true
		}
	}
	return "", false
}

func extractBinary(tgz []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(tgz))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("tarball did not contain %s", binaryName)
		}
		if err != nil {
			return nil, err
		}
		base := filepath.Base(hdr.Name)
		if base == binaryName || base == binaryName+".exe" {
			return io.ReadAll(tr)
		}
	}
}

// ApplyUpdate writes newBinary next to targetPath, sets it executable, then
// atomically renames it into place. The os.Rename(2) call replaces the file
// even while the current process is running it (POSIX keeps the open inode
// alive until the running process exits).
func ApplyUpdate(targetPath string, newBinary []byte) error {
	if strings.TrimSpace(targetPath) == "" {
		return fmt.Errorf("target path is required")
	}
	if len(newBinary) == 0 {
		return fmt.Errorf("new binary is empty")
	}
	dir := filepath.Dir(targetPath)
	tmp, err := os.CreateTemp(dir, ".pdw-cli.update.*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }
	if _, err := tmp.Write(newBinary); err != nil {
		tmp.Close()
		cleanup()
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Chmod(0o755); err != nil {
		tmp.Close()
		cleanup()
		return fmt.Errorf("chmod temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close temp: %w", err)
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		cleanup()
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, targetPath, err)
	}
	return nil
}

// ShouldUpdate reports whether `latest` is newer than `current`. Both strings
// may have a leading "v". An empty or non-semver `current` (e.g. "dev")
// always returns true so users on local builds can pull a real release.
// A non-semver `latest` is an error so we never overwrite with garbage.
func ShouldUpdate(current, latest string) (bool, error) {
	latestParts, err := parseSemver(latest)
	if err != nil {
		return false, fmt.Errorf("latest version %q: %w", latest, err)
	}
	currentParts, err := parseSemver(current)
	if err != nil {
		return true, nil
	}
	for i := 0; i < 3; i++ {
		if latestParts[i] > currentParts[i] {
			return true, nil
		}
		if latestParts[i] < currentParts[i] {
			return false, nil
		}
	}
	return false, nil
}

func parseSemver(v string) ([3]int, error) {
	var out [3]int
	v = strings.TrimSpace(v)
	// Releases are tagged "<series>/vX.Y.Z" in monorepos (e.g.
	// "pdw-cli/v0.1.0") to keep tag namespaces from colliding. Strip
	// anything up to and including the last "/".
	if i := strings.LastIndex(v, "/"); i >= 0 {
		v = v[i+1:]
	}
	v = strings.TrimPrefix(v, "v")
	if v == "" {
		return out, fmt.Errorf("empty")
	}
	// strip any pre-release / build metadata
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return out, fmt.Errorf("not semver (%q)", v)
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return out, fmt.Errorf("not semver: %w", err)
		}
		if n < 0 {
			return out, fmt.Errorf("negative component")
		}
		out[i] = n
	}
	return out, nil
}

// CurrentArch returns runtime.GOARCH; broken out for tests / overrides.
func CurrentArch() string { return runtime.GOARCH }

// CurrentOS returns runtime.GOOS; broken out for tests / overrides.
func CurrentOS() string { return runtime.GOOS }
