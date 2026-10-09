// Package clirelease serves the pdw CLI's installer and proxies its GitHub
// release through the app, so a host that GitHub refuses can still install
// pdw. GitHub's API answers 403 to many datacenter and AI-sandbox egress IPs,
// which is exactly where a cli_authorize bootstrap runs; the app's own egress
// is not refused. Only the release's pdw-cli tarballs and SHA256SUMS pass
// through: this is not an open proxy.
package clirelease

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Repo is the GitHub repository whose releases are proxied.
	Repo = "zachlatta/personal-data-warehouse"

	// InstallScriptPath serves install.sh with the app's URL filled in.
	InstallScriptPath = "/cli/install.sh"
	// LatestPath is the latest release in GitHub's JSON shape, with every
	// browser_download_url pointing back at this app.
	LatestPath = "/cli/release/latest"
	// DownloadPrefix + <tag>/<asset> streams one release asset.
	DownloadPrefix = "/cli/release/download/"

	defaultGitHubAPI      = "https://api.github.com"
	defaultGitHubDownload = "https://github.com"
	checksumFile          = "SHA256SUMS"
	assetPrefix           = "pdw-cli"

	// latestTTL bounds how often the app asks GitHub for the latest release.
	// Unauthenticated, the API allows 60 requests an hour per IP.
	latestTTL = 5 * time.Minute
)

//go:embed install.sh
var installScript string

const baseURLPlaceholder = "__PDW_BASE_URL__"

var (
	tagPattern   = regexp.MustCompile(`^pdw-cli/(v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.]+)?)$`)
	assetPattern = regexp.MustCompile(`^pdw-cli_(v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.]+)?)_(linux|darwin)_(amd64|arm64)\.tar\.gz$`)
)

// AssetName is the release tarball for one platform.
func AssetName(version, goos, goarch string) string {
	return assetPrefix + "_" + version + "_" + goos + "_" + goarch + ".tar.gz"
}

// allowedAsset reports whether asset belongs to the release tagged tag.
func allowedAsset(tag, asset string) bool {
	m := tagPattern.FindStringSubmatch(tag)
	if m == nil {
		return false
	}
	if asset == checksumFile {
		return true
	}
	a := assetPattern.FindStringSubmatch(asset)
	return a != nil && a[1] == m[1]
}

// Options configures a Proxy. Zero values take the real GitHub endpoints.
type Options struct {
	GitHubAPI      string
	GitHubDownload string
	HTTPClient     *http.Client
	Now            func() time.Time
	Logger         *slog.Logger
}

// Proxy fetches release metadata and assets from GitHub on the app's behalf.
type Proxy struct {
	api      string
	download string
	http     *http.Client
	now      func() time.Time
	logger   *slog.Logger

	mu        sync.Mutex
	latest    *release
	fetchedAt time.Time
}

type release struct {
	Tag    string
	Assets []releaseAsset
}

type releaseAsset struct {
	Name string
	Size int64
}

// New returns a Proxy.
func New(opts Options) *Proxy {
	p := &Proxy{
		api:      strings.TrimRight(opts.GitHubAPI, "/"),
		download: strings.TrimRight(opts.GitHubDownload, "/"),
		http:     opts.HTTPClient,
		now:      opts.Now,
		logger:   opts.Logger,
	}
	if p.api == "" {
		p.api = defaultGitHubAPI
	}
	if p.download == "" {
		p.download = defaultGitHubDownload
	}
	if p.http == nil {
		p.http = &http.Client{Timeout: 5 * time.Minute}
	}
	if p.now == nil {
		p.now = time.Now
	}
	if p.logger == nil {
		p.logger = slog.Default()
	}
	return p
}

// Register mounts the installer, the latest-release metadata and the asset
// downloads. None of them takes a bearer: the release is public.
func Register(mux *http.ServeMux, p *Proxy, baseURL string) {
	baseURL = strings.TrimRight(baseURL, "/")
	script := strings.Replace(installScript, baseURLPlaceholder, shellQuote(baseURL), 1)
	mux.HandleFunc(InstallScriptPath, func(w http.ResponseWriter, r *http.Request) {
		if !readOnly(w, r) {
			return
		}
		w.Header().Set("Content-Type", "text/x-shellscript; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, script)
	})
	mux.HandleFunc(LatestPath, func(w http.ResponseWriter, r *http.Request) {
		if !readOnly(w, r) {
			return
		}
		p.serveLatest(w, r, baseURL)
	})
	mux.HandleFunc(DownloadPrefix, func(w http.ResponseWriter, r *http.Request) {
		if !readOnly(w, r) {
			return
		}
		p.serveAsset(w, r)
	})
}

func readOnly(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func (p *Proxy) serveLatest(w http.ResponseWriter, r *http.Request, baseURL string) {
	rel, err := p.latestRelease(r.Context())
	if err != nil {
		p.logger.ErrorContext(r.Context(), "pdw release metadata unavailable", "error", err)
		http.Error(w, "could not fetch the latest pdw release", http.StatusBadGateway)
		return
	}
	type asset struct {
		Name string `json:"name"`
		URL  string `json:"browser_download_url"`
		Size int64  `json:"size"`
	}
	assets := make([]asset, 0, len(rel.Assets))
	for _, a := range rel.Assets {
		assets = append(assets, asset{Name: a.Name, URL: baseURL + DownloadPrefix + rel.Tag + "/" + a.Name, Size: a.Size})
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"tag_name": rel.Tag, "assets": assets})
}

// latestRelease returns the cached release while it is fresh, refreshes it
// after latestTTL, and falls back to the stale copy when GitHub refuses.
func (p *Proxy) latestRelease(ctx context.Context) (release, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if p.latest != nil && now.Sub(p.fetchedAt) < latestTTL {
		return *p.latest, nil
	}
	rel, err := p.fetchLatest(ctx)
	if err != nil {
		if p.latest != nil {
			p.logger.WarnContext(ctx, "pdw release refresh failed; serving the cached release", "tag", p.latest.Tag, "error", err)
			return *p.latest, nil
		}
		return release{}, err
	}
	p.latest, p.fetchedAt = &rel, now
	return rel, nil
}

func (p *Proxy) fetchLatest(ctx context.Context) (release, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.api+"/repos/"+Repo+"/releases/latest", nil)
	if err != nil {
		return release{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "pdw-app/clirelease")
	resp, err := p.http.Do(req)
	if err != nil {
		return release{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return release{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return release{}, fmt.Errorf("github releases/latest: http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name string `json:"name"`
			Size int64  `json:"size"`
		} `json:"assets"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return release{}, fmt.Errorf("decode release: %w", err)
	}
	if !tagPattern.MatchString(payload.TagName) {
		return release{}, fmt.Errorf("latest release tag %q is not a pdw-cli release", payload.TagName)
	}
	rel := release{Tag: payload.TagName}
	for _, a := range payload.Assets {
		if allowedAsset(payload.TagName, a.Name) {
			rel.Assets = append(rel.Assets, releaseAsset{Name: a.Name, Size: a.Size})
		}
	}
	return rel, nil
}

func (p *Proxy) serveAsset(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, DownloadPrefix)
	slash := strings.LastIndex(rest, "/")
	if slash < 0 {
		http.NotFound(w, r)
		return
	}
	tag, name := rest[:slash], rest[slash+1:]
	if !allowedAsset(tag, name) {
		http.NotFound(w, r)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, p.download+"/"+Repo+"/releases/download/"+tag+"/"+name, nil)
	if err != nil {
		http.Error(w, "bad asset request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("User-Agent", "pdw-app/clirelease")
	resp, err := p.http.Do(req)
	if err != nil {
		p.logger.ErrorContext(r.Context(), "pdw release asset fetch failed", "tag", tag, "asset", name, "error", err)
		http.Error(w, "could not fetch the release asset", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		http.NotFound(w, r)
		return
	case resp.StatusCode != http.StatusOK:
		p.logger.ErrorContext(r.Context(), "pdw release asset fetch refused", "tag", tag, "asset", name, "status", resp.StatusCode)
		http.Error(w, "could not fetch the release asset", http.StatusBadGateway)
		return
	}
	if resp.ContentLength >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	// A release's assets never change under its tag.
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	if _, err := io.Copy(w, resp.Body); err != nil {
		p.logger.WarnContext(r.Context(), "pdw release asset stream interrupted", "tag", tag, "asset", name, "error", err)
	}
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
