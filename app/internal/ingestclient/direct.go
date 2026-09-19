package ingestclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Config is the resolved app URL and signing key every uploader posts with.
type Config struct {
	BaseURL string
	Token   string
}

// ResolveConfig returns the warehouse base URL and token with the precedence
// pdw uses everywhere else: root flags, PDW_API_URL / PDW_SECRET_TOKEN (and
// their MCP_* aliases), then the `pdw login` config file. Either value may be
// empty when nothing is configured.
func ResolveConfig(getenv func(string) string, flagBaseURL, flagToken string) Config {
	var fileCfg cliconfig.Config
	if loaded, _, err := cliconfig.Resolve(getenv); err == nil {
		fileCfg = loaded
	}
	env := common.Getenv(getenv)
	return Config{
		BaseURL: common.FirstNonEmpty(flagBaseURL, env.Env("PDW_API_URL", "MCP_BASE_URL"), fileCfg.BaseURL),
		Token:   common.FirstNonEmpty(flagToken, env.Env("PDW_SECRET_TOKEN", "MCP_SECRET_TOKEN"), fileCfg.Token),
	}
}

// WithEnvFallback fills whichever of URL and token is still empty from
// PDW_API_URL / PDW_SECRET_TOKEN (and their MCP_* aliases). An uploader calls
// it after loading the project .env, which is where those values lived for
// the Python uploaders and where a machine that never ran `pdw login` still
// keeps them.
func (c Config) WithEnvFallback(getenv func(string) string) Config {
	env := common.Getenv(getenv)
	return Config{
		BaseURL: common.FirstNonEmpty(c.BaseURL, env.Env("PDW_API_URL", "MCP_BASE_URL")),
		Token:   common.FirstNonEmpty(c.Token, env.Env("PDW_SECRET_TOKEN", "MCP_SECRET_TOKEN")),
	}
}

// Problem names what is missing for uploads, or "" when configured.
func (c Config) Problem() string {
	if strings.TrimSpace(c.BaseURL) == "" {
		return "PDW_API_URL (or MCP_BASE_URL) must be set for uploads; run `pdw login`"
	}
	if strings.TrimSpace(c.Token) == "" {
		return "PDW_SECRET_TOKEN (or MCP_SECRET_TOKEN) must be set for uploads; run `pdw login`"
	}
	return ""
}

// FromEnv builds a client from the resolved config and the direct-origin
// settings: PDW_INGEST_DIRECT_URL (an explicit base) or
// PDW_INGEST_TAILSCALE_HOST (a tailnet node running the app), used only when
// it answers /healthz as the app, so an off-tailnet laptop transparently
// falls back to the public URL.
func FromEnv(getenv func(string) string, cfg Config, logger common.Logger) (*Client, error) {
	if problem := cfg.Problem(); problem != "" {
		return nil, errors.New(problem)
	}
	env := common.Getenv(getenv)
	opts := []Option{WithLogger(logger)}
	if raw := strings.TrimSpace(env("PDW_INGEST_MAX_OBJECT_BYTES")); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			opts = append(opts, WithMaxObjectBytes(n))
		}
	}
	direct := ResolveDirectOrigin(cfg.BaseURL, env("PDW_INGEST_DIRECT_URL"), env("PDW_INGEST_TAILSCALE_HOST"), env("PDW_TAILSCALE_BIN"), TailscaleIPv4, ProbeDirectOrigin, logger)
	if direct != "" {
		opts = append(opts, WithUploadBaseURL(direct))
	}
	return New(cfg.BaseURL, cfg.Token, opts...)
}

// ResolveDirectOrigin picks a same-app origin reachable off the public
// (Cloudflare) path, or "" when there is none.
func ResolveDirectOrigin(
	baseURL, explicitDirectURL, tailscaleHost, tailscaleBin string,
	resolveIPv4 func(host, bin string) string,
	probe func(directURL, hostHeader string) bool,
	logger common.Logger,
) string {
	hostHeader := ""
	if parsed, err := url.Parse(baseURL); err == nil {
		hostHeader = parsed.Host
	}
	candidate := strings.TrimSpace(explicitDirectURL)
	if candidate == "" && strings.TrimSpace(tailscaleHost) != "" {
		if ip := resolveIPv4(strings.TrimSpace(tailscaleHost), tailscaleBin); ip != "" {
			candidate = "http://" + ip
		}
	}
	if candidate == "" {
		return ""
	}
	if !probe(candidate, hostHeader) {
		if logger != nil {
			logger.Infof("Tailscale-direct ingest origin %s not reachable; using %s", candidate, baseURL)
		}
		return ""
	}
	if logger != nil {
		logger.Infof("Preferring Tailscale-direct ingest origin %s for %s (bypasses the Cloudflare body-size cap)", candidate, hostHeader)
	}
	return candidate
}

// TailscaleIPv4 returns host's tailnet IPv4 via `tailscale ip -4 <host>`.
func TailscaleIPv4(host, override string) string {
	binary := tailscaleBinary(override)
	if binary == "" {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, "ip", "-4", host).Output()
	if err != nil {
		return ""
	}
	return ParseIPv4Line(string(out))
}

// ParseIPv4Line returns the first line that is a dotted IPv4 address.
func ParseIPv4Line(output string) string {
	for _, line := range strings.Split(output, "\n") {
		ip := strings.TrimSpace(line)
		parts := strings.Split(ip, ".")
		if len(parts) != 4 {
			continue
		}
		valid := true
		for _, part := range parts {
			if part == "" || strings.Trim(part, "0123456789") != "" {
				valid = false
				break
			}
		}
		if valid {
			return ip
		}
	}
	return ""
}

func tailscaleBinary(override string) string {
	if override = strings.TrimSpace(override); override != "" {
		if info, err := os.Stat(override); err == nil && !info.IsDir() {
			return override
		}
		return ""
	}
	for _, candidate := range []string{
		"/opt/homebrew/bin/tailscale",
		"/usr/local/bin/tailscale",
		"/usr/bin/tailscale",
		"/Applications/Tailscale.app/Contents/MacOS/Tailscale",
	} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	if path, err := exec.LookPath("tailscale"); err == nil {
		return path
	}
	return ""
}

// ProbeDirectOrigin reports whether directURL answers /healthz as the app
// for hostHeader.
func ProbeDirectOrigin(directURL, hostHeader string) bool {
	client := &http.Client{Timeout: 3 * time.Second}
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(directURL, "/")+"/healthz", nil)
	if err != nil {
		return false
	}
	req.Host = hostHeader
	req.Header.Set("User-Agent", userAgent)
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode < 400
}

// DescribeRoute is the one-line summary the run logs print.
func (c *Client) DescribeRoute() string {
	if c.hostHeader != "" {
		return fmt.Sprintf("uploads -> %s (as %s)", c.uploadBaseURL, c.hostHeader)
	}
	return "uploads -> " + c.baseURL
}
