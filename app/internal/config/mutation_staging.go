package config

import (
	"errors"
	"net/netip"
	"net/url"
	"strings"
)

// MutationStagingConfig is loaded once at startup. Nil disables upload staging.
type MutationStagingConfig struct {
	Endpoint  *url.URL
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
}

func loadMutationStaging(getenv func(string) string) (*MutationStagingConfig, error) {
	names := []string{"ENDPOINT_URL", "BUCKET", "ACCESS_KEY", "SECRET_KEY", "REGION"}
	values := make([]string, len(names))
	configured := false
	for i, name := range names {
		values[i] = strings.TrimSpace(getenv("PDW_MUTATION_S3_" + name))
		configured = configured || values[i] != ""
	}
	if !configured {
		return nil, nil
	}
	for _, value := range values {
		if value == "" {
			return nil, errors.New("PDW_MUTATION_S3 requires ENDPOINT_URL, BUCKET, ACCESS_KEY, SECRET_KEY and REGION together")
		}
	}
	endpoint, err := url.Parse(values[0])
	if err != nil || !validMutationStagingEndpoint(endpoint) {
		return nil, errors.New("PDW_MUTATION_S3_ENDPOINT_URL must use HTTPS or tailnet HTTP without credentials, paths or queries")
	}
	return &MutationStagingConfig{Endpoint: endpoint, Bucket: values[1], AccessKey: values[2], SecretKey: values[3], Region: values[4]}, nil
}

// Plain HTTP is permitted only over Tailscale CGNAT or tailnet hostnames.
func validMutationStagingEndpoint(u *url.URL) bool {
	if u == nil || u.Host == "" || u.User != nil || u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	if u.Scheme != "http" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if addr, err := netip.ParseAddr(host); err == nil {
		return netip.MustParsePrefix("100.64.0.0/10").Contains(addr)
	}
	return strings.HasSuffix(host, ".ts.net") && len(strings.Split(host, ".")) >= 4
}
