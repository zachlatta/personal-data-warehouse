package mcpproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"
)

func validateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return errors.New("use an HTTPS URL without credentials or a fragment")
	}
	return nil
}
func validateResourceURL(raw string) error {
	if err := validateURL(raw); err != nil {
		return err
	}
	u, _ := url.Parse(raw)
	if u.RawQuery != "" {
		return errors.New("MCP endpoint must not contain a query string; use web authentication instead")
	}
	return nil
}

// Deny internal, link-local, multicast, documentation and transition networks.
// Dial the checked address itself, not the hostname again (DNS rebinding).
var reserved = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("3fff::/20"), netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("64:ff9b::/96"),
}

func publicIP(raw string) bool {
	ip, err := netip.ParseAddr(raw)
	if err != nil {
		return false
	}
	ip = ip.Unmap()
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, p := range reserved {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

type checkedTransport struct{ base http.RoundTripper }

func (t checkedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := validateURL(r.URL.String()); err != nil {
		return nil, err
	}
	response, err := t.base.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	if response.ContentLength > maxUpstreamBytes {
		response.Body.Close()
		return nil, errors.New("upstream response exceeds 32 MiB")
	}
	response.Body = &boundedBody{ReadCloser: response.Body, remaining: maxUpstreamBytes}
	return response, nil
}

const maxUpstreamBytes int64 = 32 << 20

type boundedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *boundedBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		var probe [1]byte
		n, err := b.ReadCloser.Read(probe[:])
		if n > 0 {
			return 0, errors.New("upstream response exceeds 32 MiB")
		}
		return 0, err
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}
func safeClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // Never route private addresses through an ambient proxy.
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, errors.New("upstream DNS lookup failed")
		}
		if len(ips) == 0 {
			return nil, errors.New("upstream has no addresses")
		}
		for _, ip := range ips {
			if !publicIP(ip.String()) {
				return nil, errors.New("upstream resolves to a non-public address")
			}
		}
		var conn net.Conn
		for _, ip := range ips {
			conn, err = (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
		}
		return nil, errors.New("upstream connection failed")
	}
	return &http.Client{Transport: checkedTransport{transport}, Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
