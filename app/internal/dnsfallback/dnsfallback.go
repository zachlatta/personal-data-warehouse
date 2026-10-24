// Package dnsfallback lets a pdw binary resolve names on Android.
//
// pdw runs on the rooted Pixel (`pdw ingest google-recorder`, `pdw heartbeat`)
// as the release linux/arm64 build. Go's own resolver reads /etc/resolv.conf,
// which Android does not have -- Android resolves through netd -- so it falls
// back to asking localhost and every lookup fails with "connection refused".
// When that file is missing, Install points Go's resolver at real servers.
package dnsfallback

import (
	"context"
	"net"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

// DefaultServers are asked in turn when PDW_DNS_SERVERS is unset: two public
// resolvers, then Tailscale's MagicDNS, which answers whenever the tailnet is
// up even on a network that blocks outside DNS.
var DefaultServers = []string{"1.1.1.1:53", "8.8.8.8:53", "100.100.100.100:53"}

const resolvConf = "/etc/resolv.conf"

// Install replaces net.DefaultResolver on a Linux-kernel host with no
// /etc/resolv.conf, and reports whether it did. Everywhere else it does
// nothing, so it is safe to call first thing in main.
func Install() bool {
	return install(os.Stat, runtime.GOOS, os.Getenv)
}

func install(stat func(string) (os.FileInfo, error), goos string, getenv func(string) string) bool {
	if goos != "linux" && goos != "android" {
		return false
	}
	if _, err := stat(resolvConf); err == nil {
		return false
	}
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial:     rotatingDial(servers(getenv), dialer.DialContext),
	}
	return true
}

// servers parses PDW_DNS_SERVERS (comma-separated, port optional).
func servers(getenv func(string) string) []string {
	var out []string
	for _, part := range strings.Split(getenv("PDW_DNS_SERVERS"), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(part); err != nil {
			part = net.JoinHostPort(strings.Trim(part, "[]"), "53")
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return append([]string(nil), DefaultServers...)
	}
	return out
}

// rotatingDial ignores the server the resolver asks for (its localhost
// default) and sends each attempt to the next configured server, so a server
// that does not answer costs one attempt's timeout, not every lookup.
func rotatingDial(servers []string, dial func(ctx context.Context, network, address string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	var next atomic.Uint64
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		server := servers[(next.Add(1)-1)%uint64(len(servers))]
		return dial(ctx, network, server)
	}
}
