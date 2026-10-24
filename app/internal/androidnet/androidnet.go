// Package androidnet lets a pdw binary reach the network on Android.
//
// pdw runs on the rooted Pixel (`pdw ingest google-recorder`, `pdw heartbeat`)
// as the release linux/arm64 build, and Go's Linux defaults do not fit
// Android in two places:
//
//   - DNS. Go's resolver reads /etc/resolv.conf, which Android does not have
//     (it resolves through netd), so it asks localhost and every lookup fails
//     with "connection refused". When that file is missing, Go's resolver is
//     pointed at real servers.
//   - TLS roots. Android 14 and later keep the trusted roots in the Conscrypt
//     APEX, and Go's Linux certificate directories name neither it nor the
//     older /system/etc/security/cacerts, so every request failed "x509:
//     certificate signed by unknown authority". SSL_CERT_DIR is set to them.
package androidnet

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

// androidCertDirs are Android's trusted-root directories, newest first.
var androidCertDirs = []string{"/apex/com.android.conscrypt/cacerts", "/system/etc/security/cacerts"}

const resolvConf = "/etc/resolv.conf"

// Install applies both fixes on a Linux-kernel host that needs them and
// reports whether it replaced the resolver. Everywhere else it does nothing,
// so it is safe to call first thing in main.
func Install() bool {
	if runtime.GOOS != "linux" && runtime.GOOS != "android" {
		return false
	}
	certDirs(os.Stat, os.Getenv, os.Setenv)
	return install(os.Stat, runtime.GOOS, os.Getenv)
}

// certDirs sets SSL_CERT_DIR to Android's root directories when they exist
// and the operator has not set it.
func certDirs(stat func(string) (os.FileInfo, error), getenv func(string) string, setenv func(string, string) error) {
	if getenv("SSL_CERT_DIR") != "" {
		return
	}
	var found []string
	for _, dir := range androidCertDirs {
		if _, err := stat(dir); err == nil {
			found = append(found, dir)
		}
	}
	if len(found) > 0 {
		_ = setenv("SSL_CERT_DIR", strings.Join(found, ":"))
	}
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
