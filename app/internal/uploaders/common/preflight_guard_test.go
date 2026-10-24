package common

import (
	"net"
	"strings"
	"testing"
)

// On the Pixel the uploader runs ON the phone the network policy guards a Mac
// against tethering to: cellular is its normal network, and Android routes
// through policy tables, so /proc/net/route has no default route and the full
// guard deferred every run ("no default network route", 2026-10-23). The
// preflight-only guard asks just whether the app answers.
func TestPreflightGuardAsksOnlyWhetherTheAppAnswers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	env := Getenv(func(string) string { return "" })
	if reason := PreflightGuard(env, "GOOGLE_RECORDER", "http://"+listener.Addr().String())(); reason != "" {
		t.Fatalf("reachable app deferred the upload: %q", reason)
	}
	closed, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := closed.Addr().String()
	closed.Close()
	if reason := PreflightGuard(env, "GOOGLE_RECORDER", "http://"+addr)(); !strings.Contains(reason, "preflight failed") {
		t.Fatalf("unreachable app reason = %q", reason)
	}
}
