package androidnet

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"testing"
)

func missing(string) (os.FileInfo, error) { return nil, fs.ErrNotExist }
func present(string) (os.FileInfo, error) { return nil, nil }
func noEnv(string) string                 { return "" }

// Android has no /etc/resolv.conf, so Go's own resolver asks [::1]:53 and
// every lookup fails ("connection refused"): measured on the Pixel 11 Pro on
// 2026-10-23 with a static linux/arm64 build. pdw installs a resolver that
// asks real servers instead, only when that file is missing.
func TestOnlyAHostWithoutResolvConfGetsTheFallback(t *testing.T) {
	original := net.DefaultResolver
	t.Cleanup(func() { net.DefaultResolver = original })

	if install(present, "linux", noEnv) || net.DefaultResolver != original {
		t.Fatal("a Linux host with /etc/resolv.conf keeps its resolver")
	}
	if install(missing, "darwin", noEnv) || net.DefaultResolver != original {
		t.Fatal("macOS resolves through the system and is left alone")
	}
	if !install(missing, "linux", noEnv) || net.DefaultResolver == original || !net.DefaultResolver.PreferGo {
		t.Fatal("a linux binary on Android must get the fallback resolver")
	}
}

func TestServersComeFromTheEnvironmentOrTheDefaults(t *testing.T) {
	if got := servers(noEnv); len(got) != len(DefaultServers) || got[0] != DefaultServers[0] {
		t.Fatalf("defaults = %v", got)
	}
	env := func(string) string { return " 9.9.9.9, [2620:fe::fe]:53 ,," }
	got := servers(env)
	if len(got) != 2 || got[0] != "9.9.9.9:53" || got[1] != "[2620:fe::fe]:53" {
		t.Fatalf("servers = %v", got)
	}
}

func TestEachQueryAttemptAsksTheNextServer(t *testing.T) {
	var asked []string
	dial := rotatingDial([]string{"a:53", "b:53", "c:53"}, func(_ context.Context, network, address string) (net.Conn, error) {
		asked = append(asked, network+" "+address)
		return nil, errors.New("not dialing in a test")
	})
	for i := 0; i < 4; i++ {
		// The address the resolver passes is its own (useless) localhost default.
		dial(context.Background(), "udp", "[::1]:53")
	}
	want := []string{"udp a:53", "udp b:53", "udp c:53", "udp a:53"}
	for i := range want {
		if asked[i] != want[i] {
			t.Fatalf("asked = %v, want %v", asked, want)
		}
	}
}

// Android 14 and later keep the trusted roots in the Conscrypt APEX, and Go's
// list of Linux certificate directories names neither it nor the older
// /system/etc/security/cacerts: with DNS fixed, every request from the Pixel
// failed "x509: certificate signed by unknown authority" (2026-10-23).
func TestAnAndroidHostTrustsAndroidsOwnRoots(t *testing.T) {
	dirs := map[string]bool{"/apex/com.android.conscrypt/cacerts": true, "/system/etc/security/cacerts": true}
	stat := func(path string) (os.FileInfo, error) {
		if dirs[path] {
			return nil, nil
		}
		return nil, fs.ErrNotExist
	}
	set := map[string]string{}
	setenv := func(k, v string) error { set[k] = v; return nil }

	certDirs(stat, noEnv, setenv)
	if set["SSL_CERT_DIR"] != "/apex/com.android.conscrypt/cacerts:/system/etc/security/cacerts" {
		t.Fatalf("SSL_CERT_DIR = %q", set["SSL_CERT_DIR"])
	}

	set = map[string]string{}
	certDirs(stat, func(k string) string {
		if k == "SSL_CERT_DIR" {
			return "/mine"
		}
		return ""
	}, setenv)
	if _, changed := set["SSL_CERT_DIR"]; changed {
		t.Fatal("an operator's SSL_CERT_DIR wins")
	}

	set = map[string]string{}
	certDirs(missing, noEnv, setenv)
	if len(set) != 0 {
		t.Fatalf("a host without Android's roots is left alone: %v", set)
	}
}
