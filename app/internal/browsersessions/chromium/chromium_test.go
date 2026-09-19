package chromium_test

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium/chromiumtest"
)

func TestDeriveKeyMatchesPythonPBKDF2(t *testing.T) {
	// hashlib.pbkdf2_hmac("sha1", b"s3cr3t", b"saltysalt", 1003, dklen=16)
	got := chromium.DeriveKey("s3cr3t")
	want, _ := hex.DecodeString("e724862a8651236b7b06675509b7ebd0")
	if len(got) != 16 {
		t.Fatalf("key length %d", len(got))
	}
	if string(got) != string(want) {
		t.Fatalf("key = %x, want %x", got, want)
	}
}

func TestDecryptValue(t *testing.T) {
	key := chromiumtest.Key("s3cr3t")
	cases := []struct {
		name      string
		encrypted []byte
		hostKey   string
		plain     string
		want      string
	}{
		{"roundtrip", chromiumtest.Encrypt(t, "token-value-123", key, "chatgpt.com", false), "chatgpt.com", "", "token-value-123"},
		{"strips domain hash prefix", chromiumtest.Encrypt(t, "real-token", key, ".chatgpt.com", true), ".chatgpt.com", "", "real-token"},
		{"empty falls back to plain", nil, "chatgpt.com", "plainval", "plainval"},
		{"unknown scheme decodes as text", []byte("legacy"), "x", "p", "legacy"},
		{"bad length is empty", []byte("v10abc"), "x", "p", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := chromium.DecryptValue(tc.encrypted, key, tc.hostKey, tc.plain); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestReadCookiesFiltersBySuffixAndDecrypts(t *testing.T) {
	key := chromiumtest.Key("pw")
	dir := t.TempDir()
	db := filepath.Join(dir, "Cookies")
	chromiumtest.WriteCookieDB(t, db, key, []Row{
		{HostKey: "chatgpt.com", Name: "__Secure-next-auth.session-token", Value: "sess-tok"},
		{HostKey: ".openai.com", Name: "oai-did", Value: "d", ExpiresUTC: 13400000000000000},
		{HostKey: "example.com", Name: "irrelevant", Value: "nope"},
		{HostKey: "chatgpt.com", Name: "plain", Value: "pv", Plain: true},
	})
	cookies, err := chromium.ReadCookies(db, key, "chatgpt.com", "openai.com")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]string{}
	for _, c := range cookies {
		names[c.Name] = c.Value
	}
	if names["__Secure-next-auth.session-token"] != "sess-tok" || names["oai-did"] != "d" || names["plain"] != "pv" {
		t.Fatalf("cookies = %v", names)
	}
	if _, ok := names["irrelevant"]; ok {
		t.Fatal("suffix filter leaked example.com")
	}
	for _, c := range cookies {
		if c.Name == "oai-did" && c.Expires().Year() != 2025 {
			t.Fatalf("expiry = %v", c.Expires())
		}
	}
	byName, err := chromium.ReadCookiesForHost(db, key, "chatgpt.com")
	if err != nil || byName["__Secure-next-auth.session-token"] != "sess-tok" {
		t.Fatalf("ReadCookiesForHost = %v, %v", byName, err)
	}
}

func TestChromiumTime(t *testing.T) {
	if !chromium.ChromiumTime(0).IsZero() {
		t.Fatal("zero should be zero time")
	}
	// 1970-01-01 is 11644473600 seconds after 1601-01-01.
	got := chromium.ChromiumTime(11644473600 * 1_000_000)
	if !got.Equal(time.Unix(0, 0)) {
		t.Fatalf("got %v", got)
	}
}

func TestCookieDBsFindsProfileAndNetworkStores(t *testing.T) {
	support := t.TempDir()
	base := filepath.Join(support, "Google/Chrome")
	for _, rel := range []string{"Default/Cookies", "Profile 1/Network/Cookies", "Cookies"} {
		path := filepath.Join(base, rel)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte("x"), 0o644)
	}
	host := chromium.Host{ApplicationSupport: support}
	chrome, _ := chromium.BrowserByKey("CHROME")
	got := host.CookieDBs(chrome)
	if len(got) != 3 {
		t.Fatalf("got %v", got)
	}
	brave, _ := chromium.BrowserByKey("brave")
	if got := host.CookieDBs(brave); got != nil {
		t.Fatalf("brave should have no stores, got %v", got)
	}
}

func TestSafeStorageKeyReportsTimeoutsDistinctly(t *testing.T) {
	host := chromium.Host{KeychainPassword: func(string, string) (string, error) { return "", context.DeadlineExceeded }}
	profile, _ := chromium.BrowserByKey("chrome")
	_, err := host.SafeStorageKey(profile)
	var kerr *chromium.KeychainError
	if !errors.As(err, &kerr) || !kerr.TimedOut || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v", err)
	}
	host.KeychainPassword = func(string, string) (string, error) { return "", errors.New("could not be found") }
	_, err = host.SafeStorageKey(profile)
	if !errors.As(err, &kerr) || kerr.TimedOut || !strings.Contains(err.Error(), "Chrome Safe Storage") || !strings.Contains(err.Error(), "allowed access") {
		t.Fatalf("err = %v", err)
	}
	host.KeychainPassword = func(string, string) (string, error) { return "s3cr3t", nil }
	key, err := host.SafeStorageKey(profile)
	if err != nil || string(key) != string(chromium.DeriveKey("s3cr3t")) {
		t.Fatalf("key mismatch: %v", err)
	}
}

func TestInstalledAndBrowserKeys(t *testing.T) {
	host := chromium.Host{FileExists: func(p string) bool { return p == "/Applications/Brave Browser.app" }}
	installed := host.Installed()
	if len(installed) != 1 || installed[0].Key != "brave" {
		t.Fatalf("installed = %v", installed)
	}
	if !strings.HasPrefix(chromium.BrowserKeys(), "chrome, brave") {
		t.Fatalf("keys = %s", chromium.BrowserKeys())
	}
	if _, ok := chromium.BrowserByKey("netscape"); ok {
		t.Fatal("netscape is not a browser")
	}
}

type Row = chromiumtest.Row
