// Package chromium reads cookies out of a Chrome-family browser (or an
// Electron app such as Slack) on macOS.
//
// Chromium stores cookies in a SQLite "Cookies" database with values
// AES-128-CBC encrypted under a key derived (PBKDF2-HMAC-SHA1, 1003 rounds,
// salt "saltysalt") from the app's "<Name> Safe Storage" keychain password.
// That password lives in the legacy login keychain, so it is readable with
// the user's consent through `security find-generic-password` (a one-time
// "Always Allow" prompt).
//
// This is the port of personal_data_warehouse.chatgpt_cookies: the machinery
// is browser-specific, not site-specific, so the Slack, ChatGPT and WHOOP
// session publishers all share it. Nothing here logs a key or a cookie value.
package chromium

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const (
	pbkdf2Salt    = "saltysalt"
	pbkdf2Rounds  = 1003
	pbkdf2KeyLen  = 16
	domainHashLen = sha256.Size

	// KeychainTimeout bounds `security`: it blocks for as long as a consent
	// prompt stays unanswered, which under launchd is forever.
	KeychainTimeout = 60 * time.Second
)

// Profile describes one Chromium-based application's cookie store.
type Profile struct {
	Key                string // cli-facing name, e.g. "chrome"
	DisplayName        string
	SupportSubdir      string // under ~/Library/Application Support
	SafeStorageService string
	SafeStorageAccount string
	AppBundle          string // /Applications path, for "is it installed?"
	HomebrewCask       string // cask to `brew install --cask`, when auto-installing
}

// Browsers is ordered by practical local-default likelihood; auto-detect
// walks this list.
var Browsers = []Profile{
	{"chrome", "Google Chrome", "Google/Chrome", "Chrome Safe Storage", "Chrome", "/Applications/Google Chrome.app", "google-chrome"},
	{"brave", "Brave", "BraveSoftware/Brave-Browser", "Brave Safe Storage", "Brave", "/Applications/Brave Browser.app", "brave-browser"},
	{"edge", "Microsoft Edge", "Microsoft Edge", "Microsoft Edge Safe Storage", "Microsoft Edge", "/Applications/Microsoft Edge.app", "microsoft-edge"},
	{"arc", "Arc", "Arc/User Data", "Arc Safe Storage", "Arc", "/Applications/Arc.app", "arc"},
	{"chromium", "Chromium", "Chromium", "Chromium Safe Storage", "Chromium", "/Applications/Chromium.app", "chromium"},
	{"vivaldi", "Vivaldi", "Vivaldi", "Vivaldi Safe Storage", "Vivaldi", "/Applications/Vivaldi.app", "vivaldi"},
}

// DefaultInstallBrowser is the browser auto-installed when none is present.
const DefaultInstallBrowser = "brave"

// BrowserKeys renders the valid --browser values for an error message.
func BrowserKeys() string {
	keys := make([]string, 0, len(Browsers))
	for _, b := range Browsers {
		keys = append(keys, b.Key)
	}
	return strings.Join(keys, ", ")
}

// BrowserByKey finds a browser profile by its cli name (case-insensitive).
func BrowserByKey(key string) (Profile, bool) {
	for _, b := range Browsers {
		if b.Key == strings.ToLower(key) {
			return b, true
		}
	}
	return Profile{}, false
}

// Host is everything about the machine a capture touches, injectable so the
// publishers can be tested without a keychain or a real browser profile.
type Host struct {
	// ApplicationSupport is ~/Library/Application Support.
	ApplicationSupport string
	// KeychainPassword reads a generic password from the login keychain.
	KeychainPassword func(service, account string) (string, error)
	// FileExists answers "is this app bundle installed?".
	FileExists func(path string) bool
}

// DefaultHost is the real machine.
func DefaultHost() Host {
	home, _ := os.UserHomeDir()
	return Host{
		ApplicationSupport: filepath.Join(home, "Library", "Application Support"),
		KeychainPassword:   SecurityFindGenericPassword,
		FileExists: func(path string) bool {
			_, err := os.Stat(path)
			return err == nil
		},
	}
}

// Installed reports the Chrome-family browsers whose bundle is present.
func (h Host) Installed() []Profile {
	var found []Profile
	for _, b := range Browsers {
		if b.AppBundle != "" && h.FileExists != nil && h.FileExists(b.AppBundle) {
			found = append(found, b)
		}
	}
	return found
}

// KeychainError is a failed or timed-out read of a Safe Storage password.
type KeychainError struct {
	Profile  Profile
	TimedOut bool
	Err      error
}

func (e *KeychainError) Error() string {
	if e.TimedOut {
		return fmt.Sprintf("could not run `security` to read %s: timed out after %d seconds",
			e.Profile.SafeStorageService, int(KeychainTimeout/time.Second))
	}
	return fmt.Sprintf("could not read %q from the keychain (is %s installed and have you allowed access?): %v",
		e.Profile.SafeStorageService, e.Profile.DisplayName, e.Err)
}

func (e *KeychainError) Unwrap() error { return e.Err }

// SecurityFindGenericPassword shells out to /usr/bin/security, bounded by
// KeychainTimeout. A timeout is reported as such so callers can explain the
// two real causes (locked keychain, one-shot "Allow" ACL).
func SecurityFindGenericPassword(service, account string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), KeychainTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "security", "find-generic-password", "-w", "-s", service, "-a", account)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "", context.DeadlineExceeded
	}
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return "", errors.New(msg)
	}
	return strings.TrimRight(stdout.String(), "\n"), nil
}

// SafeStorageKey derives the AES key for profile from its keychain password.
func (h Host) SafeStorageKey(profile Profile) ([]byte, error) {
	if h.KeychainPassword == nil {
		return nil, &KeychainError{Profile: profile, Err: errors.New("no keychain reader configured")}
	}
	password, err := h.KeychainPassword(profile.SafeStorageService, profile.SafeStorageAccount)
	if err != nil {
		return nil, &KeychainError{Profile: profile, TimedOut: errors.Is(err, context.DeadlineExceeded), Err: err}
	}
	return DeriveKey(password), nil
}

// DeriveKey is Chromium's cookie KDF: PBKDF2-HMAC-SHA1, 1003 rounds, 16 bytes.
func DeriveKey(password string) []byte {
	return pbkdf2SHA1([]byte(password), []byte(pbkdf2Salt), pbkdf2Rounds, pbkdf2KeyLen)
}

func pbkdf2SHA1(password, salt []byte, iterations, keyLen int) []byte {
	hLen := sha1.Size
	numBlocks := (keyLen + hLen - 1) / hLen
	dk := make([]byte, 0, numBlocks*hLen)
	var blockIndex [4]byte
	for block := 1; block <= numBlocks; block++ {
		binary.BigEndian.PutUint32(blockIndex[:], uint32(block))
		mac := hmac.New(sha1.New, password)
		mac.Write(salt)
		mac.Write(blockIndex[:])
		u := mac.Sum(nil)
		t := make([]byte, len(u))
		copy(t, u)
		for n := 2; n <= iterations; n++ {
			mac.Reset()
			mac.Write(u)
			u = mac.Sum(nil)
			for i := range t {
				t[i] ^= u[i]
			}
		}
		dk = append(dk, t...)
	}
	return dk[:keyLen]
}

// CookieDBs lists every per-profile Cookies database under the app's support
// directory: historically "<profile>/Cookies", on newer Chromium
// "<profile>/Network/Cookies". Stable order, deduplicated.
func (h Host) CookieDBs(profile Profile) []string {
	base := filepath.Join(h.ApplicationSupport, profile.SupportSubdir)
	if info, err := os.Stat(base); err != nil || !info.IsDir() {
		return nil
	}
	seen := map[string]bool{}
	var found []string
	for _, pattern := range []string{"Cookies", "*/Cookies", "Network/Cookies", "*/Network/Cookies"} {
		matches, _ := filepath.Glob(filepath.Join(base, pattern))
		sort.Strings(matches)
		for _, path := range matches {
			if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() || seen[path] {
				continue
			}
			seen[path] = true
			found = append(found, path)
		}
	}
	return found
}

// DecryptValue decrypts one cookie value the way Chromium wrote it on macOS.
// An empty blob or an unknown scheme falls back to the plaintext column.
func DecryptValue(encrypted, key []byte, hostKey, plain string) string {
	if len(encrypted) == 0 {
		return plain
	}
	if !bytes.HasPrefix(encrypted, []byte("v10")) && !bytes.HasPrefix(encrypted, []byte("v11")) {
		if utf8.Valid(encrypted) {
			return string(encrypted)
		}
		return plain
	}
	body := encrypted[3:]
	if len(body) == 0 || len(body)%aes.BlockSize != 0 {
		return ""
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return ""
	}
	iv := bytes.Repeat([]byte{' '}, aes.BlockSize)
	plaintext := make([]byte, len(body))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plaintext, body)
	plaintext = stripPKCS7(plaintext)
	// Newer Chromium prepends the SHA-256 of the cookie's host_key.
	domainHash := sha256.Sum256([]byte(hostKey))
	if len(plaintext) >= domainHashLen && bytes.Equal(plaintext[:domainHashLen], domainHash[:]) {
		plaintext = plaintext[domainHashLen:]
	}
	return strings.ToValidUTF8(string(plaintext), "�")
}

func stripPKCS7(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	pad := int(data[len(data)-1])
	if pad >= 1 && pad <= aes.BlockSize && len(data) >= pad {
		return data[:len(data)-pad]
	}
	return data
}

// Cookie is one decrypted row of the cookies table.
type Cookie struct {
	HostKey string
	Name    string
	Value   string
	// ExpiresUTC is Chromium's timestamp: microseconds since 1601-01-01.
	ExpiresUTC int64
}

// Expires converts the Chromium timestamp; the zero time means "no expiry".
func (c Cookie) Expires() time.Time {
	return ChromiumTime(c.ExpiresUTC)
}

// ChromiumTime converts microseconds since 1601-01-01 to a UTC time; zero in,
// zero time out.
func ChromiumTime(micros int64) time.Time {
	if micros <= 0 {
		return time.Time{}
	}
	// 1601-01-01 is 11644473600 seconds before the Unix epoch; a time.Duration
	// cannot hold four centuries of nanoseconds, so go through Unix seconds.
	const epochOffsetSeconds = 11644473600
	return time.Unix(micros/1_000_000-epochOffsetSeconds, (micros%1_000_000)*1000).UTC()
}

// ReadCookies decrypts every cookie whose host_key ends with one of the given
// suffixes, ordered by name. The database (plus any -wal/-shm sidecar) is
// copied first so a running browser's lock does not block the read and so
// pending WAL writes are visible.
func ReadCookies(dbPath string, key []byte, hostSuffixes ...string) ([]Cookie, error) {
	if len(hostSuffixes) == 0 {
		return nil, errors.New("at least one host suffix is required")
	}
	tmp, err := os.MkdirTemp("", "pdw-chromium-cookies")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	local := filepath.Join(tmp, "Cookies")
	if err := copyFile(dbPath, local); err != nil {
		return nil, fmt.Errorf("could not copy cookie store %s: %w", dbPath, err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(dbPath + suffix); err == nil {
			_ = copyFile(dbPath+suffix, local+suffix)
		}
	}
	db, err := common.OpenSQLite(local)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	clauses := make([]string, 0, len(hostSuffixes))
	args := make([]any, 0, len(hostSuffixes))
	for _, suffix := range hostSuffixes {
		clauses = append(clauses, "host_key LIKE ?")
		args = append(args, "%"+suffix)
	}
	rows, err := db.Query(
		"SELECT host_key, name, encrypted_value, value, expires_utc FROM cookies WHERE "+
			strings.Join(clauses, " OR ")+" ORDER BY name", args...)
	if err != nil {
		return nil, fmt.Errorf("could not read %s: %w", dbPath, err)
	}
	defer rows.Close()
	var found []Cookie
	for rows.Next() {
		var hostKey, name string
		var encrypted []byte
		var plain sql.NullString
		var expires sql.NullInt64
		if err := rows.Scan(&hostKey, &name, &encrypted, &plain, &expires); err != nil {
			return nil, err
		}
		decoded := DecryptValue(encrypted, key, hostKey, plain.String)
		if decoded == "" {
			continue
		}
		found = append(found, Cookie{HostKey: hostKey, Name: name, Value: decoded, ExpiresUTC: expires.Int64})
	}
	return found, rows.Err()
}

// ReadCookiesForHost is ReadCookies collapsed to name -> value (later rows win).
func ReadCookiesForHost(dbPath string, key []byte, hostSuffix string) (map[string]string, error) {
	cookies, err := ReadCookies(dbPath, key, hostSuffix)
	if err != nil {
		return nil, err
	}
	found := make(map[string]string, len(cookies))
	for _, c := range cookies {
		found[c.Name] = c.Value
	}
	return found, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
