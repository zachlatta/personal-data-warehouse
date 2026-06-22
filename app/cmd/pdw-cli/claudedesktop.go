package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// The Claude Desktop app is a claude.ai wrapper that keeps no conversation
// transcripts on disk, so there is nothing to tail. What IS local - and only on
// the machine running the desktop app - is the authenticated claude.ai session:
// the app stores (and continuously refreshes) the `sessionKey` cookie, encrypted
// in a Chromium cookie store with an AES key held in the macOS Keychain.
//
// This file is the clientside auth pusher. It decrypts that cookie and ships the
// session credential to the app's credential endpoint, which persists it in
// Postgres; the serverside Dagster poller then uses it to fetch conversations
// (the sessionKey alone authenticates the API, so it works from prod's IP). The
// token rotates ~monthly, so running this on a schedule keeps the server's copy
// fresh.
//
// All local-machine logic lives here in the Go CLI rather than a Python module,
// which is why this is native Go: Keychain via `security`, the Chromium cookie
// store via the macOS-bundled `sqlite3`, and AES/PBKDF2 from the stdlib.

const claudeDesktopSource = "claude-desktop"
const claudeDesktopCredentialEndpoint = "/ingest/claude-desktop/credential"

const (
	defaultClaudeCookiesPath     = "Library/Application Support/Claude/Cookies"
	defaultClaudeKeychainService = "Claude Safe Storage"
	defaultClaudeKeychainAccount = "Claude Key"
)

// Chromium's fixed macOS "v10" cookie-encryption KDF parameters.
const (
	chromiumKDFIterations = 1003
	chromiumKDFKeyLen     = 16
	chromiumDomainHashLen = 32
)

var chromiumKDFSalt = []byte("saltysalt")

type claudeCredential struct {
	Account    string `json:"account"`
	SessionKey string `json:"session_key"`
	OrgID      string `json:"org_id"`
	CapturedAt string `json:"captured_at"`
}

// runClaudeDesktopAuth extracts the claude.ai session credential from the local
// Claude Desktop app and pushes it to the warehouse app.
func runClaudeDesktopAuth(args []string, stdout, stderr io.Writer, getenv func(string) string, flagBaseURL, flagToken string) int {
	fs := flag.NewFlagSet("pdw ingest claude-desktop", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dryRun := fs.Bool("dry-run", false, "decrypt and print what would be pushed without contacting the app")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	account := resolveClaudeDesktopAccount(getenv)
	if account == "" {
		fmt.Fprintln(stderr, "pdw ingest claude-desktop: set CLAUDE_DESKTOP_ACCOUNT (or GMAIL_ACCOUNTS) so the credential can be keyed to an account")
		return 2
	}

	cookiesPath := strings.TrimSpace(getenv("CLAUDE_DESKTOP_COOKIES_PATH"))
	if cookiesPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest claude-desktop: cannot resolve home dir: %v\n", err)
			return 1
		}
		cookiesPath = filepath.Join(home, defaultClaudeCookiesPath)
	}
	service := firstNonEmpty(strings.TrimSpace(getenv("CLAUDE_DESKTOP_KEYCHAIN_SERVICE")), defaultClaudeKeychainService)
	keyAccount := firstNonEmpty(strings.TrimSpace(getenv("CLAUDE_DESKTOP_KEYCHAIN_ACCOUNT")), defaultClaudeKeychainAccount)
	orgOverride := strings.TrimSpace(getenv("CLAUDE_DESKTOP_ORG_ID"))

	sessionKey, orgID, err := extractClaudeSession(cookiesPath, service, keyAccount, orgOverride)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest claude-desktop: %v\n", err)
		return 1
	}

	if *dryRun {
		fmt.Fprintf(stdout, "Claude Desktop auth dry-run: account=%s org_id=%s session_key=%s... (%d chars)\n",
			account, orgID, truncate(sessionKey, 12), len(sessionKey))
		return 0
	}

	baseURL, token := resolveIngestWarehouse(getenv, flagBaseURL, flagToken)
	if baseURL == "" || token == "" {
		fmt.Fprintln(stderr, "pdw ingest claude-desktop: warehouse URL and token are required (run `pdw login` or set PDW_API_URL + PDW_SECRET_TOKEN)")
		return 1
	}

	cred := claudeCredential{
		Account:    account,
		SessionKey: sessionKey,
		OrgID:      orgID,
		CapturedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := pushClaudeCredential(baseURL, token, cred); err != nil {
		fmt.Fprintf(stderr, "pdw ingest claude-desktop: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "Claude Desktop credential pushed: account=%s org_id=%s\n", account, orgID)
	return 0
}

// resolveClaudeDesktopAccount mirrors the Python load_settings account chain so
// the credential row is keyed identically to what the serverside poller reads.
func resolveClaudeDesktopAccount(getenv func(string) string) string {
	for _, key := range []string{"CLAUDE_DESKTOP_ACCOUNT", "AGENT_SESSIONS_ACCOUNT", "APPLE_MESSAGES_ACCOUNT", "VOICE_MEMOS_ACCOUNT"} {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return v
		}
	}
	for _, entry := range strings.Split(getenv("GMAIL_ACCOUNTS"), ",") {
		if v := strings.TrimSpace(entry); v != "" {
			return v
		}
	}
	return ""
}

// extractClaudeSession decrypts the Claude Desktop cookie jar and returns the
// sessionKey plus the resolved org id (override wins over the lastActiveOrg
// cookie).
func extractClaudeSession(cookiesPath, keychainService, keychainAccount, orgOverride string) (sessionKey, orgID string, err error) {
	password, err := keychainPassword(keychainService, keychainAccount)
	if err != nil {
		return "", "", err
	}
	key := pbkdf2SHA1(password, chromiumKDFSalt, chromiumKDFIterations, chromiumKDFKeyLen)
	encrypted, err := readClaudeCookies(cookiesPath)
	if err != nil {
		return "", "", err
	}
	if len(encrypted) == 0 {
		return "", "", fmt.Errorf("no claude.ai cookies found in %s; log into the Claude Desktop app first", cookiesPath)
	}
	values := make(map[string]string, len(encrypted))
	for name, blob := range encrypted {
		plain, derr := decryptChromiumValue(blob, key)
		if derr != nil {
			continue
		}
		values[name] = plain
	}
	sessionKey = strings.TrimSpace(values["sessionKey"])
	if sessionKey == "" {
		return "", "", fmt.Errorf("claude.ai sessionKey cookie is missing or empty; log into the Claude Desktop app")
	}
	orgID = orgOverride
	if orgID == "" {
		orgID = strings.TrimSpace(values["lastActiveOrg"])
	}
	if orgID == "" {
		return "", "", fmt.Errorf("could not determine the claude.ai organization id (lastActiveOrg cookie missing); set CLAUDE_DESKTOP_ORG_ID")
	}
	return sessionKey, orgID, nil
}

// keychainPassword reads the app's cookie-encryption password from the macOS
// Keychain. macOS-only, like the desktop app itself.
func keychainPassword(service, account string) ([]byte, error) {
	cmd := exec.Command("security", "find-generic-password", "-s", service, "-a", account, "-w")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("could not read Keychain item %q/%q; is the Claude Desktop app installed and logged in? (%v)", service, account, err)
	}
	return bytes.TrimRight(out, "\r\n"), nil
}

// readClaudeCookies returns the encrypted values of every claude.ai cookie,
// keyed by name. It shells out to the macOS-bundled sqlite3 (avoiding a cgo
// sqlite dependency in this CGO_ENABLED=0 binary) against a throwaway copy of
// the store (the app holds the original open).
func readClaudeCookies(cookiesPath string) (map[string][]byte, error) {
	if _, err := os.Stat(cookiesPath); err != nil {
		return nil, fmt.Errorf("Claude Desktop cookie store not found at %s", cookiesPath)
	}
	tmp, err := os.MkdirTemp("", "pdw-claude-cookies")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	copyPath := filepath.Join(tmp, "Cookies")
	if err := copyFile(cookiesPath, copyPath); err != nil {
		return nil, fmt.Errorf("could not copy cookie store: %w", err)
	}
	// name|HEX(encrypted_value), one row per line.
	query := "SELECT name || '|' || hex(encrypted_value) FROM cookies WHERE host_key LIKE '%claude.ai%';"
	out, err := exec.Command("sqlite3", "-batch", "-noheader", copyPath, query).Output()
	if err != nil {
		return nil, fmt.Errorf("could not read cookie store with sqlite3: %w", err)
	}
	values := map[string][]byte{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, hexValue, ok := strings.Cut(line, "|")
		if !ok || hexValue == "" {
			continue
		}
		decoded, derr := hex.DecodeString(hexValue)
		if derr != nil {
			continue
		}
		values[name] = decoded
	}
	return values, nil
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

// decryptChromiumValue decrypts one macOS Chromium "v10" cookie value. It
// handles the legacy layout (plaintext is the value) and the newer one (a
// 32-byte domain SHA256 prefixes the value).
func decryptChromiumValue(encrypted, key []byte) (string, error) {
	if len(encrypted) == 0 {
		return "", nil
	}
	if !bytes.HasPrefix(encrypted, []byte("v10")) {
		return string(encrypted), nil
	}
	body := encrypted[3:]
	if len(body) == 0 || len(body)%aes.BlockSize != 0 {
		return "", fmt.Errorf("encrypted cookie value has an invalid length")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	iv := bytes.Repeat([]byte{' '}, aes.BlockSize)
	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(body))
	mode.CryptBlocks(plaintext, body)
	plaintext = pkcs7Unpad(plaintext)
	if utf8.Valid(plaintext) {
		return string(plaintext), nil
	}
	if len(plaintext) > chromiumDomainHashLen {
		return string(plaintext[chromiumDomainHashLen:]), nil
	}
	return string(plaintext), nil
}

func pkcs7Unpad(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	pad := int(data[len(data)-1])
	if pad < 1 || pad > aes.BlockSize || pad > len(data) {
		return data
	}
	for _, b := range data[len(data)-pad:] {
		if int(b) != pad {
			return data
		}
	}
	return data[:len(data)-pad]
}

// pbkdf2SHA1 is PBKDF2 with HMAC-SHA1 (RFC 2898), the Chromium cookie KDF.
// Implemented here to keep the CLI free of a crypto dependency.
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

// pushClaudeCredential signs and POSTs the credential to the app's credential
// endpoint, using the same object-upload HMAC scheme as the other ingest calls
// (the signature binds endpoint + body sha + expiry).
func pushClaudeCredential(baseURL, token string, cred claudeCredential) error {
	body, err := json.Marshal(cred)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	exp := time.Now().Add(15 * time.Minute)
	signer := pdwauth.NewService([]byte(token), time.Now)
	sig := signer.SignObjectUpload(claudeDesktopCredentialEndpoint, sha, exp)

	url := fmt.Sprintf("%s%s?content_sha256=%s&exp=%d&sig=%s",
		strings.TrimRight(baseURL, "/"), claudeDesktopCredentialEndpoint, sha, exp.Unix(), sig)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("credential push request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("credential push rejected (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
