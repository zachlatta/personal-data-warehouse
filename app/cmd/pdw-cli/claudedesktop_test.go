package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// encryptChromiumValue is the inverse of decryptChromiumValue, used to build
// round-trip fixtures. Optional domainHash mirrors newer Chromium's 32-byte
// prefix.
func encryptChromiumValue(t *testing.T, plaintext string, key []byte, domainHash []byte) []byte {
	t.Helper()
	body := append(append([]byte{}, domainHash...), []byte(plaintext)...)
	pad := aes.BlockSize - (len(body) % aes.BlockSize)
	for i := 0; i < pad; i++ {
		body = append(body, byte(pad))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	iv := bytes.Repeat([]byte{' '}, aes.BlockSize)
	out := make([]byte, len(body))
	encrypter := cipher.NewCBCEncrypter(block, iv)
	encrypter.CryptBlocks(out, body)
	return append([]byte("v10"), out...)
}

func TestDecryptChromiumValueRoundTrip(t *testing.T) {
	key := pbkdf2SHA1([]byte("test-password"), chromiumKDFSalt, chromiumKDFIterations, chromiumKDFKeyLen)

	// Legacy layout: no domain-hash prefix.
	enc := encryptChromiumValue(t, "sk-ant-sid02-secret", key, nil)
	got, err := decryptChromiumValue(enc, key)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if got != "sk-ant-sid02-secret" {
		t.Fatalf("got %q", got)
	}

	// Newer layout: a high-entropy 32-byte prefix that fails utf8 -> stripped.
	prefix := bytes.Repeat([]byte{0xff}, chromiumDomainHashLen)
	enc2 := encryptChromiumValue(t, "org-1234", key, prefix)
	got2, err := decryptChromiumValue(enc2, key)
	if err != nil {
		t.Fatalf("decrypt prefixed: %v", err)
	}
	if got2 != "org-1234" {
		t.Fatalf("got %q", got2)
	}
}

func TestDecryptChromiumValueNonV10IsPassthrough(t *testing.T) {
	got, err := decryptChromiumValue([]byte("plainvalue"), make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	if got != "plainvalue" {
		t.Fatalf("got %q", got)
	}
}

// PBKDF2-HMAC-SHA1 RFC 6070-style sanity vector: P="password", S="salt",
// c=1, dkLen=20 -> 0c60c80f961f0e71f3a9b524af6012062fe037a6.
func TestPBKDF2SHA1KnownVector(t *testing.T) {
	dk := pbkdf2SHA1([]byte("password"), []byte("salt"), 1, 20)
	if hex.EncodeToString(dk) != "0c60c80f961f0e71f3a9b524af6012062fe037a6" {
		t.Fatalf("unexpected dk %x", dk)
	}
}

func TestResolveClaudeDesktopAccount(t *testing.T) {
	cases := []struct {
		env  map[string]string
		want string
	}{
		{map[string]string{"CLAUDE_DESKTOP_ACCOUNT": "a@x.com"}, "a@x.com"},
		{map[string]string{"AGENT_SESSIONS_ACCOUNT": "b@x.com"}, "b@x.com"},
		{map[string]string{"GMAIL_ACCOUNTS": "first@x.com, second@x.com"}, "first@x.com"},
		{map[string]string{"CLAUDE_DESKTOP_ACCOUNT": "win@x.com", "GMAIL_ACCOUNTS": "lose@x.com"}, "win@x.com"},
		{map[string]string{}, ""},
	}
	for _, tc := range cases {
		getenv := func(k string) string { return tc.env[k] }
		if got := resolveClaudeDesktopAccount(getenv); got != tc.want {
			t.Errorf("env=%v: got %q want %q", tc.env, got, tc.want)
		}
	}
}

func TestPushClaudeCredentialSignsAndPosts(t *testing.T) {
	const token = "shared-secret-token"
	verifier := pdwauth.NewService([]byte(token), time.Now)

	var received claudeCredential
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != claudeDesktopCredentialEndpoint {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		q := r.URL.Query()
		body, _ := io.ReadAll(r.Body)
		// Re-verify the signature exactly as the real app does.
		if err := verifier.VerifyObjectUpload(r.URL.Path, q.Get("content_sha256"), q.Get("exp"), q.Get("sig")); err != nil {
			t.Errorf("signature verify failed: %v", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if err := json.Unmarshal(body, &received); err != nil {
			t.Errorf("bad json: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	cred := claudeCredential{
		Account:    "account@example.com",
		SessionKey: "sk-ant-sid02-secret",
		OrgID:      "org-1",
		CapturedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := pushClaudeCredential(srv.URL, token, cred); err != nil {
		t.Fatalf("push: %v", err)
	}
	if received.Account != cred.Account || received.SessionKey != cred.SessionKey || received.OrgID != cred.OrgID {
		t.Fatalf("server received %+v", received)
	}
}

func TestPushClaudeCredentialRejectsBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, "invalid or expired upload link")
	}))
	defer srv.Close()
	err := pushClaudeCredential(srv.URL, "tok", claudeCredential{Account: "a", SessionKey: "s", OrgID: "o"})
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected 403 error, got %v", err)
	}
}
