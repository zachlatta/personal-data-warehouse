// Package chromiumtest builds synthetic Chromium cookie stores for tests: a
// Cookies database whose values are encrypted exactly the way the browser
// does it, so the decryption path is exercised end to end.
package chromiumtest

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Encrypt produces a "v10" cookie value under key, optionally with the newer
// domain-hash prefix.
func Encrypt(t testing.TB, value string, key []byte, hostKey string, domainPrefix bool) []byte {
	t.Helper()
	plaintext := []byte(value)
	if domainPrefix {
		sum := sha256.Sum256([]byte(hostKey))
		plaintext = append(sum[:], plaintext...)
	}
	pad := aes.BlockSize - len(plaintext)%aes.BlockSize
	plaintext = append(plaintext, bytes.Repeat([]byte{byte(pad)}, pad)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, len(plaintext))
	cipher.NewCBCEncrypter(block, bytes.Repeat([]byte{' '}, aes.BlockSize)).CryptBlocks(out, plaintext)
	return append([]byte("v10"), out...)
}

// Row is one cookie to seed.
type Row struct {
	HostKey    string
	Name       string
	Value      string // encrypted under the store's key unless Plain is set
	Plain      bool   // store the value in the plaintext column instead
	ExpiresUTC int64
}

// WriteCookieDB creates a Cookies database at path with the given rows.
func WriteCookieDB(t testing.TB, path string, key []byte, rows []Row) {
	t.Helper()
	db, err := common.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE cookies (host_key TEXT, name TEXT, encrypted_value BLOB, value TEXT, expires_utc INTEGER)"); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		var encrypted []byte
		plain := ""
		if row.Plain {
			plain = row.Value
		} else {
			encrypted = Encrypt(t, row.Value, key, row.HostKey, false)
		}
		if _, err := db.Exec("INSERT INTO cookies VALUES (?,?,?,?,?)", row.HostKey, row.Name, encrypted, plain, row.ExpiresUTC); err != nil {
			t.Fatal(err)
		}
	}
}

// Key is a deterministic Safe Storage key for a test password.
func Key(password string) []byte { return chromium.DeriveKey(password) }
