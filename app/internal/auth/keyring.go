package auth

import (
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/base64"
)

// Key purposes. Each is an HKDF info string, so a MAC made for one purpose
// never verifies as another, and a subkey reveals nothing about the root.
const (
	purposeOAuthToken     = "pdw/oauth-token/v1"
	purposeOAuthGrant     = "pdw/oauth-grant/v1"
	purposeObjectDownload = "pdw/object-download/v1"
	purposeKeyID          = "pdw/key-id/v1"
)

// rootKey is one master secret and the values derived from it.
type rootKey struct {
	secret []byte
	id     string
	sub    map[string][]byte
}

func newRootKey(secret []byte) rootKey {
	k := rootKey{secret: secret, sub: map[string][]byte{}}
	for _, purpose := range []string{purposeOAuthToken, purposeOAuthGrant, purposeObjectDownload} {
		k.sub[purpose] = derive(secret, purpose, 32)
	}
	k.id = base64.RawURLEncoding.EncodeToString(derive(secret, purposeKeyID, 6))
	return k
}

func derive(secret []byte, purpose string, n int) []byte {
	out, err := hkdf.Key(sha256.New, secret, nil, purpose, n)
	if err != nil {
		// Only an output length over 255*32 bytes fails, and n is fixed here.
		panic(err)
	}
	return out
}

// Keyring is the current master secret plus, during a rotation window, the
// previous ones. New signatures use the current key and name it by key id;
// verification accepts any key in the ring. See "Rotating the master secret"
// in docs/agents/access-tokens.md.
type Keyring struct {
	keys []rootKey // keys[0] is current
}

// NewKeyring builds a ring from the current secret and any previous ones.
// Empty previous secrets are skipped.
func NewKeyring(current []byte, previous ...[]byte) *Keyring {
	ring := &Keyring{keys: []rootKey{newRootKey(current)}}
	for _, secret := range previous {
		if len(secret) == 0 {
			continue
		}
		ring.keys = append(ring.keys, newRootKey(secret))
	}
	return ring
}

func (r *Keyring) current() rootKey { return r.keys[0] }

// CurrentKeyID names the current master secret without revealing it.
func (r *Keyring) CurrentKeyID() string { return r.current().id }

func (r *Keyring) byID(id string) (rootKey, bool) {
	for _, k := range r.keys {
		if k.id == id {
			return k, true
		}
	}
	return rootKey{}, false
}
