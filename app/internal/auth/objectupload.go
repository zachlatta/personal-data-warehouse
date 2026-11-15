package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"time"
)

// objectUploadKind domain-separates object upload request MACs from the object
// download MACs (objectDownloadKind) and the token MACs produced by sign.
const objectUploadKind = "object-upload"

// SignObjectUpload returns the base64url HMAC authorizing a client to upload an
// object with the given content sha to a specific ingestion endpoint until exp.
// The signature covers the endpoint (so a link cannot be replayed against a
// different endpoint) and the content sha (so the link also authenticates the
// body: the server recomputes the sha over the received bytes and rejects a
// mismatch).
//
// The key is a raw secret, not a derived subkey: uploaders sign with
// PDW_INGEST_SECRET (or, until a host moves, the master secret), in Go and in
// Python (ingest_client.py), and the "object-upload" prefix already separates
// these MACs from every other kind.
func (s *Service) SignObjectUpload(endpoint, contentSHA256 string, exp time.Time) string {
	return base64.RawURLEncoding.EncodeToString(objectUploadMAC(s.keys.current().secret, endpoint, contentSHA256, exp.Unix()))
}

// VerifyObjectUpload checks the signature and expiry of an upload request's
// endpoint, content sha, and exp/sig values. Any secret in the keyring
// verifies, so an uploader keeps working through a rotation window until its
// own secret is updated.
func (s *Service) VerifyObjectUpload(endpoint, contentSHA256, expRaw, sig string) error {
	if contentSHA256 == "" || sig == "" {
		return errors.New("missing content sha or signature")
	}
	expUnix, err := strconv.ParseInt(expRaw, 10, 64)
	if err != nil {
		return errors.New("malformed expiry")
	}
	provided, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return errors.New("malformed signature")
	}
	matched := false
	for _, key := range s.uploadKeys() {
		if hmac.Equal(provided, objectUploadMAC(key, endpoint, contentSHA256, expUnix)) {
			matched = true
			break
		}
	}
	if !matched {
		return errors.New("signature mismatch")
	}
	if s.now().Unix() > expUnix {
		return errors.New("link expired")
	}
	return nil
}

func objectUploadMAC(secret []byte, endpoint, contentSHA256 string, expUnix int64) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(objectUploadKind + "\n" + endpoint + "\n" + contentSHA256 + "\n" + strconv.FormatInt(expUnix, 10)))
	return mac.Sum(nil)
}

// SetIngestSecret adds PDW_INGEST_SECRET: a key that verifies upload
// signatures and nothing else. Uploader hosts hold it instead of the master
// secret, so what a host holds can sign uploads but never authenticate as a
// bearer, administer tokens, or decide a mutation. The master secret keeps
// verifying uploads while hosts move over.
func (s *Service) SetIngestSecret(secret []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingestSecret = append([]byte(nil), secret...)
}

func (s *Service) uploadKeys() [][]byte {
	s.mu.Lock()
	ingest := s.ingestSecret
	s.mu.Unlock()
	keys := make([][]byte, 0, len(s.keys.keys)+1)
	if len(ingest) > 0 {
		keys = append(keys, ingest)
	}
	for _, key := range s.keys.keys {
		keys = append(keys, key.secret)
	}
	return keys
}
