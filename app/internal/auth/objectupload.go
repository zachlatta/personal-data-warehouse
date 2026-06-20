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
func (s *Service) SignObjectUpload(endpoint, contentSHA256 string, exp time.Time) string {
	return base64.RawURLEncoding.EncodeToString(s.objectUploadMAC(endpoint, contentSHA256, exp.Unix()))
}

// VerifyObjectUpload checks the signature and expiry of an upload request's
// endpoint, content sha, and exp/sig values.
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
	if !hmac.Equal(provided, s.objectUploadMAC(endpoint, contentSHA256, expUnix)) {
		return errors.New("signature mismatch")
	}
	if s.now().Unix() > expUnix {
		return errors.New("link expired")
	}
	return nil
}

func (s *Service) objectUploadMAC(endpoint, contentSHA256 string, expUnix int64) []byte {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(objectUploadKind + "\n" + endpoint + "\n" + contentSHA256 + "\n" + strconv.FormatInt(expUnix, 10)))
	return mac.Sum(nil)
}
