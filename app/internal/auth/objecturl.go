package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

// objectDownloadKind domain-separates object download link MACs from the
// token MACs produced by sign: those cover base64 bodies, which can never
// contain the "\n" separators used here.
const objectDownloadKind = "object-download"

// SignObjectDownload returns the signature for a temporary object download
// link covering the file ID, the (optional) Drive source account, and the
// expiry: "<kid>.<mac>", an HMAC under the current key's object-download
// subkey, named by key id so it keeps verifying through a rotation window.
func (s *Service) SignObjectDownload(fileID, account string, exp time.Time) string {
	key := s.keys.current()
	return key.id + "." + base64.RawURLEncoding.EncodeToString(objectDownloadMAC(key.sub[purposeObjectDownload], fileID, account, exp.Unix()))
}

// VerifyObjectDownload checks the signature and expiry of a download link's
// exp/sig query parameters. account binds the link to a specific Drive source
// account so a valid link for one account cannot be replayed against another.
// A signature without a key id is a link from before key ids, an HMAC under
// the raw master secret; those live an hour at most.
func (s *Service) VerifyObjectDownload(fileID, account, expRaw, sig string) error {
	if fileID == "" || sig == "" {
		return errors.New("missing file id or signature")
	}
	expUnix, err := strconv.ParseInt(expRaw, 10, 64)
	if err != nil {
		return errors.New("malformed expiry")
	}
	var candidates [][]byte
	if kid, mac, ok := strings.Cut(sig, "."); ok {
		key, known := s.keys.byID(kid)
		if !known {
			return errors.New("signed by an unknown key")
		}
		candidates, sig = [][]byte{key.sub[purposeObjectDownload]}, mac
	} else {
		for _, key := range s.keys.keys {
			candidates = append(candidates, key.secret)
		}
	}
	provided, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return errors.New("malformed signature")
	}
	matched := false
	for _, candidate := range candidates {
		if hmac.Equal(provided, objectDownloadMAC(candidate, fileID, account, expUnix)) {
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

func objectDownloadMAC(key []byte, fileID, account string, expUnix int64) []byte {
	mac := hmac.New(sha256.New, key)
	if account == "" {
		mac.Write([]byte(objectDownloadKind + "\n" + fileID + "\n" + strconv.FormatInt(expUnix, 10)))
	} else {
		mac.Write([]byte(objectDownloadKind + "\n" + fileID + "\n" + account + "\n" + strconv.FormatInt(expUnix, 10)))
	}
	return mac.Sum(nil)
}
