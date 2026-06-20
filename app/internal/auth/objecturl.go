package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"time"
)

// objectDownloadKind domain-separates object download link MACs from the
// token MACs produced by sign: those cover base64 bodies, which can never
// contain the "\n" separators used here.
const objectDownloadKind = "object-download"

// SignObjectDownload returns the base64url HMAC for a temporary object
// download link covering the file ID, the (optional) Drive source account, and
// the expiry. An empty account preserves the original single-store MAC so
// existing links and callers keep verifying.
func (s *Service) SignObjectDownload(fileID, account string, exp time.Time) string {
	return base64.RawURLEncoding.EncodeToString(s.objectDownloadMAC(fileID, account, exp.Unix()))
}

// VerifyObjectDownload checks the signature and expiry of a download link's
// exp/sig query parameters. account binds the link to a specific Drive source
// account so a valid link for one account cannot be replayed against another.
func (s *Service) VerifyObjectDownload(fileID, account, expRaw, sig string) error {
	if fileID == "" || sig == "" {
		return errors.New("missing file id or signature")
	}
	expUnix, err := strconv.ParseInt(expRaw, 10, 64)
	if err != nil {
		return errors.New("malformed expiry")
	}
	provided, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return errors.New("malformed signature")
	}
	if !hmac.Equal(provided, s.objectDownloadMAC(fileID, account, expUnix)) {
		return errors.New("signature mismatch")
	}
	if s.now().Unix() > expUnix {
		return errors.New("link expired")
	}
	return nil
}

func (s *Service) objectDownloadMAC(fileID, account string, expUnix int64) []byte {
	mac := hmac.New(sha256.New, s.secret)
	if account == "" {
		mac.Write([]byte(objectDownloadKind + "\n" + fileID + "\n" + strconv.FormatInt(expUnix, 10)))
	} else {
		mac.Write([]byte(objectDownloadKind + "\n" + fileID + "\n" + account + "\n" + strconv.FormatInt(expUnix, 10)))
	}
	return mac.Sum(nil)
}
