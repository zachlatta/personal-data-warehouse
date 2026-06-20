package auth

import (
	"strconv"
	"testing"
	"time"
)

func objectURLTestService(now time.Time) *Service {
	return NewService([]byte("0123456789abcdef0123456789abcdef"), func() time.Time { return now })
}

func TestObjectDownloadSignVerifyRoundTrip(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	svc := objectURLTestService(now)
	exp := now.Add(time.Hour)
	sig := svc.SignObjectDownload("file-1", "", exp)
	if err := svc.VerifyObjectDownload("file-1", "", strconv.FormatInt(exp.Unix(), 10), sig); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestObjectDownloadVerifyExpired(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	svc := objectURLTestService(now)
	exp := now.Add(-time.Second)
	sig := svc.SignObjectDownload("file-1", "", exp)
	if err := svc.VerifyObjectDownload("file-1", "", strconv.FormatInt(exp.Unix(), 10), sig); err == nil {
		t.Fatal("expected expired link to be rejected")
	}
}

func TestObjectDownloadVerifyTamperedSig(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	svc := objectURLTestService(now)
	exp := now.Add(time.Hour)
	sig := svc.SignObjectDownload("file-1", "", exp)
	tampered := "A" + sig[1:]
	if tampered == sig {
		tampered = "B" + sig[1:]
	}
	if err := svc.VerifyObjectDownload("file-1", "", strconv.FormatInt(exp.Unix(), 10), tampered); err == nil {
		t.Fatal("expected tampered signature to be rejected")
	}
}

func TestObjectDownloadVerifyWrongFileID(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	svc := objectURLTestService(now)
	exp := now.Add(time.Hour)
	sig := svc.SignObjectDownload("file-1", "", exp)
	if err := svc.VerifyObjectDownload("file-2", "", strconv.FormatInt(exp.Unix(), 10), sig); err == nil {
		t.Fatal("expected signature for other file to be rejected")
	}
}

func TestObjectDownloadVerifyTamperedExp(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	svc := objectURLTestService(now)
	exp := now.Add(-time.Hour)
	sig := svc.SignObjectDownload("file-1", "", exp)
	later := strconv.FormatInt(now.Add(time.Hour).Unix(), 10)
	if err := svc.VerifyObjectDownload("file-1", "", later, sig); err == nil {
		t.Fatal("expected extended expiry to be rejected")
	}
}

func TestObjectDownloadAccountBinding(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	svc := objectURLTestService(now)
	exp := now.Add(time.Hour)
	expRaw := strconv.FormatInt(exp.Unix(), 10)

	sig := svc.SignObjectDownload("file-1", "zach@hackclub.com", exp)
	if err := svc.VerifyObjectDownload("file-1", "zach@hackclub.com", expRaw, sig); err != nil {
		t.Fatalf("verify with matching account: %v", err)
	}
	// A link signed for one account must not verify against another.
	if err := svc.VerifyObjectDownload("file-1", "zach@zachlatta.com", expRaw, sig); err == nil {
		t.Fatal("expected account-swapped link to be rejected")
	}
	// Nor against the empty (single-store) account.
	if err := svc.VerifyObjectDownload("file-1", "", expRaw, sig); err == nil {
		t.Fatal("expected account link to be rejected when account dropped")
	}
	// Empty-account links stay distinct from account-bound ones.
	plain := svc.SignObjectDownload("file-1", "", exp)
	if plain == sig {
		t.Fatal("expected account binding to change the signature")
	}
}

func TestObjectDownloadVerifyMalformedInputs(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	svc := objectURLTestService(now)
	exp := now.Add(time.Hour)
	sig := svc.SignObjectDownload("file-1", "", exp)
	expRaw := strconv.FormatInt(exp.Unix(), 10)
	for name, args := range map[string][3]string{
		"empty file id": {"", expRaw, sig},
		"empty sig":     {"file-1", expRaw, ""},
		"malformed exp": {"file-1", "not-a-number", sig},
		"malformed sig": {"file-1", expRaw, "!!!not-base64url!!!"},
	} {
		if err := svc.VerifyObjectDownload(args[0], "", args[1], args[2]); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
