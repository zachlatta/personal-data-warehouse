package auth

import (
	"strconv"
	"testing"
	"time"
)

func uploadTestService() *Service {
	return NewService([]byte("0123456789abcdef0123456789abcdef"), func() time.Time { return time.Unix(1_700_000_000, 0) })
}

func TestObjectUploadSignVerifyRoundTrip(t *testing.T) {
	svc := uploadTestService()
	exp := svc.now().Add(time.Hour)
	sig := svc.SignObjectUpload("/ingest/agent-sessions/batch", "abc123", exp)
	if err := svc.VerifyObjectUpload("/ingest/agent-sessions/batch", "abc123", strconv.FormatInt(exp.Unix(), 10), sig); err != nil {
		t.Fatalf("verify round trip: %v", err)
	}
}

func TestObjectUploadVerifyExpired(t *testing.T) {
	svc := uploadTestService()
	exp := svc.now().Add(-time.Second)
	sig := svc.SignObjectUpload("/ingest/agent-sessions/batch", "abc123", exp)
	if err := svc.VerifyObjectUpload("/ingest/agent-sessions/batch", "abc123", strconv.FormatInt(exp.Unix(), 10), sig); err == nil {
		t.Fatal("expected expired link to be rejected")
	}
}

func TestObjectUploadVerifyTamperedSig(t *testing.T) {
	svc := uploadTestService()
	exp := svc.now().Add(time.Hour)
	sig := svc.SignObjectUpload("/ingest/agent-sessions/batch", "abc123", exp)
	tampered := sig[:len(sig)-1] + "A"
	if tampered == sig {
		tampered = sig[:len(sig)-1] + "B"
	}
	if err := svc.VerifyObjectUpload("/ingest/agent-sessions/batch", "abc123", strconv.FormatInt(exp.Unix(), 10), tampered); err == nil {
		t.Fatal("expected tampered signature to be rejected")
	}
}

// A signature for one endpoint must not be replayable on another endpoint.
func TestObjectUploadVerifyWrongEndpoint(t *testing.T) {
	svc := uploadTestService()
	exp := svc.now().Add(time.Hour)
	sig := svc.SignObjectUpload("/ingest/agent-sessions/batch", "abc123", exp)
	if err := svc.VerifyObjectUpload("/ingest/whatsapp/media", "abc123", strconv.FormatInt(exp.Unix(), 10), sig); err == nil {
		t.Fatal("expected signature for a different endpoint to be rejected")
	}
}

// Binding to the content sha means a signature for one body cannot be reused to
// upload a different body under the same link.
func TestObjectUploadVerifyWrongContentSHA(t *testing.T) {
	svc := uploadTestService()
	exp := svc.now().Add(time.Hour)
	sig := svc.SignObjectUpload("/ingest/agent-sessions/batch", "abc123", exp)
	if err := svc.VerifyObjectUpload("/ingest/agent-sessions/batch", "different", strconv.FormatInt(exp.Unix(), 10), sig); err == nil {
		t.Fatal("expected signature for a different content sha to be rejected")
	}
}

func TestObjectUploadVerifyTamperedExp(t *testing.T) {
	svc := uploadTestService()
	exp := svc.now().Add(time.Hour)
	sig := svc.SignObjectUpload("/ingest/agent-sessions/batch", "abc123", exp)
	later := strconv.FormatInt(exp.Add(time.Hour).Unix(), 10)
	if err := svc.VerifyObjectUpload("/ingest/agent-sessions/batch", "abc123", later, sig); err == nil {
		t.Fatal("expected tampered expiry to be rejected")
	}
}

// TestObjectUploadKnownAnswer pins the wire format so the Go signer and the
// Python ingest client (personal_data_warehouse.ingest_client.sign_object_upload)
// stay byte-identical. The same constant is asserted in the Python test suite.
func TestObjectUploadKnownAnswer(t *testing.T) {
	svc := NewService([]byte("0123456789abcdef0123456789abcdef"), func() time.Time { return time.Unix(0, 0) })
	got := svc.SignObjectUpload("/ingest/agent-sessions/batch", "abc123", time.Unix(1700003600, 0))
	const want = "vmGZqtDVzN69EfjqxTokbJxrbFgrzknfRCyTZQVbjxk"
	if got != want {
		t.Fatalf("known-answer signature = %q, want %q (Go/Python signing diverged)", got, want)
	}
}

func TestObjectUploadVerifyMalformedInputs(t *testing.T) {
	svc := uploadTestService()
	exp := svc.now().Add(time.Hour)
	sig := svc.SignObjectUpload("/ingest/agent-sessions/batch", "abc123", exp)
	for _, args := range [][3]string{
		{"abc123", "not-a-number", sig},
		{"abc123", strconv.FormatInt(exp.Unix(), 10), "!!!not-base64!!!"},
		{"", strconv.FormatInt(exp.Unix(), 10), sig},
	} {
		if err := svc.VerifyObjectUpload("/ingest/agent-sessions/batch", args[0], args[1], args[2]); err == nil {
			t.Errorf("expected malformed inputs %v to be rejected", args)
		}
	}
}
