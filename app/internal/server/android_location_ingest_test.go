package server

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestLocationReceiptIsAuthenticatedServerTime(t *testing.T) {
	var b bytes.Buffer
	z := gzip.NewWriter(&b)
	z.Write([]byte(`{"source":"android_location","record_type":"location","received_at":"forged","record":{"sample_id":"fake"}}`))
	z.Close()
	svc, stores := ingestTestService()
	svc.maxBytes = 4096
	target := signedIngestTarget("/ingest/android-location/batch", b.Bytes(), url.Values{"exported_at": {"2026-11-07T00:00:00Z"}})
	w := httptest.NewRecorder()
	svc.handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, target, bytes.NewReader(b.Bytes())))
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	put := stores["android_location"].lastFile
	if put == nil {
		t.Fatal("not stored")
	}
	zr, err := gzip.NewReader(bytes.NewReader(put.Content))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(zr)
	var row map[string]any
	json.Unmarshal(raw, &row)
	if row["received_at"] != objectsTestNow.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("receipt %v", row)
	}
	w = httptest.NewRecorder()
	svc.handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/ingest/android-location/batch", bytes.NewReader(b.Bytes())))
	if w.Code != 403 {
		t.Fatalf("unsigned accepted: %d", w.Code)
	}
}

func TestLocationReceiptRejectsMalformedAndExpandedOversize(t *testing.T) {
	if _, err := stampLocationReceipt([]byte("not gzip"), objectsTestNow, 1024); err == nil {
		t.Fatal("bad gzip accepted")
	}
	var b bytes.Buffer
	z := gzip.NewWriter(&b)
	z.Write(bytes.Repeat([]byte("x"), 2048))
	z.Close()
	if _, err := stampLocationReceipt(b.Bytes(), objectsTestNow, 1024); err == nil {
		t.Fatal("expanded limit not enforced")
	}
}
