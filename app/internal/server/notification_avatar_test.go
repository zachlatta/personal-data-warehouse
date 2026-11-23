package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNotificationAvatarSigningAndCaching(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signer := objectsTestSigner()
	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write([]byte("fake-jpeg-bytes"))
	}))
	defer upstream.Close()

	proxy := newAvatarProxy(nil, signer, nil)
	proxy.now = func() time.Time { return now }
	proxy.allowURL = func(raw string) bool { return strings.HasPrefix(raw, upstream.URL) }
	proxy.resolveURL = func(_ context.Context, key string) (string, error) {
		if key == "slack|me|T1|U1" {
			return upstream.URL + "/avatar.jpg", nil
		}
		return "", nil
	}

	signed := signAvatarURL("https://pdw.example", signer, proxy.now, "slack|me|T1|U1")
	if !strings.HasPrefix(signed, "https://pdw.example"+notificationAvatarPath+"?") {
		t.Fatalf("unexpected signed url: %s", signed)
	}
	parsed, err := url.Parse(signed)
	if err != nil {
		t.Fatal(err)
	}

	// Missing/tampered signature is rejected.
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, httptest.NewRequest(http.MethodGet, notificationAvatarPath+"?k=slack|me|T1|U1", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("unsigned request got %d, want 403", w.Code)
	}

	// Object download signature without avatar: prefix cannot replay here.
	objSig := signer.SignObjectDownload("slack|me|T1|U1", "", now.Add(time.Hour))
	w = httptest.NewRecorder()
	proxy.ServeHTTP(w, httptest.NewRequest(http.MethodGet, notificationAvatarPath+"?k=slack|me|T1|U1&exp="+parsed.Query().Get("exp")+"&sig="+objSig, nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("replayed object signature got %d, want 403", w.Code)
	}

	// Valid request fetches upstream once and caches it.
	for i := range 2 {
		w = httptest.NewRecorder()
		proxy.ServeHTTP(w, httptest.NewRequest(http.MethodGet, parsed.RequestURI(), nil))
		if w.Code != http.StatusOK || w.Body.String() != "fake-jpeg-bytes" || w.Header().Get("Content-Type") != "image/jpeg" {
			t.Fatalf("call %d: status=%d type=%q body=%q", i, w.Code, w.Header().Get("Content-Type"), w.Body.String())
		}
		if w.Header().Get("Cache-Control") != "private, max-age=3600" {
			t.Fatalf("unexpected cache-control: %q", w.Header().Get("Cache-Control"))
		}
	}
	if upstreamHits.Load() != 1 {
		t.Fatalf("expected 1 upstream fetch (cached second), got %d", upstreamHits.Load())
	}

	// Expired link is rejected.
	expired := signAvatarURL("https://pdw.example", signer, func() time.Time { return now.Add(-48 * time.Hour) }, "slack|me|T1|U1")
	expParsed, _ := url.Parse(expired)
	w = httptest.NewRecorder()
	proxy.ServeHTTP(w, httptest.NewRequest(http.MethodGet, expParsed.RequestURI(), nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("expired link got %d, want 403", w.Code)
	}
}

func TestIsAllowedAvatarURL(t *testing.T) {
	allowed := []string{
		"https://ca.slack-edge.com/T123-U123-abc-192",
		"https://avatars.slack-edge.com/2026-01-01/123_192.jpg",
		"https://lh3.googleusercontent.com/contacts/AG6tpzE123=s256-c",
		"https://secure.gravatar.com/avatar/abc",
	}
	for _, u := range allowed {
		if !isAllowedAvatarURL(u) {
			t.Errorf("expected allowed: %s", u)
		}
	}
	denied := []string{
		"http://ca.slack-edge.com/a.jpg",
		"https://localhost/a.jpg",
		"https://127.0.0.1/a.jpg",
		"https://evil.example/ca.slack-edge.com/a.jpg",
		"https://ca.slack-edge.com:8443/a.jpg",
		"https://user@ca.slack-edge.com/a.jpg",
	}
	for _, u := range denied {
		if isAllowedAvatarURL(u) {
			t.Errorf("expected denied: %s", u)
		}
	}
}
