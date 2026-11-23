package cliclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// Twelve Muse sessions from 2026-11-14 printed `pdw search: Post
// "https://.../api/tools/search": Forbidden` -- Go's whole message when an
// HTTPS proxy refuses CONNECT -- and read it as PDW refusing them.
func TestARefusedProxyConnectSaysItIsTheProxyNotPDW(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusTeapot)
	}))
	defer proxy.Close()
	proxyURL, _ := url.Parse(proxy.URL)

	client := newClient(t, "https://pdw.example.invalid")
	client.SetHTTPClient(&http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}})
	_, err := client.CallTool(context.Background(), "search", json.RawMessage(`{"query":"x"}`))
	if err == nil {
		t.Fatal("expected the proxy refusal")
	}
	msg := err.Error()
	for _, want := range []string{"your HTTPS proxy refused the connection to pdw.example.invalid; this is not a PDW error", "Forbidden"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error = %q, want it to contain %q", msg, want)
		}
	}
}
