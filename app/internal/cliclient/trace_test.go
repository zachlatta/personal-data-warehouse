package cliclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientSendsTraceID(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get("X-Trace-ID"))
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"data":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer srv.Close()
	c, err := New(srv.URL, "porygon", "token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CallTool(context.Background(), "x", nil); err != nil {
		t.Fatal(err)
	}
	c.SetTraceID("porygon-claude-300-abcd")
	if _, err := c.ListTools(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CallTool(context.Background(), "x", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != "" || got[1] != "porygon-claude-300-abcd" || got[2] != "porygon-claude-300-abcd" {
		t.Fatalf("X-Trace-ID per request = %q", got)
	}
}
