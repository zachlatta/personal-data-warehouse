package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newDownloadStub serves both halves of the download flow: the get_object
// tool call and the signed /objects/ URL the tool result points at.
func newDownloadStub(t *testing.T, obj map[string]any, content []byte) *stubServer {
	t.Helper()
	var s *stubServer
	s = newStubServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/tools/get_object":
			payload := map[string]any{}
			for k, v := range obj {
				payload[k] = v
			}
			if _, ok := payload["download_url"]; !ok {
				payload["download_url"] = s.URL + "/objects/fid?exp=1&sig=test"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": payload})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/objects/"):
			_, _ = w.Write(content)
		default:
			http.NotFound(w, r)
		}
	})
	return s
}

func TestDownloadWritesFile(t *testing.T) {
	content := []byte("attachment bytes")
	sum := sha256.Sum256(content)
	srv := newDownloadStub(t, map[string]any{
		"exists":         true,
		"filename":       "receipt.pdf",
		"content_sha256": hex.EncodeToString(sum[:]),
	}, content)
	dest := filepath.Join(t.TempDir(), "out.pdf")
	stdout, stderr, code := runCLI(t, srv.URL, "", "download", "fid", "--output", dest)
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("content = %q", got)
	}
	want := fmt.Sprintf("saved %s (%d bytes)\n", dest, len(content))
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

func TestDownloadDefaultsToObjectFilename(t *testing.T) {
	content := []byte("data")
	srv := newDownloadStub(t, map[string]any{
		"exists":   true,
		"filename": "../weird/memo.m4a",
	}, content)
	dir := t.TempDir()
	cwd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	_, stderr, code := runCLI(t, srv.URL, "", "download", "fid")
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	if _, err := os.Stat(filepath.Join(dir, "memo.m4a")); err != nil {
		t.Fatalf("expected memo.m4a in cwd: %v", err)
	}
}

func TestDownloadHashMismatch(t *testing.T) {
	srv := newDownloadStub(t, map[string]any{
		"exists":         true,
		"filename":       "receipt.pdf",
		"content_sha256": strings.Repeat("0", 64),
	}, []byte("attachment bytes"))
	dest := filepath.Join(t.TempDir(), "out.pdf")
	_, stderr, code := runCLI(t, srv.URL, "", "download", "fid", "--output", dest)
	if code != 1 || !strings.Contains(stderr, "hash mismatch") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("mismatched download must not be written: %v", err)
	}
}

func TestDownloadMissingObject(t *testing.T) {
	srv := newDownloadStub(t, map[string]any{"exists": false}, nil)
	_, stderr, code := runCLI(t, srv.URL, "", "download", "gone")
	if code != 1 || !strings.Contains(stderr, "no stored object") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
}

func TestDownloadRequiresFileID(t *testing.T) {
	srv := newDownloadStub(t, nil, nil)
	_, stderr, code := runCLI(t, srv.URL, "", "download")
	if code != 2 || !strings.Contains(stderr, "storage_file_id is required") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
}
