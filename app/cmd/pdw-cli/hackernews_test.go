package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestHackerNewsForwardsToModule(t *testing.T) {
	cap := withStubIngestExec(t, 0)
	var out, errBuf bytes.Buffer
	code := runHackerNews(
		[]string{"publish-session", "--browser", "chrome", "--dry-run"},
		strings.NewReader(""), &out, &errBuf,
		func(string) string { return "" },
		"", "",
	)
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	if !cap.called {
		t.Fatal("expected uv exec to be invoked")
	}
	want := []string{"run", "python", "-m", hackerNewsModule, "publish-session", "--browser", "chrome", "--dry-run"}
	if strings.Join(cap.argv, " ") != strings.Join(want, " ") {
		t.Fatalf("argv = %v, want %v", cap.argv, want)
	}
}

func TestHackerNewsPassesWarehouseConfig(t *testing.T) {
	cap := withStubIngestExec(t, 0)
	var out, errBuf bytes.Buffer
	runHackerNews([]string{"publish-session"}, strings.NewReader(""), &out, &errBuf,
		func(string) string { return "" }, "https://warehouse.example", "secret-token")
	if v, ok := envValue(cap.extraEnv, "PDW_API_URL"); !ok || v != "https://warehouse.example" {
		t.Fatalf("PDW_API_URL = %q ok=%v", v, ok)
	}
	if v, ok := envValue(cap.extraEnv, "PDW_SECRET_TOKEN"); !ok || v != "secret-token" {
		t.Fatalf("PDW_SECRET_TOKEN = %q ok=%v", v, ok)
	}
}

func TestHackerNewsRequiresSubcommand(t *testing.T) {
	withStubIngestExec(t, 0)
	var out, errBuf bytes.Buffer
	code := runHackerNews(nil, strings.NewReader(""), &out, &errBuf, func(string) string { return "" }, "", "")
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
}

func TestHackerNewsHelp(t *testing.T) {
	withStubIngestExec(t, 0)
	var out, errBuf bytes.Buffer
	code := runHackerNews([]string{"--help"}, strings.NewReader(""), &out, &errBuf, func(string) string { return "" }, "", "")
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	if !strings.Contains(out.String(), "publish-session") {
		t.Fatal("help should mention publish-session")
	}
}
