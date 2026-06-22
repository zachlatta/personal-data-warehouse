package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestChatGPTForwardsToModule(t *testing.T) {
	cap := withStubIngestExec(t, 0)
	var out, errBuf bytes.Buffer
	code := runChatGPT(
		[]string{"publish-session", "--browser", "brave", "--dry-run"},
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
	want := []string{"run", "python", "-m", chatgptModule, "publish-session", "--browser", "brave", "--dry-run"}
	if strings.Join(cap.argv, " ") != strings.Join(want, " ") {
		t.Fatalf("argv = %v, want %v", cap.argv, want)
	}
}

func TestChatGPTPassesWarehouseConfig(t *testing.T) {
	cap := withStubIngestExec(t, 0)
	var out, errBuf bytes.Buffer
	getenv := func(k string) string { return "" }
	runChatGPT([]string{"publish-session"}, strings.NewReader(""), &out, &errBuf, getenv,
		"https://warehouse.example", "secret-token")

	if v, ok := envValue(cap.extraEnv, "PDW_API_URL"); !ok || v != "https://warehouse.example" {
		t.Fatalf("PDW_API_URL = %q ok=%v", v, ok)
	}
	if v, ok := envValue(cap.extraEnv, "PDW_SECRET_TOKEN"); !ok || v != "secret-token" {
		t.Fatalf("PDW_SECRET_TOKEN = %q ok=%v", v, ok)
	}
}

func TestChatGPTRequiresSubcommand(t *testing.T) {
	withStubIngestExec(t, 0)
	var out, errBuf bytes.Buffer
	code := runChatGPT(nil, strings.NewReader(""), &out, &errBuf, func(string) string { return "" }, "", "")
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
}

func TestChatGPTHelp(t *testing.T) {
	withStubIngestExec(t, 0)
	var out, errBuf bytes.Buffer
	code := runChatGPT([]string{"--help"}, strings.NewReader(""), &out, &errBuf, func(string) string { return "" }, "", "")
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	if !strings.Contains(out.String(), "publish-session") {
		t.Fatal("help should mention publish-session")
	}
}
