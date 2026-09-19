package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func withFakeHackerNews(t *testing.T, code int) *capturedLocal {
	t.Helper()
	prev := hackerNewsRun
	cap := &capturedLocal{}
	hackerNewsRun = fakeLocal(cap, code)
	t.Cleanup(func() { hackerNewsRun = prev })
	return cap
}

func TestHackerNewsForwardsVerbAndFlagsToTheNativePublisher(t *testing.T) {
	cap := withFakeHackerNews(t, 0)
	var out, errBuf bytes.Buffer
	code := runHackerNews([]string{"publish-session", "--browser", "chrome", "--dry-run"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, map[string]string{"PDW_API_URL": "https://w.example", "PDW_SECRET_TOKEN": "t"}), "", "")
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	want := []string{"publish-session", "--browser", "chrome", "--dry-run"}
	if !cap.called || !reflect.DeepEqual(cap.args, want) {
		t.Fatalf("args = %v, want %v", cap.args, want)
	}
	if cap.cfg.BaseURL != "https://w.example" || cap.cfg.Token != "t" {
		t.Fatalf("cfg = %+v", cap.cfg)
	}
}

func TestHackerNewsFlagsOverrideTheEnvironmentConfig(t *testing.T) {
	cap := withFakeHackerNews(t, 0)
	var out, errBuf bytes.Buffer
	runHackerNews([]string{"publish-session"}, strings.NewReader(""), &out, &errBuf,
		isolatedEnv(t, nil), "https://warehouse.example", "secret-token")
	if cap.cfg.BaseURL != "https://warehouse.example" || cap.cfg.Token != "secret-token" {
		t.Fatalf("cfg = %+v", cap.cfg)
	}
}

func TestHackerNewsWithoutASubcommandExplainsItself(t *testing.T) {
	cap := withFakeHackerNews(t, 0)
	var out, errBuf bytes.Buffer
	code := runHackerNews(nil, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", "")
	if code != 2 || cap.called {
		t.Fatalf("exit code = %d called=%v, want 2 and no dispatch", code, cap.called)
	}
	if !strings.Contains(errBuf.String(), "publish-session") {
		t.Fatalf("stderr did not name the subcommand: %s", errBuf.String())
	}
}

func TestHackerNewsHelpNamesNoPythonLauncher(t *testing.T) {
	cap := withFakeHackerNews(t, 0)
	var out, errBuf bytes.Buffer
	code := runHackerNews([]string{"--help"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", "")
	if code != 0 || cap.called {
		t.Fatalf("exit code = %d called=%v", code, cap.called)
	}
	help := out.String()
	if !strings.Contains(help, "publish-session") {
		t.Fatal("help should mention publish-session")
	}
	if strings.Contains(help, "PDW_UV_BIN") || strings.Contains(help, "PDW_INGEST_PROJECT_DIR") {
		t.Fatal("usage still documents the uv launcher")
	}
}
