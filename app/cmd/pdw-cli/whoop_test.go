package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func withFakeWhoop(t *testing.T, code int) *capturedLocal {
	t.Helper()
	prev := whoopRun
	cap := &capturedLocal{}
	whoopRun = fakeLocal(cap, code)
	t.Cleanup(func() { whoopRun = prev })
	return cap
}

func TestWhoopForwardsVerbAndFlagsToTheNativePublisher(t *testing.T) {
	cap := withFakeWhoop(t, 0)
	var out, errBuf bytes.Buffer
	code := runWhoop([]string{"publish-session", "--browser", "chrome", "--dry-run"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, map[string]string{"PDW_API_URL": "https://w.example", "PDW_SECRET_TOKEN": "t"}), "", "")
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

func TestWhoopWithoutASubcommandExplainsItself(t *testing.T) {
	cap := withFakeWhoop(t, 0)
	var out, errBuf bytes.Buffer
	code := runWhoop(nil, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", "")
	if code != 2 || cap.called {
		t.Fatalf("exit code = %d called=%v, want 2 and no dispatch", code, cap.called)
	}
	if !strings.Contains(errBuf.String(), "publish-session") {
		t.Fatalf("stderr did not name the subcommand: %s", errBuf.String())
	}
}

func TestWhoopHelpExplainsWhyReRunsAreRare(t *testing.T) {
	// The operational surprise with this source is that publish-session is a
	// repair tool, not a routine one; the help has to say so or it invites an
	// hourly LaunchAgent nobody needs.
	var out, errBuf bytes.Buffer
	code := runWhoop([]string{"--help"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", "")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	help := out.String()
	for _, want := range []string{"app.whoop.com", "30-day", "publish-session"} {
		if !strings.Contains(help, want) {
			t.Fatalf("help missing %q", want)
		}
	}
	if strings.Contains(help, "PDW_UV_BIN") {
		t.Fatal("usage still documents the uv launcher")
	}
}
