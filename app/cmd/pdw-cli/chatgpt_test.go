package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func withFakeChatGPT(t *testing.T, code int) *capturedLocal {
	t.Helper()
	prev := chatgptRun
	cap := &capturedLocal{}
	chatgptRun = fakeLocal(cap, code)
	t.Cleanup(func() { chatgptRun = prev })
	return cap
}

func TestChatGPTForwardsVerbAndFlagsToTheNativePublisher(t *testing.T) {
	cap := withFakeChatGPT(t, 0)
	var out, errBuf bytes.Buffer
	code := runChatGPT([]string{"publish-session", "--browser", "brave", "--dry-run"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", "")
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	want := []string{"publish-session", "--browser", "brave", "--dry-run"}
	if !cap.called || !reflect.DeepEqual(cap.args, want) {
		t.Fatalf("args = %v, want %v", cap.args, want)
	}
}

func TestChatGPTPassesWarehouseConfig(t *testing.T) {
	cap := withFakeChatGPT(t, 0)
	var out, errBuf bytes.Buffer
	runChatGPT([]string{"publish-session"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "https://warehouse.example", "secret-token")
	if cap.cfg.BaseURL != "https://warehouse.example" || cap.cfg.Token != "secret-token" {
		t.Fatalf("cfg = %+v", cap.cfg)
	}
}

func TestChatGPTPropagatesExitCode(t *testing.T) {
	withFakeChatGPT(t, 1)
	var out, errBuf bytes.Buffer
	if code := runChatGPT([]string{"publish-session"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", ""); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
}

func TestChatGPTRequiresSubcommand(t *testing.T) {
	cap := withFakeChatGPT(t, 0)
	var out, errBuf bytes.Buffer
	code := runChatGPT(nil, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", "")
	if code != 2 || cap.called {
		t.Fatalf("exit code = %d called=%v, want 2 and no dispatch", code, cap.called)
	}
}

func TestChatGPTHelp(t *testing.T) {
	cap := withFakeChatGPT(t, 0)
	var out, errBuf bytes.Buffer
	code := runChatGPT([]string{"--help"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", "")
	if code != 0 || cap.called {
		t.Fatalf("exit code = %d called=%v", code, cap.called)
	}
	if !strings.Contains(out.String(), "publish-session") {
		t.Fatal("help should mention publish-session")
	}
	if strings.Contains(chatgptUsage, "PDW_UV_BIN") {
		t.Fatal("usage still documents the uv launcher")
	}
}

func TestChatGPTUnknownVerbIsRefusedByThePublisher(t *testing.T) {
	// The real publisher owns the verb check; pdw forwards it unchanged.
	var out, errBuf bytes.Buffer
	code := runChatGPT([]string{"bogus"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", "")
	if code != 2 || !strings.Contains(errBuf.String(), "bogus") {
		t.Fatalf("exit=%d stderr=%s", code, errBuf.String())
	}
}
