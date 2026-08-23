package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestWhoopForwardsToModule(t *testing.T) {
	cap := withStubIngestExec(t, 0)
	var out, errBuf bytes.Buffer

	code := runWhoop(
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
	want := []string{"run", "python", "-m", whoopModule, "publish-session", "--browser", "chrome", "--dry-run"}
	if strings.Join(cap.argv, " ") != strings.Join(want, " ") {
		t.Fatalf("argv = %v, want %v", cap.argv, want)
	}
}

func TestWhoopWithoutASubcommandExplainsItself(t *testing.T) {
	var out, errBuf bytes.Buffer

	code := runWhoop(nil, strings.NewReader(""), &out, &errBuf, func(string) string { return "" }, "", "")

	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
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

	code := runWhoop([]string{"--help"}, strings.NewReader(""), &out, &errBuf, func(string) string { return "" }, "", "")

	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	help := out.String()
	for _, want := range []string{"app.whoop.com", "30-day", "publish-session"} {
		if !strings.Contains(help, want) {
			t.Fatalf("help missing %q", want)
		}
	}
}
