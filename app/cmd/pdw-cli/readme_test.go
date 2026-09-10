package main

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/guide"
)

// runUnconfigured runs the CLI with no URL, token or config file, which is
// the state of a machine that has not run `pdw login` yet.
func runUnconfigured(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	env := map[string]string{"HOME": t.TempDir(), "XDG_CONFIG_HOME": t.TempDir()}
	code = run(args, strings.NewReader(""), &outBuf, &errBuf, func(k string) string { return env[k] })
	return outBuf.String(), errBuf.String(), code
}

func TestBarePdwPrintsTheGuideWithoutAServerOrToken(t *testing.T) {
	stdout, stderr, code := runUnconfigured(t)
	if code != 0 {
		t.Fatalf("bare pdw exit = %d (stderr=%s)", code, stderr)
	}
	want, _ := guide.Render(guide.SurfaceCLI, "")
	if stdout != want {
		t.Fatal("bare pdw did not print guide.Render(SurfaceCLI)")
	}
	for _, s := range []string{"pdw search", "pdw columns", "pdw login", "search first", "`pdw readme <topic>`"} {
		if !strings.Contains(strings.ToLower(stdout), strings.ToLower(s)) {
			t.Fatalf("guide does not mention %q", s)
		}
	}
	if strings.Contains(stderr, "not configured") {
		t.Fatalf("the guide must not need configuration: %s", stderr)
	}
}

func TestReadmeTopicsRenderAndUnknownTopicListsThem(t *testing.T) {
	for _, topic := range guide.Topics(guide.SurfaceCLI) {
		stdout, stderr, code := runUnconfigured(t, "readme", topic.Name)
		if code != 0 || strings.TrimSpace(stdout) == "" {
			t.Fatalf("pdw readme %s: exit %d, stderr=%s", topic.Name, code, stderr)
		}
	}
	stdout, stderr, code := runUnconfigured(t, "readme", "finances")
	if code == 0 || stdout != "" {
		t.Fatalf("unknown topic should fail on stderr only (stdout=%q)", stdout)
	}
	if !strings.Contains(stderr, "finance") || !strings.Contains(stderr, "agent-sessions") {
		t.Fatalf("unknown topic should list the real ones: %s", stderr)
	}
	if _, _, code := runUnconfigured(t, "readme", "a", "b"); code == 0 {
		t.Fatal("two topics should be rejected")
	}
}

func TestReadmeHelpListsEveryTopic(t *testing.T) {
	stdout, _, code := runUnconfigured(t, "readme", "--help")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	for _, topic := range guide.Topics(guide.SurfaceCLI) {
		if !strings.Contains(stdout, topic.Name) {
			t.Fatalf("readme --help omits %q", topic.Name)
		}
	}
	if strings.Contains(stdout, "AUTO-UPDATE") {
		t.Fatal("readme --help printed the global usage")
	}
}

func TestSchemaIsNoLongerTheDefaultCommand(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("bare pdw must not call the server")
	})
	_, _, code := runCLI(t, srv.URL, "")
	if code != 0 {
		t.Fatalf("bare pdw exit = %d", code)
	}
}

func TestCallAndDescribeRedirectReadmeToTheCommand(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("call readme must not reach the server")
	})
	_, errOut, code := runCLI(t, srv.URL, "", "call", "readme")
	if code == 0 || !strings.Contains(errOut, "pdw readme") {
		t.Fatalf("call readme should redirect: exit %d, %s", code, errOut)
	}
	for _, alias := range []string{"guide", "docs", "manual"} {
		_, errOut, code := runCLI(t, srv.URL, "", alias)
		if code == 0 || !strings.Contains(errOut, "pdw readme") {
			t.Fatalf("pdw %s should redirect to pdw readme: exit %d, %s", alias, code, errOut)
		}
	}
}

func TestUsageAndGuideAgreeOnTheTopicList(t *testing.T) {
	for _, topic := range guide.Topics(guide.SurfaceCLI) {
		if !strings.Contains(usage, topic.Name) {
			t.Fatalf("global usage omits readme topic %q", topic.Name)
		}
	}
}
