package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func withFakeSlack(t *testing.T, code int) *capturedLocal {
	t.Helper()
	prev := slackRun
	cap := &capturedLocal{}
	slackRun = fakeLocal(cap, code)
	t.Cleanup(func() { slackRun = prev })
	return cap
}

func TestSlackForwardsVerbAndFlagsToTheNativePublisher(t *testing.T) {
	cap := withFakeSlack(t, 0)
	var out, errBuf bytes.Buffer
	code := runSlack([]string{"publish-session", "--team-id", "T0266FRGM", "--dry-run"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "https://w.example", "tok")
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	want := []string{"publish-session", "--team-id", "T0266FRGM", "--dry-run"}
	if !cap.called || !reflect.DeepEqual(cap.args, want) {
		t.Fatalf("args = %v, want %v", cap.args, want)
	}
	if cap.cfg.BaseURL != "https://w.example" || cap.cfg.Token != "tok" {
		t.Fatalf("cfg = %+v", cap.cfg)
	}
}

func TestSlackRequiresSubcommandAndPrintsHelp(t *testing.T) {
	cap := withFakeSlack(t, 0)
	var out, errBuf bytes.Buffer
	if code := runSlack(nil, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", ""); code != 2 || cap.called {
		t.Fatalf("no-args exit=%d called=%v", code, cap.called)
	}
	if !strings.Contains(errBuf.String(), "publish-session") {
		t.Fatalf("stderr did not name the subcommand: %s", errBuf.String())
	}
	out.Reset()
	if code := runSlack([]string{"--help"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil), "", ""); code != 0 || cap.called {
		t.Fatalf("--help exit=%d called=%v", code, cap.called)
	}
	if !strings.Contains(out.String(), "sends nothing to Slack") || strings.Contains(out.String(), "Always Allow") {
		t.Fatalf("help: %s", out.String())
	}
}

// Slack flagged the session's polling volume and polling went back to the
// workspace token on 2026-10-03; the help kept saying the warehouse polls with
// the session every five minutes. It now says what the session is for.
func TestSlackHelpDoesNotClaimTheSessionPolls(t *testing.T) {
	if strings.Contains(slackUsage, "with this session every") {
		t.Fatalf("help still says the warehouse polls with the session: %s", slackUsage)
	}
	for _, want := range []string{"workspace token", "SLACK_ASSET_FRESHNESS_USE_SESSION"} {
		if !strings.Contains(slackUsage, want) {
			t.Fatalf("help lacks %q: %s", want, slackUsage)
		}
	}
}

func TestRunDispatchesSlackWithoutAPIConfig(t *testing.T) {
	cap := withFakeSlack(t, 3)
	var out, errBuf bytes.Buffer
	code := run([]string{"slack", "publish-session"}, strings.NewReader(""), &out, &errBuf, isolatedEnv(t, nil))
	if code != 3 || !cap.called {
		t.Fatalf("exit=%d called=%v stderr=%s", code, cap.called, errBuf.String())
	}
}
