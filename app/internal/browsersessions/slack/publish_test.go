package slack_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/slack"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolveTeamID(t *testing.T) {
	lookup := func(ids ...string) slack.WorkspaceLookup {
		return func(string) ([]string, error) { return ids, nil }
	}
	if got, err := slack.ResolveTeamID("", "E09V59WQY1E", lookup("T0266FRGM")); err != nil || got != "T0266FRGM" {
		t.Fatalf("got %q, %v", got, err)
	}
	if got, _ := slack.ResolveTeamID("T0266FRGM", "", lookup("T_OTHER")); got != "T0266FRGM" {
		t.Fatalf("a session naming a workspace is left alone, got %q", got)
	}
	if _, err := slack.ResolveTeamID("", "E1", lookup("T1", "T2")); err == nil || !strings.Contains(err.Error(), "more than one workspace") {
		t.Fatalf("err = %v", err)
	}
	if _, err := slack.ResolveTeamID("", "E_UNKNOWN", lookup()); err == nil || !strings.Contains(err.Error(), "no workspace") {
		t.Fatalf("err = %v", err)
	}
	if _, err := slack.ResolveTeamID("", "", lookup()); err == nil || !strings.Contains(err.Error(), "neither") {
		t.Fatalf("err = %v", err)
	}
	failing := func(string) ([]string, error) { return nil, errors.New("boom") }
	if _, err := slack.ResolveTeamID("", "E1", failing); err == nil || !strings.Contains(err.Error(), "workspace lookup failed: boom") {
		t.Fatalf("err = %v", err)
	}
}

func TestWorkspaceLookupRequiresCredentials(t *testing.T) {
	_, err := slack.WorkspaceIDsForEnterprise(ingestclient.Config{}, "")("E1")
	if err == nil || !strings.Contains(err.Error(), "pdw login") {
		t.Fatalf("err = %v", err)
	}
}

func TestResolveAccount(t *testing.T) {
	if got := slack.ResolveAccount("", envOf(map[string]string{"SLACK_ACCOUNTS": "zrl", "AGENT_SESSIONS_ACCOUNT": "zach@hackclub.com"})); got != "zrl" {
		t.Fatalf("got %q", got)
	}
	if got := slack.ResolveAccount("", envOf(map[string]string{"SLACK_ACCOUNTS": "zrl, other"})); got != "zrl" {
		t.Fatalf("got %q", got)
	}
	if got := slack.ResolveAccount("chosen", envOf(map[string]string{"SLACK_ACCOUNTS": "zrl"})); got != "chosen" {
		t.Fatalf("got %q", got)
	}
	if got := slack.ResolveAccount("", envOf(map[string]string{"GMAIL_ACCOUNTS": "a@b.com,c@d.com"})); got != "a@b.com" {
		t.Fatalf("got %q", got)
	}
	if got := slack.ResolveAccount("", envOf(nil)); got != "zrl" {
		t.Fatalf("got %q", got)
	}
}

type fakeDeps struct {
	deps      slack.Deps
	published *ingestclient.SlackSession
}

func newDeps(session slack.Session, discoverErr error, probe map[string]any, workspaces []string) *fakeDeps {
	f := &fakeDeps{}
	f.deps = slack.Deps{
		Discover: func(source string) (slack.Session, error) { return session, discoverErr },
		Probe:    func(slack.Session) map[string]any { return probe },
		Workspaces: func(string) ([]string, error) {
			return workspaces, nil
		},
		KnownWorkspace: func(teamID string) (bool, error) {
			for _, known := range workspaces {
				if known == teamID {
					return true, nil
				}
			}
			return false, nil
		},
		Publisher: func() (slack.Publisher, error) {
			return func(s ingestclient.SlackSession) (map[string]any, error) {
				f.published = &s
				return map[string]any{"token_sha256": "abc"}, nil
			}, nil
		},
	}
	return f
}

var liveSession = slack.Session{Source: "slack-app", Token: "xoxc-supersecrettoken", CookieD: "xoxd-supersecretcookie",
	EnterpriseID: "E09V59WQY1E", UserID: "U09UE480JHH", TeamURL: "https://hackclub.slack.com/"}

var okProbe = map[string]any{"ok": true, "channels": 812, "ims": 3627, "mpims": 2788, "total_conversations": 7227, "with_latest_marker": 7227}

func run(t *testing.T, deps slack.Deps, env map[string]string, args ...string) (int, map[string]any, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := slack.RunWith(args, &stdout, &stderr, envOf(env), deps)
	var report map[string]any
	if stdout.Len() > 0 {
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
			t.Fatalf("stdout is not JSON: %s", stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "supersecret") || strings.Contains(stderr.String(), "supersecret") {
		t.Fatalf("secret leaked: %s %s", stdout.String(), stderr.String())
	}
	return code, report, stderr.String()
}

func TestRunPublishesResolvingTheWorkspaceFromTheEnterprise(t *testing.T) {
	f := newDeps(liveSession, nil, okProbe, []string{"T0266FRGM"})
	code, report, _ := run(t, f.deps, map[string]string{"SLACK_ACCOUNTS": "zrl"}, "publish-session")
	if code != 0 {
		t.Fatalf("code = %d report = %v", code, report)
	}
	if report["published"] != true || report["team_id"] != "T0266FRGM" || report["account"] != "zrl" {
		t.Fatalf("report = %v", report)
	}
	if f.published == nil || f.published.TeamID != "T0266FRGM" || f.published.EnterpriseID != "E09V59WQY1E" ||
		f.published.SessionToken != "xoxc-supersecrettoken" || f.published.SessionCookie != "xoxd-supersecretcookie" ||
		f.published.SessionKey != "default" || f.published.SourceApp != "slack-app" || f.published.CookieExpiresAt != "" {
		t.Fatalf("published = %+v", f.published)
	}
	session := report["session"].(map[string]any)
	if session["enterprise_id"] != "E09V59WQY1E" || session["token_sha256"] != liveSession.Fingerprint() {
		t.Fatalf("session = %v", session)
	}
}

func TestRunDryRunValidatesWithoutPublishing(t *testing.T) {
	f := newDeps(liveSession, nil, okProbe, nil)
	f.deps.Publisher = func() (slack.Publisher, error) { t.Fatal("must not publish"); return nil, nil }
	code, report, _ := run(t, f.deps, nil, "publish-session", "--dry-run")
	if code != 0 || report["published"] != false || report["client_counts"].(map[string]any)["total_conversations"] != float64(7227) {
		t.Fatalf("code = %d report = %v", code, report)
	}
}

func TestRunExplicitFlagsWin(t *testing.T) {
	f := newDeps(liveSession, nil, okProbe, []string{"T1", "T2"})
	code, report, _ := run(t, f.deps, nil, "publish-session", "--team-id", "T2", "--account", "chosen", "--session-key", "alt")
	if code != 0 || report["team_id"] != "T2" || f.published.Account != "chosen" || f.published.SessionKey != "alt" {
		t.Fatalf("code = %d report = %v published = %+v", code, report, f.published)
	}
}

func TestRunExitCodes(t *testing.T) {
	t.Run("capture failure is 1 with the error", func(t *testing.T) {
		f := newDeps(slack.Session{}, &slack.CaptureError{Msg: "found no xoxc- token"}, nil, nil)
		code, report, _ := run(t, f.deps, nil, "publish-session")
		if code != 1 || report["error"] != "found no xoxc- token" || report["likely_cause"] != nil {
			t.Fatalf("code = %d report = %v", code, report)
		}
	})
	t.Run("a keychain prompt timeout explains itself", func(t *testing.T) {
		f := newDeps(slack.Session{}, errors.New("could not run `security` to read Slack Safe Storage: timed out after 60 seconds"), nil, nil)
		code, report, _ := run(t, f.deps, nil, "publish-session")
		blob := strings.ToLower(report["likely_cause"].(string) + report["fix"].(string))
		if code != 1 || !strings.Contains(blob, "keychain") || !strings.Contains(blob, "always allow") || !strings.Contains(blob, "locked") {
			t.Fatalf("code = %d report = %v", code, report)
		}
	})
	t.Run("client.counts failure is 2", func(t *testing.T) {
		f := newDeps(liveSession, nil, map[string]any{"ok": false, "error": "not_allowed_token_type"}, nil)
		code, report, _ := run(t, f.deps, nil, "publish-session")
		if code != 2 || report["client_counts"].(map[string]any)["error"] != "not_allowed_token_type" || report["published"] != false {
			t.Fatalf("code = %d report = %v", code, report)
		}
	})
	t.Run("ambiguous enterprise is 3", func(t *testing.T) {
		f := newDeps(liveSession, nil, okProbe, []string{"T1", "T2"})
		code, report, _ := run(t, f.deps, nil, "publish-session")
		if code != 3 || !strings.Contains(report["error"].(string), "more than one workspace") || f.published != nil {
			t.Fatalf("code = %d report = %v", code, report)
		}
	})
	t.Run("unconfigured publisher is 1", func(t *testing.T) {
		f := newDeps(slack.Session{TeamID: "T1"}, nil, okProbe, []string{"T1"})
		f.deps.Publisher = func() (slack.Publisher, error) { return nil, errors.New("PDW_API_URL must be set") }
		code, report, _ := run(t, f.deps, nil, "publish-session")
		if code != 1 || !strings.Contains(report["error"].(string), "PDW_API_URL") {
			t.Fatalf("code = %d report = %v", code, report)
		}
	})
}

// The 2026-09-23 shape: the Hack Club token was signed out, another
// workspace's token still answered, and that workspace got published as zrl.
func TestRunRefusesAWorkspaceTheWarehouseHasNeverSynced(t *testing.T) {
	stranger := slack.Session{Source: "slack-app", Token: "xoxc-supersecrettoken", CookieD: "xoxd-supersecretcookie",
		TeamID: "T0A4T3P6VUG", UserID: "U0A9TGMBR70", TeamURL: "https://example-other.slack.com/"}
	f := newDeps(stranger, nil, okProbe, []string{"T0266FRGM"})
	code, report, _ := run(t, f.deps, map[string]string{"SLACK_ACCOUNTS": "zrl"}, "publish-session")
	if code != 3 || f.published != nil || report["known_workspace"] != false {
		t.Fatalf("code = %d report = %v published = %+v", code, report, f.published)
	}
	msg := report["error"].(string)
	for _, want := range []string{"T0A4T3P6VUG", "example-other.slack.com", "never synced", "--team-id"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error does not mention %q: %s", want, msg)
		}
	}
	// The explicit flag is the deliberate override, and a known workspace passes.
	f = newDeps(stranger, nil, okProbe, []string{"T0266FRGM"})
	if code, _, _ := run(t, f.deps, nil, "publish-session", "--team-id", "T0A4T3P6VUG"); code != 0 || f.published == nil {
		t.Fatalf("explicit --team-id was refused: code = %d", code)
	}
	f = newDeps(slack.Session{TeamID: "T0266FRGM", TeamURL: "https://hackclub.slack.com/"}, nil, okProbe, []string{"T0266FRGM"})
	if code, report, _ := run(t, f.deps, nil, "publish-session"); code != 0 || report["known_workspace"] != true {
		t.Fatalf("known workspace refused: code = %d report = %v", code, report)
	}
}

func TestRunVerbDispatch(t *testing.T) {
	f := newDeps(liveSession, nil, okProbe, nil)
	if code, _, err := run(t, f.deps, nil); code != 2 || !strings.Contains(err, "usage: pdw slack publish-session") {
		t.Fatalf("code = %d stderr = %q", code, err)
	}
	var helpOut, helpErr bytes.Buffer
	if code := slack.RunWith([]string{"--help"}, &helpOut, &helpErr, envOf(nil), f.deps); code != 0 || !strings.Contains(helpOut.String(), "usage:") || helpErr.Len() != 0 {
		t.Fatalf("code = %d stdout = %q stderr = %q", code, helpOut.String(), helpErr.String())
	}
	if code, _, err := run(t, f.deps, nil, "frobnicate"); code != 2 || !strings.Contains(err, "unknown command") {
		t.Fatalf("code = %d stderr = %q", code, err)
	}
	if code, _, err := run(t, f.deps, nil, "publish-session", "--bogus"); code != 2 || !strings.Contains(err, "bogus") {
		t.Fatalf("code = %d stderr = %q", code, err)
	}
	helpOut.Reset()
	helpErr.Reset()
	if code := slack.RunWith([]string{"publish-session", "--help"}, &helpOut, &helpErr, envOf(nil), f.deps); code != 0 || !strings.Contains(helpOut.String(), "usage:") || helpErr.Len() != 0 {
		t.Fatalf("code = %d stdout = %q stderr = %q", code, helpOut.String(), helpErr.String())
	}
	if code, _, err := run(t, f.deps, nil, "publish-session", "extra"); code != 2 || !strings.Contains(err, "unexpected argument") {
		t.Fatalf("code = %d stderr = %q", code, err)
	}
}
