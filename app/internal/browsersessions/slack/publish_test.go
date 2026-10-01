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

func newDeps(workspaces []string) *fakeDeps {
	f := &fakeDeps{}
	f.deps = slack.Deps{
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

const cookieLine = "xoxd-supersecretcookie%2Fx%3D"

func run(t *testing.T, deps slack.Deps, env map[string]string, stdin string, args ...string) (int, map[string]any, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := slack.RunWith(args, strings.NewReader(stdin), &stdout, &stderr, envOf(env), deps)
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

// The point of the manual flow: on 2026-09-29 every hourly run of the old
// desktop-app capture called auth.test and client.counts with Zach's own
// desktop session from a Go TLS stack, and Slack signed him out of every
// device each time (`unexpected_scraping`, "Go-based tool"). Deps has no Slack
// client at all now, so nothing here can talk to Slack; this pins the report
// saying so and the credential reaching the warehouse untouched.
func TestRunPublishesThePastedWorkspaceSessionWithoutCallingSlack(t *testing.T) {
	f := newDeps([]string{"T0266FRGM"})
	code, report, _ := run(t, f.deps, map[string]string{"SLACK_ACCOUNTS": "zrl"}, snippetOutput+"\n"+cookieLine+"\n", "publish-session")
	if code != 0 {
		t.Fatalf("code = %d report = %v", code, report)
	}
	if report["published"] != true || report["team_id"] != "T0266FRGM" || report["account"] != "zrl" {
		t.Fatalf("report = %v", report)
	}
	p := f.published
	if p == nil || p.TeamID != "T0266FRGM" || p.EnterpriseID != "E09V59WQY1E" || p.UserID != "U09UE480JHH" ||
		p.SessionToken != "xoxc-workspacesupersecret" || p.SessionCookie != cookieLine ||
		p.SessionKey != "default" || p.SourceApp != slack.SourceWeb || !strings.Contains(p.UserAgent, "Chrome/140") ||
		p.TeamURL != "https://hackclub.slack.com/" {
		t.Fatalf("published = %+v", p)
	}
	if !strings.Contains(report["next"].(string), "reviewed Slack write") {
		t.Fatalf("report does not say when the session is first used: %v", report)
	}
}

func TestRunPrintsInstructionsOnlyToATerminal(t *testing.T) {
	f := newDeps([]string{"T0266FRGM"})
	_, _, stderr := run(t, f.deps, nil, snippetOutput+"\n"+cookieLine+"\n", "publish-session")
	if strings.Contains(stderr, "localConfig_v2") {
		t.Fatalf("piped input still printed the instructions: %s", stderr)
	}
	f.deps.Interactive = func() bool { return true }
	_, _, stderr = run(t, f.deps, nil, snippetOutput+"\n"+cookieLine+"\n", "publish-session")
	for _, want := range []string{"app.slack.com", slack.ConsoleSnippet, `"d"`, "xoxd-"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("instructions lack %q: %s", want, stderr)
		}
	}
}

func TestRunResolvesAnOrgOnlyPasteToItsWorkspace(t *testing.T) {
	org := `{"user_agent":"UA","teams":[{"id":"E09V59WQY1E","url":"https://hackclub.enterprise.slack.com/","user_id":"U09UE480JHH","token":"xoxc-orgsupersecret"}]}`
	f := newDeps([]string{"T0266FRGM"})
	code, report, _ := run(t, f.deps, nil, org+"\n"+cookieLine, "publish-session")
	if code != 0 || f.published.TeamID != "T0266FRGM" || f.published.EnterpriseID != "E09V59WQY1E" || report["team_id"] != "T0266FRGM" {
		t.Fatalf("code = %d report = %v published = %+v", code, report, f.published)
	}
}

func TestRunDryRunValidatesWithoutPublishing(t *testing.T) {
	f := newDeps([]string{"T0266FRGM"})
	f.deps.Publisher = func() (slack.Publisher, error) { t.Fatal("must not publish"); return nil, nil }
	code, report, _ := run(t, f.deps, nil, snippetOutput+"\n"+cookieLine, "publish-session", "--dry-run")
	if code != 0 || report["published"] != false || report["team_id"] != "T0266FRGM" {
		t.Fatalf("code = %d report = %v", code, report)
	}
}

func TestRunExplicitFlagsWin(t *testing.T) {
	f := newDeps([]string{"T0266FRGM"})
	code, report, _ := run(t, f.deps, nil, snippetOutput+"\n"+cookieLine, "publish-session", "--team-id", "T0A4T3P6VUG", "--account", "chosen", "--session-key", "alt")
	if code != 0 || report["team_id"] != "T0A4T3P6VUG" || f.published.Account != "chosen" || f.published.SessionKey != "alt" ||
		f.published.SessionToken != "xoxc-strangersupersecret" {
		t.Fatalf("code = %d report = %v published = %+v", code, report, f.published)
	}
}

// When Slack changes localConfig_v2 and the snippet stops working, a token
// found some other way can still be published by naming who it belongs to.
func TestRunPublishesABareTokenOnlyWithItsIdentity(t *testing.T) {
	bare := "xoxc-baresupersecret-1234567890\n" + cookieLine + "\n"
	f := newDeps([]string{"T0266FRGM"})
	if code, report, _ := run(t, f.deps, nil, bare, "publish-session"); code != 1 || !strings.Contains(report["error"].(string), "--user-id") || f.published != nil {
		t.Fatalf("code = %d report = %v", code, report)
	}
	code, report, _ := run(t, f.deps, nil, bare, "publish-session", "--team-id", "T0266FRGM", "--user-id", "U09UE480JHH")
	if code != 0 || f.published.SessionToken != "xoxc-baresupersecret-1234567890" || f.published.UserID != "U09UE480JHH" || f.published.TeamID != "T0266FRGM" {
		t.Fatalf("code = %d report = %v published = %+v", code, report, f.published)
	}
}

func TestRunExitCodes(t *testing.T) {
	t.Run("a paste missing the cookie is 1 and says so", func(t *testing.T) {
		f := newDeps([]string{"T0266FRGM"})
		code, report, _ := run(t, f.deps, nil, snippetOutput+"\n", "publish-session")
		if code != 1 || !strings.Contains(report["error"].(string), `"d" cookie`) || f.published != nil {
			t.Fatalf("code = %d report = %v", code, report)
		}
	})
	t.Run("ambiguous enterprise is 3", func(t *testing.T) {
		org := `{"teams":[{"id":"E1","user_id":"U1","token":"xoxc-orgsupersecret"}]}`
		f := newDeps([]string{"T1", "T2"})
		code, report, _ := run(t, f.deps, nil, org+"\n"+cookieLine, "publish-session")
		if code != 3 || !strings.Contains(report["error"].(string), "more than one workspace") || f.published != nil {
			t.Fatalf("code = %d report = %v", code, report)
		}
	})
	t.Run("unconfigured publisher is 1", func(t *testing.T) {
		f := newDeps([]string{"T0266FRGM"})
		f.deps.Publisher = func() (slack.Publisher, error) { return nil, errors.New("PDW_API_URL must be set") }
		code, report, _ := run(t, f.deps, nil, snippetOutput+"\n"+cookieLine, "publish-session")
		if code != 1 || !strings.Contains(report["error"].(string), "PDW_API_URL") {
			t.Fatalf("code = %d report = %v", code, report)
		}
	})
}

// The 2026-09-23 shape: another workspace's session got published as zrl.
// A paste of only a stranger workspace is refused unless --team-id names it.
func TestRunRefusesAWorkspaceTheWarehouseHasNeverSynced(t *testing.T) {
	stranger := `{"teams":[{"id":"T0A4T3P6VUG","url":"https://example-other.slack.com/","user_id":"U0A9TGMBR70","token":"xoxc-strangersupersecret"}]}`
	f := newDeps([]string{"T0266FRGM"})
	code, report, _ := run(t, f.deps, map[string]string{"SLACK_ACCOUNTS": "zrl"}, stranger+"\n"+cookieLine, "publish-session")
	if code != 3 || f.published != nil {
		t.Fatalf("code = %d report = %v published = %+v", code, report, f.published)
	}
	msg := report["error"].(string)
	for _, want := range []string{"T0A4T3P6VUG", "example-other.slack.com", "never synced", "--team-id"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error does not mention %q: %s", want, msg)
		}
	}
	if code, _, _ := run(t, f.deps, nil, stranger+"\n"+cookieLine, "publish-session", "--team-id", "T0A4T3P6VUG"); code != 0 || f.published == nil {
		t.Fatalf("explicit --team-id was refused: code = %d", code)
	}
}

func TestRunVerbDispatch(t *testing.T) {
	f := newDeps(nil)
	if code, _, err := run(t, f.deps, nil, ""); code != 2 || !strings.Contains(err, "usage: pdw slack publish-session") {
		t.Fatalf("code = %d stderr = %q", code, err)
	}
	var helpOut, helpErr bytes.Buffer
	if code := slack.RunWith([]string{"--help"}, strings.NewReader(""), &helpOut, &helpErr, envOf(nil), f.deps); code != 0 ||
		!strings.Contains(helpOut.String(), "usage:") || !strings.Contains(helpOut.String(), slack.ConsoleSnippet) || helpErr.Len() != 0 {
		t.Fatalf("code = %d stdout = %q stderr = %q", code, helpOut.String(), helpErr.String())
	}
	if code, _, err := run(t, f.deps, nil, "", "frobnicate"); code != 2 || !strings.Contains(err, "unknown command") {
		t.Fatalf("code = %d stderr = %q", code, err)
	}
	if code, _, err := run(t, f.deps, nil, "", "publish-session", "--bogus"); code != 2 || !strings.Contains(err, "bogus") {
		t.Fatalf("code = %d stderr = %q", code, err)
	}
	if code, _, err := run(t, f.deps, nil, "", "publish-session", "--source", "slack-app"); code != 2 || !strings.Contains(err, "source") {
		t.Fatalf("the desktop-app --source flag must be gone: code = %d stderr = %q", code, err)
	}
	if code, _, err := run(t, f.deps, nil, "", "publish-session", "extra"); code != 2 || !strings.Contains(err, "unexpected argument") {
		t.Fatalf("code = %d stderr = %q", code, err)
	}
}
