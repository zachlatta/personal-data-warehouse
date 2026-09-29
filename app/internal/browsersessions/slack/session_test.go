package slack_test

import (
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/slack"
)

// snippetOutput is what ConsoleSnippet copies from app.slack.com: the shape of
// localConfig_v2 on crobat's Slack client, 2026-09-29 (one stranger
// workspace, the Hack Club org, and the Hack Club workspace).
const snippetOutput = `{"user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36","teams":[` +
	`{"id":"T0A4T3P6VUG","name":"Other","url":"https://example-other.slack.com/","user_id":"U0A9TGMBR70","enterprise_id":"","token":"xoxc-strangersupersecret"},` +
	`{"id":"E09V59WQY1E","name":"Hack Club","url":"https://hackclub.enterprise.slack.com/","user_id":"U09UE480JHH","enterprise_id":"","token":"xoxc-orgsupersecret"},` +
	`{"id":"T0266FRGM","name":"Hack Club","url":"https://hackclub.slack.com/","user_id":"U09UE480JHH","enterprise_id":"E09V59WQY1E","token":"xoxc-workspacesupersecret"}]}`

func TestParsePasteReadsTheSnippetAndTheCookie(t *testing.T) {
	paste, err := slack.ParsePaste(snippetOutput + "\nxoxd-supersecretcookie%2Fabc%3D\n")
	if err != nil {
		t.Fatal(err)
	}
	if paste.CookieD != "xoxd-supersecretcookie%2Fabc%3D" || !strings.Contains(paste.UserAgent, "Chrome/140") {
		t.Fatalf("paste = %+v", paste)
	}
	if len(paste.Teams) != 3 {
		t.Fatalf("teams = %+v", paste.Teams)
	}
	org, workspace := paste.Teams[1], paste.Teams[2]
	// An org entry's id is the ENTERPRISE id; storing it as team_id would fork
	// every warehouse row keyed by the workspace.
	if org.TeamID != "" || org.EnterpriseID != "E09V59WQY1E" || org.Token != "xoxc-orgsupersecret" {
		t.Fatalf("org = %+v", org)
	}
	if workspace.TeamID != "T0266FRGM" || workspace.EnterpriseID != "E09V59WQY1E" || workspace.UserID != "U09UE480JHH" ||
		workspace.TeamURL != "https://hackclub.slack.com/" || workspace.UserAgent != paste.UserAgent ||
		workspace.CookieD != paste.CookieD || workspace.Source != slack.SourceWeb {
		t.Fatalf("workspace = %+v", workspace)
	}
}

func TestParsePasteAcceptsTheCookieInAnyShapeDevToolsShowsIt(t *testing.T) {
	for raw, want := range map[string]string{
		"xoxd-abc%2Fdef%2Bg%3D%3D":      "xoxd-abc%2Fdef%2Bg%3D%3D",
		"d=xoxd-abc%2Fdef;":             "xoxd-abc%2Fdef",
		`"xoxd-abc%2Fdef"`:              "xoxd-abc%2Fdef",
		"xoxd-abc/def+g==":              "xoxd-abc%2Fdef%2Bg%3D%3D", // "Show URL-decoded" ticked
		"  xoxd-abc%2Fdef \t":           "xoxd-abc%2Fdef",
		"d\txoxd-abc%2Fdef\t.slack.com": "xoxd-abc%2Fdef", // a copied table row
	} {
		paste, err := slack.ParsePaste(snippetOutput + "\n" + raw)
		if err != nil || paste.CookieD != want {
			t.Fatalf("%q -> %q, %v; want %q", raw, paste.CookieD, err, want)
		}
	}
}

func TestParsePasteTakesTheWholeLocalConfig(t *testing.T) {
	// The whole localConfig_v2 value, teams keyed by id, is also accepted: it
	// is what someone copies from the Application panel instead.
	raw := `{"lastActiveTeamId":"T0266FRGM","teams":{"T0266FRGM":{"id":"T0266FRGM","user_id":"U09UE480JHH","enterprise_id":"E09V59WQY1E","url":"https://hackclub.slack.com/","token":"xoxc-workspacesupersecret"}}}`
	paste, err := slack.ParsePaste(raw + "\nxoxd-supersecretcookie")
	if err != nil || len(paste.Teams) != 1 || paste.Teams[0].TeamID != "T0266FRGM" || paste.Teams[0].Token != "xoxc-workspacesupersecret" {
		t.Fatalf("paste = %+v, %v", paste, err)
	}
}

func TestParsePasteKeepsABareTokenForTheFlags(t *testing.T) {
	paste, err := slack.ParsePaste("xoxc-baresupersecret-1234567890\nxoxd-supersecretcookie\n")
	if err != nil || len(paste.Teams) != 0 || len(paste.BareTokens) != 1 || paste.BareTokens[0] != "xoxc-baresupersecret-1234567890" {
		t.Fatalf("paste = %+v, %v", paste, err)
	}
}

func TestReadPasteStopsAsSoonAsBothHalvesArrive(t *testing.T) {
	// Interactive use: the reader must not wait for EOF once it has a team and
	// a cookie, or a terminal user has to know to press Ctrl-D.
	r := &countingReader{lines: []string{snippetOutput, "xoxd-supersecretcookie", "never read"}}
	paste, err := slack.ReadPaste(r)
	if err != nil || paste.CookieD != "xoxd-supersecretcookie" || len(paste.Teams) != 3 {
		t.Fatalf("paste = %+v, %v", paste, err)
	}
	if r.served != 2 {
		t.Fatalf("read %d lines, want 2", r.served)
	}
}

func TestReadPasteNamesWhatIsMissing(t *testing.T) {
	if _, err := slack.ReadPaste(strings.NewReader(snippetOutput + "\n")); err == nil || !strings.Contains(err.Error(), `"d" cookie`) {
		t.Fatalf("err = %v", err)
	}
	if _, err := slack.ReadPaste(strings.NewReader("xoxd-supersecretcookie\n")); err == nil || !strings.Contains(err.Error(), "xoxc") {
		t.Fatalf("err = %v", err)
	}
	if _, err := slack.ReadPaste(strings.NewReader("")); err == nil {
		t.Fatal("empty input accepted")
	}
}

func TestRedactedCarriesNoSecret(t *testing.T) {
	paste, _ := slack.ParsePaste(snippetOutput + "\nxoxd-supersecretcookie")
	blob := strings.Join(func() []string {
		var out []string
		for k, v := range paste.Teams[2].Redacted() {
			out = append(out, k, strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(toString(v), "\n", ""), " ", "")))
		}
		return out
	}(), " ")
	if strings.Contains(blob, "supersecret") || !strings.Contains(blob, paste.Teams[2].Fingerprint()) {
		t.Fatalf("redacted = %s", blob)
	}
}

func TestTheSnippetReadsLocalConfigAndCopiesTheUserAgent(t *testing.T) {
	for _, want := range []string{"localStorage.localConfig_v2", "navigator.userAgent", "copy(", "user_id", "enterprise_id", "token"} {
		if !strings.Contains(slack.ConsoleSnippet, want) {
			t.Fatalf("snippet lacks %q: %s", want, slack.ConsoleSnippet)
		}
	}
}

type countingReader struct {
	lines  []string
	served int
	buf    []byte
}

func (r *countingReader) Read(p []byte) (int, error) {
	if len(r.buf) == 0 {
		if r.served >= len(r.lines) {
			return 0, ioEOF
		}
		r.buf = []byte(r.lines[r.served] + "\n")
		r.served++
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}
