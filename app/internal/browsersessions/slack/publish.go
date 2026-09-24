package slack

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// WorkspaceLookup returns the workspace ids the warehouse knows under an
// enterprise (org) id.
type WorkspaceLookup func(enterpriseID string) ([]string, error)

// ResolveTeamID is the workspace id this credential's writes must be keyed
// by. A client session on Enterprise Grid only knows the org, but every
// warehouse row is keyed by the workspace. Guessing is not acceptable: keying
// to the wrong id forks the dataset silently, so an ambiguous or unknown org
// is an error.
func ResolveTeamID(teamID, enterpriseID string, lookup WorkspaceLookup) (string, error) {
	if teamID != "" {
		return teamID, nil
	}
	if enterpriseID == "" {
		return "", captureErrorf("the session reported neither a workspace nor an enterprise id")
	}
	candidates, err := lookup(enterpriseID)
	if err != nil {
		return "", captureErrorf("workspace lookup failed: %v", err)
	}
	switch len(candidates) {
	case 1:
		return candidates[0], nil
	case 0:
		return "", captureErrorf("no workspace in the warehouse belongs to enterprise %s; run a Slack sync first, or pass --team-id explicitly", enterpriseID)
	default:
		return "", captureErrorf("enterprise %s covers more than one workspace (%s); pass --team-id to say which one this credential is for", enterpriseID, strings.Join(candidates, ", "))
	}
}

// WorkspaceIDsForEnterprise asks the app's read-only SQL tool -- the same
// static-bearer HTTP API `pdw sql` uses -- rather than Postgres directly,
// because this runs on a laptop with neither the warehouse credential nor a
// route to the database.
func WorkspaceIDsForEnterprise(cfg ingestclient.Config, clientName string) WorkspaceLookup {
	return func(enterpriseID string) ([]string, error) {
		if strings.TrimSpace(cfg.BaseURL) == "" || strings.TrimSpace(cfg.Token) == "" {
			return nil, captureErrorf("PDW_API_URL/PDW_SECRET_TOKEN are not set; run `pdw login` first")
		}
		statement := "SELECT team_id FROM " + warehouse.SQLRelation("slack_teams") +
			" WHERE enterprise_id = " + common.SQLLiteral(enterpriseID) + " ORDER BY team_id"
		rows, err := common.SQLToolQuery(cfg.BaseURL, clientName, cfg.Token,
			"Which workspace does this Slack enterprise session belong to?", statement, 60*time.Second)
		if err != nil {
			return nil, err
		}
		var found []string
		for _, row := range rows {
			if id := stringOf(row["team_id"]); id != "" {
				found = append(found, id)
			}
		}
		return found, nil
	}
}

// KnownWorkspaceLookup answers whether the warehouse has ever synced a
// workspace under this id, for any account.
type KnownWorkspaceLookup func(teamID string) (bool, error)

// WorkspaceIsKnown is the guard against publishing somebody else's workspace
// under Zach's account label. On 2026-09-23 the Hack Club token in the Slack
// desktop app had been signed out, a second workspace's token still answered
// auth.test, and the capture published THAT session as `zrl`: every Slack
// write keys its target by the session's workspace, so mark-read and send
// were refused for a day while the row read `ok`. The same SQL-tool shape as
// WorkspaceIDsForEnterprise, for the same laptop-without-a-database reason.
func WorkspaceIsKnown(cfg ingestclient.Config, clientName string) KnownWorkspaceLookup {
	return func(teamID string) (bool, error) {
		if strings.TrimSpace(cfg.BaseURL) == "" || strings.TrimSpace(cfg.Token) == "" {
			return false, captureErrorf("PDW_API_URL/PDW_SECRET_TOKEN are not set; run `pdw login` first")
		}
		statement := "SELECT team_id FROM " + warehouse.SQLRelation("slack_teams") +
			" WHERE team_id = " + common.SQLLiteral(teamID) + " LIMIT 1"
		rows, err := common.SQLToolQuery(cfg.BaseURL, clientName, cfg.Token,
			"Does the warehouse sync this Slack workspace?", statement, 60*time.Second)
		if err != nil {
			return false, err
		}
		for _, row := range rows {
			if stringOf(row["team_id"]) == teamID {
				return true, nil
			}
		}
		return false, nil
	}
}

// ResolveAccount is the label the credential is stored under. SLACK_ACCOUNTS
// comes first and the generic personal-email fallbacks last, because the SYNC
// looks this credential up by its own Slack account label; publishing under a
// different label stores a credential that reads healthy everywhere and that
// the sync can never find.
func ResolveAccount(explicit string, getenv func(string) string) string {
	for _, candidate := range []string{
		explicit,
		getenv("SLACK_ACCOUNT"),
		firstCSV(getenv("SLACK_ACCOUNTS")),
		getenv("AGENT_SESSIONS_ACCOUNT"),
		getenv("APPLE_MESSAGES_ACCOUNT"),
		firstCSV(getenv("GMAIL_ACCOUNTS")),
	} {
		if candidate != "" {
			return candidate
		}
	}
	return "zrl"
}

func firstCSV(raw string) string {
	return strings.TrimSpace(strings.Split(raw, ",")[0])
}

// Publisher posts the captured session to the warehouse.
type Publisher func(ingestclient.SlackSession) (map[string]any, error)

// Deps are the seams Run wires to the real machine; tests inject fakes.
type Deps struct {
	Discover       func(source string) (Session, error)
	Probe          func(Session) map[string]any
	Workspaces     WorkspaceLookup
	KnownWorkspace KnownWorkspaceLookup
	Publisher      func() (Publisher, error)
}

func defaultDeps(getenv func(string) string, cfg ingestclient.Config, stderr io.Writer) Deps {
	host := chromium.DefaultHost()
	client := NewClient()
	return Deps{
		Discover:       func(source string) (Session, error) { return Discover(host, source, client.AuthTest) },
		Probe:          client.ProbeClientCounts,
		Workspaces:     WorkspaceIDsForEnterprise(cfg, getenv("PDW_CLIENT_NAME")),
		KnownWorkspace: WorkspaceIsKnown(cfg, getenv("PDW_CLIENT_NAME")),
		Publisher: func() (Publisher, error) {
			ic, err := ingestclient.FromEnv(getenv, cfg, common.NewWriterLogger(stderr))
			if err != nil {
				return nil, err
			}
			return ic.PublishSlackSession, nil
		},
	}
}

const usage = `usage: pdw slack publish-session [--account LABEL] [--session-key KEY] [--source SOURCE] [--team-id ID] [--dry-run]

Capture the local Slack client session and publish it to the warehouse.
Run it from a GUI terminal on the Mac signed in to the Slack desktop app and
choose "Always Allow" on the keychain prompt.
`

// Run is the `pdw slack` entry point; args start with the verb.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	return RunWith(args, stdout, stderr, getenv, defaultDeps(getenv, cfg, stderr))
}

// RunWith is Run with injectable dependencies.
func RunWith(args []string, stdout, stderr io.Writer, getenv func(string) string, deps Deps) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	if args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(stdout, usage)
		return 0
	}
	if args[0] != "publish-session" {
		fmt.Fprintf(stderr, "pdw slack: unknown command %q\n%s", args[0], usage)
		return 2
	}
	fs := common.NewFlagSet("pdw slack publish-session")
	account := fs.String("account", "", "Account label the credential is stored under.")
	sessionKey := fs.String("session-key", "default", "Session key.")
	source := fs.String("source", "", "Force a session source (default: the Slack desktop app).")
	teamID := fs.String("team-id", "", "Workspace id, when the org covers several.")
	dryRun := fs.Bool("dry-run", false, "Capture and validate without publishing.")
	positionals, code, done := common.ParseArgs(fs, args[1:], usage, stdout, stderr)
	if done {
		return code
	}
	if len(positionals) > 0 {
		fmt.Fprintf(stderr, "pdw slack publish-session: unexpected argument %q\n", positionals[0])
		return 2
	}

	report := map[string]any{"published": false}
	emit := func() { printJSON(stdout, report) }

	session, err := deps.Discover(*source)
	if err != nil {
		report = map[string]any{"error": err.Error()}
		if strings.Contains(err.Error(), "timed out") {
			// `security` blocking to its timeout means it put up a prompt and
			// nobody answered -- which in a LaunchAgent is always true.
			report["likely_cause"] = "the macOS keychain prompted and no one could answer: either the login keychain is locked (Mac asleep or at the login screen), or the ACL on 'Slack Safe Storage' is a one-shot 'Allow' instead of 'Always Allow'"
			report["fix"] = "unlock the Mac, then run `pdw slack publish-session` once from a GUI terminal and choose 'Always Allow'"
		}
		emit()
		return 1
	}
	report["session"] = session.Redacted()

	probe := deps.Probe(session)
	report["client_counts"] = probe
	if ok, _ := probe["ok"].(bool); !ok {
		// A session that cannot answer "what changed" is useless for the sync.
		report["error"] = "client.counts failed; not publishing"
		emit()
		return 2
	}
	if *dryRun {
		emit()
		return 0
	}

	resolvedTeam := *teamID
	if resolvedTeam == "" {
		resolvedTeam, err = ResolveTeamID(session.TeamID, session.EnterpriseID, deps.Workspaces)
		if err != nil {
			report["error"] = err.Error()
			emit()
			return 3
		}
		// A workspace the warehouse has never synced is the wrong workspace:
		// the desktop app is signed into something else, and a session for it
		// stored under this account would make every Slack write refuse its
		// target while the credential row reads healthy. --team-id is the
		// deliberate override.
		if deps.KnownWorkspace != nil {
			known, err := deps.KnownWorkspace(resolvedTeam)
			if err != nil {
				report["error"] = err.Error()
				emit()
				return 3
			}
			report["known_workspace"] = known
			if !known {
				report["error"] = fmt.Sprintf(
					"the captured session belongs to workspace %s (%s), which the warehouse has never synced under any account; "+
						"the Slack desktop app is signed into the wrong workspace -- sign in to the workspace the warehouse syncs and rerun, "+
						"or pass --team-id %s to publish it anyway",
					resolvedTeam, session.TeamURL, resolvedTeam)
				emit()
				return 3
			}
		}
	}
	resolvedAccount := ResolveAccount(*account, getenv)
	publish, err := deps.Publisher()
	if err != nil {
		report["error"] = err.Error()
		emit()
		return 1
	}
	expires := ""
	if !session.CookieExpiresAt.IsZero() {
		expires = isoformat(session.CookieExpiresAt)
	}
	ack, err := publish(ingestclient.SlackSession{
		Account:         resolvedAccount,
		SessionKey:      *sessionKey,
		SessionToken:    session.Token,
		SessionCookie:   session.CookieD,
		TeamID:          resolvedTeam,
		EnterpriseID:    session.EnterpriseID,
		UserID:          session.UserID,
		TeamURL:         session.TeamURL,
		CookieExpiresAt: expires,
		SourceApp:       session.Source,
	})
	if err != nil {
		report["error"] = fmt.Sprintf("publish failed: %v", err)
		emit()
		return 1
	}
	report["published"] = true
	report["account"] = resolvedAccount
	report["team_id"] = resolvedTeam
	report["acknowledgement"] = ack
	emit()
	return 0
}

func printJSON(w io.Writer, v any) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
