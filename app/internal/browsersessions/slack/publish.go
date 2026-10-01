package slack

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

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

// Publisher posts the session to the warehouse.
type Publisher func(ingestclient.SlackSession) (map[string]any, error)

// Deps are the seams Run wires to the real machine; tests inject fakes. There
// is deliberately no Slack client among them: see the package comment.
type Deps struct {
	Workspaces     WorkspaceLookup
	KnownWorkspace KnownWorkspaceLookup
	Publisher      func() (Publisher, error)
	// Interactive reports whether stdin is a terminal, which is when the
	// instructions are worth printing.
	Interactive func() bool
}

func defaultDeps(getenv func(string) string, cfg ingestclient.Config, stdin io.Reader, stderr io.Writer) Deps {
	return Deps{
		Workspaces:     WorkspaceIDsForEnterprise(cfg, getenv("PDW_CLIENT_NAME")),
		KnownWorkspace: WorkspaceIsKnown(cfg, getenv("PDW_CLIENT_NAME")),
		Publisher: func() (Publisher, error) {
			ic, err := ingestclient.FromEnv(getenv, cfg, common.NewWriterLogger(stderr))
			if err != nil {
				return nil, err
			}
			return ic.PublishSlackSession, nil
		},
		Interactive: func() bool {
			f, ok := stdin.(*os.File)
			if !ok {
				return false
			}
			info, err := f.Stat()
			return err == nil && info.Mode()&os.ModeCharDevice != 0
		},
	}
}

// Instructions is what a person at a terminal needs to produce the paste.
var Instructions = `Publish a Slack web session for the warehouse's Slack polling and reviewed writes.

1. Open a new private (incognito) window, go to https://app.slack.com and sign
   in to Hack Club there. This session must be used by nothing but the
   warehouse: every sign-out-everywhere Slack applied in September was a
   session it knew as the desktop app suddenly used by another client. When
   you are done, close the window -- do not sign out, which ends the session.
2. Open DevTools (Cmd-Opt-J in Chrome). If the console refuses pastes, type
   "allow pasting" first. Paste this and press Enter; it copies one line of
   JSON to the clipboard:

   ` + ConsoleSnippet + `

3. Paste that line here and press Enter.
4. In DevTools > Application > Cookies > https://app.slack.com, copy the value
   of the cookie named "d" (it starts with xoxd-). Paste it here and press Enter.

pdw sends nothing to Slack. The warehouse first uses the session on its next
Slack freshness pass (every five minutes).
`

const usage = `usage: pdw slack publish-session [--account LABEL] [--session-key KEY] [--team-id ID] [--user-id ID] [--dry-run]

Read a Slack web session from stdin and publish it to the warehouse.

`

// verifyHint says when the session is first used: the next freshness pass,
// which polls with it (Zach's choice, 2026-10-01).
const verifyHint = `the warehouse polls with this session on its next Slack freshness pass (every five minutes); if Slack refuses it, that pass logs so and falls back to the slower workspace token`

// Run is the `pdw slack` entry point; args start with the verb.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	return RunWith(args, stdin, stdout, stderr, getenv, defaultDeps(getenv, cfg, stdin, stderr))
}

// RunWith is Run with injectable dependencies.
func RunWith(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, deps Deps) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	if args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(stdout, usage+Instructions)
		return 0
	}
	if args[0] != "publish-session" {
		fmt.Fprintf(stderr, "pdw slack: unknown command %q\n%s", args[0], usage)
		return 2
	}
	fs := common.NewFlagSet("pdw slack publish-session")
	account := fs.String("account", "", "Account label the credential is stored under.")
	sessionKey := fs.String("session-key", "default", "Session key.")
	teamID := fs.String("team-id", "", "Workspace id to publish (and the workspace a bare token belongs to).")
	userID := fs.String("user-id", "", "Slack user id a bare token belongs to (only needed without the snippet's JSON).")
	dryRun := fs.Bool("dry-run", false, "Read and check the paste without publishing.")
	positionals, code, done := common.ParseArgs(fs, args[1:], usage+Instructions, stdout, stderr)
	if done {
		return code
	}
	if len(positionals) > 0 {
		fmt.Fprintf(stderr, "pdw slack publish-session: unexpected argument %q\n", positionals[0])
		return 2
	}

	report := map[string]any{"published": false}
	emit := func() { printJSON(stdout, report) }
	fail := func(code int, err string) int {
		report["error"] = err
		emit()
		return code
	}

	if deps.Interactive != nil && deps.Interactive() {
		fmt.Fprint(stderr, Instructions+"\n")
	}
	paste, err := ReadPaste(stdin)
	if err != nil {
		return fail(1, err.Error())
	}
	sessions := paste.Teams
	if len(sessions) == 0 {
		// A bare token carries no identity, and guessing whose it is would
		// store a credential the sync and the send executor both refuse.
		if *teamID == "" || *userID == "" {
			return fail(1, "a bare xoxc token says nothing about whose it is; paste the console snippet's JSON instead, or pass --team-id and --user-id")
		}
		for _, tok := range paste.BareTokens {
			sessions = append(sessions, Session{Source: SourceWeb, Token: tok, CookieD: paste.CookieD,
				TeamID: *teamID, UserID: *userID, UserAgent: paste.UserAgent})
		}
	}

	chosen, resolvedTeam, candidates, code, reason := selectSession(sessions, *teamID, deps)
	report["candidates"] = candidates
	if code != 0 {
		return fail(code, reason)
	}
	report["session"] = chosen.Redacted()
	report["team_id"] = resolvedTeam
	if *dryRun {
		emit()
		return 0
	}
	resolvedAccount := ResolveAccount(*account, getenv)
	publish, err := deps.Publisher()
	if err != nil {
		return fail(1, err.Error())
	}
	ack, err := publish(ingestclient.SlackSession{
		Account:       resolvedAccount,
		SessionKey:    *sessionKey,
		SessionToken:  chosen.Token,
		SessionCookie: chosen.CookieD,
		TeamID:        resolvedTeam,
		EnterpriseID:  chosen.EnterpriseID,
		UserID:        chosen.UserID,
		TeamURL:       chosen.TeamURL,
		SourceApp:     chosen.Source,
		UserAgent:     chosen.UserAgent,
	})
	if err != nil {
		return fail(1, fmt.Sprintf("publish failed: %v", err))
	}
	report["published"] = true
	report["account"] = resolvedAccount
	report["acknowledgement"] = ack
	report["next"] = verifyHint
	emit()
	return 0
}

// selectSession picks the session to publish from every team the paste named.
// Workspace-scoped sessions come before org (Enterprise Grid) ones: an org
// session can act in a sibling workspace, and a write must land in the one the
// warehouse syncs.
// Each candidate must resolve to one workspace the warehouse syncs (unless
// --team-id names it). The report lists every candidate and why it was passed
// over.
func selectSession(sessions []Session, explicitTeam string, deps Deps) (Session, string, []map[string]any, int, string) {
	ordered := make([]Session, 0, len(sessions))
	for _, s := range sessions {
		if s.TeamID != "" {
			ordered = append(ordered, s)
		}
	}
	for _, s := range sessions {
		if s.TeamID == "" {
			ordered = append(ordered, s)
		}
	}
	if explicitTeam != "" {
		sort.SliceStable(ordered, func(i, j int) bool {
			return ordered[i].TeamID == explicitTeam && ordered[j].TeamID != explicitTeam
		})
	}
	var reports []map[string]any
	var reasons []string
	worst := 0
	skip := func(entry map[string]any, code int, reason string) {
		entry["skipped"] = reason
		reasons = append(reasons, reason)
		if code > worst {
			worst = code
		}
	}
	for _, s := range ordered {
		entry := map[string]any{"session": s.Redacted()}
		reports = append(reports, entry)
		if s.UserID == "" {
			skip(entry, 1, fmt.Sprintf("the entry for %s carries no user_id", s.TeamURL))
			continue
		}
		if explicitTeam != "" {
			if s.TeamID != "" && s.TeamID != explicitTeam {
				skip(entry, 3, fmt.Sprintf("session is for workspace %s, not --team-id %s", s.TeamID, explicitTeam))
				continue
			}
			entry["team_id"] = explicitTeam
			entry["chosen"] = true
			return s, explicitTeam, reports, 0, ""
		}
		resolved, err := ResolveTeamID(s.TeamID, s.EnterpriseID, deps.Workspaces)
		if err != nil {
			skip(entry, 3, err.Error())
			continue
		}
		entry["team_id"] = resolved
		if deps.KnownWorkspace != nil {
			known, err := deps.KnownWorkspace(resolved)
			if err != nil {
				skip(entry, 3, err.Error())
				continue
			}
			if !known {
				// A workspace the warehouse has never synced is the wrong
				// workspace: a session for it stored under this account would
				// make every Slack write refuse its target while the credential
				// row reads healthy.
				skip(entry, 3, fmt.Sprintf(
					"the pasted session for workspace %s (%s) is one the warehouse has never synced under any account; "+
						"sign in to the workspace the warehouse syncs and paste again, or pass --team-id %s to publish it anyway",
					resolved, s.TeamURL, resolved))
				continue
			}
		}
		entry["chosen"] = true
		return s, resolved, reports, 0, ""
	}
	if worst == 0 {
		worst = 1
		reasons = append(reasons, "the paste named no Slack team")
	}
	return Session{}, "", reports, worst, strings.Join(reasons, "; ")
}

func printJSON(w io.Writer, v any) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
