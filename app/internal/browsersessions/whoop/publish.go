package whoop

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// ResolveAccount is the label keying the credential and the sync cursors:
// WHOOP_ACCOUNT, then the voice-memos account, then the first Gmail account
// (the Python settings loader's default), "" when none.
func ResolveAccount(explicit string, getenv func(string) string) string {
	candidates := []string{explicit, getenv("WHOOP_ACCOUNT"), getenv("VOICE_MEMOS_ACCOUNT")}
	for _, part := range strings.Split(getenv("GMAIL_ACCOUNTS"), ",") {
		candidates = append(candidates, part)
	}
	for _, c := range candidates {
		if v := strings.TrimSpace(c); v != "" {
			return v
		}
	}
	return ""
}

// Publisher posts the captured session to the warehouse.
type Publisher func(ingestclient.WhoopSession) (map[string]any, error)

// Deps are the seams Run wires to the real machine; tests inject fakes.
type Deps struct {
	Capture   func(browser string, now time.Time) (Session, error)
	Publisher func() (Publisher, error)
	Now       func() time.Time
}

func defaultDeps(getenv func(string) string, cfg ingestclient.Config, stderr io.Writer) Deps {
	host := chromium.DefaultHost()
	return Deps{
		Capture: func(browser string, now time.Time) (Session, error) { return Capture(host, browser, now) },
		Publisher: func() (Publisher, error) {
			ic, err := ingestclient.FromEnv(getenv, cfg, common.NewWriterLogger(stderr))
			if err != nil {
				return nil, err
			}
			return ic.PublishWhoopPrivateSession, nil
		},
		Now: time.Now,
	}
}

const usage = `usage: pdw whoop publish-session [--account LABEL] [--session-key KEY] [--browser NAME] [--dry-run]

Capture the app.whoop.com browser session from a local Chrome-family browser
and publish it to the warehouse for the whoop_private poller. Run it on the
Mac whose browser holds the WHOOP login; --dry-run verifies cookie decryption.
`

// Run is the `pdw whoop` entry point; args start with the verb. The Python
// command loaded the repo .env through its settings loader (account
// resolution and the warehouse URL/token), so the project .env
// (PDW_INGEST_PROJECT_DIR, else the working directory) is layered under the
// process environment here too, never overriding it.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "whoop publish-session: %v\n", err)
		return 1
	}
	cfg = cfg.WithEnvFallback(env)
	return RunWith(args, stdout, stderr, env, defaultDeps(env, cfg, stderr))
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
		fmt.Fprintf(stderr, "pdw whoop: unknown command %q\n%s", args[0], usage)
		return 2
	}
	fs := common.NewFlagSet("pdw whoop publish-session")
	account := fs.String("account", "", "account label keying the credential")
	sessionKey := fs.String("session-key", "default", "session key")
	browser := fs.String("browser", "", "chrome, brave, edge, arc, chromium, vivaldi")
	dryRun := fs.Bool("dry-run", false, "capture and report without publishing (verifies cookie decryption)")
	positionals, code, done := common.ParseArgs(fs, args[1:], usage, stdout, stderr)
	if done {
		return code
	}
	if len(positionals) > 0 {
		fmt.Fprintf(stderr, "pdw whoop publish-session: unexpected argument %q\n", positionals[0])
		return 2
	}
	now := time.Now
	if deps.Now != nil {
		now = deps.Now
	}
	moment := now().UTC()

	captured, err := deps.Capture(*browser, moment)
	if err != nil {
		fmt.Fprintf(stderr, "whoop publish-session: %v\n", err)
		return 2
	}
	report := captured.Redacted()
	// round(hours, 1) rendered as a Python float ("12.0", not "12").
	report["access_token_valid_for_hours"] = json.Number(common.FloatRepr(math.Round(captured.AccessExpiresAt.Sub(moment).Hours()*10) / 10))
	if !captured.AccessExpiresAt.After(moment) {
		// Not fatal: the poller refreshes on first use.
		report["note"] = "access token already expired; the server refreshes it on first use"
	}
	emit := func() {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
	}
	if *dryRun {
		report["published"] = false
		emit()
		return 0
	}
	resolvedAccount := ResolveAccount(*account, getenv)
	if resolvedAccount == "" {
		fmt.Fprintln(stderr, "no account resolved; pass --account (it keys the credential and the sync cursors)")
		return 1
	}
	publish, err := deps.Publisher()
	if err != nil {
		fmt.Fprintf(stderr, "whoop publish-session: cannot publish; warehouse upload is not configured: %v\n", err)
		return 1
	}
	ack, err := publish(ingestclient.WhoopSession{
		Account:          resolvedAccount,
		SessionKey:       *sessionKey,
		AccessToken:      captured.AccessToken,
		RefreshToken:     captured.RefreshToken,
		AccessExpiresAt:  Isoformat(captured.AccessExpiresAt),
		RefreshExpiresAt: Isoformat(captured.RefreshExpiresAt),
		SourceBrowser:    captured.Browser,
	})
	if err != nil {
		fmt.Fprintf(stderr, "whoop publish-session: publish failed: %v\n", err)
		return 1
	}
	report["published"] = true
	report["account"] = resolvedAccount
	report["acknowledgement"] = ack
	emit()
	return 0
}
