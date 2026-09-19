package hackernews

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// ResolveAccount is the HN username the session is published under: the
// explicit flag, then HACKER_NEWS_ACCOUNT, then the cookie's own user id.
func ResolveAccount(explicit string, getenv func(string) string, capturedUserID string) string {
	for _, c := range []string{explicit, getenv("HACKER_NEWS_ACCOUNT"), capturedUserID} {
		if v := strings.TrimSpace(c); v != "" {
			return v
		}
	}
	return ""
}

// Publisher posts the captured session to the warehouse.
type Publisher func(ingestclient.HackerNewsSession) (map[string]any, error)

// Deps are the seams Run wires to the real machine; tests inject fakes.
type Deps struct {
	Capture    func(browser string) (Session, error)
	CheckLogin func(cookieHeader, userID string) (bool, error)
	Publisher  func() (Publisher, error)
}

func defaultDeps(getenv func(string) string, cfg ingestclient.Config, stderr io.Writer) Deps {
	host := chromium.DefaultHost()
	return Deps{
		Capture:    func(browser string) (Session, error) { return Capture(host, browser) },
		CheckLogin: NewValidator().CheckLogin,
		Publisher: func() (Publisher, error) {
			ic, err := ingestclient.FromEnv(getenv, cfg, common.NewWriterLogger(stderr))
			if err != nil {
				return nil, err
			}
			return ic.PublishHackerNewsSession, nil
		},
	}
}

const usage = `usage: pdw hn publish-session [--account NAME] [--session-key KEY] [--browser NAME] [--dry-run] [--skip-check]

Capture the news.ycombinator.com login cookie from a local Chrome-family
browser, check it against a login-only HN page, and publish it to the
warehouse so the hacker_news sync can read the upvoted and hidden lists. Run
it on the Mac whose browser is signed in to HN; --dry-run verifies cookie
decryption and the check without publishing. The cookie never reaches stdout.
`

// Run is the `pdw hn` entry point; args start with the verb. The project
// .env (PDW_INGEST_PROJECT_DIR, else the working directory) is layered under
// the process environment, never overriding it, as the Python command's
// settings loader did.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "hn publish-session: %v\n", err)
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
		fmt.Fprintf(stderr, "pdw hn: unknown command %q\n%s", args[0], usage)
		return 2
	}
	fs := common.NewFlagSet("pdw hn publish-session")
	account := fs.String("account", "", "HN username the session belongs to")
	sessionKey := fs.String("session-key", "default", "session key")
	browser := fs.String("browser", "", "chrome, brave, edge, arc, chromium, vivaldi")
	dryRun := fs.Bool("dry-run", false, "capture and validate without publishing (verifies cookie decryption)")
	skipCheck := fs.Bool("skip-check", false, "do not validate the cookie against HN")
	positionals, code, done := common.ParseArgs(fs, args[1:], usage, stdout, stderr)
	if done {
		return code
	}
	if len(positionals) > 0 {
		fmt.Fprintf(stderr, "pdw hn publish-session: unexpected argument %q\n", positionals[0])
		return 2
	}

	captured, err := deps.Capture(*browser)
	if err != nil {
		fmt.Fprintf(stderr, "hn publish-session: %v\n", err)
		return 2
	}
	resolvedAccount := ResolveAccount(*account, getenv, captured.UserID)
	if resolvedAccount == "" {
		fmt.Fprintln(stderr, "no account resolved; pass --account <hn-username>")
		return 1
	}
	report := captured.Redacted()
	report["account"] = resolvedAccount
	if captured.UserID != resolvedAccount {
		fmt.Fprintf(stderr, "hn publish-session: the browser is signed in as %q but the account is %q; refusing to publish one user's cookie under another's name.\n", captured.UserID, resolvedAccount)
		return 2
	}

	if !*skipCheck {
		ok, err := deps.CheckLogin(captured.CookieHeader, resolvedAccount)
		if err != nil {
			fmt.Fprintf(stderr, "hn publish-session: could not check the cookie against news.ycombinator.com: %v\n", err)
			return 1
		}
		if !ok {
			fmt.Fprintln(stderr, "hn publish-session: news.ycombinator.com answered a login page for this cookie; sign in again in that browser and retry.")
			return 1
		}
		report["validated"] = true
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
	publish, err := deps.Publisher()
	if err != nil {
		fmt.Fprintf(stderr, "hn publish-session: cannot publish; warehouse upload is not configured: %v\n", err)
		return 1
	}
	ack, err := publish(ingestclient.HackerNewsSession{
		Account:       resolvedAccount,
		SessionKey:    *sessionKey,
		SessionToken:  captured.CookieHeader,
		SourceBrowser: captured.Browser,
	})
	if err != nil {
		fmt.Fprintf(stderr, "hn publish-session: publish failed: %v\n", err)
		return 1
	}
	report["published"] = true
	report["acknowledgement"] = ack
	emit()
	return 0
}
