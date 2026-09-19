package chatgpt

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// ResolveAccount is the label the stored session is keyed by, "" when none.
func ResolveAccount(explicit string, getenv func(string) string) string {
	candidates := []string{explicit, getenv("CHATGPT_ACCOUNT"), getenv("AGENT_SESSIONS_ACCOUNT"),
		getenv("APPLE_MESSAGES_ACCOUNT"), getenv("VOICE_MEMOS_ACCOUNT")}
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
type Publisher func(account, sessionToken, sessionKey, sourceBrowser string) (map[string]any, error)

// Deps are the seams Run wires to the real machine; tests inject fakes.
type Deps struct {
	Setup     Setup
	Validator interface {
		Validate(cookieHeader string) (Validation, error)
	}
	Publisher func() (Publisher, error)
	Now       func() time.Time
}

func defaultDeps(stdin io.Reader, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) Deps {
	host := chromium.DefaultHost()
	return Deps{
		Setup:     DefaultSetup(host, stdin, stderr),
		Validator: NewValidator(),
		Publisher: func() (Publisher, error) {
			ic, err := ingestclient.FromEnv(getenv, cfg, common.NewWriterLogger(stderr))
			if err != nil {
				return nil, err
			}
			return ic.PublishChatGPTSession, nil
		},
		Now: time.Now,
	}
}

const usage = `usage: pdw chatgpt publish-session [--browser NAME] [--no-install] [--account LABEL] [--session-key KEY] [--non-interactive] [--dry-run]

Capture the chatgpt.com session from a local Chrome-family browser, validate
it against ChatGPT, and publish it to the warehouse for the server-side poller.
`

// Run is the `pdw chatgpt` entry point; args start with the verb.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	return RunWith(args, stdout, stderr, getenv, defaultDeps(stdin, stderr, getenv, cfg))
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
		fmt.Fprintf(stderr, "pdw chatgpt: unknown command %q\n%s", args[0], usage)
		return 2
	}
	fs := common.NewFlagSet("pdw chatgpt publish-session")
	browser := fs.String("browser", "", "Force a specific browser (chrome, brave, edge, arc, chromium, vivaldi). Default: auto-detect.")
	noInstall := fs.Bool("no-install", false, "Do not auto-install a browser if none is found (error instead).")
	account := fs.String("account", "", "Account label/key for the stored session (default: $CHATGPT_ACCOUNT/fallback).")
	sessionKey := fs.String("session-key", "default", "Session key, for multiple ChatGPT accounts.")
	nonInteractive := fs.Bool("non-interactive", false, "Never install a browser or prompt to sign in (for the scheduled LaunchAgent).")
	dryRun := fs.Bool("dry-run", false, "Validate and report the session without publishing it.")
	positionals, code, done := common.ParseArgs(fs, args[1:], usage, stdout, stderr)
	if done {
		return code
	}
	if len(positionals) > 0 {
		fmt.Fprintf(stderr, "pdw chatgpt publish-session: unexpected argument %q\n", positionals[0])
		return 2
	}

	resolvedAccount := ResolveAccount(*account, getenv)
	if resolvedAccount == "" {
		fmt.Fprintln(stderr, "no ChatGPT account configured: pass --account or set CHATGPT_ACCOUNT/GMAIL_ACCOUNTS (this only labels/keys the stored session).")
		return 1
	}

	// Under launchd nobody can answer a prompt or watch a browser install, so
	// the scheduled path must only ever read what is already there.
	profile, err := deps.Setup.EnsureBrowser(*browser, !(*noInstall || *nonInteractive))
	if err != nil {
		fmt.Fprintf(stderr, "pdw chatgpt: %v\n", err)
		return 1
	}
	var captured CapturedSession
	if *nonInteractive {
		captured, err = deps.Setup.Discover(profile.Key)
	} else {
		captured, err = deps.Setup.EnsureLoggedIn(profile)
	}
	if err != nil {
		fmt.Fprintf(stderr, "pdw chatgpt: %v\n", err)
		return 1
	}

	validation, err := deps.Validator.Validate(captured.CookieHeader)
	if err != nil {
		var authErr *AuthError
		if errors.As(err, &authErr) {
			fmt.Fprintf(stderr, "pdw chatgpt: found a session in %s but ChatGPT rejected it (%v). Log into chatgpt.com in that browser and retry.\n", captured.Browser, err)
			return 1
		}
		fmt.Fprintf(stderr, "pdw chatgpt: could not validate the session found in %s: %v\n", captured.Browser, err)
		return 1
	}
	now := time.Now
	if deps.Now != nil {
		now = deps.Now
	}
	if validation.Blocked {
		// The Python client impersonated Chrome at the TLS layer; a plain Go
		// client cannot, so the challenge is expected. Publishing an
		// unvalidated session still beats none: the server-side Dagster poller
		// validates it for real and reports on /pipelines.
		fmt.Fprintf(stderr, "pdw chatgpt: WARNING: local validation was blocked: %s. Publishing the session found in %s (%d chatgpt.com cookies) unvalidated; the server-side poller validates it and reports on /pipelines.\n",
			validation.BlockedReason, captured.Browser, captured.CookieCount)
	} else {
		fmt.Fprintf(stdout, "Found ChatGPT session in %s (%d chatgpt.com cookies). Signed in as %s.\n",
			captured.Browser, captured.CookieCount, validation.SignedInAs)
		// Publish a near-dead session anyway - some ingest beats none - but say
		// so on stderr, which is what the LaunchAgent's run log captures.
		if warning := TokenExpiryWarning(validation.AccessTokenExpiry, now()); warning != "" {
			fmt.Fprintf(stderr, "pdw chatgpt: WARNING: %s\n", warning)
		}
	}

	if *dryRun {
		fmt.Fprintln(stdout, "--dry-run: not publishing the session.")
		return 0
	}
	publish, err := deps.Publisher()
	if err != nil {
		fmt.Fprintf(stderr, "pdw chatgpt: cannot publish; warehouse upload is not configured: %v. Run `pdw login` (or set PDW_API_URL + PDW_SECRET_TOKEN).\n", err)
		return 1
	}
	ack, err := publish(resolvedAccount, captured.CookieHeader, *sessionKey, captured.Browser)
	if err != nil {
		fmt.Fprintf(stderr, "pdw chatgpt: publish failed: %v\n", err)
		return 1
	}
	tokenSHA, _ := ack["token_sha256"].(string)
	if len(tokenSHA) > 12 {
		tokenSHA = tokenSHA[:12]
	}
	fmt.Fprintf(stdout, "Published ChatGPT session for %s (key=%s, browser=%s, token_sha256=%s...). The server-side poller will pick it up on its next tick.\n",
		resolvedAccount, *sessionKey, captured.Browser, tokenSHA)
	return 0
}
