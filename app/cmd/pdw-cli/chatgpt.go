package main

import (
	"fmt"
	"io"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chatgpt"
)

// chatgptRun is the native publisher (app/internal/browsersessions/chatgpt).
// It parses its own flags, so pdw forwards everything after "chatgpt"
// verbatim, verb included. A package var so tests can capture the dispatch.
var chatgptRun localCommand = chatgpt.Run

const chatgptUsage = `pdw chatgpt - manage the ChatGPT web session used for server-side ingestion.

USAGE
  pdw chatgpt publish-session [flags]

ChatGPT's desktop app encrypts its conversations under a key we cannot read, so
the warehouse syncs ChatGPT server-side via its backend API. That needs a
chatgpt.com web session, which only exists in a browser. publish-session reads
your login from a local Chrome-family browser (Chrome/Brave/Edge/Arc), decrypts
the session cookie (you'll be asked once to allow keychain access), validates it
against ChatGPT, and publishes it to the warehouse.

The captured access token is minted only by a browser sign-in and carries a hard
10-day expiry that nothing server-side can renew, so the chatgpt-auth LaunchAgent
re-publishes hourly to keep the server's copy as fresh as the browser's. When the
browser's own session is what has lapsed, publish-session says so and you sign
into chatgpt.com again.

FLAGS (see "pdw chatgpt publish-session --help")
  --browser NAME     Force a browser (chrome|brave|edge|arc|chromium|vivaldi).
  --account EMAIL    Account label/key for the session (default $CHATGPT_ACCOUNT/fallback).
  --session-key KEY  Session key for multiple accounts (default "default").
  --non-interactive  Never install a browser or prompt to sign in (used by the
                     hourly chatgpt-auth LaunchAgent).
  --dry-run          Validate and report without publishing.

The session is posted to the warehouse over the same URL + token pdw uses for
everything else: run "pdw login" once (or set PDW_API_URL + PDW_SECRET_TOKEN).

ENVIRONMENT
  PDW_API_URL            Warehouse URL the session is published to (else "pdw login").
  PDW_SECRET_TOKEN       App secret token used to sign the upload (else "pdw login").
`

// runChatGPT dispatches `pdw chatgpt <subcommand>` to the native publisher
// with the same warehouse-config plumbing as `pdw ingest`.
func runChatGPT(
	args []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
	getenv func(string) string,
	flagBaseURL, flagToken string,
) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "pdw chatgpt: a subcommand is required (e.g. publish-session)")
		fmt.Fprint(stderr, chatgptUsage)
		return 2
	}
	if args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(stdout, chatgptUsage)
		return 0
	}
	return chatgptRun(args, stdin, stdout, stderr, getenv, resolveLocalConfig(getenv, flagBaseURL, flagToken))
}
