package main

import (
	"fmt"
	"io"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/hackernews"
)

// hackerNewsRun is the native publisher (app/internal/browsersessions/hackernews).
// It parses its own flags, so pdw forwards everything after "hn" verbatim, verb
// included. A package var so tests can capture the dispatch.
var hackerNewsRun localCommand = hackernews.Run

const hackerNewsUsage = `pdw hn - manage the Hacker News login used for the login-only lists.

USAGE
  pdw hn publish-session [flags]

Your own stories and comments, your favorites, and every discussion under them
are public and the warehouse reads them with no credential. The lists HN shows
only to you -- upvoted and hidden -- need your login. publish-session reads the
news.ycombinator.com "user" cookie from a local Chrome-family browser (you'll be
asked once to allow keychain access), checks it against HN, and publishes it to
the warehouse over the same signed endpoint everything else uses.

The cookie is long-lived, so this is setup rather than a chore. Re-run it when
/pipelines shows the hacker_news pipeline in action_required: that means HN
answered a login page for the stored cookie.

FLAGS (see "pdw hn publish-session --help")
  --browser NAME     Force a browser (chrome|brave|edge|arc|chromium|vivaldi).
  --account NAME     HN username the session belongs to (default: configured).
  --session-key KEY  Session key for multiple accounts (default "default").
  --dry-run          Capture and validate without publishing.

ENVIRONMENT
  PDW_API_URL             Warehouse URL the session is published to (else "pdw login").
  PDW_SECRET_TOKEN        App secret token used to sign the upload (else "pdw login").
`

// runHackerNews dispatches `pdw hn <subcommand>` to the native publisher with
// the same warehouse-config plumbing as `pdw ingest` and `pdw whoop`.
func runHackerNews(
	args []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
	getenv func(string) string,
	flagBaseURL, flagToken string,
) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "pdw hn: a subcommand is required (e.g. publish-session)")
		fmt.Fprint(stderr, hackerNewsUsage)
		return 2
	}
	if args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(stdout, hackerNewsUsage)
		return 0
	}
	return hackerNewsRun(args, stdin, stdout, stderr, getenv, resolveLocalConfig(getenv, flagBaseURL, flagToken))
}
