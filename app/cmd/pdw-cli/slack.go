package main

import (
	"fmt"
	"io"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/slack"
)

// slackRun is the native publisher (app/internal/browsersessions/slack). It
// parses its own flags, so pdw forwards everything after "slack" verbatim,
// verb included. A package var so tests can capture the dispatch.
var slackRun localCommand = slack.Run

const slackUsage = `pdw slack - manage the Slack client session the warehouse syncs with.

USAGE
  pdw slack publish-session [flags]

Slack's public API cannot say which conversations have new messages:
conversations.list returns no last-message marker, so finding one costs a
conversations.history call per conversation -- far more than Slack's rate limit
allows, which is why backfills starve. Slack's own client answers it in a single
request (client.counts), but only for a real signed-in session.

publish-session reads that session from the Slack desktop app on this Mac (the
xoxc token and the "d" cookie, which are useless apart), checks it against Slack,
and publishes it to the warehouse. macOS will ask once to allow keychain
access -- choose "Always Allow", because a one-shot "Allow" makes every later
run fail.

The session cookie is good for about a year and rolls forward as you use Slack,
so this is setup, not a chore. The hourly slack-auth LaunchAgent re-publishes so
the server's copy never lags the app's.

FLAGS (see "pdw slack publish-session --help")
  --account LABEL     Account the credential is stored under.
  --session-key KEY   Session key for multiple accounts (default "default").
  --source NAME       Force a session source (default: the Slack desktop app).
  --team-id T...      Workspace id, when the enterprise covers several.
  --dry-run           Capture, validate and report without publishing.

The session is posted over the same URL + token pdw uses for everything else:
run "pdw login" once (or set PDW_API_URL + PDW_SECRET_TOKEN).

ENVIRONMENT
  PDW_API_URL             Warehouse URL the session is published to.
  PDW_SECRET_TOKEN        App secret token used to sign the upload.
`

// runSlack dispatches `pdw slack <subcommand>` to the native publisher with
// the same warehouse-config plumbing as `pdw ingest`.
func runSlack(
	args []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
	getenv func(string) string,
	flagBaseURL, flagToken string,
) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "pdw slack: a subcommand is required (e.g. publish-session)")
		fmt.Fprint(stderr, slackUsage)
		return 2
	}
	if args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(stdout, slackUsage)
		return 0
	}
	return slackRun(args, stdin, stdout, stderr, getenv, resolveLocalConfig(getenv, flagBaseURL, flagToken))
}
