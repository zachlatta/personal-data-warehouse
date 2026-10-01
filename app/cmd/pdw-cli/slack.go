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

const slackUsage = `pdw slack - manage the Slack web session the warehouse syncs with.

USAGE
  pdw slack publish-session [flags] < paste

The warehouse polls your DMs, group DMs and channels with this session every
five minutes (Slack rate-limits it far less than the workspace token), and
reviewed Slack writes -- a message sent as you, a conversation marked read --
use it too.

publish-session reads that session from what you paste -- the JSON a DevTools
console snippet copies on app.slack.com, then the "d" cookie -- and publishes
it to the warehouse. It sends nothing to Slack: the old hourly capture from the
Slack desktop app called Slack with your desktop login from a Go program, and
Slack signed you out of every device each time it ran. Run it once from a
terminal to see the steps; re-run it when the freshness pass logs that Slack
refused the session (it falls back to the slower workspace token meanwhile).

FLAGS (see "pdw slack publish-session --help")
  --account LABEL     Account the credential is stored under.
  --session-key KEY   Session key for multiple accounts (default "default").
  --team-id T...      Workspace to publish, when the paste names several.
  --user-id U...      Your user id, only for a bare xoxc token pasted without the JSON.
  --dry-run           Read and check the paste without publishing.

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
