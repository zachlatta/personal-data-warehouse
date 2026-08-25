package main

import (
	"fmt"
	"io"
)

// slackModule is the Python module implementing `pdw slack`. Like the ingest
// uploaders it runs via `uv run python -m <module>` and parses its own flags,
// so pdw forwards everything after the verb verbatim.
const slackModule = "personal_data_warehouse.slack_setup"

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

FLAGS (forwarded; see "pdw slack publish-session --help")
  --account LABEL     Account the credential is stored under.
  --session-key KEY   Session key for multiple accounts (default "default").
  --source NAME       Force a session source (default: the Slack desktop app).
  --team-id T...      Workspace id, when the enterprise covers several.
  --dry-run           Capture, validate and report without publishing.

The session is posted over the same URL + token pdw uses for everything else:
run "pdw login" once (or set PDW_API_URL + PDW_SECRET_TOKEN).

ENVIRONMENT
  PDW_UV_BIN              uv launcher path (default: uv on PATH).
  PDW_INGEST_PROJECT_DIR  Repo checkout uv runs in (default: current directory).
  PDW_API_URL             Warehouse URL the session is published to.
  PDW_SECRET_TOKEN        App secret token used to sign the upload.
`

// runSlack dispatches `pdw slack <subcommand>` to the Python module, reusing the
// same uv launcher and warehouse-config plumbing as `pdw ingest`.
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
	// publish-session is the only verb; the Python module parses the rest.
	rest := args
	if args[0] == "publish-session" {
		rest = args[1:]
	}
	argv := ingestArgv(slackModule, rest)
	return ingestExec(
		ingestUvBin(getenv),
		argv,
		ingestProjectDir(getenv),
		ingestEnvAdditions(getenv, flagBaseURL, flagToken),
		stdin,
		stdout,
		stderr,
	)
}
