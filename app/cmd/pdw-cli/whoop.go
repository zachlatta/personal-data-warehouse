package main

import (
	"fmt"
	"io"
)

// whoopModule is the Python module implementing `pdw whoop`. Like the ingest
// uploaders it runs via `uv run python -m <module>` and parses its own flags,
// so pdw forwards everything after the verb verbatim.
const whoopModule = "personal_data_warehouse.whoop_private_setup"

const whoopUsage = `pdw whoop - manage the WHOOP browser session used for high-resolution sync.

USAGE
  pdw whoop publish-session [flags]

The public WHOOP developer API carries no time series at all. The private API
the WHOOP web app itself uses carries per-6-second heart rate, the sleep
hypnogram, journal entries and the trend metrics -- but it enforces MFA, so
there is no unattended login. The credential is therefore a captured browser
session: log in to app.whoop.com in Chrome once, then run this.

publish-session reads the session cookies from a local Chrome-family browser
(you'll be asked once to allow keychain access) and publishes them to the
warehouse over the same signed endpoint everything else uses.

You should rarely need to re-run it. Every refresh issues a NEW refresh token
and slides its 30-day window forward, so a healthy sync keeps itself alive
indefinitely. Re-run this only after the session is revoked, the password
changes, or sync has been down for more than 30 days -- /pipelines will say so.

FLAGS (forwarded; see "pdw whoop publish-session --help")
  --browser NAME     Force a browser (chrome|brave|edge|arc|chromium|vivaldi).
  --account EMAIL    Account label/key for the session (default: configured account).
  --session-key KEY  Session key for multiple accounts (default "default").
  --dry-run          Capture and report without publishing (verifies decryption).

ENVIRONMENT
  PDW_UV_BIN              uv launcher path (default: uv on PATH).
  PDW_INGEST_PROJECT_DIR  Repo checkout uv runs in (default: current directory).
  PDW_API_URL             Warehouse URL the session is published to (else "pdw login").
  PDW_SECRET_TOKEN        App secret token used to sign the upload (else "pdw login").
`

// runWhoop dispatches `pdw whoop <subcommand>` to the Python module, reusing the
// same uv launcher and warehouse-config plumbing as `pdw ingest` and `pdw chatgpt`.
func runWhoop(
	args []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
	getenv func(string) string,
	flagBaseURL, flagToken string,
) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "pdw whoop: a subcommand is required (e.g. publish-session)")
		fmt.Fprint(stderr, whoopUsage)
		return 2
	}
	if args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(stdout, whoopUsage)
		return 0
	}
	argv := ingestArgv(whoopModule, args)
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
