package main

import (
	"fmt"
	"io"
)

// chatgptModule is the Python module implementing `pdw chatgpt`. Like the
// ingest uploaders it runs via `uv run python -m <module>` and parses its own
// flags, so pdw forwards everything after the verb verbatim.
const chatgptModule = "personal_data_warehouse_chatgpt.cli"

const chatgptUsage = `pdw chatgpt - manage the ChatGPT web session used for server-side ingestion.

USAGE
  pdw chatgpt publish-session [flags]

ChatGPT's desktop app encrypts its conversations under a key we cannot read, so
the warehouse syncs ChatGPT server-side via its backend API. That needs a
chatgpt.com web session, which only exists in a browser. publish-session reads
your login from a local Chrome-family browser (Chrome/Brave/Edge/Arc), decrypts
the session cookie (you'll be asked once to allow keychain access), validates it
against ChatGPT, and publishes it to the warehouse. Re-run it whenever the
server reports the session expired.

FLAGS (forwarded to the uploader; see "pdw chatgpt publish-session --help")
  --browser NAME     Force a browser (chrome|brave|edge|arc|chromium|vivaldi).
  --account EMAIL    Account label/key for the session (default $CHATGPT_ACCOUNT/fallback).
  --session-key KEY  Session key for multiple accounts (default "default").
  --dry-run          Validate and report without publishing.

The session is posted to the warehouse over the same URL + token pdw uses for
everything else: run "pdw login" once (or set PDW_API_URL + PDW_SECRET_TOKEN).

ENVIRONMENT
  PDW_UV_BIN              uv launcher path (default: uv on PATH).
  PDW_INGEST_PROJECT_DIR  Repo checkout uv runs in (default: current directory).
  PDW_API_URL            Warehouse URL the session is published to (else "pdw login").
  PDW_SECRET_TOKEN       App secret token used to sign the upload (else "pdw login").
`

// runChatGPT dispatches `pdw chatgpt <subcommand>` to the Python module, reusing
// the same uv launcher and warehouse-config plumbing as `pdw ingest`.
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
	argv := ingestArgv(chatgptModule, args)
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
