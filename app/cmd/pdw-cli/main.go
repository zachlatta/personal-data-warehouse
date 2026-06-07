// Command pdw is a small command-line client for the personal data warehouse
// HTTP API. It discovers the available tools at runtime by calling
// GET /api/tools and lets the user invoke any of them by name. See `pdw help`
// for usage.
//
// The command is installed as `pdw`. The source directory, GitHub release
// artifacts, and self-update tar member keep the historical `pdw-cli` name so
// that binaries installed before the rename can still self-update; see
// internal/selfupdate and the release workflow.
package main

import (
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Getenv))
}
