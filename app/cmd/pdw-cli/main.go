// Command pdw-cli is a small command-line client for the personal data
// warehouse HTTP API. It discovers the available tools at runtime by calling
// GET /api/tools and lets the user invoke any of them by name. See `pdw-cli
// help` for usage.
package main

import (
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Getenv))
}
