package main

import (
	"fmt"
	"io"
	"sort"
	"strings"

	mwapplecontacts "github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applecontacts"
	mwapplenotes "github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applenotes"
)

// mutationWorkers maps a `pdw mutations <provider>` name to the local worker
// that applies that provider's approved mutations through its Mac app. A
// package var so tests can capture the dispatch.
var mutationWorkers = map[string]localCommand{
	"apple-notes":    mwapplenotes.Spec.Run,
	"apple-contacts": mwapplecontacts.Spec.Run,
}

func mutationWorkerNames() []string {
	names := make([]string, 0, len(mutationWorkers))
	for name := range mutationWorkers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

const mutationsUsage = `pdw mutations - apply approved local-only mutations through this Mac's apps.

USAGE
  pdw mutations <provider> [--once]

Apple Notes and Apple Contacts have no server API, so a reviewed mutation for
them is applied here, by asking the app itself over AppleScript. Without
flags the process is the resident worker: it LISTENs on the warehouse for
approvals, drains the queue, polls as a fallback, and stops on SIGTERM/SIGINT.
With --once it applies one batch and exits, which is what the ingest
uploaders' wrappers run before a scan.

PROVIDERS
  apple-notes      apple_notes.create_note / update_note through Notes.app
  apple-contacts   apple_contacts.create_contact / update_contact /
                   merge_contacts through Contacts.app

FLAGS
  --once           Apply one batch and exit instead of staying resident.

Run "pdw mutations <provider> --help" for the provider's environment
(POSTGRES_DATABASE_URL, the <PREFIX>_MUTATIONS_ENABLED kill switch, batch and
poll settings). The worker talks to Postgres directly, not to the app.
`

// runMutations dispatches `pdw mutations <provider>` to the native worker.
func runMutations(
	args []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
	getenv func(string) string,
	flagBaseURL, flagToken string,
) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "pdw mutations: a provider is required")
		fmt.Fprint(stderr, mutationsUsage)
		return 2
	}
	if args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(stdout, mutationsUsage)
		return 0
	}
	run, ok := mutationWorkers[args[0]]
	if !ok {
		fmt.Fprintf(stderr, "pdw mutations: unknown provider %q; valid providers: %s\n", args[0], strings.Join(mutationWorkerNames(), ", "))
		return 2
	}
	return run(args[1:], stdin, stdout, stderr, getenv, resolveLocalConfig(getenv, flagBaseURL, flagToken))
}
