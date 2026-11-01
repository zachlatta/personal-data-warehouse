package printer

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

const envPrefix = "PRINTER"

// Spec is the printer worker. Nothing is reclaimed from a stale `executing`
// claim: the job may already be at the printer, so a stuck print waits for a
// human exactly like gmail.send_email.
var Spec = queue.Spec{
	DisplayName: "Printer",
	Command:     "printer",
	Provider:    Provider,
	WorkerName:  "printer_mutation_worker",
	LockID:      LockID,
	EnvPrefix:   envPrefix,
	Idempotent:  nil,
	NewExecutor: func(queue.Store) queue.Executor {
		getenv := queue.Getenv(os.Getenv)
		return NewExecutor(strings.TrimSpace(getenv(URIEnv)), ExecRunner, queue.Spec{EnvPrefix: envPrefix}.Enabled(getenv))
	},
}

// Run is the `pdw mutations printer` entry point. Like the scanner's
// SCANNER_HOST, the printer's LAN address is configuration, never a default in
// this public repo, so the worker refuses to start without PRINTER_URI.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	if !wantsHelp(args) && strings.TrimSpace(queue.Getenv(getenv)(URIEnv)) == "" {
		fmt.Fprintf(stderr, "%s must be set to the printer's IPP endpoint (ipp://<host>/ipp/print)\n", URIEnv)
		return 2
	}
	return Spec.Run(args, stdin, stdout, stderr, getenv, cfg)
}

func wantsHelp(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "-h", "-help", "--help", "help":
			return true
		}
	}
	return false
}
