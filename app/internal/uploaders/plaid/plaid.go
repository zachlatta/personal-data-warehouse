package plaid

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
)

const prog = "pdw ingest plaid"

const usageText = `usage: ` + prog + ` [-h] {link,update,sync,items,unlink} ...

Link Plaid items and manage Plaid-backed personal finance data.

positional arguments:
  {link,update,sync,items,unlink}
    link                link a genuinely new institution (not an existing-Item repair)
    update              repair consent for an existing Item without replacing it
    sync                (not a CLI operation: runs as the Dagster plaid_finance_sync asset)
    items               list linked Plaid items with their row counts
    unlink              retire a linked Plaid item: revoke it at Plaid and delete its warehouse rows

options:
  -h, --help            show this help message and exit
`

const linkUsage = `usage: ` + prog + ` link [-h] [--host HOST] [--port PORT] [--no-browser]

options:
  -h, --help    show this help message and exit
  --host HOST   local host for the Plaid Link callback server
  --port PORT   local port for the Plaid Link callback server (0 picks an open port)
  --no-browser  print the local Link URL without opening a browser
`

const updateUsage = `usage: ` + prog + ` update [-h] [--host HOST] [--port PORT] [--no-browser] item_id

positional arguments:
  item_id       existing item id or unambiguous prefix (see ` + "`plaid items`" + `)

options:
  -h, --help    show this help message and exit
  --host HOST   local host for the Plaid Link callback server
  --port PORT   local port for the Plaid Link callback server (0 picks an open port)
  --no-browser  print the local Link URL without opening a browser
`

const syncUsage = `usage: ` + prog + ` sync [-h]

Not a CLI operation: the Plaid sync runs in the production Dagster deployment
every 30 minutes as the plaid_finance_sync asset.

options:
  -h, --help  show this help message and exit
`

const itemsUsage = `usage: ` + prog + ` items [-h]

options:
  -h, --help  show this help message and exit
`

const unlinkUsage = `usage: ` + prog + ` unlink [-h] [--yes] [--dry-run] [--skip-remote] item_id

positional arguments:
  item_id        item id, or an unambiguous prefix of one (see ` + "`plaid items`" + `)

options:
  -h, --help     show this help message and exit
  --yes          skip the confirmation prompt
  --dry-run      print what would be revoked and deleted
  --skip-remote  do not call Plaid /item/remove (for an item already revoked in the Plaid dashboard)
`

// SyncRefusal is what `pdw ingest plaid sync` prints: the sync is the
// Dagster asset's job, not the CLI's.
const SyncRefusal = prog + ` sync: the Plaid sync is not a CLI operation.
It runs in the production Dagster deployment every 30 minutes as the
plaid_finance_sync asset; check marts_ops.pipeline_health (pipeline = 'plaid')
or the Dagster UI instead.`

// Options is the parsed command line.
type Options struct {
	Command    string
	ItemID     string
	Host       string
	Port       int
	NoBrowser  bool
	Yes        bool
	DryRun     bool
	SkipRemote bool
	Help       bool
}

// usageError carries argparse's exit code 2 shape: usage line + message.
type usageError struct {
	usage   string
	message string
}

func (e *usageError) Error() string { return e.message }

// ParseArgs mirrors build_parser() in cli.py.
func ParseArgs(args []string) (Options, error) {
	opts := Options{Host: "127.0.0.1"}
	if len(args) == 0 {
		return opts, nil
	}
	switch args[0] {
	case "-h", "--help":
		opts.Help = true
		return opts, nil
	case "link", "update", "sync", "items", "unlink":
		opts.Command = args[0]
	default:
		if strings.HasPrefix(args[0], "-") {
			return opts, &usageError{usage: usageText, message: "unrecognized arguments: " + strings.Join(args, " ")}
		}
		return opts, &usageError{usage: usageText, message: fmt.Sprintf(
			"argument command: invalid choice: '%s' (choose from link, update, sync, items, unlink)", args[0])}
	}
	usage := commandUsage(opts.Command)
	var positionals []string
	rest := args[1:]
	for i := 0; i < len(rest); i++ {
		arg := rest[i]
		if arg == "-h" || arg == "--help" {
			opts.Help = true
			return opts, nil
		}
		name, value, hasValue := strings.Cut(arg, "=")
		if !strings.HasPrefix(arg, "--") {
			if strings.HasPrefix(arg, "-") && len(arg) > 1 {
				return opts, &usageError{usage: usage, message: "unrecognized arguments: " + arg}
			}
			positionals = append(positionals, arg)
			continue
		}
		takeValue := func() (string, error) {
			if hasValue {
				return value, nil
			}
			if i+1 >= len(rest) {
				return "", &usageError{usage: usage, message: fmt.Sprintf("argument %s: expected one argument", name)}
			}
			i++
			return rest[i], nil
		}
		switch {
		case (opts.Command == "link" || opts.Command == "update") && name == "--host":
			v, err := takeValue()
			if err != nil {
				return opts, err
			}
			opts.Host = v
		case (opts.Command == "link" || opts.Command == "update") && name == "--port":
			v, err := takeValue()
			if err != nil {
				return opts, err
			}
			port, convErr := strconv.Atoi(strings.TrimSpace(v))
			if convErr != nil {
				return opts, &usageError{usage: usage, message: fmt.Sprintf("argument --port: invalid int value: '%s'", v)}
			}
			opts.Port = port
		case (opts.Command == "link" || opts.Command == "update") && name == "--no-browser" && !hasValue:
			opts.NoBrowser = true
		case opts.Command == "unlink" && name == "--yes" && !hasValue:
			opts.Yes = true
		case opts.Command == "unlink" && name == "--dry-run" && !hasValue:
			opts.DryRun = true
		case opts.Command == "unlink" && name == "--skip-remote" && !hasValue:
			opts.SkipRemote = true
		default:
			return opts, &usageError{usage: usage, message: "unrecognized arguments: " + arg}
		}
	}
	wantsItem := opts.Command == "update" || opts.Command == "unlink"
	if wantsItem {
		if len(positionals) == 0 {
			return opts, &usageError{usage: usage, message: "the following arguments are required: item_id"}
		}
		opts.ItemID = positionals[0]
		positionals = positionals[1:]
	}
	if len(positionals) > 0 {
		return opts, &usageError{usage: usage, message: "unrecognized arguments: " + strings.Join(positionals, " ")}
	}
	return opts, nil
}

func commandUsage(command string) string {
	switch command {
	case "link":
		return linkUsage
	case "update":
		return updateUsage
	case "sync":
		return syncUsage
	case "items":
		return itemsUsage
	case "unlink":
		return unlinkUsage
	}
	return usageText
}

// Run is the `pdw ingest plaid` entry point. cfg is the app URL/token every
// uploader receives; Plaid needs Postgres and Plaid credentials from getenv
// (layered over the repo .env) rather than the app, but takes it for
// uniformity.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	_ = cfg
	opts, err := ParseArgs(args)
	if err != nil {
		var usageErr *usageError
		if ue, ok := err.(*usageError); ok {
			usageErr = ue
		}
		if usageErr != nil {
			fmt.Fprint(stderr, firstLine(usageErr.usage))
			fmt.Fprintf(stderr, "%s: error: %s\n", prog, usageErr.message)
		} else {
			fmt.Fprintf(stderr, "%s: error: %s\n", prog, err)
		}
		return 2
	}
	if opts.Help {
		fmt.Fprint(stdout, commandUsage(opts.Command))
		return 0
	}
	if opts.Command == "" {
		// The Python parser defaulted a bare invocation to `sync`; that no
		// longer exists on the CLI, so say what to run instead.
		fmt.Fprint(stderr, usageText)
		fmt.Fprintln(stderr, SyncRefusal)
		return 2
	}
	if opts.Command == "sync" {
		fmt.Fprintln(stderr, SyncRefusal)
		return 1
	}
	return runWithDeps(opts, stdin, stdout, stderr, WithDotenv(getenv), nil, nil)
}

// runWithDeps is Run after parsing, with the store and client injectable.
// A nil newStore/newClient uses Postgres and the real Plaid API.
func runWithDeps(
	opts Options,
	stdin io.Reader,
	stdout, stderr io.Writer,
	getenv func(string) string,
	newStore func(cfg Config) (Store, error),
	newClient func(cfg Config) API,
) int {
	cfg, err := LoadConfig(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "%s: %s\n", prog, err)
		return 1
	}
	if newStore == nil {
		newStore = func(cfg Config) (Store, error) { return OpenPostgres(cfg.DatabaseURL) }
	}
	if newClient == nil {
		newClient = func(cfg Config) API { return NewClient(cfg, nil) }
	}
	store, err := newStore(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "%s: %s\n", prog, Redact(err.Error(), cfg.Secret, cfg.DatabaseURL))
		return 1
	}
	defer store.Close()
	ctx := context.Background()
	switch opts.Command {
	case "link", "update":
		flow := &linkFlow{
			cfg:        cfg,
			store:      store,
			client:     newClient(cfg),
			newServer:  NewLinkServer,
			open:       OpenBrowser,
			stdout:     stdout,
			stderr:     stderr,
			host:       opts.Host,
			port:       opts.Port,
			noBrowser:  opts.NoBrowser,
			update:     opts.Command == "update",
			updateItem: opts.ItemID,
			now:        time.Now,
		}
		return flow.run(ctx)
	case "items":
		code, err := runItems(ctx, store, stdout)
		if err != nil {
			fmt.Fprintf(stderr, "%s items: %s\n", prog, Redact(err.Error(), cfg.Secret, cfg.DatabaseURL))
		}
		return code
	case "unlink":
		if err := store.EnsurePlaidTables(ctx); err != nil {
			fmt.Fprintf(stderr, "%s unlink: %s\n", prog, Redact(err.Error(), cfg.Secret, cfg.DatabaseURL))
			return 1
		}
		items, err := store.LoadItemTokens(ctx)
		if err != nil {
			fmt.Fprintf(stderr, "%s unlink: %s\n", prog, Redact(err.Error(), cfg.Secret, cfg.DatabaseURL))
			return 1
		}
		item, err := ResolveItem(items, opts.ItemID)
		if err != nil {
			fmt.Fprintf(stderr, "%s unlink: %s\n", prog, err)
			fmt.Fprintln(stderr, "Run `pdw ingest plaid items` to list linked items.")
			return 2
		}
		confirm := confirmOnStdin(stdin, stdout)
		if opts.Yes {
			confirm = func(string) bool { return true }
		}
		code, err := UnlinkItem(ctx, store, newClient(cfg), item, confirm, stdout, opts.DryRun, opts.SkipRemote)
		if err != nil {
			fmt.Fprintf(stderr, "%s unlink: %s\n", prog, Redact(err.Error(), cfg.Secret, cfg.DatabaseURL, item.AccessToken))
		}
		return code
	}
	fmt.Fprint(stderr, usageText)
	return 2
}

func firstLine(usage string) string {
	line, _, _ := strings.Cut(usage, "\n")
	return line + "\n"
}
