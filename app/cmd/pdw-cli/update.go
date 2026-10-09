package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
	"github.com/zachlatta/personal-data-warehouse/app/internal/selfupdate"
)

func runVersion(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		fmt.Fprintln(stderr, "pdw version: unexpected arguments")
		return 2
	}
	fmt.Fprintln(stdout, version)
	return 0
}

// runUpdate replaces this binary with the latest release, read through the
// warehouse app's release proxy (--base-url > PDW_API_URL > saved config >
// the default app), never from GitHub directly.
func runUpdate(args []string, stdout, stderr io.Writer, getenv func(string) string, flagBase string) int {
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	check := fs.Bool("check", false, "only report whether an update is available")
	force := fs.Bool("force", false, "reinstall even if already on the latest version")
	target := fs.String("target", "", "path to replace (default: this binary)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(stderr, "pdw update:", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintln(stderr, "pdw update: unexpected positional arguments")
		return 2
	}

	var savedBase string
	if loaded, _, err := cliconfig.Resolve(getenv); err == nil {
		savedBase = loaded.BaseURL
	}
	appURL := firstNonEmpty(flagBase, getenv("PDW_API_URL"), savedBase, defaultBaseURL)
	resolvedTarget := *target
	if resolvedTarget == "" {
		exe, err := os.Executable()
		if err != nil {
			fmt.Fprintf(stderr, "pdw update: cannot determine current binary path: %v\n", err)
			return 1
		}
		resolvedTarget = exe
	}

	client := selfupdate.NewClient(appURL)

	ctx := context.Background()
	rel, err := client.LatestRelease(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "pdw update:", err)
		return 1
	}

	newer, err := selfupdate.ShouldUpdate(version, rel.Version)
	if err != nil {
		fmt.Fprintln(stderr, "pdw update:", err)
		return 1
	}

	if *check {
		if newer {
			fmt.Fprintf(stdout, "update available: %s -> %s\n", version, rel.Version)
		} else {
			fmt.Fprintf(stdout, "already up to date (%s)\n", rel.Version)
		}
		return 0
	}

	if !newer && !*force {
		fmt.Fprintf(stdout, "already up to date (%s)\n", rel.Version)
		return 0
	}

	binary, err := client.FetchBinary(ctx, rel, selfupdate.CurrentOS(), selfupdate.CurrentArch())
	if err != nil {
		fmt.Fprintln(stderr, "pdw update:", err)
		return 1
	}
	if err := selfupdate.ApplyUpdate(resolvedTarget, binary); err != nil {
		fmt.Fprintln(stderr, "pdw update:", err)
		return 1
	}
	fmt.Fprintf(stdout, "updated %s to %s\n", resolvedTarget, rel.Version)
	return 0
}
