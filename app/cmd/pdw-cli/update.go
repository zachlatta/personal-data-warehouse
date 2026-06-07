package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

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

func runUpdate(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	check := fs.Bool("check", false, "only report whether an update is available")
	force := fs.Bool("force", false, "reinstall even if already on the latest version")
	repo := fs.String("repo", "", "GitHub repo in owner/name form (default $PDW_REPO or "+defaultRepo+")")
	target := fs.String("target", "", "path to replace (default: this binary)")
	api := fs.String("github-api", "", "GitHub API base URL (default https://api.github.com; for tests)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(stderr, "pdw update:", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintln(stderr, "pdw update: unexpected positional arguments")
		return 2
	}

	// PDW_REPO is the current name; PDW_CLI_REPO is still honored for configs
	// written before the pdw-cli -> pdw rename.
	resolvedRepo := firstNonEmpty(*repo, getenv("PDW_REPO"), getenv("PDW_CLI_REPO"), defaultRepo)
	resolvedTarget := *target
	if resolvedTarget == "" {
		exe, err := os.Executable()
		if err != nil {
			fmt.Fprintf(stderr, "pdw update: cannot determine current binary path: %v\n", err)
			return 1
		}
		resolvedTarget = exe
	}

	client := selfupdate.NewClient(resolvedRepo)
	if *api != "" {
		client.SetBaseURL(*api)
	}

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
