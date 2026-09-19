package common

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// The uploader command-line scaffolding every `pdw ingest <source>` entry
// point shares: flag parsing with argparse's exit codes, the project .env,
// the nonblocking run lock, and the one guard hook tests need.

// ExitUsage is argparse's exit status for a bad or missing argument.
const ExitUsage = 2

// ExitFailure is the status for a run that started and failed (an uncaught
// exception in the Python CLIs).
const ExitFailure = 1

// NewFlagSet builds a FlagSet that reports rather than prints: a bad flag
// comes back as an error the caller formats once, and -h/--help as
// flag.ErrHelp (so the caller prints usage to stdout and exits 0, like
// argparse).
func NewFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}

// ParseInterleaved parses args allowing positionals and flags in any order
// (argparse's default), returning the positionals. A "--" ends flag parsing.
func ParseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var positionals []string
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			return nil, err
		}
		remaining := fs.Args()
		if len(remaining) == 0 {
			return positionals, nil
		}
		consumed := len(rest) - len(remaining)
		if consumed > 0 && rest[consumed-1] == "--" {
			return append(positionals, remaining...), nil
		}
		positionals = append(positionals, remaining[0])
		rest = remaining[1:]
	}
}

// ParseArgs runs the parser and classifies the outcome: help requested (print
// usage, exit 0), a usage error (exit 2), or success.
func ParseArgs(fs *flag.FlagSet, args []string, usage string, stdout, stderr io.Writer) (positionals []string, code int, done bool) {
	positionals, err := ParseInterleaved(fs, args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, usage)
			return nil, 0, true
		}
		fmt.Fprintf(stderr, "%s: error: %v\n", fs.Name(), err)
		return nil, ExitUsage, true
	}
	return positionals, 0, false
}

// UsageError reports an argparse-style argument error and returns ExitUsage.
func UsageError(fs *flag.FlagSet, stderr io.Writer, format string, args ...any) int {
	fmt.Fprintf(stderr, "%s: error: %s\n", fs.Name(), fmt.Sprintf(format, args...))
	return ExitUsage
}

// ValidateMode accepts the two upload modes every uploader has.
func ValidateMode(mode string) error {
	if mode != "incremental" && mode != "full" {
		return fmt.Errorf("argument --mode: invalid choice: %q (choose from 'incremental', 'full')", mode)
	}
	return nil
}

// ProjectDir is the directory whose .env an uploader loads:
// PDW_INGEST_PROJECT_DIR when set (the launchd/systemd wrappers pin it to the
// repo checkout), else the current working directory. This mirrors the
// Python uploaders, which were run by `uv` inside that checkout and called
// python-dotenv's load_dotenv() there.
func ProjectDir(getenv func(string) string) string {
	if dir := strings.TrimSpace(getenv("PDW_INGEST_PROJECT_DIR")); dir != "" {
		return ExpandUser(dir)
	}
	if cwd, err := os.Getwd(); err == nil {
		return cwd
	}
	return "."
}

// LoadProjectDotenv applies <ProjectDir>/.env to the process environment
// (existing variables win, as python-dotenv does) and returns a Getenv that
// layers the file's values beneath getenv, so an injected lookup (tests)
// sees them too. A missing file is not an error.
func LoadProjectDotenv(getenv func(string) string) (Getenv, error) {
	path := filepath.Join(ProjectDir(getenv), ".env")
	values, err := LoadDotenv(path)
	if err != nil {
		return Getenv(getenv), fmt.Errorf("load %s: %w", path, err)
	}
	for key, value := range values {
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
	return func(name string) string {
		if v := getenv(name); v != "" {
			return v
		}
		if v, ok := values[name]; ok {
			return v
		}
		return ""
	}, nil
}

// OverrideBeforeUploadCheck replaces the network guard every uploader takes
// before its first upload. It exists for tests: the real guard shells out to
// `route`/`networksetup` and dials the app, neither of which a unit test
// should depend on. nil means the real guard.
var OverrideBeforeUploadCheck func() string

// UploadGuard is the guard an uploader passes to its runner: the network
// policy under <prefix>_UPLOAD (falling back to the voice-memos settings)
// plus the app preflight with <prefix>_UPLOAD_PREFLIGHT_TIMEOUT_SECONDS.
func UploadGuard(getenv Getenv, prefix, baseURL string) func() string {
	if OverrideBeforeUploadCheck != nil {
		return OverrideBeforeUploadCheck
	}
	policy := NetworkPolicyFromEnv(getenv, prefix+"_UPLOAD", "VOICE_MEMOS_UPLOAD")
	return BeforeUploadCheck(policy, baseURL, PreflightTimeout(getenv, prefix))
}

// AcquireRunLock takes the uploader's nonblocking lock. When another run
// holds it the skip line is printed and done is true with exit 0 -- an
// overlapping LaunchAgent tick is not a failure.
func AcquireRunLock(path, skipMessage string, stdout, stderr io.Writer) (lock *RunLock, code int, done bool) {
	lock, acquired, err := TryRunLock(path)
	if err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return nil, ExitFailure, true
	}
	if !acquired {
		fmt.Fprintln(stdout, skipMessage)
		return nil, 0, true
	}
	return lock, 0, false
}
