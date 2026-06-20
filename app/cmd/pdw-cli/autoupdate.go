package main

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
)

// autoUpdateCommand is the hidden subcommand the foreground process spawns in
// the background to perform a throttled self-update. It's deliberately ugly so
// it never collides with a real command and never shows up in help.
const autoUpdateCommand = "__auto-update"

// autoUpdateDebounce bounds how often pdw will kick off a background
// self-update. At most one update attempt fires per window regardless of how
// many times the CLI is invoked, so a burst of calls (e.g. an agent running
// many SQL queries) costs at most one GitHub check.
const autoUpdateDebounce = 5 * time.Minute

// autoUpdateStateFile is the stamp file (next to config.json) recording when
// the last background update attempt fired.
const autoUpdateStateFile = "auto-update.json"

// Injection seams so the debounce decision can be tested without a real clock
// or spawning processes. Production wires them to time.Now and the detached
// background spawn.
var (
	nowFunc     = time.Now
	spawnUpdate = spawnBackgroundUpdate
)

// autoUpdateState is the on-disk stamp shape.
type autoUpdateState struct {
	LastAttempt time.Time `json:"last_attempt"`
}

// maybeAutoUpdate kicks off a throttled, best-effort background self-update.
// It never blocks the foreground command and swallows every error: a warehouse
// query must never fail because an update check did. The flow is
//
//  1. bail unless auto-update applies to this command/build/env,
//  2. bail if the debounce window hasn't elapsed since the last attempt,
//  3. record a fresh stamp up front (so concurrent and subsequent invocations
//     inside the window don't also spawn an updater), then
//  4. spawn a detached worker that does the actual download + replace.
func maybeAutoUpdate(cmd string, getenv func(string) string) {
	if !autoUpdateEnabled(cmd, version, getenv) {
		return
	}
	statePath, err := autoUpdateStatePath(getenv)
	if err != nil {
		// No place to record an attempt means no debounce; skip rather than
		// spawn an updater that would re-fire on every single invocation.
		return
	}
	now := nowFunc()
	last, ok := readAutoUpdateStamp(statePath)
	if !dueForAutoUpdate(now, last, ok, autoUpdateDebounce) {
		return
	}
	if err := writeAutoUpdateStamp(statePath, now); err != nil {
		// Same reasoning: without a recorded stamp there is no debounce.
		return
	}
	_ = spawnUpdate()
}

// autoUpdateEnabled reports whether a background self-update should be
// considered for this invocation.
func autoUpdateEnabled(cmd, ver string, getenv func(string) string) bool {
	if truthyEnv(getenv("PDW_NO_AUTO_UPDATE")) {
		return false
	}
	// "dev" / empty are local or test builds with no real version to compare
	// against; updating would clobber a hand-built binary. Skipping here also
	// keeps the Go test suite from ever spawning a real updater.
	if ver == "" || ver == "dev" {
		return false
	}
	// These commands either perform the update themselves or are diagnostic
	// meta-commands that should stay hermetic and fast.
	switch cmd {
	case "update", autoUpdateCommand, "version", "help":
		return false
	}
	return true
}

// dueForAutoUpdate reports whether enough time has elapsed since the last
// recorded attempt. A missing stamp is always due. A stamp dated in the future
// (clock skew) is treated as not due, so a bad clock can't cause a spawn storm.
func dueForAutoUpdate(now, last time.Time, haveStamp bool, debounce time.Duration) bool {
	if !haveStamp {
		return true
	}
	return now.Sub(last) >= debounce
}

// autoUpdateStatePath resolves the stamp file path, which lives alongside the
// CLI's config.json so it follows the same XDG/HOME resolution.
func autoUpdateStatePath(getenv func(string) string) (string, error) {
	cfgPath, err := cliconfig.Path(getenv)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(cfgPath), autoUpdateStateFile), nil
}

// readAutoUpdateStamp returns the last attempt time. A missing or unparseable
// stamp yields ok=false, which callers treat as "due now".
func readAutoUpdateStamp(path string) (time.Time, bool) {
	body, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, false
	}
	var st autoUpdateState
	if err := json.Unmarshal(body, &st); err != nil || st.LastAttempt.IsZero() {
		return time.Time{}, false
	}
	return st.LastAttempt, true
}

// writeAutoUpdateStamp records the attempt time, creating the config directory
// if needed. It writes atomically (temp + rename) so a concurrent reader never
// sees a half-written file that would fail to parse and defeat the debounce.
func writeAutoUpdateStamp(path string, t time.Time) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	body, err := json.Marshal(autoUpdateState{LastAttempt: t})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".auto-update.json.*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return err
	}
	return nil
}

// spawnBackgroundUpdate launches a detached copy of this binary running the
// hidden auto-update worker. It returns as soon as the child starts; the child
// outlives this process and replaces the binary on disk for the next run.
func spawnBackgroundUpdate() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, autoUpdateCommand)
	cmd.Env = os.Environ()
	// Detach: no stdio (nil => /dev/null) and a fresh process group so a
	// Ctrl-C / SIGHUP aimed at the foreground command can't interrupt the
	// update mid-flight.
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	// Release so we don't have to Wait; the child is reparented to init.
	return cmd.Process.Release()
}

// runAutoUpdateWorker is the hidden background command. It reuses the same
// machinery as `pdw update` but stays silent — output is irrelevant in a
// detached process, and any failure is simply retried after the next debounce
// window. Args are forwarded so tests can point it at a fake GitHub.
func runAutoUpdateWorker(args []string, getenv func(string) string) int {
	return runUpdate(args, io.Discard, io.Discard, getenv)
}

// truthyEnv reports whether an env value means "on" (1/true/yes/on,
// case-insensitive). Empty and 0/false/no/off are "off".
func truthyEnv(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
