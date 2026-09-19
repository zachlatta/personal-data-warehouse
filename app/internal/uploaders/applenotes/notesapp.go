package applenotes

import (
	"os/exec"
	"runtime"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// The uploader only sees this Mac's NoteStore.sqlite, and macOS only pulls
// Notes iCloud changes while Notes.app is running. With the app quit the
// store silently freezes: healthy runs, selected=0, while other devices'
// edits never arrive. Every run therefore makes sure Notes.app is running.

const notesStoreContainer = "group.com.apple.notes"

// AppKick reports what the pre-snapshot check did.
type AppKick struct {
	Attempted bool
	Launched  bool
	Reason    string
}

// ProcessRunner runs a command and reports (exit code, stderr, error).
type ProcessRunner func(args ...string) (int, string, error)

// DefaultProcessRunner executes the command.
func DefaultProcessRunner(args ...string) (int, string, error) {
	cmd := exec.Command(args[0], args[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), string(out), nil
		}
		return -1, string(out), err
	}
	return 0, string(out), nil
}

// EnsureNotesAppRunning launches Notes.app hidden when the store is the
// system one and nothing is running; failures never block the upload.
func EnsureNotesAppRunning(storePath string, logger common.Logger, getenv common.Getenv, platform string, run ProcessRunner) AppKick {
	if platform == "" {
		platform = runtime.GOOS
	}
	if run == nil {
		run = DefaultProcessRunner
	}
	if !getenv.Enabled("APPLE_NOTES_OPEN_NOTES_APP") {
		return AppKick{Reason: "disabled by APPLE_NOTES_OPEN_NOTES_APP"}
	}
	if platform != "darwin" {
		return AppKick{Reason: "not macOS (platform=" + platform + ")"}
	}
	if !pathHasComponent(common.ExpandUser(storePath), notesStoreContainer) {
		return AppKick{Reason: "store path is not the system Notes store"}
	}
	code, _, err := run("pgrep", "-x", "Notes")
	if err != nil {
		logger.Warningf("Could not launch Notes.app to resume iCloud sync: %v", err)
		return AppKick{Attempted: true, Reason: "launch error: " + err.Error()}
	}
	if code == 0 {
		return AppKick{Attempted: true, Reason: "Notes.app already running"}
	}
	code, out, err := run("open", "-g", "-j", "-a", "Notes")
	if err != nil {
		logger.Warningf("Could not launch Notes.app to resume iCloud sync: %v", err)
		return AppKick{Attempted: true, Reason: "launch error: " + err.Error()}
	}
	if code != 0 {
		detail := strings.TrimSpace(out)
		if detail == "" {
			detail = "exit " + itoa(code)
		}
		logger.Warningf("Could not launch Notes.app to resume iCloud sync: %s", detail)
		return AppKick{Attempted: true, Reason: "launch failed: " + detail}
	}
	logger.Infof("Launched Notes.app (hidden) so the local store receives iCloud changes")
	return AppKick{Attempted: true, Launched: true, Reason: "launched"}
}

func pathHasComponent(path, component string) bool {
	for _, part := range strings.Split(path, "/") {
		if part == component {
			return true
		}
	}
	return false
}
