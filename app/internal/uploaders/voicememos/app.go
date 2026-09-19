package voicememos

import (
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// The uploader only sees this Mac's Voice Memos container, and macOS only
// pulls Voice Memos CloudKit changes while voicememod (or the app that starts
// it) is alive. With both quit the store silently freezes: healthy runs,
// selected=0, while memos recorded on other devices never arrive. Every run
// therefore makes sure one of them is running before it scans.

// VoiceMemosStoreContainer is the only store path the kick applies to; test
// and CI runs point the uploader at temporary directories and must never
// launch a GUI app.
const VoiceMemosStoreContainer = "group.com.apple.VoiceMemos.shared"

// VoiceMemosBundleID is what `open -b` launches: the bundle is VoiceMemos.app
// while the app presents as "Voice Memos", and `open -a "Voice Memos"` fails.
const VoiceMemosBundleID = "com.apple.VoiceMemos"

// VoiceMemosProcessNames are the process names as `pgrep -x` sees them: the
// daemon that actually syncs, and the app binary that keeps it alive.
var VoiceMemosProcessNames = []string{"voicememod", "VoiceMemos"}

// AppKick reports what the pre-scan check did. Attempted is false when the
// check does not apply; Launched is true only when this run started the app.
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
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), stderr.String(), nil
		}
		return -1, stderr.String(), err
	}
	return 0, stderr.String(), nil
}

// EnsureVoiceMemosAppRunning launches Voice Memos hidden when the store is the
// system one and neither the daemon nor the app is running. Failures never
// block the upload: a run against a stale store still uploads what synced.
func EnsureVoiceMemosAppRunning(recordingsPath string, logger common.Logger, getenv common.Getenv, platform string, run ProcessRunner) AppKick {
	if platform == "" {
		platform = runtime.GOOS
	}
	if run == nil {
		run = DefaultProcessRunner
	}
	if !getenv.Enabled("VOICE_MEMOS_OPEN_APP") {
		return AppKick{Reason: "disabled by VOICE_MEMOS_OPEN_APP"}
	}
	if platform != "darwin" {
		return AppKick{Reason: "not macOS (platform=" + platform + ")"}
	}
	if !pathHasComponent(common.ExpandUser(recordingsPath), VoiceMemosStoreContainer) {
		return AppKick{Reason: "recordings path is not the system Voice Memos store"}
	}
	for _, name := range VoiceMemosProcessNames {
		code, _, err := run("pgrep", "-x", name)
		if err != nil {
			logger.Warningf("Could not launch Voice Memos to resume iCloud sync: %v", err)
			return AppKick{Attempted: true, Reason: "launch error: " + err.Error()}
		}
		if code == 0 {
			return AppKick{Attempted: true, Reason: name + " already running"}
		}
	}
	code, stderr, err := run("open", "-g", "-j", "-b", VoiceMemosBundleID)
	if err != nil {
		logger.Warningf("Could not launch Voice Memos to resume iCloud sync: %v", err)
		return AppKick{Attempted: true, Reason: "launch error: " + err.Error()}
	}
	if code != 0 {
		detail := strings.TrimSpace(stderr)
		if detail == "" {
			detail = "exit " + strconv.Itoa(code)
		}
		logger.Warningf("Could not launch Voice Memos to resume iCloud sync: %s", detail)
		return AppKick{Attempted: true, Reason: "launch failed: " + detail}
	}
	logger.Infof("Launched Voice Memos (hidden) so the local store receives iCloud changes")
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
