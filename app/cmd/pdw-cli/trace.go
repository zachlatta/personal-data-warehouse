package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mcpproxy"
)

// A trace id names the agent session a pdw invocation belongs to. It travels
// as X-Trace-ID and the server forwards it to proxied MCP connections, so the
// skills service can deduplicate repeated reads of one skill within a session
// and its audit feed can group a session's calls. Before this, every
// `pdw call skills__…` opened a new upstream session and looked like a new
// agent. (Ported from the retired sk CLI.)
//
// Resolution order: PDW_TRACE_ID, then SKILLS_TRACE_ID, when a harness sets
// one; nothing when PDW_NO_TRACE=1; otherwise the nearest ancestor process that
// is not a shell or shell-like wrapper. Each Claude Code tool call is a fresh
// `zsh -c` whose parent is the long-lived agent process, so hashing that
// ancestor's pid and start time gives one id per session with no cooperation
// from the host.

// processInfoFunc reports a process's parent, start time, and command name.
type processInfoFunc func(pid int) (ppid int, started, comm string, err error)

func resolveTraceID(getenv func(string) string) string {
	for _, env := range []string{"PDW_TRACE_ID", "SKILLS_TRACE_ID"} {
		if v := strings.TrimSpace(getenv(env)); v != "" {
			return mcpproxy.SanitizeTraceID(v)
		}
	}
	if getenv("PDW_NO_TRACE") == "1" {
		return ""
	}
	host, _ := os.Hostname()
	return sessionTraceID(host, os.Getppid(), processInfo)
}

// sessionTraceID walks up from pid to the first ancestor that is not a shell
// or wrapper and derives a stable id from it. It returns "" when the tree
// cannot be read, since a missing trace only costs deduplication.
func sessionTraceID(host string, pid int, info processInfoFunc) string {
	for hops := 0; hops < 12 && pid > 1; hops++ {
		ppid, started, comm, err := info(pid)
		if err != nil {
			return ""
		}
		if !isShellLike(comm) {
			sum := sha256.Sum256([]byte(host + "|" + strconv.Itoa(pid) + "|" + started + "|" + comm))
			return mcpproxy.SanitizeTraceID(strings.TrimSuffix(host, ".local") + "-" + shortComm(comm) + "-" + strconv.Itoa(pid) + "-" + hex.EncodeToString(sum[:4]))
		}
		if ppid <= 1 || ppid == pid {
			return ""
		}
		pid = ppid
	}
	return ""
}

// isShellLike reports whether a process merely hosts the one that matters:
// shells (including login shells, whose name starts with "-") and the usual
// command wrappers.
func isShellLike(comm string) bool {
	name := strings.TrimPrefix(filepath.Base(strings.TrimSpace(comm)), "-")
	switch name {
	case "sh", "bash", "zsh", "fish", "dash", "ksh", "tcsh", "csh",
		"env", "sudo", "doas", "nohup", "timeout", "xargs", "script", "caffeinate", "nice", "time", "login":
		return true
	}
	return false
}

func shortComm(comm string) string {
	name := strings.TrimPrefix(filepath.Base(strings.TrimSpace(comm)), "-")
	if len(name) > 16 {
		name = name[:16]
	}
	return name
}

// processInfo reads one process's parent, start time, and command via ps,
// present on every macOS and Linux host pdw runs on. It is bounded so a wedged
// ps never slows a call noticeably.
func processInfo(pid int) (int, string, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-o", "ppid=,lstart=,comm=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, "", "", err
	}
	return parsePSLine(string(out))
}

// parsePSLine splits "  123 Wed Sep 24 13:41:02 2026 /usr/bin/node" into its
// three fields; lstart is five space-separated tokens on both platforms.
func parsePSLine(line string) (int, string, string, error) {
	fields := strings.Fields(line)
	if len(fields) < 7 {
		return 0, "", "", os.ErrNotExist
	}
	ppid, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, "", "", err
	}
	return ppid, strings.Join(fields[1:6], " "), strings.Join(fields[6:], " "), nil
}
