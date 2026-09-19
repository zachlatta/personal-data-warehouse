// Package applescript is the shared plumbing for mutations that drive a macOS
// app over AppleScript, the Go twin of personal_data_warehouse.apple_automation.
//
// Apple Notes and Apple Contacts have no server API, so their mutation
// executors ask the app itself, on a Mac that is signed in, through osascript.
// Everything here is the part that does not depend on which app: string
// escaping (AppleScript has no parameter binding, so an unescaped quote is
// arbitrary script execution, not a failed write), the subprocess runner, and
// the mapping from an osascript failure onto the mutation worker's status
// vocabulary.
package applescript

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// DefaultScriptTimeout bounds the osascript subprocess. It sits well above
// the in-script `with timeout` so a hung app surfaces as our timeout with our
// message, not as an opaque killed process.
const DefaultScriptTimeout = 180 * time.Second

// InScriptTimeoutSeconds is the `with timeout of N seconds` every script
// wraps its `tell application` block in.
const InScriptTimeoutSeconds = 120

// Runner executes one AppleScript and returns its stdout with the trailing
// newline removed. A non-zero exit is returned as an error carrying
// osascript's stderr; a subprocess timeout is returned as ErrTimeout.
type Runner func(script string) (string, error)

// ErrTimeout is what the default runner returns when osascript itself does
// not finish within DefaultScriptTimeout.
var ErrTimeout = errors.New("osascript did not finish in time")

// Run is the default Runner: `/usr/bin/osascript -` with the script on stdin.
func Run(script string) (string, error) {
	return RunWithTimeout(script, DefaultScriptTimeout)
}

// RunWithTimeout is Run with an explicit subprocess bound.
func RunWithTimeout(script string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/usr/bin/osascript", "-")
	cmd.Stdin = strings.NewReader(script)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "", ErrTimeout
	}
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = strings.TrimSpace(stdout.String())
		}
		if message == "" {
			message = "osascript failed"
		}
		return "", errors.New(message)
	}
	return strings.TrimRight(stdout.String(), "\n"), nil
}

// String renders a Go string as an AppleScript string expression.
//
// Quotes and backslashes are escaped. Newlines cannot appear inside an
// AppleScript string literal at all, so they are spliced in as `linefeed`
// terms, which is why this returns an expression rather than a literal.
func String(value string) string {
	segments := strings.Split(value, "\n")
	parts := make([]string, 0, len(segments))
	for _, segment := range segments {
		escaped := strings.ReplaceAll(segment, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `"`, `\"`)
		parts = append(parts, `"`+escaped+`"`)
	}
	return strings.Join(parts, " & linefeed & ")
}

// Dedent strips the indentation a script template carries from Go source.
func Dedent(script string) string {
	lines := strings.Split(strings.TrimSpace(script), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	return strings.Join(lines, "\n")
}

// Classify maps an osascript failure onto (status, error).
//
// The distinction that matters: a missing record will never appear, so
// retrying it forever is noise; a busy or unlaunched app is transient; a
// refused Automation grant needs a human at that Mac and is not a code
// failure at all.
func Classify(message string, appName string) (status string, errorText string) {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(message, "-1743") || strings.Contains(message, "Not authorized to send Apple events"):
		return "blocked_missing_credentials", fmt.Sprintf(
			"Automation permission for %s is not granted to this worker. "+
				"Grant it in System Settings > Privacy & Security > Automation. "+
				"osascript said: %s", appName, message)
	case strings.Contains(message, "-1712") || strings.Contains(lower, "timed out"):
		return "failed_retryable", message
	case strings.Contains(message, "-1728") || strings.Contains(message, "-2753") || strings.Contains(message, "Invalid key form"):
		return "failed_terminal", message
	case strings.Contains(message, "-600") || strings.Contains(lower, "not running"):
		return "failed_retryable", message
	}
	return "failed_terminal", message
}
