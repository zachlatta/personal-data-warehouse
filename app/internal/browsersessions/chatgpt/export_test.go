package chatgpt

import "testing"

// SetHostOS pins hostOS for one test, so the macOS-only install path is
// testable on the Linux CI runner.
func SetHostOS(t *testing.T, goos string) {
	t.Helper()
	prev := hostOS
	hostOS = goos
	t.Cleanup(func() { hostOS = prev })
}
