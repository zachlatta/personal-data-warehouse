//go:build linux

package scripts

import (
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"testing"
)

// The probe runs in its own test subprocess: seccomp must never be installed
// in the parent's test runner. It exercises actual syscalls, not JS stubs.
func init() {
	if len(os.Args) == 2 && os.Args[1] == "--script-sandbox-probe" {
		if restrictWorker() != nil {
			os.Exit(2)
		}
		if fd, e := unix.Open("/etc/passwd", unix.O_RDONLY, 0); e != unix.EPERM {
			if fd >= 0 {
				unix.Close(fd)
			}
			os.Exit(3)
		}
		if fd, e := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0); e != unix.EPERM {
			if fd >= 0 {
				unix.Close(fd)
			}
			os.Exit(4)
		}
		os.Exit(0)
	}
}
func TestWorkerDeniesActualFilesystemAndNetworkSyscalls(t *testing.T) {
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(exe, "--script-sandbox-probe")
	cmd.Env = []string{"GOMAXPROCS=1"}
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("sandbox probe: %v %s", e, out)
	}
}
