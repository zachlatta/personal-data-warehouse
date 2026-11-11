//go:build linux

package scripts

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Applied to all Go runtime threads (TSYNC), not merely the current goroutine.
// Denied syscalls return EPERM; setup failure refuses script execution.
func restrictWorker() error {
	if err := unix.Setrlimit(unix.RLIMIT_CPU, &unix.Rlimit{Cur: 3, Max: 3}); err != nil {
		return err
	}
	if err := unix.Setrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	arch := uint32(unix.AUDIT_ARCH_X86_64)
	if runtime.GOARCH == "arm64" {
		arch = unix.AUDIT_ARCH_AARCH64
	} else if runtime.GOARCH != "amd64" {
		return fmt.Errorf("unsupported sandbox architecture")
	}
	filter := []unix.SockFilter{
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 4},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: arch, Jt: 1},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_KILL_PROCESS},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
		// Reject alternate x32 syscall numbering on amd64 too.
		{Code: unix.BPF_JMP | unix.BPF_JGE | unix.BPF_K, K: 0x40000000, Jf: 1},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)},
	}
	for _, nr := range []uint32{unix.SYS_OPENAT, unix.SYS_OPENAT2, unix.SYS_SOCKET, unix.SYS_SOCKETPAIR, unix.SYS_CONNECT, unix.SYS_BIND, unix.SYS_LISTEN, unix.SYS_ACCEPT, unix.SYS_ACCEPT4, unix.SYS_EXECVE, unix.SYS_EXECVEAT, unix.SYS_PTRACE, unix.SYS_PROCESS_VM_READV, unix.SYS_PROCESS_VM_WRITEV, unix.SYS_MOUNT, unix.SYS_UMOUNT2, unix.SYS_UNSHARE, unix.SYS_SETNS, unix.SYS_BPF, unix.SYS_IO_URING_SETUP, unix.SYS_MEMFD_CREATE, unix.SYS_PIDFD_GETFD, unix.SYS_OPEN_BY_HANDLE_AT} {
		filter = append(filter, unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: nr, Jf: 1}, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)})
	}
	// amd64 retains legacy open/creat syscalls which arm64 does not have.
	if runtime.GOARCH == "amd64" {
		for _, nr := range []uint32{2, 85} {
			filter = append(filter, unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: nr, Jf: 1}, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)})
		}
	}
	filter = append(filter, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW})
	program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	result, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER, unix.SECCOMP_FILTER_FLAG_TSYNC, uintptr(unsafe.Pointer(&program)))
	if errno != 0 {
		return errno
	}
	if result != 0 {
		return fmt.Errorf("seccomp could not synchronize every thread")
	}
	return nil
}

// Wasm has a hard 32 MiB linear memory limit. The worker as a whole is also
// killed on RSS >128 MiB or cumulative CPU >2s (Linux proc ticks are 100Hz).
// RSS sampling is a watchdog, not a delegated cgroup or hard address-space cap.
func watchWorker(p *os.Process, done <-chan struct{}, violation func(string)) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case <-tick.C:
			stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", p.Pid))
			if err != nil {
				return
			}
			end := strings.LastIndexByte(string(stat), ')')
			if end < 0 {
				_ = p.Kill()
				return
			}
			fields := strings.Fields(string(stat[end+1:]))
			if len(fields) < 22 {
				continue
			}
			user, _ := strconv.ParseUint(fields[11], 10, 64)
			system, _ := strconv.ParseUint(fields[12], 10, 64)
			rss, _ := strconv.ParseUint(fields[21], 10, 64)
			if user+system > 200 || rss*uint64(os.Getpagesize()) > 128*1024*1024 {
				if user+system > 200 {
					violation("cpu_limit")
				} else {
					violation("memory_limit")
				}
				_ = p.Kill()
				return
			}
		}
	}
}
