package common

import (
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// RunLock is an exclusive flock on a lock file, taken by every uploader so a
// LaunchAgent that fires while the previous run is still uploading skips
// instead of racing it on the state file.
type RunLock struct {
	file *os.File
}

// TryRunLock takes the lock without waiting. acquired is false when another
// process holds it; the caller prints a skip message and exits 0.
func TryRunLock(path string) (lock *RunLock, acquired bool, err error) {
	return acquireRunLock(path, 0)
}

// WaitRunLock takes the lock, waiting up to wait for the holder to release it.
func WaitRunLock(path string, wait time.Duration) (lock *RunLock, acquired bool, err error) {
	return acquireRunLock(path, wait)
}

func acquireRunLock(path string, wait time.Duration) (*RunLock, bool, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, false, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, false, err
	}
	deadline := time.Now().Add(wait)
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &RunLock{file: file}, true, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			file.Close()
			return nil, false, err
		}
		if wait <= 0 || time.Now().After(deadline) {
			file.Close()
			return nil, false, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Release drops the lock.
func (l *RunLock) Release() {
	if l == nil || l.file == nil {
		return
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	_ = l.file.Close()
	l.file = nil
}
