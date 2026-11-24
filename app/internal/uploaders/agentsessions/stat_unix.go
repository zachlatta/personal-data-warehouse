//go:build unix

package agentsessions

import (
	"io/fs"
	"syscall"
)

func fileInode(info fs.FileInfo) uint64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok && st != nil {
		return st.Ino
	}
	return 0
}
