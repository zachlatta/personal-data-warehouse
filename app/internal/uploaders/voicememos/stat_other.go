//go:build !darwin

package voicememos

import "syscall"

// Linux has no birth time in Stat_t; Python fell back to st_ctime there.
func birthtime(stat *syscall.Stat_t) (int64, int64) {
	return stat.Ctim.Sec, stat.Ctim.Nsec
}
